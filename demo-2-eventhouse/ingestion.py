"""Producer adapter for queued CSV ingestion and KQL reconciliation."""

import csv
import hashlib
import re
import shutil
import tempfile
import uuid
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path

from eventhouse_core import (
    CSV_HEADER,
    ConfigurationError,
    batch_identifier,
    batch_verification_query,
    identifier,
)


class InvalidBatch(ValueError):
    pass


@dataclass(frozen=True)
class Submission:
    status: str
    timestamp_utc: str
    operation_id: str | None = None
    diagnostic: str | None = None


@dataclass(frozen=True)
class Verification:
    status: str
    timestamp_utc: str
    operation_id: str | None = None
    row_count: int | None = None
    total_minor_units: int | None = None
    distinct_sales_line_ids: int | None = None
    diagnostic: str | None = None


@dataclass(frozen=True)
class IngestionOutcome:
    status: str
    timestamp_utc: str
    source_id: str | None = None
    service_operation_id: str | None = None
    error_code: str | None = None
    diagnostic: str | None = None


def _now():
    return datetime.now(timezone.utc).isoformat()


def _validate_expectations(batch_id, sha256, row_count, total):
    batch_identifier(batch_id)
    if not isinstance(sha256, str) or not re.fullmatch(r"[a-f0-9]{64}", sha256):
        raise InvalidBatch("Expected a lowercase SHA-256 digest")
    if type(row_count) is not int or row_count < 1:
        raise InvalidBatch("Expected a positive row count")
    if type(total) is not int or total < 0:
        raise InvalidBatch("Expected a nonnegative integer minor-unit total")


def _integer(value, field, bits):
    if not re.fullmatch(r"-?[0-9]+", value):
        raise InvalidBatch(f"{field} must be an integer")
    number = int(value)
    if not -(2 ** (bits - 1)) <= number < 2 ** (bits - 1):
        raise InvalidBatch(f"{field} is outside its KQL integer range")
    return number


def _inspect_csv(path, batch_id, sha256, row_count, total):
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    if digest.hexdigest() != sha256:
        raise InvalidBatch("CSV checksum does not match the manifest")

    count = 0
    amount_sum = 0
    seen = set()
    try:
        with path.open("r", encoding="utf-8", newline="") as source:
            reader = csv.reader(source, strict=True)
            if tuple(next(reader, ())) != CSV_HEADER:
                raise InvalidBatch("CSV header does not match contract v1")
            for row in reader:
                if len(row) != len(CSV_HEADER):
                    raise InvalidBatch("CSV row has the wrong number of columns")
                sales_id = _integer(row[0], "SalesLineID", 64)
                if sales_id in seen:
                    raise InvalidBatch("CSV contains duplicate SalesLineID values")
                seen.add(sales_id)
                if row[1] != batch_id:
                    raise InvalidBatch("CSV BatchID does not match the manifest")
                try:
                    stamp = datetime.fromisoformat(row[2].replace("Z", "+00:00"))
                    sales_date = date.fromisoformat(row[5])
                except ValueError as exc:
                    raise InvalidBatch("CSV contains an invalid timestamp or date") from exc
                if stamp.tzinfo is None or stamp.utcoffset() != timedelta(0):
                    raise InvalidBatch("SaleTimestamp must be UTC")
                if sales_date.isoformat() != row[5]:
                    raise InvalidBatch("Date must use the ISO date format")
                _integer(row[3], "CustomerID", 32)
                _integer(row[4], "ProductID", 32)
                quantity = _integer(row[6], "Quantity", 32)
                price = _integer(row[7], "UnitPriceMinorUnits", 64)
                amount = _integer(row[8], "SalesAmountMinorUnits", 64)
                if (
                    quantity <= 0
                    or price < 0
                    or amount < 0
                    or price > 10**18 - 1
                    or amount > 10**18 - 1
                    or amount != quantity * price
                ):
                    raise InvalidBatch("CSV contains invalid or inexact monetary values")
                count += 1
                amount_sum += amount
    except (UnicodeError, csv.Error) as exc:
        raise InvalidBatch("CSV is not valid UTF-8 with the required CSV syntax") from exc
    if count != row_count or amount_sum != total:
        raise InvalidBatch("CSV count or exact total does not match the manifest")


def _exact_integer(value):
    try:
        decimal = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError("KQL returned a nonnumeric result") from exc
    if not decimal.is_finite() or decimal != decimal.to_integral_value():
        raise ValueError("KQL returned a noninteger result")
    return int(decimal)


class EventhouseAdapter:
    """Single-writer adapter; the producer owns the durable submission ledger."""

    def __init__(self, *, database, table, mapping, ingest_client, query_client):
        self.database = identifier(database, "KQL database")
        self.table = identifier(table, "sales table")
        self.mapping = identifier(mapping, "CSV mapping")
        self.ingest_client = ingest_client
        self.query_client = query_client

    def submit(self, staged_path, batch_id, sha256, row_count, sales_amount_minor_units):
        try:
            _validate_expectations(batch_id, sha256, row_count, sales_amount_minor_units)
            if not isinstance(staged_path, str) or staged_path.startswith(
                ("abfss://", "https://", "file:")
            ):
                raise InvalidBatch("The staged CSV must be a driver-local file path")
        except (ConfigurationError, InvalidBatch) as exc:
            return Submission("permanent_failure", _now(), diagnostic=str(exc))

        try:
            from azure.core.exceptions import AzureError
            from azure.kusto.data import DataFormat
            from azure.kusto.data.exceptions import KustoError
            from azure.kusto.ingest import (
                FileDescriptor,
                IngestionProperties,
                IngestionStatus,
                ReportLevel,
                ValidationImplications,
                ValidationOptions,
                ValidationPolicy,
            )
        except ImportError:
            return Submission(
                "retryable_failure", _now(), diagnostic="The Kusto ingestion SDK is not installed"
            )

        source_id = str(uuid.uuid4())
        # A stable logical-batch tag is independent of the attempt's source ID.
        tag = "chanel-v1-" + hashlib.sha256(batch_id.encode("utf-8")).hexdigest()
        try:
            with tempfile.TemporaryDirectory(prefix="chanel-eventhouse-") as directory:
                snapshot = Path(directory) / "batch.csv"
                try:
                    shutil.copyfile(staged_path, snapshot)
                    _inspect_csv(snapshot, batch_id, sha256, row_count, sales_amount_minor_units)
                except (ConfigurationError, InvalidBatch) as exc:
                    return Submission("permanent_failure", _now(), diagnostic=str(exc))
                except OSError as exc:
                    return Submission(
                        "retryable_failure",
                        _now(),
                        diagnostic=f"Cannot read the staged CSV ({type(exc).__name__})",
                    )

                properties = IngestionProperties(
                    database=self.database,
                    table=self.table,
                    data_format=DataFormat.CSV,
                    ingestion_mapping_reference=self.mapping,
                    ignore_first_record=True,
                    ingest_if_not_exists=[tag],
                    ingest_by_tags=[tag],
                    report_level=ReportLevel.FailuresAndSuccesses,
                    validation_policy=ValidationPolicy(
                        ValidationOptions.ValidateCsvInputConstantColumns,
                        ValidationImplications.Fail,
                    ),
                )
                descriptor = FileDescriptor(str(snapshot), snapshot.stat().st_size, source_id)
                result = self.ingest_client.ingest_from_file(descriptor, properties)
                if result.status != IngestionStatus.QUEUED:
                    return Submission(
                        "unknown",
                        _now(),
                        source_id,
                        "Unexpected ingestion result; reconcile before retrying",
                    )
                return Submission("submitted", _now(), str(result.source_id))
        except (AzureError, KustoError, OSError, TimeoutError) as exc:
            return Submission(
                "unknown",
                _now(),
                source_id,
                f"Submission outcome is uncertain ({type(exc).__name__}); reconcile before retrying",
            )

    def ingestion_status(self, source_id):
        """Peek terminal status without deleting shared queue messages."""
        try:
            source_id = str(uuid.UUID(source_id))
        except (ValueError, TypeError, AttributeError):
            return IngestionOutcome("unknown", _now(), diagnostic="Invalid ingestion source ID")
        try:
            from azure.core.exceptions import AzureError
            from azure.kusto.data.exceptions import KustoError
            from azure.kusto.ingest.status import KustoIngestStatusQueues
        except ImportError:
            return IngestionOutcome(
                "unknown", _now(), source_id, diagnostic="The Kusto ingestion SDK is not installed"
            )

        try:
            queues = KustoIngestStatusQueues(self.ingest_client)
            matches = []
            for status, queue in (("failed", queues.failure), ("succeeded", queues.success)):
                for message in queue.peek(32):
                    if str(message.IngestionSourceId).lower() == source_id:
                        matches.append((status, message))
        except (AzureError, KustoError, OSError, TimeoutError, ValueError, ZeroDivisionError) as exc:
            return IngestionOutcome(
                "unknown", _now(), source_id,
                diagnostic=f"Ingestion status lookup failed ({type(exc).__name__})",
            )
        if not matches:
            return IngestionOutcome(
                "pending", _now(), source_id,
                diagnostic="No terminal status is visible; reconcile before retrying",
            )
        statuses = {status for status, _ in matches}
        if len(statuses) != 1:
            return IngestionOutcome(
                "unknown", _now(), source_id, diagnostic="Conflicting ingestion statuses",
            )
        status, message = matches[0]
        operation = getattr(message, "OperationId", None)
        operation = str(operation) if operation is not None else None
        error_code = getattr(message, "ErrorCode", None)
        if not isinstance(error_code, str) or not re.fullmatch(r"[A-Za-z0-9_.-]{1,100}", error_code):
            error_code = None
        return IngestionOutcome(status, _now(), source_id, operation, error_code)

    def verify(self, batch_id, sha256, row_count, sales_amount_minor_units, operation_id=None):
        try:
            _validate_expectations(batch_id, sha256, row_count, sales_amount_minor_units)
        except (ConfigurationError, InvalidBatch) as exc:
            return Verification("failed", _now(), operation_id, diagnostic=str(exc))
        try:
            from azure.core.exceptions import AzureError
            from azure.kusto.data.exceptions import KustoError
        except ImportError:
            return Verification(
                "unknown", _now(), operation_id, diagnostic="The Kusto data SDK is not installed"
            )

        try:
            response = self.query_client.execute_query(
                self.database, batch_verification_query(self.table, batch_id)
            )
            result = list(response.primary_results[0])
            if len(result) != 1:
                raise ValueError("Unexpected KQL result shape")
            observed = result[0]
            count = _exact_integer(observed["RowCount"])
            distinct = _exact_integer(observed["DistinctSalesLineIDs"])
            amount = _exact_integer(observed["AmountMinorUnits"])
            invalid = _exact_integer(observed["InvalidRows"])
        except (AzureError, KustoError, OSError, TimeoutError) as exc:
            return Verification(
                "unknown",
                _now(),
                operation_id,
                diagnostic=f"KQL reconciliation failed ({type(exc).__name__})",
            )
        except (IndexError, KeyError, TypeError, ValueError) as exc:
            return Verification(
                "unknown",
                _now(),
                operation_id,
                diagnostic=f"KQL reconciliation returned invalid evidence ({type(exc).__name__})",
            )

        if invalid or distinct != count or count > row_count:
            status = "failed"
            diagnostic = "Invalid or duplicate facts were found in KQL"
        elif count < row_count:
            status = "pending"
            diagnostic = "The complete batch is not yet visible in KQL"
        elif amount != sales_amount_minor_units:
            status = "failed"
            diagnostic = "KQL exact total does not match the manifest"
        else:
            status = "verified"
            diagnostic = None
        return Verification(status, _now(), operation_id, count, amount, distinct, diagnostic)
