"""Closed-file delivery to a OneLake landing folder."""

import csv
import hashlib
import io
import re
import uuid
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlsplit

from fabric_storage import OneLakeStore, StorageConflict, StorageError
from generator import HEADER
from runner import Submission


@dataclass(frozen=True)
class Verification:
    status: str
    row_count: int | None = None
    total_minor_units: int | None = None
    distinct_sales_line_ids: int | None = None
    diagnostic: str | None = None


def _landing(uri):
    if not isinstance(uri, str):
        raise ValueError("A OneLake landing URI is required")
    try:
        parsed = urlsplit(uri)
        if (parsed.scheme != "abfss" or parsed.hostname != "onelake.dfs.fabric.microsoft.com"
                or parsed.password is not None or parsed.port is not None or parsed.query or parsed.fragment):
            raise ValueError
        workspace = str(uuid.UUID(parsed.username))
        parts = parsed.path.strip("/").split("/")
        lakehouse = str(uuid.UUID(parts[0]))
    except (ValueError, TypeError, AttributeError, IndexError):
        raise ValueError("Invalid OneLake landing URI") from None
    path = "/".join(parts[1:])
    if (not path.startswith("Files/") or not re.fullmatch(r"[A-Za-z0-9._/-]+", path)
            or any(part in ("", ".", "..") for part in path.split("/"))):
        raise ValueError("OneLake landing must be a safe Files descendant")
    return workspace, lakehouse, path


def _inspect(payload, batch_id, sha256, row_count, total):
    if hashlib.sha256(payload).hexdigest() != sha256:
        raise ValueError("Destination checksum differs from the manifest")
    try:
        text = payload.decode("utf-8")
        reader = csv.DictReader(io.StringIO(text, newline=""), strict=True)
        if tuple(reader.fieldnames or ()) != HEADER:
            raise ValueError("CSV header differs from the contract")
        count, amount, ids = 0, 0, set()
        for row in reader:
            if None in row or any(value is None for value in row.values()) or row["BatchID"] != batch_id:
                raise ValueError("CSV row or batch identity differs from the contract")
            sales_id = int(row["SalesLineID"])
            quantity, price, line_amount = (int(row[name]) for name in
                                            ("Quantity", "UnitPriceMinorUnits", "SalesAmountMinorUnits"))
            if (sales_id in ids or quantity <= 0 or price < 0 or line_amount != quantity * price):
                raise ValueError("CSV contains duplicate keys or invalid amounts")
            ids.add(sales_id)
            count += 1
            amount += line_amount
    except (UnicodeError, csv.Error, KeyError, TypeError, ValueError):
        raise ValueError("CSV failed identity, key or amount validation") from None
    if count != row_count or amount != total:
        raise ValueError("CSV count or total differs from the manifest")
    return count, amount, len(ids)


class OneLakeFileAdapter:
    def __init__(self, landing_uri, token_provider, *, store_factory=OneLakeStore):
        workspace, lakehouse, self.directory = _landing(landing_uri)
        self.store = store_factory(workspace, lakehouse, token_provider)

    def _path(self, batch_id):
        if not isinstance(batch_id, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,83}", batch_id):
            raise ValueError("Invalid batch ID")
        return f"{self.directory}/sales_{batch_id}.csv"

    def submit(self, staged_path, batch_id, sha256, row_count, sales_amount_minor_units):
        try:
            path = self._path(batch_id)
            if not isinstance(staged_path, str) or staged_path.startswith(("abfss://", "https://", "file:")):
                raise ValueError("Staged CSV must be a driver-local file path")
            payload = Path(staged_path).read_bytes()
        except OSError as exc:
            return Submission("retryable_failure", diagnostic=f"Cannot read staged CSV ({type(exc).__name__})")
        except ValueError as exc:
            return Submission("permanent_failure", diagnostic=str(exc))
        try:
            _inspect(payload, batch_id, sha256, row_count, sales_amount_minor_units)
        except ValueError as exc:
            return Submission("permanent_failure", diagnostic=str(exc))
        try:
            self.store.create(path, payload)
        except StorageConflict:
            try:
                existing, _ = self.store.read(path)
                _inspect(existing, batch_id, sha256, row_count, sales_amount_minor_units)
            except ValueError:
                return Submission("permanent_failure", diagnostic="Existing destination file differs from the batch")
            except (FileNotFoundError, StorageError):
                return Submission("unknown", diagnostic="Destination conflict requires reconciliation")
        except StorageError:
            return Submission("unknown", diagnostic="Destination write outcome is uncertain; reconcile before retry")
        return Submission("submitted", operation_id=path)

    def verify(self, batch_id, sha256, row_count, sales_amount_minor_units, operation_id=None):
        try:
            path = self._path(batch_id)
            if operation_id is not None and operation_id != path:
                raise ValueError("Destination operation does not match the batch")
            payload, _ = self.store.read(path)
            count, total, distinct = _inspect(payload, batch_id, sha256, row_count, sales_amount_minor_units)
        except FileNotFoundError:
            return Verification("pending", diagnostic="Destination file is not yet visible")
        except StorageError:
            return Verification("unknown", diagnostic="Destination readback failed")
        except ValueError as exc:
            return Verification("failed", diagnostic=str(exc))
        return Verification("verified", count, total, distinct)
