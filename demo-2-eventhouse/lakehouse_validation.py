"""Spark checks and dimension preload for the Demo 2 shortcut."""

import re
import time
from datetime import date

from eventhouse_core import batch_identifier, identifier, lakehouse_join_query


class VerificationError(RuntimeError):
    pass


class NotVisible(VerificationError):
    pass


def _table_name(lakehouse, table, schema=None):
    parts = [identifier(lakehouse, "lakehouse")]
    if schema:
        parts.append(identifier(schema, "lakehouse schema"))
    parts.append(identifier(table, "table"))
    return ".".join(parts)


def _dimension_frames(spark, data, generator):
    from pyspark.sql.types import DateType, IntegerType, StringType, StructField, StructType

    run = generator.RunConfig(
        run_id=data.get("run_id") or "dimension_preload",
        start_date=data["start_date"],
        date_count=data["date_count"],
        products=data["product_count"],
        customers=data["customer_count"],
        seed=data["seed"],
    )
    dimensions = generator.dimensions(run)
    definitions = {
        "DimDate": (
            ("Date", DateType()),
            ("Year", IntegerType()),
            ("MonthNumber", IntegerType()),
            ("MonthName", StringType()),
        ),
        "DimProduct": (
            ("ProductID", IntegerType()),
            ("ProductName", StringType()),
            ("ProductCategory", StringType()),
        ),
        "DimCustomer": (
            ("CustomerID", IntegerType()),
            ("CustomerName", StringType()),
            ("CustomerRegion", StringType()),
        ),
    }
    result = {}
    for name, fields in definitions.items():
        names = [field[0] for field in fields]
        rows = []
        for source in dimensions[name]:
            row = dict(source)
            if name == "DimDate":
                row["Date"] = date.fromisoformat(row["Date"])
            rows.append(tuple(row[column] for column in names))
        schema = StructType([StructField(column, kind, False) for column, kind in fields])
        result[name] = spark.createDataFrame(rows, schema)
    return result


def _verify_dimension(spark, qualified_name, expected):
    if not spark.catalog.tableExists(qualified_name):
        raise VerificationError(f"Required dimension {qualified_name} does not exist")
    actual = spark.table(qualified_name)
    names = expected.columns
    if actual.columns != names:
        raise VerificationError(f"Dimension {qualified_name} has an unexpected schema")
    if [field.dataType for field in actual.schema] != [
        field.dataType for field in expected.schema
    ]:
        raise VerificationError(f"Dimension {qualified_name} has unexpected column types")
    if actual.exceptAll(expected).limit(1).count() or expected.exceptAll(actual).limit(1).count():
        raise VerificationError(f"Dimension {qualified_name} differs from the shared generator")


def preload_dimensions(spark, data, generator, lakehouse, schema=None):
    """Create missing dimensions; never overwrite a mismatching existing table."""
    for name, expected in _dimension_frames(spark, data, generator).items():
        qualified = _table_name(lakehouse, name, schema)
        if not spark.catalog.tableExists(qualified):
            expected.write.format("delta").mode("errorifexists").saveAsTable(qualified)
        _verify_dimension(spark, qualified, expected)


def verify_dimensions(spark, data, generator, lakehouse, schema=None):
    for name, expected in _dimension_frames(spark, data, generator).items():
        _verify_dimension(spark, _table_name(lakehouse, name, schema), expected)


def measured_batches(manifest):
    if (
        not isinstance(manifest, dict)
        or type(manifest.get("contract_version")) is not int
        or manifest["contract_version"] != 1
    ):
        raise VerificationError("Manifest contract version must be 1")
    batches = manifest.get("batches")
    if not isinstance(batches, dict):
        raise VerificationError("Manifest batches must be keyed by batch number")
    measured = []
    seen = set()
    for batch in batches.values():
        if not isinstance(batch, dict) or batch.get("batch_class") not in (
            "measured",
            "bootstrap",
            "negative-test",
        ):
            raise VerificationError("Manifest batch class is missing or invalid")
        try:
            batch_id = batch_identifier(batch["batch_id"])
        except (KeyError, ValueError) as exc:
            raise VerificationError("Manifest batch identity is invalid") from exc
        if batch_id in seen:
            raise VerificationError("Manifest batch IDs must be unique across all classes")
        seen.add(batch_id)
        if batch["batch_class"] != "measured":
            continue
        try:
            checksum = batch["sha256"]
            count = batch["row_count"]
            total = batch["sales_amount_minor_units"]
        except (KeyError, ValueError) as exc:
            raise VerificationError("Manifest batch identity is invalid") from exc
        if (
            not isinstance(checksum, str)
            or not re.fullmatch(r"[a-f0-9]{64}", checksum)
            or type(count) is not int
            or count < 1
            or type(total) is not int
            or total < 0
        ):
            raise VerificationError("Manifest batch identity, checksum, count or total is invalid")
        for field in (
            "amount_by_date",
            "amount_by_product_category",
            "amount_by_customer_region",
        ):
            groups = batch.get(field)
            if not isinstance(groups, dict) or any(
                not isinstance(key, str) or type(value) is not int or value < 0
                for key, value in groups.items()
            ):
                raise VerificationError(f"Manifest {field} is invalid")
            if sum(groups.values()) != total:
                raise VerificationError(f"Manifest {field} does not reconcile to the batch total")
        measured.append(batch)
    if not measured:
        raise VerificationError("Manifest contains no measured batches")
    return measured


def reservation_bounds(reservation):
    if not isinstance(reservation, dict):
        raise VerificationError("A sales-line reservation is required")
    values = []
    for field in ("start", "end", "count"):
        value = reservation.get(field)
        pattern = r"[1-9][0-9]*" if field == "count" else r"(?:0|-?[1-9][0-9]*)"
        if (
            not isinstance(value, str)
            or len(value.lstrip("-")) > 20
            or not re.fullmatch(pattern, value)
        ):
            raise VerificationError(f"Reservation {field} must be a canonical decimal string")
        values.append(int(value))
    start, end, count = values
    if not -(2**63) <= start <= end < 2**63 or end - start + 1 != count:
        raise VerificationError("Reservation range is not a valid inclusive BIGINT range")
    return start, end, count


def _validate_reservation_batches(reservation, batches):
    if (
        not isinstance(reservation, dict)
        or type(reservation.get("version")) is not int
        or reservation["version"] != 1
        or reservation.get("batch_class") != "measured"
        or not isinstance(reservation.get("run_id"), str)
        or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}", reservation["run_id"])
    ):
        raise VerificationError("Measured reservation identity is invalid")
    for batch in batches:
        number = batch.get("batch_number") if isinstance(batch, dict) else None
        if (
            type(number) is not int
            or number < 1
            or batch.get("batch_id") != f"{reservation['run_id']}_{number:04d}"
        ):
            raise VerificationError("Measured batch identity does not match the reserved run")


def measured_reservation(manifest, batches, generator_sha256):
    reservation = manifest.get("sales_line_id_reservation") if isinstance(manifest, dict) else None
    _validate_reservation_batches(reservation, batches)
    if (
        not isinstance(manifest.get("run_id"), str)
        or reservation["run_id"] != manifest["run_id"]
        or not isinstance(generator_sha256, str)
        or not re.fullmatch(r"[a-f0-9]{64}", generator_sha256)
        or reservation.get("generator_sha256") != generator_sha256
        or any(
            not isinstance(reservation.get(field), str)
            or not re.fullmatch(r"[a-f0-9]{64}", reservation[field])
            for field in ("allocator_id", "fingerprint")
        )
    ):
        raise VerificationError("Measured reservation does not match the run and packaged generator")
    _, _, count = reservation_bounds(reservation)
    if count != sum(batch["row_count"] for batch in batches):
        raise VerificationError("Reservation count does not match the measured run")
    return reservation


def _grouped_amounts(frame, column):
    from pyspark.sql import functions as F

    rows = frame.groupBy(column).agg(
        F.sum(F.col("SalesAmountMinorUnits").cast("decimal(38,0)")).alias("Amount")
    ).collect()
    return {
        row[column].isoformat() if isinstance(row[column], date) else row[column]: int(row["Amount"])
        for row in rows
    }


def verify_lakehouse_batch(spark, batch, lakehouse, shortcut, schema=None):
    """Validate a batch without collecting or reporting propagation statistics."""
    from pyspark.sql import functions as F

    batch_id = batch_identifier(batch["batch_id"])
    fact_name = _table_name(lakehouse, shortcut, schema)
    facts = spark.table(fact_name).where(F.col("BatchID") == batch_id)
    raw_count = facts.count()
    if raw_count < batch["row_count"]:
        raise NotVisible(f"Batch {batch_id} is not yet fully visible in the shortcut")
    if raw_count != batch["row_count"]:
        raise VerificationError(f"Batch {batch_id} has an unexpected number of facts")
    if facts.where(F.col("SalesLineID").isNull()).limit(1).count():
        raise VerificationError(f"Batch {batch_id} has null fact keys")
    if facts.select("SalesLineID").distinct().count() != raw_count:
        raise VerificationError(f"Batch {batch_id} has duplicate fact keys")

    joined = spark.sql(lakehouse_join_query(lakehouse, shortcut, schema)).where(
        F.col("BatchID") == batch_id
    )
    if joined.count() != raw_count:
        raise VerificationError(f"Batch {batch_id} does not preserve fact rows through joins")
    invalid = (
        F.col("Date").isNull()
        | F.col("SaleTimestamp").isNull()
        | F.col("CustomerName").isNull()
        | F.col("CustomerRegion").isNull()
        | F.col("ProductName").isNull()
        | F.col("ProductCategory").isNull()
        | F.col("Year").isNull()
        | F.col("MonthNumber").isNull()
        | F.col("MonthName").isNull()
        | F.col("Quantity").isNull()
        | (F.col("Quantity") <= 0)
        | F.col("UnitPrice").isNull()
        | F.col("SalesAmount").isNull()
        | (F.col("UnitPriceMinorUnits") < 0)
        | (F.col("SalesAmountMinorUnits") < 0)
        | (
            F.col("UnitPrice") * F.lit(100)
            != F.col("UnitPriceMinorUnits").cast("decimal(20,0)")
        )
        | (
            F.col("SalesAmount") * F.lit(100)
            != F.col("SalesAmountMinorUnits").cast("decimal(20,0)")
        )
        | (
            F.col("SalesAmountMinorUnits").cast("decimal(38,0)")
            != F.col("Quantity").cast("decimal(38,0)")
            * F.col("UnitPriceMinorUnits").cast("decimal(38,0)")
        )
    )
    if joined.where(invalid).limit(1).count():
        raise VerificationError(f"Batch {batch_id} has invalid facts or orphaned dimensions")
    totals = joined.agg(
        F.sum(F.col("SalesAmountMinorUnits").cast("decimal(38,0)")).alias("Amount"),
        F.sum("SalesAmount").alias("DecimalAmount"),
    ).first()
    amount = totals["Amount"]
    if amount is None or int(amount) != batch["sales_amount_minor_units"]:
        raise VerificationError(f"Batch {batch_id} has an incorrect exact total")
    if totals["DecimalAmount"] is None or totals["DecimalAmount"] * 100 != amount:
        raise VerificationError(f"Batch {batch_id} has an incorrect decimal total")
    for column, field in (
        ("Date", "amount_by_date"),
        ("ProductCategory", "amount_by_product_category"),
        ("CustomerRegion", "amount_by_customer_region"),
    ):
        if _grouped_amounts(joined, column) != batch[field]:
            raise VerificationError(f"Batch {batch_id} has incorrect {field} totals")
    return {"batch_id": batch_id, "row_count": raw_count, "sales_amount_minor_units": int(amount)}


def verify_batch_with_timeout(
    spark, batch, lakehouse, shortcut, schema, timeout_seconds, poll_interval_seconds,
    *, clock=time.monotonic, sleep=time.sleep,
):
    for value in (timeout_seconds, poll_interval_seconds):
        if type(value) is not int or value <= 0:
            raise VerificationError("Verification timeout and poll interval must be positive integers")
    deadline = clock() + timeout_seconds
    while True:
        try:
            if not spark.catalog.tableExists(_table_name(lakehouse, shortcut, schema)):
                raise NotVisible("Shortcut table is not yet visible")
            result = verify_lakehouse_batch(spark, batch, lakehouse, shortcut, schema)
            if clock() > deadline:
                raise TimeoutError("Batch verification exceeded its configured timeout")
            return result
        except NotVisible as exc:
            remaining = deadline - clock()
            if remaining <= 0:
                raise TimeoutError("Batch did not become visible before its configured timeout") from exc
            sleep(min(poll_interval_seconds, remaining))


def verify_run_keys(spark, batches, lakehouse, shortcut, schema=None, *, reservation):
    if not batches or any(
        not isinstance(batch, dict) or batch.get("batch_class") != "measured"
        for batch in batches
    ):
        raise VerificationError("Run key verification accepts measured batches only")
    _validate_reservation_batches(reservation, batches)
    from pyspark.sql import functions as F

    all_facts = spark.table(_table_name(lakehouse, shortcut, schema))
    batch_ids = [batch["batch_id"] for batch in batches]
    facts = all_facts.where(F.col("BatchID").isin(batch_ids))
    expected = sum(batch["row_count"] for batch in batches)
    start, end, count = reservation_bounds(reservation)
    if count != expected:
        raise VerificationError("Reservation count does not match the measured run")
    keys = facts.select("SalesLineID")
    if (
        facts.count() != expected
        or facts.where(F.col("SalesLineID").isNull()).limit(1).count()
        or keys.distinct().count() != expected
    ):
        raise VerificationError("Measured run has missing or duplicate sales-line keys")
    if facts.where(
        (F.col("SalesLineID") < F.lit(start)) | (F.col("SalesLineID") > F.lit(end))
    ).limit(1).count():
        raise VerificationError("Measured run has sales-line keys outside its reserved range")
    other_keys = all_facts.where(
        F.col("BatchID").isNull() | ~F.col("BatchID").isin(batch_ids)
    ).select("SalesLineID")
    if keys.join(other_keys, "SalesLineID", "left_semi").limit(1).count():
        raise VerificationError("Measured run has sales-line keys already used by another batch or run")
