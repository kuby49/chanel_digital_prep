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


def validate_batch_evidence(batch):
    if not isinstance(batch, dict):
        raise VerificationError("Manifest batch identity is invalid")
    try:
        batch_identifier(batch["batch_id"])
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


def measured_batches(manifest, *, synthetic_run_id=None):
    if (
        not isinstance(manifest, dict)
        or type(manifest.get("contract_version")) is not int
        or manifest["contract_version"] != 1
    ):
        raise VerificationError("Manifest contract version must be 1")
    run_id = manifest.get("run_id")
    if not isinstance(run_id, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}", run_id):
        raise VerificationError("Manifest run identity is invalid")
    if synthetic_run_id is not None:
        if run_id != synthetic_run_id or not re.fullmatch(r"demo2synthetic_[0-9a-f]{12}", run_id):
            raise VerificationError("Synthetic manifest is outside the reserved run")
    elif run_id.casefold().startswith("demo2synthetic_"):
        raise VerificationError("Production verification cannot consume a synthetic run")
    expected_class = "bootstrap" if synthetic_run_id is not None else "measured"
    batches = manifest.get("batches")
    if not isinstance(batches, dict):
        raise VerificationError("Manifest batches must be keyed by batch number")
    measured = []
    seen = set()
    for key, batch in batches.items():
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
        if batch["batch_class"] != expected_class:
            if synthetic_run_id is not None:
                raise VerificationError("Synthetic manifest may contain only bootstrap batches")
            continue
        number = batch.get("batch_number")
        if (
            type(number) is not int
            or number < 1
            or key != str(number)
            or batch_id != f"{run_id}_{number:04d}"
        ):
            raise VerificationError("Measured batch identity does not match the run")
        validate_batch_evidence(batch)
        measured.append(batch)
    if not measured:
        raise VerificationError("Manifest contains no expected batches")
    return measured


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


def verify_run_keys(spark, batches, lakehouse, shortcut, schema=None, *, synthetic_run_id=None):
    if not isinstance(batches, list) or not batches:
        raise VerificationError("Run key verification accepts measured batches only")
    expected = {}
    for batch in batches:
        if synthetic_run_id is None:
            if not isinstance(batch, dict) or batch.get("batch_class") != "measured":
                raise VerificationError("Run key verification accepts measured batches only")
        else:
            validate_synthetic_batch(batch, synthetic_run_id)
        try:
            batch_id = batch_identifier(batch["batch_id"])
            count = batch["row_count"]
        except (KeyError, ValueError) as exc:
            raise VerificationError("Measured batch identity or row count is invalid") from exc
        if batch_id in expected or type(count) is not int or count < 1:
            raise VerificationError("Measured batch identity or row count is invalid")
        expected[batch_id] = count
    from pyspark.sql import functions as F

    facts = spark.table(_table_name(lakehouse, shortcut, schema)).where(
        F.col("BatchID").isin(list(expected))
    )
    rows = facts.groupBy("BatchID").agg(
        F.count("*").alias("row_count"),
        F.countDistinct("SalesLineID").alias("distinct_keys"),
    ).collect()
    observed = {row["BatchID"]: (row["row_count"], row["distinct_keys"]) for row in rows}
    if set(observed) != set(expected) or any(
        observed[batch_id] != (count, count) for batch_id, count in expected.items()
    ):
        raise VerificationError("Measured batch has missing or duplicate sales-line keys")


def validate_synthetic_batch(batch, run_id):
    validate_batch_evidence(batch)
    if (not re.fullmatch(r"demo2synthetic_[0-9a-f]{12}", run_id)
            or batch.get("batch_class") != "bootstrap"
            or type(batch.get("batch_number")) is not int
            or batch["batch_number"] < 1
            or batch["batch_id"] != f"{run_id}_{batch['batch_number']:04d}"):
        raise VerificationError("Batch is outside the reserved synthetic bootstrap run")
