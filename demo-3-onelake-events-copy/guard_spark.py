"""Fabric notebook phases for source admission and exact Delta readback."""

import csv
import io
import json
from datetime import date, datetime

import generator
from audit import BatchAudit
from delivery_runtime import GuardedDelivery
from fabric_storage import OneLakeStore


def onelake_endpoint(value):
    if isinstance(value, str):
        value = value.removeprefix("https://").removesuffix("/")
    if value != "onelake.dfs.fabric.microsoft.com":
        raise RuntimeError("unexpected OneLake endpoint")
    return value


def query_target(spark, notebookutils, settings, config, batch_id):
    from pyspark.sql import functions as F

    number = int(batch_id.rsplit("_", 1)[1])
    payload, expected = generator.batch_bytes(config, number)
    if expected["batch_id"] != batch_id:
        raise ValueError("unexpected batch ID during readback")
    rows = []
    for row in csv.reader(io.StringIO(payload.decode("utf-8"))):
        if tuple(row) == generator.HEADER:
            continue
        rows.append((int(row[0]), row[1], datetime.fromisoformat(row[2]), int(row[3]),
                     int(row[4]), date.fromisoformat(row[5]), int(row[6]), int(row[7]), int(row[8])))
    schema = ("SalesLineID LONG, BatchID STRING, SaleTimestamp TIMESTAMP, CustomerID INT, "
              "ProductID INT, Date DATE, Quantity INT, UnitPriceMinorUnits LONG, "
              "SalesAmountMinorUnits LONG")
    expected_rows = spark.createDataFrame(rows, schema)
    endpoint = onelake_endpoint(notebookutils.conf.get("trident.onelake.endpoint"))
    root = f"abfss://{settings['workspace_id']}@{endpoint}/{settings['item_id']}/Tables/dbo"
    fact = spark.read.format("delta").load(f"{root}/factsales")
    actual = fact.where(F.col("BatchID") == batch_id)
    actual = actual.select(*generator.HEADER)
    if actual.schema.simpleString() != expected_rows.schema.simpleString():
        raise RuntimeError("fact readback schema differs from the v1 source mapping")
    differences = actual.exceptAll(expected_rows).unionByName(expected_rows.exceptAll(actual)).count()
    metrics = actual.agg(
        F.count("*").alias("row_count"),
        F.countDistinct("SalesLineID").alias("distinct_sales_line_ids"),
        F.min("SalesLineID").alias("first_sales_line_id"),
        F.max("SalesLineID").alias("last_sales_line_id"),
        F.sum("SalesAmountMinorUnits").alias("sales_amount_minor_units"),
    ).first().asDict()
    joined = actual
    for dimension, key, value in (("dimdate", "Date", "Year"), ("dimproduct", "ProductID", "ProductCategory"),
                                  ("dimcustomer", "CustomerID", "CustomerRegion")):
        lookup = spark.read.format("delta").load(f"{root}/{dimension}").select(key, value)
        joined = joined.join(lookup, key, "left")
    if joined.count() != metrics["row_count"]:
        raise RuntimeError("dimension join expanded fact rows")
    orphans = joined.where(F.col("Year").isNull() | F.col("ProductCategory").isNull()
                           | F.col("CustomerRegion").isNull()).count()
    groups = {}
    if not orphans:
        for column, field in (("Date", "amount_by_date"), ("ProductCategory", "amount_by_product_category"),
                              ("CustomerRegion", "amount_by_customer_region")):
            groups[field] = {str(row[0]): row[1] for row in joined.groupBy(column).agg(
                F.sum("SalesAmountMinorUnits")).collect()}
    return {**metrics, **groups, "orphan_rows": orphans, "row_difference_count": differences}


def execute(spark, notebookutils, settings, arguments):
    context = notebookutils.runtime.context
    if (context["isForPipeline"] is not True or context["currentWorkspaceId"] != settings["workspace_id"]
            or context["defaultLakehouseId"] != settings["item_id"]):
        raise RuntimeError("guard must run in its deployment-bound pipeline and Lakehouse")
    app_id = spark.sparkContext.applicationId
    if not isinstance(app_id, str) or not app_id:
        raise RuntimeError("Spark application identity is unavailable")
    print(json.dumps({"phase": arguments["phase"], "spark_application_id": app_id}))
    config = generator.RunConfig(**generator.strict_json_loads(settings["approved_config"]))

    def token():
        return notebookutils.credentials.getToken("storage")

    target_store = OneLakeStore(settings["workspace_id"], settings["item_id"], token)
    producer_store = OneLakeStore(settings["producer_workspace_id"], settings["producer_item_id"], token)
    audit = BatchAudit(target_store, settings["workspace_id"], settings["item_id"])
    guard = GuardedDelivery(generator, config, producer_store, audit)
    if arguments["phase"] == "prepare":
        return json.dumps(guard.prepare(
            arguments["job_id"], arguments["subject"], arguments["event_id"] or None, arguments["origin"]))
    if arguments["phase"] == "finish":
        prepared = generator.strict_json_loads(arguments["prepared_json"])
        copy = generator.strict_json_loads(arguments["copy_json"])
        guard.finish(arguments["job_id"], prepared, {
            "status": "Succeeded", "pipeline_run_id": arguments["job_id"], "activity_run_id": None,
            "files_read": copy["filesRead"], "rows_read": copy["rowsRead"], "rows_copied": copy["rowsCopied"],
        }, lambda batch: query_target(spark, notebookutils, settings, config, batch))
        return json.dumps({"decision": "completed", "batch_id": prepared["batch_id"]})
    raise ValueError("unrecognized guarded delivery phase")
