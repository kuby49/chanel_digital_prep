"""Reset and run Demo 1 using the configured Lakehouse and refresh_schedule_id.

The producer bundles simple_data and supplies config, spark and get_token.
Fabric provides notebookutils; requests is required for its public APIs.
"""

from datetime import datetime, timedelta, timezone
import time
from urllib.parse import unquote

import requests


FABRIC = "https://api.fabric.microsoft.com"
TERMINAL = {"Completed", "Failed", "Cancelled", "Canceled", "Deduped"}
ACTIVE = {"NotStarted", "InProgress"}
WAIT_SECONDS = 3600
HEADER = (
    "SalesLineID,BatchID,SaleTimestamp,CustomerID,ProductID,Date,"
    "Quantity,UnitPriceMinorUnits,SalesAmountMinorUnits\n"
)


def _request(context, method, path, *, body=None, params=None):
    response = requests.request(
        method, FABRIC + path,
        headers={
            "Authorization": "Bearer " + context["get_token"](FABRIC),
            "x-ms-fabric-skill": "spark-cli",
        },
        json=body, params=params, timeout=60,
    )
    response.raise_for_status()
    return response.json() if response.content else None


def _root(context):
    demo = context["config"]["demo-1"]
    return (f"/v1/workspaces/{demo['workspace_id']}/lakehouses/{demo['lakehouse_id']}"
            "/jobs/refreshMaterializedLakeViews")


def _schedule(context):
    return _root(context) + "/schedules/" + context["config"]["demo-1"]["refresh_schedule_id"]


def _jobs(context):
    path = _root(context) + "/instances"
    rows = []
    token = None
    while True:
        page = _request(context, "GET", path,
                        params={"continuationToken": unquote(token)} if token else None)
        rows.extend(page["value"])
        token = page.get("continuationToken")
        if not token:
            return rows


def _wait_idle(context, *, cancel):
    deadline = time.monotonic() + WAIT_SECONDS
    cancelled = set()
    while True:
        jobs = _jobs(context)
        unknown = [job for job in jobs if job["status"] not in TERMINAL | ACTIVE]
        if unknown:
            raise RuntimeError(f"Unknown MLV refresh status: {unknown[0]['status']}")
        active = [job for job in jobs if job["status"] in ACTIVE]
        if not active:
            return
        if cancel:
            demo = context["config"]["demo-1"]
            for job in active:
                if job["id"] not in cancelled:
                    _request(context, "POST", f"/v1/workspaces/{demo['workspace_id']}/items/"
                             f"{demo['lakehouse_id']}/jobs/instances/{job['id']}/cancel")
                    cancelled.add(job["id"])
        if time.monotonic() >= deadline:
            raise TimeoutError("MLV refresh did not stop; reset was not started")
        time.sleep(10)


def _identifier(name):
    return "`" + name.replace("`", "``") + "`"


def _names(context):
    demo = context["config"]["demo-1"]
    workspace = _request(context, "GET", "/v1/workspaces/" + demo["workspace_id"])["displayName"]
    base = ".".join(map(_identifier, (workspace, demo["lakehouse_name"])))
    bronze_schema = base + "." + _identifier(demo["bronze_schema"])
    silver_schema = base + "." + _identifier(demo["silver_schema"])
    return (bronze_schema, silver_schema,
            bronze_schema + "." + _identifier(demo["bronze_view"]),
            silver_schema + "." + _identifier(demo["silver_view"]))


def _landing(context):
    demo = context["config"]["demo-1"]
    if demo["landing_path"] != "Files/incoming/sales":
        raise ValueError("Demo 1 landing_path must be the dedicated Files/incoming/sales folder")
    return (f"abfss://{demo['workspace_id']}@onelake.dfs.fabric.microsoft.com/"
            f"{demo['lakehouse_id']}/{demo['landing_path']}")


def _dimensions(context, silver):
    from simple_data import dimensions

    spark = context["spark"]
    for name, rows in dimensions(context["config"]["data"]).items():
        spark.createDataFrame(rows).createOrReplaceTempView("_demo1_" + name)
    spark.sql(f"""CREATE OR REPLACE TABLE {silver}.`DimDate`
        USING DELTA TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true') AS
        SELECT CAST(Date AS DATE) AS Date, CAST(Year AS INT) AS Year,
               CAST(MonthNumber AS INT) AS MonthNumber, MonthName FROM _demo1_DimDate""")
    spark.sql(f"""CREATE OR REPLACE TABLE {silver}.`DimProduct`
        USING DELTA TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true') AS
        SELECT CAST(ProductID AS INT) AS ProductID, ProductName, ProductCategory
        FROM _demo1_DimProduct""")
    spark.sql(f"""CREATE OR REPLACE TABLE {silver}.`DimCustomer`
        USING DELTA TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true') AS
        SELECT CAST(CustomerID AS INT) AS CustomerID, CustomerName, CustomerRegion
        FROM _demo1_DimCustomer""")


def reset(context):
    """Restore the fixed landing folder, dimensions and empty MLVs."""
    from notebookutils import mssparkutils

    stop(context, failed=True)
    spark = context["spark"]
    bronze_schema, silver_schema, bronze, silver = _names(context)
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {bronze_schema}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {silver_schema}")
    demo = context["config"]["demo-1"]
    for schema, name, view in (
        (silver_schema, demo["silver_view"], silver),
        (bronze_schema, demo["bronze_view"], bronze),
    ):
        existing = spark.sql(f"SHOW TABLES IN {schema}").collect()
        if any(row["tableName"].lower() == name.lower() for row in existing):
            spark.sql(f"DROP MATERIALIZED LAKE VIEW {view}")
    landing = _landing(context)
    fs = mssparkutils.fs
    if fs.exists(landing) and not fs.rm(landing, True):
        raise RuntimeError("Could not clear the Demo 1 landing folder")
    if not fs.mkdirs(landing) or not fs.put(landing + "/bootstrap_header_only_v1.csv", HEADER, False):
        raise RuntimeError("Could not create the Demo 1 header-only baseline")
    _dimensions(context, silver_schema)
    spark.sql(f"""CREATE MATERIALIZED LAKE VIEW {bronze}
        USING OneLake_Files
        OPTIONS ('format' = 'csv', 'path' = '{landing}/', 'header' = 'true',
                 'inferSchema' = 'false', 'columnNameOfCorruptRecord' = '_corrupt_record')
        TBLPROPERTIES ('schema_mode' = 'FIXED', 'refresh_mode' = 'APPEND_ONLY')
        AS SELECT SalesLineID, BatchID, SaleTimestamp, CustomerID, ProductID, `Date`,
                  Quantity, UnitPriceMinorUnits, SalesAmountMinorUnits, _corrupt_record, __filepath__""")
    spark.sql(f"""CREATE MATERIALIZED LAKE VIEW {silver} AS
        SELECT CAST(b.SalesLineID AS BIGINT) AS SalesLineID, b.BatchID,
               CAST(b.SaleTimestamp AS TIMESTAMP) AS SaleTimestamp,
               CAST(b.CustomerID AS INT) AS CustomerID, CAST(b.ProductID AS INT) AS ProductID,
               CAST(b.`Date` AS DATE) AS `Date`, CAST(b.Quantity AS INT) AS Quantity,
               CAST(b.UnitPriceMinorUnits AS BIGINT) AS UnitPriceMinorUnits,
               CAST(b.SalesAmountMinorUnits AS BIGINT) AS SalesAmountMinorUnits,
               CAST(CAST(b.UnitPriceMinorUnits AS DECIMAL(18, 0)) / 100 AS DECIMAL(18, 2)) AS UnitPrice,
               CAST(CAST(b.SalesAmountMinorUnits AS DECIMAL(18, 0)) / 100 AS DECIMAL(18, 2)) AS SalesAmount,
               c.CustomerName, c.CustomerRegion, p.ProductName, p.ProductCategory,
               d.Year, d.MonthNumber, d.MonthName
        FROM {bronze} b
        INNER JOIN {silver_schema}.`DimCustomer` c ON CAST(b.CustomerID AS INT) = c.CustomerID
        INNER JOIN {silver_schema}.`DimProduct` p ON CAST(b.ProductID AS INT) = p.ProductID
        INNER JOIN {silver_schema}.`DimDate` d ON CAST(b.`Date` AS DATE) = d.`Date`""")


def start(context):
    """Enable the existing native five-minute full-lineage schedule."""
    schedule = _request(context, "GET", _schedule(context))
    if schedule.get("executionData"):
        raise RuntimeError("Demo 1 schedule must refresh the full lakehouse lineage")
    now = datetime.now(timezone.utc).replace(microsecond=0)
    _request(context, "PATCH", _schedule(context), body={
        "enabled": True,
        "configuration": {
            "type": "Cron", "interval": context["config"]["demo-1"]["refresh_interval_minutes"],
            "startDateTime": (now + timedelta(seconds=30)).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "endDateTime": (now + timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "localTimeZoneId": "UTC",
        },
    })


def stop(context, *, failed=False):
    """Stop the schedule and drain or cancel this lakehouse's MLV refresh."""
    path = _schedule(context)
    schedule = _request(context, "GET", path)
    if not failed:
        if not schedule["enabled"]:
            raise RuntimeError("Demo 1 MLV schedule stopped before the final refresh")
        since = datetime.now(timezone.utc)
        deadline = time.monotonic() + WAIT_SECONDS
        while True:
            jobs = _jobs(context)
            for job in jobs:
                started = job.get("startTimeUtc")
                if started:
                    stamp = datetime.fromisoformat(started.replace("Z", "+00:00"))
                    if stamp.tzinfo is None:
                        stamp = stamp.replace(tzinfo=timezone.utc)
                    if stamp >= since:
                        if job["status"] == "Completed":
                            break
                        if job["status"] == "Failed":
                            raise RuntimeError(f"MLV refresh failed: {job.get('failureReason')}")
            else:
                if time.monotonic() >= deadline:
                    raise TimeoutError("No MLV refresh completed after the last file")
                time.sleep(10)
                continue
            break
    if schedule["enabled"]:
        body = {"enabled": False, "configuration": schedule["configuration"]}
        if schedule.get("executionData"):
            body["executionData"] = schedule["executionData"]
        _request(context, "PATCH", path, body=body)
    _wait_idle(context, cancel=failed)
