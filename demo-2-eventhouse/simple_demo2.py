"""Fixed-target Eventhouse lifecycle for the shared Fabric demo."""

import json
import time
from datetime import date
from urllib.error import HTTPError
from urllib.parse import quote
from urllib.request import Request, urlopen

from azure.kusto.data import KustoClient, KustoConnectionStringBuilder

from eventhouse_core import discover_mirror_location, identifier, schema_commands, shortcut_payload


KUSTO_AUDIENCE = "https://kusto.kusto.windows.net"
FABRIC_AUDIENCE = "https://api.fabric.microsoft.com"
MIRROR_WAIT_SECONDS = 1800
MIRROR_POLL_SECONDS = 15


def _client(context):
    settings = context["config"]["demo-2"]
    connection = KustoConnectionStringBuilder.with_token_provider(
        settings["query_endpoint"], lambda: context["get_token"](KUSTO_AUDIENCE)
    )
    return KustoClient(connection)


def _checked(response):
    if response.errors_count:
        raise RuntimeError("Kusto request failed: " + "; ".join(response.get_exceptions()))
    return response


def _fabric(context, method, path, body=None):
    data = None if body is None else json.dumps(body).encode("utf-8")
    request = Request(
        FABRIC_AUDIENCE + path,
        data=data,
        method=method,
        headers={
            "Authorization": "Bearer " + context["get_token"](FABRIC_AUDIENCE),
            "Content-Type": "application/json",
            "x-ms-fabric-skill": "eventhouse-cli",
        },
    )
    with urlopen(request, timeout=60) as response:
        return json.load(response)


def _restore_dimensions(context):
    from simple_data import dimensions

    demo = context["config"]["demo-2"]
    spark = context["spark"]
    root = (
        f"abfss://{demo['workspace_id']}@onelake.dfs.fabric.microsoft.com/"
        f"{demo['lakehouse_id']}/Tables"
    )
    schemas = {
        "DimDate": "Date DATE, Year INT, MonthNumber INT, MonthName STRING",
        "DimProduct": "ProductID INT, ProductName STRING, ProductCategory STRING",
        "DimCustomer": "CustomerID INT, CustomerName STRING, CustomerRegion STRING",
    }
    for name, rows in dimensions(context["config"]["data"]).items():
        if name == "DimDate":
            rows = [{**row, "Date": date.fromisoformat(row["Date"])} for row in rows]
        spark.createDataFrame(rows, schemas[name]).write.format("delta").mode("overwrite").save(
            f"{root}/{name}"
        )


def _ensure_shortcut(context, client):
    demo = context["config"]["demo-2"]
    table = identifier(demo["sales_table"], "sales table")
    database = demo["kql_database_name"]
    row = _checked(client.execute_mgmt(
        database, f".show table {table} policy mirroring"
    )).primary_results[0][0]
    policy = row["Policy"]
    if isinstance(policy, str):
        policy = json.loads(policy)
    location = discover_mirror_location(policy, table)
    payload = shortcut_payload(location, demo["shortcut_name"])
    target = payload["target"]["oneLake"]
    if target["workspaceId"] != demo["workspace_id"] or target["itemId"] != demo["kql_database_id"]:
        raise ValueError("Mirroring policy location does not refer to the configured KQL database")

    root = f"/v1/workspaces/{demo['workspace_id']}/items/{demo['lakehouse_id']}/shortcuts"
    try:
        current = _fabric(context, "GET", f"{root}/Tables/{quote(demo['shortcut_name'], safe='')}")
    except HTTPError as exc:
        if exc.code != 404:
            raise
        current = None
    if current is None or current.get("target", {}).get("oneLake") != target:
        _fabric(context, "POST", root + "?shortcutConflictPolicy=CreateOrOverwrite", payload)


def reset(context):
    """Clear the existing sales table and restore its native OneLake surface."""
    demo = context["config"]["demo-2"]
    database = demo["kql_database_name"]
    table = identifier(demo["sales_table"], "sales table")
    mapping = identifier(demo["csv_mapping"], "CSV mapping")
    with _client(context) as client:
        for command in schema_commands(table, mapping)[:2]:
            _checked(client.execute_mgmt(database, command))
        _checked(client.execute_mgmt(
            database, f".alter-merge table {table} policy mirroring dataformat=parquet with (IsEnabled=false)"
        ))
        _checked(client.execute_mgmt(database, f".clear table {table} data"))
        _checked(client.execute_mgmt(
            database,
            f".alter-merge table {table} policy mirroring dataformat=parquet "
            f"with (IsEnabled=true, TargetLatencyInMinutes={int(demo['target_latency_minutes'])})",
        ))
        _ensure_shortcut(context, client)
    _restore_dimensions(context)


def start(context):
    """No separate trigger is needed for inline ingestion."""


def submit_batch(context, csv_bytes):
    """Ingest the producer's unchanged header-bearing UTF-8 CSV synchronously."""
    demo = context["config"]["demo-2"]
    table = identifier(demo["sales_table"], "sales table")
    mapping = identifier(demo["csv_mapping"], "CSV mapping")
    csv_text = csv_bytes.decode("utf-8")
    command = (
        f".ingest inline into table {table} with "
        f"(format='csv', ingestionMappingReference='{mapping}', ignoreFirstRecord=true) <|\n"
        + csv_text
    )
    with _client(context) as client:
        _checked(client.execute_mgmt(demo["kql_database_name"], command))


def _show_results(context, client):
    demo = context["config"]["demo-2"]
    table = identifier(demo["sales_table"], "sales table")
    result = _checked(client.execute_query(demo["kql_database_name"], f"{table} | take 10"))
    print("Eventhouse sales sample:")
    for row in result.primary_results[0]:
        print(row)

    root = (
        f"abfss://{demo['workspace_id']}@onelake.dfs.fabric.microsoft.com/"
        f"{demo['lakehouse_id']}/Tables"
    )
    shortcut = identifier(demo["shortcut_name"], "shortcut")
    query = f"""
        SELECT f.SalesLineID, f.BatchID, f.SaleTimestamp,
               c.CustomerName, c.CustomerRegion, p.ProductName, p.ProductCategory,
               d.Date, d.Year, d.MonthNumber, d.MonthName, f.Quantity,
               CAST(CAST(f.UnitPriceMinorUnits AS DECIMAL(20,0)) / 100 AS DECIMAL(18,2)) AS UnitPrice,
               CAST(CAST(f.SalesAmountMinorUnits AS DECIMAL(20,0)) / 100 AS DECIMAL(18,2)) AS SalesAmount
        FROM delta.`{root}/{shortcut}` f
        LEFT JOIN delta.`{root}/DimCustomer` c ON f.CustomerID = c.CustomerID
        LEFT JOIN delta.`{root}/DimProduct` p ON f.ProductID = p.ProductID
        LEFT JOIN delta.`{root}/DimDate` d ON to_date(f.Date, 'yyyy-MM-dd') = d.Date
    """
    print("OneLake shortcut sales joined to dimensions:")
    context["spark"].sql(query).show(10, truncate=False)


def stop(context, *, failed=False):
    """Wait for native mirroring on success; never remove results."""
    if failed:
        return
    demo = context["config"]["demo-2"]
    table = identifier(demo["sales_table"], "sales table")
    end = time.monotonic() + MIRROR_WAIT_SECONDS
    with _client(context) as client:
        while True:
            rows = _checked(client.execute_mgmt(
                demo["kql_database_name"], f".show table {table} operations mirroring-statistics"
            )).primary_results[0]
            if len(rows) != 1:
                raise RuntimeError("OneLake mirroring statistics did not return one table")
            row = rows[0]
            if (
                row["IsEnabled"]
                and not row["IsExportRunning"]
                and row["LastExportResult"] == "Completed"
                and row["CompletionPercentage"] == 100
                and row["PendingDataSize"] == 0
            ):
                break
            if time.monotonic() >= end:
                raise TimeoutError("OneLake mirroring did not finish; rerun the demo to reset")
            time.sleep(MIRROR_POLL_SECONDS)
        _show_results(context, client)
