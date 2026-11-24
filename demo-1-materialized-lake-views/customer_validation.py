"""Read-only admission for bounded on-demand and native recurring demonstrations."""

import base64
from datetime import date, timedelta
import hashlib
import json
import os
from pathlib import Path
import re
import struct
from urllib.error import HTTPError, URLError
from urllib.request import Request, build_opener

from definition_plan import bootstrap_csv_bytes, render_bronze_sql, render_silver_sql
from fresh_deploy import verify_readback
from managed_refresh import _page as _mlv_page, check_idle, check_schedule, recurring_run
from ownership import (
    STATE_FILE, CandidateBlocked, _az_json, _resource, _state_lock, _unique_pairs, _uuid,
)
from provision_synthetic import FabricClient, _NoRedirect, _object_response, _verify_confirmed_receipt


def _token(audience, tenant, principal):
    credential = _az_json("account", "get-access-token", "--resource", audience, "--tenant", tenant)
    try:
        token = credential["accessToken"]
        encoded = token.split(".")[1]
        claims = json.loads(base64.urlsafe_b64decode(encoded + "=" * (-len(encoded) % 4)))
        if credential["tenant"] != tenant or claims["tid"] != tenant or claims["oid"] != principal:
            raise ValueError
    except (AttributeError, IndexError, KeyError, TypeError, ValueError):
        raise CandidateBlocked("Readiness token does not match the configured tenant/principal.") from None
    return token


def _bootstrap(workspace, item, tenant, principal):
    url = (f"https://onelake.dfs.fabric.microsoft.com/{workspace}/{item}"
           "/Files/incoming/sales/bootstrap_header_only_v1.csv")
    request = Request(url, headers={
        "Authorization": "Bearer " + _token("https://storage.azure.com", tenant, principal),
        "x-ms-version": "2021-06-08",
    })
    try:
        with build_opener(_NoRedirect()).open(request, timeout=30) as response:
            content = response.read(1024)
    except (HTTPError, URLError, OSError):
        raise CandidateBlocked("Cannot independently read the owned landing bootstrap.") from None
    if hashlib.sha256(content).digest() != hashlib.sha256(bootstrap_csv_bytes()).digest():
        raise CandidateBlocked("Landing bootstrap no longer matches CSV v1.")


def _sql_readback(endpoint, lakehouse, tenant, principal, data):
    try:
        import pyodbc
    except ImportError:
        raise CandidateBlocked("pyodbc and a SQL Server ODBC driver are required for no-Spark readiness.") from None
    host = endpoint.get("connectionString")
    if (endpoint.get("provisioningStatus") != "Success" or not isinstance(host, str)
            or not re.fullmatch(r"[a-z0-9-]+\.datawarehouse\.fabric\.microsoft\.com", host)
            or not re.fullmatch(r"[A-Za-z][A-Za-z0-9_]*", lakehouse)):
        raise CandidateBlocked("Lakehouse SQL endpoint is unavailable or invalid.")
    drivers = pyodbc.drivers()
    driver = next((name for name in ("ODBC Driver 18 for SQL Server", "ODBC Driver 17 for SQL Server")
                   if name in drivers), None)
    if driver is None:
        raise CandidateBlocked("No supported SQL Server ODBC driver is installed.")
    token = _token("https://database.windows.net", tenant, principal).encode("utf-16-le")
    connection = None
    try:
        connection = pyodbc.connect(
            f"Driver={{{driver}}};Server={host};Database={lakehouse};Encrypt=yes;TrustServerCertificate=no;",
            attrs_before={1256: struct.pack("<I", len(token)) + token}, timeout=20,
        )
        connection.timeout = 20
        cursor = connection.cursor()
        start = date.fromisoformat(data["start_date"])
        end = start + timedelta(days=data["date_count"] - 1)
        for table, key, count, minimum, maximum, invalid in (
            ("DimDate", "[Date]", data["date_count"], start, end, "MonthName IS NULL"),
            ("DimProduct", "ProductID", data["product_count"], 1, data["product_count"],
             "ProductName IS NULL OR ProductCategory IS NULL"),
            ("DimCustomer", "CustomerID", data["customer_count"], 1, data["customer_count"],
             "CustomerName IS NULL OR CustomerRegion IS NULL"),
        ):
            row = cursor.execute(
                f"SELECT COUNT_BIG(*), COUNT(DISTINCT {key}), MIN({key}), MAX({key}), "
                f"COUNT_BIG(CASE WHEN {invalid} THEN 1 END) FROM silver.{table.lower()}"
            ).fetchone()
            if tuple(row) != (count, count, minimum, maximum, 0):
                raise CandidateBlocked(f"Live {table} does not match the producer dimension range.")
        cursor.execute(
            "SELECT TOP (0) SalesLineID,BatchID,SaleTimestamp,CustomerID,ProductID,[Date],Quantity,"
            "UnitPriceMinorUnits,SalesAmountMinorUnits,_corrupt_record,__filepath__ FROM bronze.sales_files"
        )
        cursor.execute(
            "SELECT TOP (0) SalesLineID,BatchID,SalesAmountMinorUnits,payload_variants,max_file_rows,"
            "missing_source,CustomerName,ProductName,MonthName FROM silver.sales_joined"
        )
        row = cursor.execute(
            "SELECT COUNT_BIG(*) FROM silver.sales_joined WHERE payload_variants IS NULL OR payload_variants <> 1 "
            "OR max_file_rows IS NULL OR max_file_rows <> 1 OR missing_source IS NULL OR missing_source <> 0 "
            "OR CustomerName IS NULL OR ProductName IS NULL OR MonthName IS NULL"
        ).fetchone()
        if row[0] != 0:
            raise CandidateBlocked("Existing Silver output has quality or replay diagnostics.")
    except pyodbc.Error as exc:
        raise CandidateBlocked(f"SQL readiness readback failed (SQLSTATE {exc.args[0]}).") from None
    finally:
        if connection is not None:
            connection.close()


def _page(client, path):
    if re.fullmatch(
        r"/v1/workspaces/[0-9a-fA-F-]+/lakehouses/[0-9a-fA-F-]+/"
        r"jobs/refreshMaterializedLakeViews/(?:instances|schedules)", path,
    ):
        return _mlv_page(client, path, read=_object_response)
    _, body = _object_response(client, path)
    if (not isinstance(body.get("value"), list) or body.get("continuationUri")
            or body.get("continuationToken") or any(not isinstance(row, dict) for row in body["value"])):
        raise CandidateBlocked("Readiness metadata is incomplete or paginated.")
    return body["value"]


def validate_customer(config, prefix, state_dir, *, _preparing_schedule=False):
    demo, shared, data = (config.get(key) for key in ("demo-1", "shared", "data"))
    if not isinstance(demo, dict) or demo.get("readiness_scope") != "customer-run":
        raise CandidateBlocked("The header-only bootstrap exists; validate-only requires explicit customer-run scope.")
    if not isinstance(shared, dict) or not isinstance(data, dict):
        raise CandidateBlocked("Customer-run producer identity, data and target configuration are required.")
    if (config.get("prefix", prefix) != prefix or not isinstance(config.get("capacity"), dict)
            or not isinstance(shared.get("target_bindings"), dict)):
        raise ValueError("Customer-run prefix, capacity and selected target bindings must be explicit and consistent.")
    tenant, principal = _uuid(config.get("tenant_id")), _uuid(shared.get("identity_id"))
    recurring = recurring_run(data)
    for key in ("date_count", "product_count", "customer_count"):
        if type(data.get(key)) is not int or data[key] < 1:
            raise ValueError(f"data.{key} must be a positive integer")
    if not isinstance(data.get("run_id"), str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}", data["run_id"]):
        raise ValueError("A fresh customer run_id is required.")
    if not isinstance(data.get("start_date"), str):
        raise ValueError("data.start_date must be an ISO date.")
    date.fromisoformat(data["start_date"])
    if (demo.get("bronze_schema"), demo.get("bronze_view"), demo.get("silver_schema"),
            demo.get("silver_view")) != ("bronze", "sales_files", "silver", "sales_joined"):
        raise CandidateBlocked("Customer-run schema/view names do not match the deployed native lineage.")
    render_bronze_sql(config, prefix)
    render_silver_sql(config, prefix)
    azure_config = Path(os.environ.get("AZURE_CONFIG_DIR", ""))
    if not azure_config.is_absolute() or not azure_config.is_dir():
        raise CandidateBlocked("Select an existing isolated AZURE_CONFIG_DIR before readiness validation.")
    if not state_dir.is_dir():
        raise CandidateBlocked("Demo 1 original ownership journal directory is missing.")
    with _state_lock(state_dir):
        journal_path = state_dir / STATE_FILE
        if journal_path.is_symlink() or not journal_path.is_file():
            raise CandidateBlocked("Original ownership journal must be a regular file.")
        with journal_path.open(encoding="utf-8") as file:
            journal = json.load(file, object_pairs_hook=_unique_pairs)
        if (not isinstance(journal, dict) or type(journal.get("contract_version")) is not int
                or journal["contract_version"] != 1 or journal.get("track") != "demo-1"
                or journal.get("tenant_id") != tenant or journal.get("prefix") != prefix
                or journal.get("pending_operations") != [] or journal.get("uncertain_operations") != []):
            raise CandidateBlocked("Original ownership journal is mismatched or unresolved.")
        owner = journal.get("owner")
        if not isinstance(owner, dict):
            raise CandidateBlocked("Journaled ownership identity is missing.")
        owner_id = _uuid(owner.get("id"))
        if owner.get("principal_id") != principal or owner.get("marker") != f"{prefix}-demo1-{owner_id}":
            raise CandidateBlocked("Producer identity does not match the journaled owner principal.")
        expected = journal.get("expected")
        if (expected != {"workspace_name": demo.get("workspace_name"),
                         "lakehouse_name": demo.get("lakehouse_name"),
                         "capacity_id": config.get("capacity", {}).get("id")}):
            raise CandidateBlocked("Customer configuration conflicts with the journaled resource binding.")
        workspace, item = _uuid(demo.get("workspace_id")), _uuid(demo.get("lakehouse_id"))
        target = {"workspace_id": workspace, "item_id": item, "fact": "silver.sales_joined"}
        if shared.get("target_bindings", {}).get("demo-1") != target:
            raise CandidateBlocked("Selected Demo 1 producer target binding does not match the owned fact.")
        landing = f"abfss://{workspace}@onelake.dfs.fabric.microsoft.com/{item}/Files/incoming/sales/"
        if demo.get("landing_uri") != landing:
            raise CandidateBlocked("Demo 1 producer landing does not match the owned file-backed lineage.")
        client = FabricClient(tenant, principal, 300, azure_config)
        live_lakehouse = None
        for kind, resource_id, path, name in (
            ("Workspace", workspace, "/v1/workspaces", expected["workspace_name"]),
            ("Lakehouse", item, f"/v1/workspaces/{workspace}/lakehouses", expected["lakehouse_name"]),
        ):
            record = journal.get(kind.lower())
            _resource(record, kind, owner_id, name, owner["marker"])
            if (record.get("id") != resource_id or record.get("create_path") != path
                    or not isinstance(record.get("create_request"), dict)):
                raise CandidateBlocked(f"{kind} create evidence conflicts with the selected target.")
            request = {"displayName": name, "description": owner["marker"]}
            if kind == "Workspace":
                request["capacityId"] = expected["capacity_id"]
            else:
                request["creationPayload"] = {"enableSchemas": True}
                if record.get("workspace_id") != workspace or record["readback"].get("workspace_id") != workspace:
                    raise CandidateBlocked("Journaled Lakehouse parent conflicts with the selected target.")
            if record["create_request"] != request:
                raise CandidateBlocked(f"{kind} create intent conflicts with the selected configuration.")
            _verify_confirmed_receipt(record, kind, path, resource_id)
            _, live = _object_response(client, f"{path}/{resource_id}")
            verify_readback(journal, kind, resource_id, live)
            if kind == "Lakehouse":
                live_lakehouse = live
        capacities = _page(client, "/v1/capacities")
        if not any(row.get("id") == expected["capacity_id"] and row.get("state") == "Active"
                   for row in capacities):
            raise CandidateBlocked("Owned Workspace capacity is not Active.")
        roles = _page(client, f"/v1/workspaces/{workspace}/roleAssignments")
        if not any(isinstance(row.get("principal"), dict) and row["principal"].get("id") == principal
                   and row.get("role") in ("Admin", "Member", "Contributor") for row in roles):
            raise CandidateBlocked("Producer lacks a direct write/refresh role on the Demo 1 workspace.")
        refresh_path = f"/v1/workspaces/{workspace}/lakehouses/{item}/jobs/refreshMaterializedLakeViews"
        history = _page(client, refresh_path + "/instances")
        check_idle(history, item)
        schedules = _page(client, refresh_path + "/schedules")
        if recurring:
            check_schedule(config, prefix, state_dir, journal, schedules, preparing=_preparing_schedule)
        elif any(row.get("enabled") is not False for row in schedules):
            raise CandidateBlocked("The bounded customer run requires no enabled competing refresh schedule.")
        _bootstrap(workspace, item, tenant, principal)
        _sql_readback(live_lakehouse["properties"].get("sqlEndpointProperties", {}),
                      expected["lakehouse_name"], tenant, principal, data)
        if recurring:
            check_idle(_page(client, refresh_path + "/instances"), item)
            schedules = _page(client, refresh_path + "/schedules")
            check_schedule(config, prefix, state_dir, journal, schedules, preparing=_preparing_schedule)
    if recurring:
        return [
            "Owned target, landing/bootstrap, producer role, dimensions and SQL outputs verified read-only.",
            "Preparation only; no recurring readiness claimed." if _preparing_schedule else
            "Owned bounded five-minute full-lineage schedule verified; future start and no active job required.",
            "Ten files x 1000 rows are configured for 60-second source intervals; downstream refresh is five minutes "
            "plus processing, not a 60-second freshness SLA. Reconcile only the ten customer BatchIDs "
            "against the delivered manifest, excluding synthetic/bootstrap baseline; delivery, latency, "
            "per-view mode and measured acceptance remain unproven.",
        ]
    return [
        "Owned target, landing/bootstrap, producer role, dimensions and SQL outputs verified read-only.",
        "Native lineage has a completed refresh and no active job or enabled schedule.",
        "Ready for the bounded customer run using a coordinated on-demand refresh; no new batch delivery "
        "or per-view refresh mode is claimed. This is not ten-file acceptance readiness.",
    ]
