"""Read selected candidate identities without claiming deployment ownership."""

import math

from fabric_delivery import _landing
from fabric_runtime import _kusto_endpoint
from sales_ids import AllocationError, validate_fabric_targets


class TargetReadbackBlocked(RuntimeError):
    pass


def selected_candidates(config, selected):
    if (not isinstance(config, dict) or not isinstance(selected, list) or not selected
            or any(name not in ("demo-1", "demo-2", "demo-3") for name in selected)
            or len(set(selected)) != len(selected)):
        raise TargetReadbackBlocked("Selected demos must be distinct known targets.")
    shared = config.get("shared")
    if not isinstance(shared, dict):
        raise TargetReadbackBlocked("Selected targets require shared.target_bindings.")
    try:
        targets = validate_fabric_targets(shared.get("target_bindings"))
    except AllocationError:
        raise TargetReadbackBlocked("Selected target candidates are missing or invalid.") from None
    if set(targets) != set(selected):
        raise TargetReadbackBlocked("Target candidates must match exactly the selected demos.")
    return targets


def _item_matches(item, workspace_id, item_id, item_type):
    return (isinstance(item, dict) and item.get("id") == item_id
            and item.get("workspaceId") == workspace_id and item.get("type") == item_type
            and isinstance(item.get("displayName"), str) and bool(item["displayName"].strip()))


def _configured_landing(config, name, candidate):
    settings = config.get(name)
    if not isinstance(settings, dict):
        raise TargetReadbackBlocked(f"{name} producer adapter configuration is missing.")
    try:
        workspace, lakehouse, _ = _landing(settings.get("landing_uri"))
    except ValueError:
        raise TargetReadbackBlocked(f"{name} requires a valid OneLake landing_uri.") from None
    if name == "demo-3" and (workspace, lakehouse) != (candidate["workspace_id"], candidate["item_id"]):
        raise TargetReadbackBlocked(f"{name} landing identity differs from its fact candidate.")
    return workspace, lakehouse


def _read_item(client, workspace_id, item_id, item_type, collection, deadline, name):
    _, workspace = client.get(f"/v1/workspaces/{workspace_id}", deadline)
    if not isinstance(workspace, dict) or workspace.get("id") != workspace_id or workspace.get("type") != "Workspace":
        raise TargetReadbackBlocked(f"{name} workspace identity did not match the live readback.")
    _, item = client.get(f"/v1/workspaces/{workspace_id}/items/{item_id}", deadline)
    _, typed = client.get(f"/v1/workspaces/{workspace_id}/{collection}/{item_id}", deadline)
    if (not _item_matches(item, workspace_id, item_id, item_type)
            or not _item_matches(typed, workspace_id, item_id, item_type)
            or typed["displayName"] != item["displayName"]):
        raise TargetReadbackBlocked(f"{name} item identity, type or workspace did not match the live readbacks.")
    return typed


def _database_adapter(config, candidate, item):
    settings = config.get("demo-2")
    if not isinstance(settings, dict):
        raise TargetReadbackBlocked("demo-2 producer adapter configuration is missing.")
    properties = item.get("properties")
    if not isinstance(properties, dict) or properties.get("databaseType") != "ReadWrite":
        raise TargetReadbackBlocked("demo-2 database is not an observed read-write KQL database.")
    try:
        _kusto_endpoint(properties.get("queryServiceUri"))
        _kusto_endpoint(properties.get("ingestionServiceUri"))
        _kusto_endpoint(settings.get("query_endpoint"))
        _kusto_endpoint(settings.get("ingestion_endpoint"))
    except ValueError:
        raise TargetReadbackBlocked("demo-2 requires valid Fabric Kusto query and ingestion endpoints.") from None
    if (settings["query_endpoint"] != properties["queryServiceUri"]
            or settings["ingestion_endpoint"] != properties["ingestionServiceUri"]
            or settings.get("kql_database_name") != item["displayName"]
            or settings.get("sales_table") != candidate["fact"]
            or settings.get("csv_mapping") != "SalesCsvV1"):
        raise TargetReadbackBlocked("demo-2 adapter settings differ from the live database or target candidate.")
    return properties


def read_selected_targets(config, selected, clients, *, timeout=60):
    """Verify CLI-principal metadata and adapter identity only, not readiness."""
    targets = selected_candidates(config, selected)
    if (not isinstance(clients, dict) or set(clients) != set(selected)
            or not isinstance(timeout, (int, float)) or isinstance(timeout, bool)
            or not math.isfinite(timeout) or timeout <= 0):
        raise TargetReadbackBlocked("Invalid target readback request.")
    # All clients share the same monotonic clock source in production.
    clock = clients[selected[0]]._clock
    deadline = clock() + timeout
    observations = []
    for name in sorted(selected):
        client = clients[name]
        candidate = targets[name]
        workspace_id, item_id = candidate["workspace_id"], candidate["item_id"]
        item_type = "KQLDatabase" if name == "demo-2" else "Lakehouse"
        collection = "kqlDatabases" if name == "demo-2" else "lakehouses"
        typed = _read_item(client, workspace_id, item_id, item_type, collection, deadline, name)
        if name == "demo-2":
            props = _database_adapter(config, candidate, typed)
            parent_id = props.get("parentEventhouseItemId")
            try:
                # Reuse the canonical UUID validation used for allocator candidates.
                validate_fabric_targets({name: {"workspace_id": workspace_id, "item_id": parent_id,
                                               "fact": candidate["fact"]}})
            except AllocationError:
                raise TargetReadbackBlocked("demo-2 Eventhouse parent identity is invalid.") from None
            _, parent = client.get(f"/v1/workspaces/{workspace_id}/items/{parent_id}", deadline)
            if not _item_matches(parent, workspace_id, parent_id, "Eventhouse"):
                raise TargetReadbackBlocked("demo-2 Eventhouse parent did not match the live readback.")
        else:
            landing_workspace, landing_lakehouse = _configured_landing(config, name, candidate)
            if (landing_workspace, landing_lakehouse) != (workspace_id, item_id):
                _read_item(client, landing_workspace, landing_lakehouse, "Lakehouse",
                           "lakehouses", deadline, name + " landing")
        observations.append(f"{name}: CLI-principal workspace and {item_type} item identities and "
                            "configured adapter item metadata matched live readbacks; fact identity, "
                            "landing-to-fact linkage, ownership, runtime permissions and allocator safety "
                            "remain unverified.")
    return observations
