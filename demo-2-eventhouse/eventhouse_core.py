"""Versioned schema and OneLake helpers for the Eventhouse demonstration."""

import json
import re
import uuid
from dataclasses import dataclass
from urllib.parse import quote, unquote, urlsplit


CSV_COLUMNS = (
    ("SalesLineID", "long"),
    ("BatchID", "string"),
    ("SaleTimestamp", "datetime"),
    ("CustomerID", "int"),
    ("ProductID", "int"),
    ("Date", "string"),
    ("Quantity", "int"),
    ("UnitPriceMinorUnits", "long"),
    ("SalesAmountMinorUnits", "long"),
)
CSV_HEADER = tuple(name for name, _ in CSV_COLUMNS)
IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]{0,127}\Z")
BATCH_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}\Z")
ONELAKE_STORAGE_HOST = re.compile(
    r"(?:(?P<region>[a-z0-9-]+)-)?onelake\.(?:dfs|blob)\.fabric\.microsoft\.com\Z"
)
ONELAKE_API_HOST = re.compile(
    r"(?:(?P<region>[a-z0-9-]+)-)?api\.onelake\.fabric\.microsoft\.com\Z"
)


class ConfigurationError(ValueError):
    pass


def identifier(value, label="identifier"):
    if not isinstance(value, str) or not IDENTIFIER.fullmatch(value):
        raise ConfigurationError(f"Invalid {label}")
    return value


def batch_identifier(value):
    if not isinstance(value, str) or not BATCH_ID.fullmatch(value):
        raise ConfigurationError("Invalid batch ID")
    return value


def schema_commands(table, mapping):
    table = identifier(table, "sales table")
    mapping = identifier(mapping, "CSV mapping")
    columns = ", ".join(f"{name}: {kind}" for name, kind in CSV_COLUMNS)
    definitions = [
        {"column": name, "DataType": kind, "Properties": {"Ordinal": str(index)}}
        for index, (name, kind) in enumerate(CSV_COLUMNS)
    ]
    mapping_json = json.dumps(definitions, separators=(",", ":"))
    return (
        f".create-merge table {table} ({columns})",
        f".create-or-alter table {table} ingestion csv mapping "
        f"'{mapping}' '{mapping_json}'",
        f".alter-merge table {table} policy mirroring dataformat=parquet "
        "with (IsEnabled=true, TargetLatencyInMinutes=5)",
    )


def batch_verification_query(table, batch_id):
    table = identifier(table, "sales table")
    batch_id = batch_identifier(batch_id)
    return f"""\
{table}
| where BatchID == '{batch_id}'
| summarize Rows=count(),
    Amount=sum(todecimal(SalesAmountMinorUnits)),
    Invalid=countif(isnull(SalesLineID) or isnull(SaleTimestamp)
        or isnull(CustomerID) or isnull(ProductID) or isempty(Date)
        or isnull(Quantity) or Quantity <= 0
        or isnull(UnitPriceMinorUnits) or UnitPriceMinorUnits < 0
        or isnull(SalesAmountMinorUnits) or SalesAmountMinorUnits < 0
        or todecimal(SalesAmountMinorUnits) !=
            todecimal(Quantity) * todecimal(UnitPriceMinorUnits))
    by SalesLineID
| summarize RowCount=sum(Rows), DistinctSalesLineIDs=count(),
    AmountMinorUnits=sum(Amount), InvalidRows=sum(Invalid)
"""


@dataclass(frozen=True)
class MirrorLocation:
    workspace: str
    item: str
    relative_path: str
    policy_path: str
    abfss_path: str


def _path_parts(path):
    parts = [unquote(part) for part in path.split("/") if part]
    if any(part in (".", "..") or "/" in part or "\\" in part for part in parts):
        raise ConfigurationError("Mirroring policy returned an unsafe path")
    return parts


def _parse_location(connection_string, table):
    if not isinstance(connection_string, str):
        raise ConfigurationError("Mirroring policy contains an invalid location")
    # Connection strings can contain credentials. Never return or log them.
    address = connection_string.split(";", 1)[0]
    try:
        parsed = urlsplit(address)
        host = (parsed.hostname or "").lower()
        port = parsed.port
    except ValueError as exc:
        raise ConfigurationError("Mirroring policy returned an invalid location") from exc
    host_match = ONELAKE_STORAGE_HOST.fullmatch(host) or ONELAKE_API_HOST.fullmatch(host)
    if not host_match or port is not None:
        raise ConfigurationError("Mirroring policy did not return a OneLake host")
    parts = _path_parts(parsed.path)
    if parsed.scheme.lower() == "https":
        if parsed.username is not None or len(parts) < 4:
            raise ConfigurationError("Unexpected OneLake HTTPS table location")
        workspace, item, *relative = parts
    elif parsed.scheme.lower() == "abfss":
        if not parsed.username or parsed.password is not None or len(parts) < 3:
            raise ConfigurationError("Unexpected OneLake ABFSS table location")
        workspace = unquote(parsed.username)
        item, *relative = parts
    else:
        raise ConfigurationError("Unsupported OneLake location scheme")
    if (
        not workspace
        or workspace in (".", "..")
        or "/" in workspace
        or "\\" in workspace
        or relative[0] != "Tables"
        or relative[-1] != table
    ):
        raise ConfigurationError("Mirroring policy location does not match the table")
    relative_path = "/".join(relative)
    safe_workspace = quote(workspace, safe="-._")
    safe_path = "/".join(quote(part, safe="-._") for part in (item, *relative))
    region = host_match.group("region")
    dfs_host = f"{region + '-' if region else ''}onelake.dfs.fabric.microsoft.com"
    if parsed.scheme.lower() == "https":
        policy_path = f"https://{host}/{safe_workspace}/{safe_path}"
    else:
        policy_path = f"abfss://{safe_workspace}@{host}/{safe_path}"
    return MirrorLocation(
        workspace=workspace,
        item=item,
        relative_path=relative_path,
        policy_path=policy_path,
        abfss_path=f"abfss://{safe_workspace}@{dfs_host}/{safe_path}",
    )


def discover_mirror_location(policy, table):
    """Use only the location returned in the table's applied mirroring policy."""
    identifier(table, "sales table")
    if not isinstance(policy, dict):
        raise ConfigurationError("Mirroring policy readback is missing")
    if policy.get("IsEnabled") is not True:
        raise ConfigurationError("OneLake mirroring is not enabled")
    if policy.get("Format", "").lower() != "parquet":
        raise ConfigurationError("OneLake mirroring format is not parquet")
    if type(policy.get("TargetLatencyInMinutes")) is not int or policy["TargetLatencyInMinutes"] != 5:
        raise ConfigurationError("The applied OneLake target must be five minutes")
    locations = policy.get("ConnectionStrings")
    if not isinstance(locations, list) or not locations:
        raise ConfigurationError("Mirroring policy did not return a table location")
    parsed = [_parse_location(location, table) for location in locations]
    if len(set(parsed)) != 1:
        raise ConfigurationError("Mirroring policy returned ambiguous table locations")
    return parsed[0]


def _as_uuid(value, label):
    try:
        return str(uuid.UUID(value))
    except (ValueError, TypeError, AttributeError) as exc:
        raise ConfigurationError(f"Could not resolve {label} to an ID") from exc


def shortcut_payload(location, shortcut_name, resolve_workspace=None, resolve_item=None):
    """Resolve named paths by listing Fabric items, never by assuming an item ID."""
    identifier(shortcut_name, "shortcut name")
    try:
        workspace_id = _as_uuid(location.workspace, "source workspace")
    except ConfigurationError:
        if resolve_workspace is None:
            raise
        workspace_id = _as_uuid(resolve_workspace(location.workspace), "source workspace")
    try:
        item_id = _as_uuid(location.item, "source item")
    except ConfigurationError:
        if resolve_item is None:
            raise
        item_id = _as_uuid(resolve_item(workspace_id, location.item), "source item")
    return {
        "path": "Tables",
        "name": shortcut_name,
        "target": {
            "oneLake": {
                "workspaceId": workspace_id,
                "itemId": item_id,
                "path": location.relative_path,
            }
        },
    }


def lakehouse_join_query(lakehouse, shortcut, schema=None):
    lakehouse = identifier(lakehouse, "lakehouse")
    shortcut = identifier(shortcut, "shortcut name")
    prefix = f"`{lakehouse}`"
    if schema:
        prefix += f".`{identifier(schema, 'lakehouse schema')}`"

    def table(name):
        return f"{prefix}.`{identifier(name)}`"

    return f"""\
SELECT f.SalesLineID, f.BatchID, f.SaleTimestamp, f.CustomerID, f.ProductID,
       to_date(f.Date, 'yyyy-MM-dd') AS Date, f.Quantity,
       f.UnitPriceMinorUnits, f.SalesAmountMinorUnits,
       CAST(CAST(f.UnitPriceMinorUnits AS DECIMAL(20,0)) /
            CAST(100 AS DECIMAL(3,0)) AS DECIMAL(18,2)) AS UnitPrice,
       CAST(CAST(f.SalesAmountMinorUnits AS DECIMAL(20,0)) /
            CAST(100 AS DECIMAL(3,0)) AS DECIMAL(18,2)) AS SalesAmount,
       c.CustomerName, c.CustomerRegion, p.ProductName, p.ProductCategory,
       d.Year, d.MonthNumber, d.MonthName
FROM {table(shortcut)} f
LEFT JOIN {table("DimCustomer")} c ON f.CustomerID = c.CustomerID
LEFT JOIN {table("DimProduct")} p ON f.ProductID = p.ProductID
LEFT JOIN {table("DimDate")} d ON to_date(f.Date, 'yyyy-MM-dd') = d.Date
"""
