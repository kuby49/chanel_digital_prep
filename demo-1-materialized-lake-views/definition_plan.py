"""Render documented Demo 1 MLV SQL and a bounded managed-refresh request."""

from datetime import datetime, timedelta, timezone
import re
from pathlib import Path


SALES_COLUMNS = (
    "SalesLineID",
    "BatchID",
    "SaleTimestamp",
    "CustomerID",
    "ProductID",
    "Date",
    "Quantity",
    "UnitPriceMinorUnits",
    "SalesAmountMinorUnits",
)

DEFAULT_CSV_OPTIONS = {
    "header": "true",
    "delimiter": ",",
    "quote": '"',
    "escape": "\\",
    "nullValue": "\\N",
    "multiLine": "false",
    "encoding": "UTF-8",
    "inferSchema": "false",
    "columnNameOfCorruptRecord": "_corrupt_record",
    "dateFormat": "yyyy-MM-dd",
    "ignoreLeadingWhiteSpace": "false",
    "ignoreTrailingWhiteSpace": "false",
}
SUPPORTED_CSV_OPTIONS = set(DEFAULT_CSV_OPTIONS) | {"sep", "comment"}
REQUIRED_CSV_OPTIONS = {
    "header": "true",
    "delimiter": ",",
    "encoding": "UTF-8",
    "inferSchema": "false",
    "columnNameOfCorruptRecord": "_corrupt_record",
}
DEFAULT_VERIFICATION_TIMEOUT_SECONDS = 900


def effective_verification_timeout_seconds(config):
    """Resolve the Demo 1 per-batch timeout, defaulting to the shared 900 seconds."""
    if not isinstance(config, dict):
        raise ValueError("config must be an object")

    execution = config.get("execution", {})
    if not isinstance(execution, dict):
        raise ValueError("execution must be an object")
    default_timeout = execution.get(
        "verification_timeout_seconds", DEFAULT_VERIFICATION_TIMEOUT_SECONDS
    )
    if (
        not isinstance(default_timeout, int)
        or isinstance(default_timeout, bool)
        or default_timeout <= 0
    ):
        raise ValueError("execution.verification_timeout_seconds must be a positive integer")

    demo = config.get("demo-1", {})
    if not isinstance(demo, dict):
        raise ValueError("demo-1 must be an object")
    if "verification_timeout_seconds" not in demo:
        return default_timeout

    override = demo["verification_timeout_seconds"]
    if not isinstance(override, int) or isinstance(override, bool) or override <= 0:
        raise ValueError("demo-1.verification_timeout_seconds must be a positive integer")
    return override


def _identifier(value):
    if not isinstance(value, str) or not value:
        raise ValueError("SQL identifier components must be nonempty strings")
    return f"`{value.replace('`', '``')}`"


def _literal(value):
    return "'" + str(value).replace("\\", "\\\\").replace("'", "''") + "'"


def _qualified(workspace, lakehouse, schema, name):
    return ".".join(_identifier(value) for value in (workspace, lakehouse, schema, name))


def _resolved_workspace_name(value, prefix):
    if not isinstance(prefix, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,62}", prefix):
        raise ValueError("prefix must be a nonempty safe name")
    if not isinstance(value, str) or not value:
        raise ValueError("demo-1 workspace_name must be a nonempty template")
    resolved = value.replace("{prefix}", prefix)
    if "{prefix}" in resolved:
        raise ValueError("workspace_name contains an unresolved template")
    return resolved


def _as_option_string(value):
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, str):
        return value
    raise ValueError("CSV option values must be strings or booleans")


def render_bronze_sql(config, prefix):
    """Render the documented file-backed MLV syntax; does not execute it."""
    if not isinstance(config, dict) or config.get("contract_version") != 1:
        raise ValueError("config contract_version must be 1")
    demo = config.get("demo-1")
    if not isinstance(demo, dict):
        raise ValueError("config must contain a demo-1 object")
    workspace = _resolved_workspace_name(demo.get("workspace_name"), prefix)
    lakehouse = demo.get("lakehouse_name")
    schema = demo.get("bronze_schema")
    view = demo.get("bronze_view")
    path = demo.get("landing_uri")
    if not all(isinstance(value, str) and value for value in (lakehouse, schema, view)):
        raise ValueError("lakehouse_name, bronze_schema, and bronze_view must be nonempty")
    if not isinstance(path, str) or not path.startswith("abfss://") or any(c.isspace() for c in path):
        raise ValueError("landing_uri must be a nonempty whitespace-free abfss URI")

    options = dict(DEFAULT_CSV_OPTIONS)
    custom_options = demo.get("csv_options", {})
    if not isinstance(custom_options, dict):
        raise ValueError("demo-1 csv_options must be an object")
    if set(custom_options) - SUPPORTED_CSV_OPTIONS:
        raise ValueError("csv_options contains an unsupported or reserved option")
    options.update({name: _as_option_string(value) for name, value in custom_options.items()})
    if any(options.get(name) != value for name, value in REQUIRED_CSV_OPTIONS.items()):
        raise ValueError("CSV v1 requires a header, comma delimiter, UTF-8, no inference, and corrupt-row capture")

    rendered_options = [
        ("format", "csv"),
        ("path", path),
        *options.items(),
    ]
    option_sql = ",\n".join(f"    {_literal(name)} = {_literal(value)}" for name, value in rendered_options)
    projection = ",\n    ".join(
        _identifier(column) for column in (*SALES_COLUMNS, "_corrupt_record", "__filepath__")
    )
    return (
        f"CREATE MATERIALIZED LAKE VIEW IF NOT EXISTS "
        f"{_qualified(workspace, lakehouse, schema, view)}\n"
        "USING OneLake_Files\n"
        "OPTIONS (\n"
        f"{option_sql}\n"
        ")\n"
        "TBLPROPERTIES (\n"
        "    'schema_mode' = 'FIXED',\n"
        "    'refresh_mode' = 'APPEND_ONLY'\n"
        ")\n"
        "AS SELECT\n"
        f"    {projection};"
    )


def render_silver_sql(config, prefix):
    """Fill the focused silver SQL template with fully qualified names."""
    if not isinstance(config, dict) or config.get("contract_version") != 1:
        raise ValueError("config contract_version must be 1")
    demo = config.get("demo-1")
    if not isinstance(demo, dict):
        raise ValueError("config must contain a demo-1 object")
    workspace = _resolved_workspace_name(demo.get("workspace_name"), prefix)
    lakehouse = demo.get("lakehouse_name")
    bronze_schema = demo.get("bronze_schema")
    silver_schema = demo.get("silver_schema")
    bronze_view = demo.get("bronze_view")
    silver_view = demo.get("silver_view")
    if not all(isinstance(value, str) and value for value in (
        lakehouse, bronze_schema, silver_schema, bronze_view, silver_view
    )):
        raise ValueError("Demo 1 lakehouse, schema, and view names must be nonempty")

    names = {
        "bronze": _qualified(workspace, lakehouse, bronze_schema, bronze_view),
        "silver": _qualified(workspace, lakehouse, silver_schema, silver_view),
        "dim_customer": _qualified(workspace, lakehouse, silver_schema, "DimCustomer"),
        "dim_product": _qualified(workspace, lakehouse, silver_schema, "DimProduct"),
        "dim_date": _qualified(workspace, lakehouse, silver_schema, "DimDate"),
    }
    template = (Path(__file__).with_name("silver-candidate.sql")).read_text(encoding="utf-8")
    for key, value in names.items():
        template = template.replace("{{" + key + "}}", value)
    if "{{" in template or "}}" in template:
        raise ValueError("silver SQL template contains an unresolved identifier")
    return template


def _parse_utc(value, name):
    if not isinstance(value, str):
        raise ValueError(f"{name} must be an ISO-8601 timestamp")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise ValueError(f"{name} must be an ISO-8601 timestamp") from error
    if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0):
        raise ValueError(f"{name} must be UTC and include a timezone")
    return parsed.astimezone(timezone.utc)


def build_refresh_schedule_request(
    start_utc,
    end_utc,
    *,
    file_count,
    interval_seconds,
    processing_margin_minutes,
    refresh_interval_minutes=5,
):
    """Build the public API payload and reject windows too short for final data."""
    start = _parse_utc(start_utc, "start_utc")
    end = _parse_utc(end_utc, "end_utc")
    if not isinstance(file_count, int) or isinstance(file_count, bool) or file_count < 1:
        raise ValueError("file_count must be a positive integer")
    if not isinstance(interval_seconds, int) or isinstance(interval_seconds, bool) or interval_seconds < 0:
        raise ValueError("interval_seconds must be a nonnegative integer")
    if not isinstance(processing_margin_minutes, int) or isinstance(processing_margin_minutes, bool) or processing_margin_minutes < 0:
        raise ValueError("processing_margin_minutes must be a nonnegative integer")
    if not isinstance(refresh_interval_minutes, int) or isinstance(refresh_interval_minutes, bool) or refresh_interval_minutes < 5:
        raise ValueError("refresh_interval_minutes must be at least five")
    if end <= start:
        raise ValueError("end_utc must be later than start_utc")

    last_delivery = start + timedelta(seconds=(file_count - 1) * interval_seconds)
    earliest_end = last_delivery + timedelta(
        minutes=refresh_interval_minutes + processing_margin_minutes
    )
    if end < earliest_end:
        raise ValueError(
            "end_utc must be at least last delivery + one refresh interval + processing margin "
            f"({earliest_end.isoformat()})"
        )
    return {
        "enabled": True,
        "configuration": {
            "startDateTime": start.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "endDateTime": end.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "localTimeZoneId": "UTC",
            "type": "Cron",
            "interval": refresh_interval_minutes,
        },
    }
