"""Validate persisted Spark artifacts before Demo 3 ingestion."""

import re


def validate_copy_protocol(detail):
    if (not isinstance(detail, dict) or type(detail.get("minReaderVersion")) is not int
            or type(detail.get("minWriterVersion")) is not int
            or detail["minReaderVersion"] != 1 or detail["minWriterVersion"] != 2):
        raise RuntimeError("FactSales Delta protocol is not compatible with the verified Copy connector")


def matches_amount_view(ddl, query, view_reference, table_reference):
    header = re.match(
        r"^\s*CREATE\s+VIEW\s+(?P<workspace>`(?:``|[^`])+`\.)?"
        + re.escape(view_reference)
        + r"(?=\s|\()",
        ddl,
        re.IGNORECASE,
    )
    body = re.search(r"\bAS\s+(SELECT\s+.*)\s*$", ddl, re.IGNORECASE | re.DOTALL)
    if not header or not body:
        return False
    expected_source = f"FROM {table_reference}"
    if query.count(expected_source) != 1:
        return False
    expected = query.replace(
        expected_source, f"FROM {header.group('workspace') or ''}{table_reference}"
    )
    normalize = lambda value: " ".join(value.strip().removesuffix(";").lower().split())
    return normalize(body.group(1)) == normalize(expected)
