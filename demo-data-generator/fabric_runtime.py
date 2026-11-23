"""Build destination adapters for the Fabric-hosted producer."""

import re
from urllib.parse import urlsplit

from fabric_delivery import OneLakeFileAdapter


def _kusto_endpoint(value):
    if not isinstance(value, str):
        raise ValueError("A Fabric Kusto endpoint is required")
    try:
        parsed = urlsplit(value)
        if (parsed.scheme != "https" or parsed.username is not None or parsed.password is not None
                or parsed.port is not None or parsed.path not in ("", "/") or parsed.query or parsed.fragment
                or not re.fullmatch(r"(?:[a-z0-9-]+\.)+kusto\.fabric\.microsoft\.com", parsed.hostname or "")):
            raise ValueError
    except ValueError:
        raise ValueError("Kusto endpoint must be a Fabric Kusto HTTPS origin") from None
    return value.rstrip("/")


def build_adapters(config, selected, notebookutils):
    if (not isinstance(config, dict) or not isinstance(selected, list) or not selected
            or len(selected) != len(set(selected))
            or any(name not in ("demo-1", "demo-2", "demo-3") for name in selected)):
        raise ValueError("Select distinct demo destinations")
    adapters = {}
    for name in selected:
        settings = config.get(name)
        if not isinstance(settings, dict):
            raise ValueError(f"{name} settings are required")
        if name != "demo-2":
            adapters[name] = OneLakeFileAdapter(
                settings.get("landing_uri"), lambda: notebookutils.credentials.getToken("storage"))
            continue
        ingestion_endpoint = _kusto_endpoint(settings.get("ingestion_endpoint"))
        query_endpoint = _kusto_endpoint(settings.get("query_endpoint"))
        try:
            from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
            from azure.kusto.ingest import QueuedIngestClient
            from ingestion import EventhouseAdapter
        except ImportError:
            raise RuntimeError("The published Kusto SDK and Eventhouse adapter are required") from None
        token = lambda: notebookutils.credentials.getToken("kusto")
        ingest_client = QueuedIngestClient(KustoConnectionStringBuilder.with_token_provider(ingestion_endpoint, token))
        query_client = KustoClient(KustoConnectionStringBuilder.with_token_provider(query_endpoint, token))
        adapters[name] = EventhouseAdapter(
            database=settings.get("kql_database_name"), table=settings.get("sales_table"),
            mapping=settings.get("csv_mapping"), ingest_client=ingest_client, query_client=query_client)
    return adapters
