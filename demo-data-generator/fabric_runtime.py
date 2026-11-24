"""Runtime checks and destination adapters for the Fabric-hosted producer."""

import hashlib
import re
import time
import uuid
from urllib import error, parse, request

from fabric_delivery import OneLakeFileAdapter, _landing
from generator import RunConfig, canonical_bytes, strict_json_loads


STARTUP_HEADROOM_SECONDS = 900
MAX_ACTIVITY_SECONDS = 7 * 24 * 60 * 60


def _kusto_endpoint(value):
    if not isinstance(value, str):
        raise ValueError("A Fabric Kusto endpoint is required")
    try:
        parsed = parse.urlsplit(value)
        if (parsed.scheme != "https" or parsed.username is not None or parsed.password is not None
                or parsed.port is not None or parsed.path not in ("", "/") or parsed.query or parsed.fragment
                or not re.fullmatch(r"(?:[a-z0-9-]+\.)+kusto\.fabric\.microsoft\.com", parsed.hostname or "")):
            raise ValueError
    except ValueError:
        raise ValueError("Kusto endpoint must be a Fabric Kusto HTTPS origin") from None
    return value.rstrip("/")


class _NoRedirect(request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


class RuntimeFabricClient:
    """Read Fabric metadata with the notebook's own token."""

    def __init__(self, token_provider, *, skill="spark-cli", opener=None, clock=time.monotonic):
        if not callable(token_provider) or skill not in ("spark-cli", "eventhouse-cli"):
            raise ValueError("Invalid runtime Fabric client")
        self.token_provider = token_provider
        self.skill = skill
        self.opener = opener or request.build_opener(_NoRedirect())
        self._clock = clock

    def get(self, path, deadline=None):
        if (not isinstance(path, str) or not path.startswith("/v1/workspaces/")
                or "\\" in path or "#" in path or any(ord(char) < 32 for char in path)):
            raise ValueError("Invalid runtime Fabric metadata path")
        deadline = self._clock() + 60 if deadline is None else deadline
        remaining = deadline - self._clock()
        if remaining <= 0:
            raise RuntimeError("Runtime Fabric metadata read exceeded its deadline")
        token = self.token_provider()
        if not isinstance(token, str) or not token or any(ord(char) < 33 or ord(char) > 126 for char in token):
            raise RuntimeError("Runtime Fabric token is invalid")
        req = request.Request("https://api.fabric.microsoft.com" + path,
                              headers={"Accept": "application/json", "Authorization": "Bearer " + token,
                                       "x-ms-fabric-skill": self.skill}, method="GET")
        try:
            with self.opener.open(req, timeout=min(30, remaining)) as response:
                status, headers = response.status, response.headers
                raw = response.read(8 * 1024 * 1024 + 1)
        except error.HTTPError as exc:
            status = exc.code
            exc.close()
            raise RuntimeError(f"Runtime Fabric metadata read failed (HTTP {status})") from None
        except (error.URLError, OSError, TimeoutError):
            raise RuntimeError("Runtime Fabric metadata read failed or timed out") from None
        if status != 200 or len(raw) > 8 * 1024 * 1024:
            raise RuntimeError("Runtime Fabric metadata response was unsuccessful or too large")
        try:
            return headers, strict_json_loads(raw.decode("utf-8"))
        except (UnicodeError, ValueError, RecursionError):
            raise RuntimeError("Runtime Fabric metadata response is invalid JSON") from None

    def list_all(self, path):
        deadline = self._clock() + 120
        values, seen = [], set()
        next_path = path
        for _ in range(100):
            _, page = self.get(next_path, deadline)
            if not isinstance(page, dict) or not isinstance(page.get("value"), list):
                raise RuntimeError("Runtime workspace role listing is invalid")
            values.extend(page["value"])
            if len(values) > 10000:
                raise RuntimeError("Runtime workspace role listing is too large")
            token = page.get("continuationToken")
            uri = page.get("continuationUri")
            token = None if token == "null" else token
            uri = None if uri == "null" else uri
            if not token:
                if uri:
                    raise RuntimeError("Runtime workspace role continuation is incomplete")
                return values
            if not isinstance(token, str) or token in seen:
                raise RuntimeError("Runtime workspace role continuation is invalid")
            seen.add(token)
            next_path = path + "?" + parse.urlencode({"continuationToken": token})
        raise RuntimeError("Runtime workspace role listing exceeded its page limit")


def _uuid(value):
    try:
        parsed = uuid.UUID(value)
    except (ValueError, TypeError, AttributeError):
        raise ValueError("Runtime configuration requires nonzero UUID identities") from None
    if not parsed.int:
        raise ValueError("Runtime configuration requires nonzero UUID identities")
    return str(parsed)


def _tenant(document):
    common = document.get("common", {})
    if not isinstance(common, dict):
        raise ValueError("Runtime common configuration is invalid")
    values = []
    for source in (document, common):
        for field in ("tenant_id", "tenant"):
            value = source.get(field)
            if isinstance(value, dict):
                value = value.get("id")
            if value is not None:
                values.append(_uuid(value))
    if not values or len(set(values)) != 1:
        raise ValueError("Runtime tenant configuration is missing or conflicting")
    return values[0]


def run_config(document):
    data = document.get("data") if isinstance(document, dict) else None
    fields = ("run_id", "seed", "start_date", "date_count", "product_count", "customer_count",
              "rows_per_file", "file_count", "interval_seconds", "max_duration_seconds", "currency")
    if (not isinstance(data, dict) or any(data.get(field) is None for field in fields)
            or data.get("batch_class", "measured") != "measured" or data["currency"] != "USD"):
        raise ValueError("A complete measured USD producer configuration is required")
    run = RunConfig(run_id=data["run_id"], seed=data["seed"], start_date=data["start_date"],
                    date_count=data["date_count"], products=data["product_count"],
                    customers=data["customer_count"], rows_per_file=data["rows_per_file"],
                    file_count=data["file_count"], interval_seconds=data["interval_seconds"])
    try:
        run.validate()
    except (ValueError, TypeError, OverflowError):
        raise ValueError("Invalid measured producer parameters") from None
    duration = data["max_duration_seconds"]
    if (type(duration) is not int or duration <= (run.file_count - 1) * run.interval_seconds
            or duration + STARTUP_HEADROOM_SECONDS > MAX_ACTIVITY_SECONDS):
        raise ValueError("Producer duration must exceed the final batch offset and fit the pipeline timeout")
    return run, duration


def require_write_roles(client, document, selected, targets, producer_workspace, identity_id):
    workspaces = {producer_workspace}
    for name in selected:
        if name == "demo-2":
            workspaces.add(targets[name]["workspace_id"])
        else:
            workspaces.add(_landing(document[name]["landing_uri"])[0])
    for workspace in sorted(workspaces):
        roles = client.list_all(f"/v1/workspaces/{workspace}/roleAssignments")
        if not any(isinstance(role, dict) and role.get("role") in ("Admin", "Member", "Contributor")
                   and isinstance(role.get("principal"), dict)
                   and role["principal"].get("id") == identity_id
                   and role["principal"].get("type") in ("User", "ServicePrincipal") for role in roles):
            raise RuntimeError(f"Runtime identity lacks a directly assigned write role in workspace {workspace}")


def prepare_measured(document, selected, probe_id, binding, context, identities, clients, table_client=None):
    """Check actual notebook identity, selected targets and write roles before staging."""
    from target_readback import read_selected_targets, selected_candidates

    if (not isinstance(document, dict) or type(document.get("contract_version")) is not int
            or document["contract_version"] != 1 or not isinstance(selected, list) or not selected
            or any(not isinstance(name, str) or name not in ("demo-1", "demo-2", "demo-3") for name in selected)
            or len(selected) != len(set(selected))):
        raise ValueError("Select distinct measured destinations under contract v1")
    run, duration = run_config(document)
    fields = {"nonce", "run_id", "config_sha256", "tenant_id", "identity_id", "producer"}
    if (not isinstance(binding, dict) or set(binding) != fields
            or not isinstance(probe_id, str) or not re.fullmatch(r"[a-z0-9][a-z0-9-]{15,63}", probe_id)
            or binding["nonce"] != probe_id or binding["run_id"] != run.run_id
            or binding["config_sha256"] != hashlib.sha256(canonical_bytes(document)).hexdigest()):
        raise ValueError("Measured notebook parameters differ from the run binding")
    producer = binding["producer"]
    if not isinstance(producer, dict) or set(producer) != {"workspace_id", "staging_lakehouse_id", "notebook_id"}:
        raise ValueError("Measured producer binding is invalid")
    producer = {key: _uuid(value) for key, value in producer.items()}
    tenant, identity = _uuid(binding["tenant_id"]), _uuid(binding["identity_id"])
    shared = document.get("shared")
    if (not isinstance(shared, dict) or _tenant(document) != tenant or _uuid(shared.get("identity_id")) != identity
            or _uuid(shared.get("workspace_id")) != producer["workspace_id"]
            or _uuid(shared.get("staging_lakehouse_id")) != producer["staging_lakehouse_id"]
            or (shared.get("producer_notebook_id") is not None
                and _uuid(shared["producer_notebook_id"]) != producer["notebook_id"])):
        raise ValueError("Measured producer identities differ from the effective configuration")
    if (not isinstance(context, dict) or context.get("isForPipeline") is not True
            or context.get("currentWorkspaceId") != producer["workspace_id"]
            or context.get("defaultLakehouseId") != producer["staging_lakehouse_id"]
            or context.get("currentNotebookId") != producer["notebook_id"]):
        raise ValueError("Measured run is not executing in the bound producer notebook")
    audiences = {"pbi", "storage"} | ({"kusto"} if "demo-2" in selected else set())
    if (not isinstance(identities, dict) or set(identities) != audiences
            or any(value != (tenant, identity) for value in identities.values())):
        raise ValueError("Runtime token identities do not match the configured tenant and principal")
    targets = selected_candidates(document, selected)
    if not isinstance(clients, dict) or set(clients) != set(selected):
        raise ValueError("Runtime metadata clients must match the selected destinations")
    read_selected_targets(document, selected, clients, table_client=table_client)
    require_write_roles(clients[selected[0]], document, selected, targets, producer["workspace_id"], identity)
    return run, duration, targets, producer


def stop_requested(store, run_id, nonce, config_sha256):
    expected = {"version": 1, "run_id": run_id, "nonce": nonce, "config_sha256": config_sha256}
    try:
        raw, _ = store.read(f"Files/producer/runs/{run_id}/stop.json")
    except FileNotFoundError:
        return False
    try:
        marker = strict_json_loads(raw.decode("utf-8"))
    except (UnicodeError, ValueError, RecursionError):
        raise RuntimeError("Stop request is invalid; manual reconciliation required") from None
    if not isinstance(marker, dict) or type(marker.get("version")) is not int or marker != expected:
        raise RuntimeError("Stop request differs from the active run; manual reconciliation required")
    return True


def build_adapters(config, selected, notebookutils, *, token_provider=None):
    if (not isinstance(config, dict) or not isinstance(selected, list) or not selected
            or len(selected) != len(set(selected))
            or any(name not in ("demo-1", "demo-2", "demo-3") for name in selected)):
        raise ValueError("Select distinct demo destinations")
    token_provider = token_provider or notebookutils.credentials.getToken
    if not callable(token_provider):
        raise ValueError("A runtime token provider is required")
    adapters = {}
    for name in selected:
        settings = config.get(name)
        if not isinstance(settings, dict):
            raise ValueError(f"{name} settings are required")
        if name != "demo-2":
            adapters[name] = OneLakeFileAdapter(
                settings.get("landing_uri"), lambda: token_provider("storage"))
            continue
        ingestion_endpoint = _kusto_endpoint(settings.get("ingestion_endpoint"))
        query_endpoint = _kusto_endpoint(settings.get("query_endpoint"))
        try:
            from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
            from azure.kusto.ingest import QueuedIngestClient
            from ingestion import EventhouseAdapter
        except ImportError:
            raise RuntimeError("The published Kusto SDK and Eventhouse adapter are required") from None
        token = lambda: token_provider("kusto")
        ingest_client = QueuedIngestClient(KustoConnectionStringBuilder.with_token_provider(ingestion_endpoint, token))
        query_client = KustoClient(KustoConnectionStringBuilder.with_token_provider(query_endpoint, token))
        adapters[name] = EventhouseAdapter(
            database=settings.get("kql_database_name"), table=settings.get("sales_table"),
            mapping=settings.get("csv_mapping"), ingest_client=ingest_client, query_client=query_client)
    return adapters
