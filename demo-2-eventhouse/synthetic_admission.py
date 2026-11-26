"""Read-only admission for the isolated, two-batch Demo 2 fixture.

The proof supplies locators, not decisions. It must contain canonical UUIDs under
producer_workspace_id, staging_lakehouse_id, notebook_id, environment_id,
pipeline_id, preflight_parent_job_id, preflight_child_job_id, auth_parent_job_id,
auth_child_job_id, auth_source_id, auth_service_operation_id; and nonce,
effective_config_sha256, definition_sha256 and auth_csv_sha256.
"""

import base64
import contextlib
import hashlib
import json
import re
import runpy
import uuid
from datetime import datetime, timedelta, timezone
from urllib.request import Request


def _block(message):
    from deploy import Blocked
    raise Blocked(message)


def _require(condition, message):
    if not condition:
        _block(message)


def _uuid(value, label):
    try:
        parsed = uuid.UUID(value)
        if not parsed.int or str(parsed) != value:
            raise ValueError
    except (ValueError, TypeError, AttributeError):
        _block(f"Invalid canonical UUID: {label}.")
    return value


def _digest(value, label):
    _require(isinstance(value, str) and re.fullmatch(r"[a-f0-9]{64}", value) is not None,
             f"Invalid {label} digest.")
    return value


def _utc(value):
    try:
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if result.utcoffset() != timedelta(0):
            raise ValueError
        return result
    except (ValueError, TypeError, AttributeError):
        _block("Evidence timestamp is not valid UTC.")


def _job_utc(value):
    # Fabric's generic job API labels these fields UTC but may omit the offset.
    if isinstance(value, str) and re.fullmatch(
            r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,7})?", value):
        try:
            return datetime.fromisoformat(value).replace(tzinfo=timezone.utc)
        except ValueError:
            _block("Job timestamp is not valid UTC.")
    return _utc(value)


def _json(raw):
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError("duplicate key")
            result[key] = value
        return result

    try:
        return json.loads(raw, object_pairs_hook=pairs, parse_constant=lambda _: (_ for _ in ()).throw(ValueError()))
    except (ValueError, TypeError, UnicodeError):
        _block("Evidence is not unambiguous JSON.")


def _equal(left, right):
    try:
        return json.dumps(left, sort_keys=True, separators=(",", ":"), allow_nan=False) == json.dumps(
            right, sort_keys=True, separators=(",", ":"), allow_nan=False)
    except (TypeError, ValueError):
        return False


def _read_bytes(client, uri):
    """Use the existing authenticated no-redirect opener, not JSON-only onelake()."""
    from deploy import STORAGE_AUDIENCE
    from email.utils import format_datetime

    try:
        request = Request(uri, method="GET", headers={
            "Authorization": "Bearer " + client._token(STORAGE_AUDIENCE),
            "x-ms-version": "2020-10-02",
            "x-ms-date": format_datetime(datetime.now(timezone.utc), usegmt=True),
            "Cache-Control": "no-cache",
        })
        with client.opener.open(request, timeout=client.deadline.remaining()) as response:
            _require(response.status == 200, "OneLake byte read failed.")
            data = response.read(12 * 1024 * 1024 + 1)
        _require(len(data) <= 12 * 1024 * 1024, "OneLake byte response exceeds the limit.")
        return data
    except Exception as exc:
        from deploy import Blocked
        if isinstance(exc, Blocked):
            raise
        _block("Authenticated OneLake byte read is unavailable.")


def _read_job(deployment, proof, which):
    workspace, notebook, pipeline = (proof[k] for k in
                                     ("producer_workspace_id", "notebook_id", "pipeline_id"))
    parent, child = proof[f"{which}_parent_job_id"], proof[f"{which}_child_job_id"]
    base = f"/v1/workspaces/{workspace}"
    status, _, job = deployment.client.fabric("GET", f"{base}/items/{pipeline}/jobs/instances/{parent}")
    _require(status == 200 and isinstance(job, dict) and job.get("id") == parent
             and job.get("itemId") == pipeline and job.get("jobType") == "Pipeline"
             and job.get("status") == "Completed", "Pipeline job readback does not match.")
    start, end = _job_utc(job.get("startTimeUtc")), _job_utc(job.get("endTimeUtc"))
    _require(start <= end <= datetime.now(timezone.utc) + timedelta(seconds=5)
             and end - start <= timedelta(days=1), "Pipeline time window is invalid.")
    payload = {"filters": [], "lastUpdatedAfter": (start - timedelta(minutes=3)).isoformat().replace("+00:00", "Z"),
               "lastUpdatedBefore": (end + timedelta(minutes=3)).isoformat().replace("+00:00", "Z")}
    status, headers, activity = deployment.client.fabric(
        "POST", f"{base}/datapipelines/pipelineruns/{parent}/queryactivityruns", payload)
    _require(not any(k.lower() in ("x-ms-continuation-token", "continuationtoken", "link") and v
                     for k, v in headers.items()), "Pipeline activities are paginated.")
    if isinstance(activity, dict):
        _require(not activity.get("continuationToken") and not activity.get("continuationUri"),
                 "Pipeline activities are paginated.")
        activity = activity.get("value")
    _require(status == 200 and isinstance(activity, list) and len(activity) == 1
             and isinstance(activity[0], dict), "Pipeline activity linkage is ambiguous.")
    activity = activity[0]
    inputs, output = activity.get("input"), activity.get("output")
    _require(isinstance(inputs, dict) and isinstance(output, dict)
             and isinstance(output.get("result"), dict)
             and activity.get("activityName") == "RunProducer"
             and activity.get("activityType") == "TridentNotebook"
             and activity.get("status") == "Succeeded"
             and activity.get("pipelineRunId") == parent
             and activity.get("activityRunId") == child and output["result"].get("runId") == child
             and (activity.get("retryAttempt") is None
                  or type(activity.get("retryAttempt")) is int and activity["retryAttempt"] == 0)
             and inputs.get("workspaceId") == workspace and inputs.get("notebookId") == notebook,
             "Pipeline activity does not link the exact notebook job.")
    status, _, job = deployment.client.fabric("GET", f"{base}/items/{notebook}/jobs/instances/{child}")
    _require(status == 200 and isinstance(job, dict) and job.get("id") == child
             and job.get("itemId") == notebook and job.get("jobType") == "PipelineRunNotebook"
             and job.get("status") == "Completed",
             "Linked notebook job readback does not match.")
    result = _json(output["result"].get("exitValue"))
    _require(isinstance(result, dict), "Notebook exit evidence is unavailable.")
    properties = job.get("properties")
    if properties is not None:
        _require(isinstance(properties, dict), "Notebook job properties are invalid.")
        if properties.get("exitValue") is not None:
            _require(_equal(_json(properties["exitValue"]), result),
                     "Notebook activity and job exit evidence contradict each other.")
    return result, start, end


def _definition(deployment, proof):
    from deploy import _operation_id
    path = (f"/v1/workspaces/{proof['producer_workspace_id']}/items/"
            f"{proof['notebook_id']}/getDefinition?format=ipynb")
    status, headers, body = deployment.client.fabric("POST", path)
    if status == 202:
        body = deployment._poll_operation(_operation_id(headers), headers.get("retry-after"))
    else:
        _require(status == 200, "Notebook definition readback is unavailable.")
    try:
        definition = body.get("definition", body)
        parts = [p for p in definition["parts"] if p["path"] == "notebook-content.ipynb"
                 and p["payloadType"] == "InlineBase64"]
        if len(parts) != 1:
            raise ValueError
        notebook = _json(base64.b64decode(parts[0]["payload"], validate=True).decode("utf-8"))
        if not isinstance(notebook, dict):
            raise ValueError
        digest = hashlib.sha256(json.dumps(notebook, sort_keys=True).encode("utf-8")).hexdigest()
    except (KeyError, TypeError, ValueError, UnicodeError, AttributeError):
        _block("Notebook definition readback is invalid.")
    _require(digest == proof["definition_sha256"], "Live producer notebook definition differs.")


def _preflight(evidence, start, end, proof, settings, config, database):
    producer = evidence.get("producer")
    target = evidence.get("targets")
    _require(type(evidence.get("version")) is int and evidence["version"] == 1
             and evidence.get("kind") == "producer-runtime-preflight"
             and evidence.get("authority") == "runtime-observation-only"
             and evidence.get("status") == "blocked" and evidence.get("nonce") == proof["nonce"]
             and evidence.get("run_id") == config["run_id"]
             and evidence.get("tenant_id") == settings["tenant_id"]
             and evidence.get("identity_id") == settings["producer_identity"]
             and evidence.get("selected_demos") == ["demo-2"]
             and evidence.get("effective_config_sha256") == proof["effective_config_sha256"],
             "Preflight identity or binding mismatch.")
    issued, expires = _utc(evidence.get("issued_at_utc")), _utc(evidence.get("expires_at_utc"))
    now = datetime.now(timezone.utc)
    _require(start < issued <= end + timedelta(seconds=5) and issued <= now
             and now < expires <= issued + timedelta(minutes=5), "Preflight evidence is stale or outside its job.")
    expected_producer = {"workspace_id": proof["producer_workspace_id"],
                         "staging_lakehouse_id": proof["staging_lakehouse_id"],
                         "notebook_id": proof["notebook_id"], "environment_id": proof["environment_id"],
                         "definition_sha256": proof["definition_sha256"], "job_id": None,
                         "observed_run_id": None,
                         "observed_notebook_id": proof["notebook_id"],
                         "runtime_context_checks": {k: True for k in ("pipeline", "workspace", "lakehouse", "notebook")}}
    _require(producer == expected_producer
             and isinstance(producer.get("runtime_context_checks"), dict)
             and all(v is True for v in producer["runtime_context_checks"].values()),
             "Preflight producer context differs.")
    _require(evidence.get("token_identities") == {
        audience: {"identity_id": settings["producer_identity"], "tenant_id": settings["tenant_id"]}
        for audience in ("pbi", "storage", "kusto")}, "Preflight token identities differ.")
    _require(isinstance(target, dict) and set(target) == {"demo-2"}
             and isinstance(target["demo-2"], dict), "Preflight target is unavailable.")
    t = target["demo-2"]
    expected = dict(config["target_bindings"]["demo-2"], database=database["name"],
                    mapping=settings["mapping"], query_service_uri=database["query_uri"],
                    ingestion_service_uri=database["ingestion_uri"])
    _require(_equal({k: v for k, v in t.items() if k != "checks"}, expected),
             "Preflight target or endpoints differ.")
    required = [
        ("runtime-binding", proof["notebook_id"], "notebook-runtime-context", "passed"),
        *(("token-identity", a, "runtime-token-claims", "passed") for a in ("pbi", "storage", "kusto")),
        ("runtime-dependencies", proof["environment_id"], "runtime-import-and-version", "passed"),
        ("query", database["query_uri"], "kusto-read-only-query", "passed"),
        ("table-schema-read", settings["table"], "kusto-read-only-management", "passed"),
        ("mapping-read", settings["table"], "kusto-read-only-management", "passed"),
        ("status-queue", database["ingestion_uri"], "sdk-success-and-failure-nondestructive-peek", "passed"),
        ("conditional-storage", proof["staging_lakehouse_id"], "read-only-synthetic-probe", "not-run"),
        ("effective-ingestion-authorization", database["id"], "authoritative-role-or-approved-ingestion-probe-required", "unverified"),
    ]
    checks = t.get("checks")
    _require(isinstance(checks, list) and len(checks) == len(required)
             and all(isinstance(item, dict) and set(item) == {"capability", "resource", "method", "result"}
                     for item in checks)
             and sorted((c["capability"], c["resource"], c["method"], c["result"]) for c in checks)
             == sorted(required)
             and evidence.get("status_queue_peeks") == {"success": "passed", "failure": "passed"},
             "Preflight capabilities or queue peeks differ.")


def _generator(config):
    from deploy import ROOT
    path = ROOT / "demo-data-generator" / "generator.py"
    try:
        raw = path.read_bytes()
        digest = hashlib.sha256(raw.decode("utf-8").replace("\r\n", "\n").replace("\r", "\n").encode()).hexdigest()
        _require(digest == config["generator_sha256"], "Local producer generator differs from staged fixture.")
        module = runpy.run_path(str(path))
        _require(path.read_bytes() == raw, "Producer generator changed during readback.")
        return module
    except Exception as exc:
        from deploy import Blocked
        if isinstance(exc, Blocked):
            raise
        _block("Bound local producer generator is unavailable.")


def _manifest(deployment, config, proof, generator):
    run = generator["RunConfig"](**config["producer_config"])
    uri = config["manifest_uri"]
    manifest = _json(_read_bytes(deployment.client, uri))
    _require(isinstance(manifest, dict) and type(manifest.get("contract_version")) is int
             and manifest["contract_version"] == 1 and manifest.get("run_id") == config["run_id"]
             and _equal(manifest.get("config"), config["producer_config"])
             and manifest.get("generator_sha256") == config["generator_sha256"]
             and manifest.get("destinations") == ["demo-2"]
             and _equal(manifest.get("target_bindings"), config["target_bindings"])
             and "scheduler" not in manifest and "sales_line_id_reservation" not in manifest
             and isinstance(manifest.get("batches"), dict) and set(manifest["batches"]) == {"1", "2"},
             "Staged synthetic manifest is not the expected untouched fixture.")
    for number in (1, 2):
        payload, expected = generator["batch_bytes"](run, number)
        batch = manifest["batches"][str(number)]
        _require(isinstance(batch, dict) and all(_equal(batch.get(k), v) for k, v in expected.items())
                 and _equal(batch.get("deliveries"), {"demo-2": {"status": "pending", "timestamp_utc": None}}),
                 "Staged batch metadata or delivery is not pending.")
        _utc(batch.get("generated_at_utc"))
        _require(_read_bytes(deployment.client, uri.rsplit("/", 1)[0] + "/" + expected["filename"]) == payload,
                 "Staged batch bytes differ from the generator.")


def _queue(deployment, database, proof):
    try:
        from azure.identity import AzureCliCredential
        from azure.kusto.data import KustoConnectionStringBuilder
        from azure.kusto.ingest import QueuedIngestClient
        from ingestion import EventhouseAdapter
        with contextlib.ExitStack() as cleanup:
            credential = AzureCliCredential(tenant_id=deployment.s["tenant_id"])
            cleanup.callback(credential.close)
            client = QueuedIngestClient(KustoConnectionStringBuilder.with_token_provider(
                database["ingestion_uri"],
                lambda: credential.get_token("https://kusto.kusto.windows.net/.default").token))
            cleanup.callback(client.close)
            adapter = EventhouseAdapter(database=database["name"], table=deployment.s["table"],
                                        mapping=deployment.s["mapping"], ingest_client=client, query_client=None)
            outcome = adapter.ingestion_status(proof["auth_source_id"])
    except Exception:
        _block("Independent non-destructive ingestion status queue read failed.")
    _require(outcome.status == "succeeded" and outcome.source_id == proof["auth_source_id"]
             and outcome.service_operation_id == proof["auth_service_operation_id"],
             "Independent queue outcome does not match the authorization source and operation.")


def _authorization(deployment, config, proof, generator, exit_value, database, start, end):
    from dataclasses import replace
    from eventhouse_core import batch_verification_query
    from ingestion import _exact_integer

    run = generator["RunConfig"](**config["producer_config"])
    auth = replace(run, run_id=run.run_id.replace("demo2synthetic_", "demo2synthetic_auth_", 1),
                   file_count=1, rows_per_file=1, interval_seconds=0)
    payload, expected = generator["batch_bytes"](auth, 1)
    batch_id = expected["batch_id"]
    _require(expected["sha256"] == proof["auth_csv_sha256"], "Authorization CSV digest differs.")
    root = config["manifest_uri"].rsplit("/", 2)[0] + "/" + auth.run_id
    manifest = _json(_read_bytes(deployment.client, root + "/manifest.json"))
    _require(isinstance(manifest, dict) and type(manifest.get("contract_version")) is int
             and manifest["contract_version"] == 1 and manifest.get("run_id") == auth.run_id
             and _equal(manifest.get("config"), vars(auth))
             and manifest.get("generator_sha256") == config["generator_sha256"]
             and manifest.get("destinations") == ["demo-2"]
             and _equal(manifest.get("target_bindings"), config["target_bindings"])
             and isinstance(manifest.get("batches"), dict) and set(manifest["batches"]) == {"1"},
             "Authorization manifest identity differs.")
    batch = manifest["batches"]["1"]
    scheduler = manifest.get("scheduler")
    _require(isinstance(scheduler, dict) and scheduler.get("status") == "completed"
             and isinstance(scheduler.get("result"), dict)
             and scheduler["result"].get("verified") is True
             and scheduler["result"].get("errors") == [],
             "Authorization scheduler is not terminally verified.")
    _require(isinstance(batch, dict) and all(_equal(batch.get(k), v) for k, v in expected.items())
             and _read_bytes(deployment.client, root + "/" + expected["filename"]) == payload,
             "Authorization fixture bytes or metadata differ.")
    delivery = batch.get("deliveries")
    _require(isinstance(delivery, dict) and set(delivery) == {"demo-2"}
             and isinstance(delivery["demo-2"], dict), "Authorization delivery is missing.")
    d = delivery["demo-2"]
    outcome = d.get("ingestion_outcome")
    _require(d.get("status") == "verified" and d.get("submission_status") == "submitted"
             and d.get("operation_id") == proof["auth_source_id"]
             and d.get("verified_row_count") == 1 and type(d.get("verified_row_count")) is int
             and d.get("verified_distinct_sales_line_ids") == 1
             and type(d.get("verified_distinct_sales_line_ids")) is int
             and d.get("verified_total_minor_units") == expected["sales_amount_minor_units"]
             and type(d.get("verified_total_minor_units")) is int
             and isinstance(outcome, dict) and outcome.get("status") == "succeeded"
             and outcome.get("source_id") == proof["auth_source_id"]
             and outcome.get("service_operation_id") == proof["auth_service_operation_id"],
             "Authorization delivery is not terminally verified.")
    times = [_utc(d.get("submission_started_at_utc")), _utc(d.get("submission_result_at_utc")),
             _utc(outcome.get("timestamp_utc")), _utc(d.get("verified_at_utc"))]
    _require(times == sorted(times) and start <= times[0]
             and times[-1] <= end + timedelta(seconds=5),
             "Authorization delivery timestamps do not fit the linked job.")
    _require(exit_value.get("kind") == "demo2-synthetic-auth-v1" and exit_value.get("ready") is False
             and exit_value.get("status") == "verified" and exit_value.get("run_id") == auth.run_id
             and exit_value.get("batch_class") == "bootstrap"
             and exit_value.get("manifest_path") == f"Files/producer/runs/{auth.run_id}/manifest.json"
             and exit_value.get("batch_id") == batch_id
             and type(exit_value.get("row_count")) is int and exit_value["row_count"] == 1
             and type(exit_value.get("sales_amount_minor_units")) is int
             and exit_value["sales_amount_minor_units"] == expected["sales_amount_minor_units"]
             and exit_value.get("source_id") == proof["auth_source_id"]
             and exit_value.get("ingestion_outcome") == outcome
             and exit_value.get("verified_at_utc") == d["verified_at_utc"]
             and exit_value.get("producer_job_id") is None, "Authorization notebook exit differs.")
    rows = deployment._kql(database, batch_verification_query(deployment.s["table"], batch_id), query=True)
    try:
        valid = len(rows) == 1 and all(_exact_integer(rows[0][key]) == value for key, value in {
            "RowCount": 1, "DistinctSalesLineIDs": 1,
            "AmountMinorUnits": expected["sales_amount_minor_units"], "InvalidRows": 0}.items())
    except (KeyError, ValueError, TypeError):
        valid = False
    _require(valid, "Authorization rows do not reconcile.")
    rows = deployment._kql(database, f"{deployment.s['table']} | where BatchID == '{batch_id}' "
                           "| summarize WrongIds=countif(SalesLineID != 1)", query=True)
    try:
        valid = len(rows) == 1 and _exact_integer(rows[0]["WrongIds"]) == 0
    except (KeyError, ValueError, TypeError):
        valid = False
    _require(valid, "Authorization SalesLineID differs.")
    _queue(deployment, database, proof)


def verify_synthetic_runtime_access(deployment, monitor_config, proof):
    """Authorize only the isolated fixture; all unsupported or ambiguous evidence blocks."""
    try:
        return _verify(deployment, monitor_config, proof)
    except Exception as exc:
        from deploy import Blocked
        if isinstance(exc, Blocked):
            raise
        raise Blocked("Synthetic admission evidence could not be verified.") from exc


def _verify(deployment, config, proof):
    from monitor_controller import SENTINEL_SHA256
    from deploy import ONELAKE, _journal_target, verify_mapping

    _require(isinstance(proof, dict) and isinstance(config, dict), "Synthetic admission proof is missing.")
    fields = ("producer_workspace_id", "staging_lakehouse_id", "notebook_id", "environment_id", "pipeline_id",
              "preflight_parent_job_id", "preflight_child_job_id", "auth_parent_job_id", "auth_child_job_id",
              "auth_source_id", "auth_service_operation_id")
    for field in fields:
        _uuid(proof.get(field), field)
    for field in ("definition_sha256", "effective_config_sha256", "auth_csv_sha256"):
        _digest(proof.get(field), field)
    _require(isinstance(proof.get("nonce"), str)
             and re.fullmatch(r"[A-Za-z0-9_-]{16,128}", proof["nonce"]) is not None,
             "Invalid preflight nonce.")
    _require(len({proof[k] for k in ("preflight_parent_job_id", "preflight_child_job_id",
                                      "auth_parent_job_id", "auth_child_job_id")}) == 4,
             "Pipeline job evidence is replayed or ambiguous.")
    s = deployment.s
    suffix = re.fullmatch(r"demo2synthetic-([a-f0-9]{12})", s["prefix"])
    _require(suffix is not None and config.get("run_id") == f"demo2synthetic_{suffix[1]}"
             and s["data"]["run_id"] == config["run_id"]
             and s["workspace_name"] == f"{s['prefix']}-demo2-eventhouse"
             and s["table"] == "SyntheticFactSales" and s["mapping"] == "SalesCsvV1"
             and s["database_name"] == "SalesDatabase"
             and s["shared_workspace_id"] == proof["producer_workspace_id"]
             and s["shared_lakehouse_id"] == proof["staging_lakehouse_id"],
             "The requested scope is not the isolated synthetic deployment.")
    _uuid(s["tenant_id"], "tenant_id")
    _uuid(s["producer_identity"], "producer_identity")
    pc = config.get("producer_config")
    expected_pc = {"run_id": config["run_id"], "batch_class": "bootstrap", "start_date": "2026-01-01",
                   "seed": 42, "date_count": 4, "products": 3, "customers": 3, "file_count": 2,
                   "rows_per_file": 5, "interval_seconds": 30}
    _require(isinstance(pc, dict) and pc == expected_pc
             and all(type(pc[k]) is int for k in ("seed", "date_count", "products", "customers",
                                                    "file_count", "rows_per_file", "interval_seconds"))
             and type(s["max_duration"]) is int and s["max_duration"] == 300
             and _equal(s["data"], {"run_id": pc["run_id"], "seed": pc["seed"],
                                    "start_date": pc["start_date"], "date_count": pc["date_count"],
                                    "product_count": pc["products"], "customer_count": pc["customers"]})
             and type(s["expected_batch_count"]) is int and s["expected_batch_count"] == pc["file_count"]
             and type(s["rows_per_file"]) is int and s["rows_per_file"] == pc["rows_per_file"]
             and type(s["interval_seconds"]) is int and s["interval_seconds"] == pc["interval_seconds"],
             "Synthetic producer configuration differs from the bounded fixture.")
    _digest(config.get("generator_sha256"), "generator")
    root = f"{ONELAKE}/{proof['producer_workspace_id']}/{proof['staging_lakehouse_id']}"
    _require(config.get("manifest_uri") == f"{root}/Files/producer/runs/{config['run_id']}/manifest.json"
             and config.get("sentinel_uri") == f"{root}/Files/preflight/producer-monitor-heartbeat-v1.txt",
             "Synthetic OneLake paths differ from the producer binding.")
    database = deployment.verify_target_ownership()
    target = config.get("target_bindings")
    _require(isinstance(target, dict) and set(target) == {"demo-2"}
             and isinstance(target["demo-2"], dict), "Synthetic target binding is missing.")
    binding = target["demo-2"]
    _uuid(binding.get("workspace_id"), "target workspace")
    _uuid(binding.get("item_id"), "target database")
    _require(binding == _journal_target(s, deployment.ledger)
             and binding["item_id"] == database["id"]
             and binding["workspace_id"] != proof["producer_workspace_id"]
             and database["name"] == s["database_name"], "Synthetic target is not the owned distinct target.")
    effective = {
        "contract_version": 1,
        "common": {"tenant_id": s["tenant_id"]},
        "shared": {"workspace_id": proof["producer_workspace_id"],
                   "staging_lakehouse_id": proof["staging_lakehouse_id"],
                   "identity_id": s["producer_identity"], "target_bindings": target},
        "data": {"run_id": pc["run_id"], "batch_class": pc["batch_class"],
                 "start_date": pc["start_date"], "seed": pc["seed"], "date_count": pc["date_count"],
                 "product_count": pc["products"], "customer_count": pc["customers"],
                 "file_count": pc["file_count"], "rows_per_file": pc["rows_per_file"],
                 "interval_seconds": pc["interval_seconds"], "max_duration_seconds": s["max_duration"],
                 "currency": "USD"},
    }
    fingerprint = hashlib.sha256(json.dumps(effective, sort_keys=True, separators=(",", ":"),
                                             ensure_ascii=False, allow_nan=False).encode("utf-8")).hexdigest()
    _require(fingerprint == proof["effective_config_sha256"],
             "Preflight effective configuration digest differs from the bounded fixture.")
    verify_mapping(deployment._kql(database, f".show table {s['table']} ingestion csv mappings"), s["mapping"])
    _definition(deployment, proof)
    preflight, start, end = _read_job(deployment, proof, "preflight")
    _preflight(preflight, start, end, proof, s, config, database)
    auth, auth_start, auth_end = _read_job(deployment, proof, "auth")
    generator = _generator(config)
    _authorization(deployment, config, proof, generator, auth, database, auth_start, auth_end)
    _manifest(deployment, config, proof, generator)
    _require(hashlib.sha256(_read_bytes(deployment.client, config["sentinel_uri"])).hexdigest() == SENTINEL_SHA256,
             "Producer sentinel bytes differ.")
    # Queue and file reads can take time: admission still requires a fresh preflight at return.
    _require(datetime.now(timezone.utc) < _utc(preflight["expires_at_utc"]), "Preflight expired during admission.")
    return _utc(preflight["expires_at_utc"])
