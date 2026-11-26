"""Opt-in Demo 2 bootstrap staging; no ingestion is performed here."""

import copy
import hashlib
import re
import uuid
from dataclasses import replace

from fabric_ledger import FabricLedger
from generator import (RunConfig, batch_bytes, batch_metadata_matches, canonical_bytes,
                       new_manifest, strict_json_loads, validate_fabric_targets,
                       validate_run_manifest)


RUN_ID = re.compile(r"demo2synthetic_[0-9a-f]{12}\Z")


def _uuid(value):
    try:
        parsed = uuid.UUID(value)
    except (TypeError, AttributeError, ValueError):
        raise ValueError("Synthetic fixture requires canonical resource identities") from None
    if not parsed.int or str(parsed) != value:
        raise ValueError("Synthetic fixture requires canonical resource identities")
    return value


def prepare_fixture(document, selected, probe_id, request, context, identities, *,
                    kind="demo2-synthetic-stage-v1", published_binding=None):
    """Bind the fixture to one runtime and one separately provisioned target."""
    expected_fields = {"kind", "nonce", "config_sha256", "tenant_id",
                       "identity_id", "producer", "target"}
    if kind != "demo2-synthetic-stage-v1":
        expected_fields.add("kusto")
    if (not isinstance(document, dict) or type(document.get("contract_version")) is not int
            or document["contract_version"] != 1 or selected != ["demo-2"]
            or not isinstance(request, dict)
            or set(request) != expected_fields
            or kind not in ("demo2-synthetic-stage-v1", "demo2-synthetic-auth-v1",
                            "demo2-synthetic-deliver-v1")
            or request["kind"] != kind
            or not isinstance(probe_id, str)
            or not re.fullmatch(r"[a-z0-9][a-z0-9-]{15,63}", probe_id)
            or request["nonce"] != probe_id):
        raise ValueError("Explicit bound Demo 2 synthetic staging request is required")
    if request["config_sha256"] != hashlib.sha256(canonical_bytes(document)).hexdigest():
        raise ValueError("Synthetic request differs from the effective configuration")
    data = document.get("data")
    shared = document.get("shared")
    if not isinstance(data, dict) or not isinstance(shared, dict):
        raise ValueError("Synthetic configuration needs data and shared bindings")
    fields = {"run_id", "batch_class", "start_date", "seed", "date_count", "product_count",
              "customer_count", "file_count", "rows_per_file", "interval_seconds",
              "max_duration_seconds", "currency"}
    if (set(data) != fields or not isinstance(data["run_id"], str)
            or not RUN_ID.fullmatch(data["run_id"]) or data["batch_class"] != "bootstrap"
            or data["start_date"] != "2026-01-01"
            or any(type(data[name]) is not int or data[name] != expected for name, expected in
                   (("seed", 42), ("date_count", 4), ("product_count", 3), ("customer_count", 3)))
            or type(data["file_count"]) is not int or data["file_count"] != 2
            or type(data["rows_per_file"]) is not int or data["rows_per_file"] != 5
            or type(data["interval_seconds"]) is not int or data["interval_seconds"] != 30
            or type(data["max_duration_seconds"]) is not int or data["max_duration_seconds"] != 300
            or data["currency"] != "USD"):
        raise ValueError("Synthetic fixture must match the bounded two-batch Demo 2 specification")
    run = RunConfig(run_id=data["run_id"], batch_class="bootstrap", start_date=data["start_date"],
                    seed=data["seed"], date_count=data["date_count"], products=data["product_count"],
                    customers=data["customer_count"], file_count=2, rows_per_file=5,
                    interval_seconds=data["interval_seconds"])
    run.validate()
    targets = shared.get("target_bindings")
    validate_fabric_targets(targets)
    if set(targets) != {"demo-2"} or targets["demo-2"]["fact"] != "SyntheticFactSales" \
            or targets["demo-2"] != request["target"]:
        raise ValueError("Synthetic fixture must bind only the isolated Demo 2 fact")
    if kind != "demo2-synthetic-stage-v1":
        from fabric_runtime import _kusto_endpoint

        kusto = request["kusto"]
        if (not isinstance(kusto, dict)
                or set(kusto) != {"database", "fact", "mapping", "query_service_uri",
                                  "ingestion_service_uri"}
                or kusto["database"] != "SalesDatabase"
                or kusto["fact"] != targets["demo-2"]["fact"]
                or kusto["mapping"] != "SalesCsvV1"):
            raise ValueError("Synthetic Kusto destination is not the isolated fact")
        query = _kusto_endpoint(kusto["query_service_uri"])
        ingest = _kusto_endpoint(kusto["ingestion_service_uri"])
        if ingest != query.replace("https://", "https://ingest-", 1):
            raise ValueError("Synthetic query and ingestion endpoints differ")
    producer = request["producer"]
    if not isinstance(producer, dict) or set(producer) != {
            "workspace_id", "staging_lakehouse_id", "notebook_id"}:
        raise ValueError("Synthetic producer binding is invalid")
    for field in producer:
        _uuid(producer[field])
    tenant, identity = _uuid(request["tenant_id"]), _uuid(request["identity_id"])
    if (shared.get("workspace_id") != producer["workspace_id"]
            or shared.get("staging_lakehouse_id") != producer["staging_lakehouse_id"]
            or shared.get("identity_id") != identity
            or not isinstance(document.get("common"), dict)
            or document["common"].get("tenant_id") != tenant):
        raise ValueError("Synthetic fixture differs from configured producer identities")
    from fabric_runtime import notebook_context_matches

    if not notebook_context_matches(shared, producer, context, published_binding):
        raise ValueError("Synthetic fixture is not running in the bound producer notebook")
    if identities != {"storage": (tenant, identity), "kusto": (tenant, identity)}:
        raise ValueError("Synthetic runtime identities do not match the bound principal")
    return run, targets, producer


def authorization_run(run):
    if not RUN_ID.fullmatch(run.run_id) or run.batch_class != "bootstrap":
        raise ValueError("Authorization probe requires a reserved bootstrap run")
    probe = replace(run, run_id=run.run_id.replace("demo2synthetic_", "demo2synthetic_auth_", 1),
                    file_count=1, rows_per_file=1, interval_seconds=0)
    probe.validate()
    return probe


def verified_authorization(store, run, targets):
    auth = authorization_run(run)
    expected = new_manifest(auth, ["demo-2"])
    expected["target_bindings"] = copy.deepcopy(targets)
    path = f"Files/producer/runs/{auth.run_id}/manifest.json"
    manifest = strict_json_loads(store.read(path)[0].decode("utf-8"))
    validate_run_manifest(manifest, expected)
    if set(manifest["batches"]) != {"1"} or not batch_metadata_matches(
            manifest["batches"]["1"], batch_bytes(auth, 1)[1]):
        raise ValueError("Authorization probe batch differs from the deterministic one-row input")
    batch = manifest["batches"]["1"]
    if store.read(f"Files/producer/runs/{auth.run_id}/{batch['filename']}")[0] != batch_bytes(auth, 1)[0]:
        raise ValueError("Authorization probe staged bytes differ")
    delivery = batch["deliveries"]["demo-2"]
    scheduler = manifest.get("scheduler")
    if (batch["batch_id"] != auth.run_id + "_0001" or batch["row_count"] != 1
            or delivery.get("status") != "verified"
            or not isinstance(delivery.get("operation_id"), str)
            or not isinstance(delivery.get("ingestion_outcome"), dict)
            or delivery["ingestion_outcome"].get("status") != "succeeded"
            or delivery["ingestion_outcome"].get("source_id") != delivery["operation_id"]
            or not isinstance(scheduler, dict) or not isinstance(scheduler.get("result"), dict)
            or scheduler.get("status") != "completed"
            or scheduler.get("result", {}).get("verified") is not True):
        raise ValueError("Authorization probe has no exact terminal ingestion and KQL verification")
    return delivery["operation_id"]


def stage_fixture(store, run, targets):
    if (not RUN_ID.fullmatch(run.run_id) or run.batch_class != "bootstrap"
            or run.file_count != 2 or run.rows_per_file != 5):
        raise ValueError("Only reserved synthetic bootstrap runs can be staged")
    validate_fabric_targets(targets)
    if set(targets) != {"demo-2"} or targets["demo-2"]["fact"] != "SyntheticFactSales":
        raise ValueError("Only the isolated Demo 2 target is supported")
    with FabricLedger(store, run, ["demo-2"], selected_targets=targets) as ledger:
        for number in (1, 2):
            ledger.stage_batch(number)
        return ledger.manifest_path, ledger.manifest


def peek_status_queues(ingest_client):
    """Non-destructive SDK reads; never expose or consume queue contents."""
    from azure.kusto.ingest.status import KustoIngestStatusQueues

    queues = KustoIngestStatusQueues(ingest_client)
    for name in ("success", "failure"):
        result = getattr(queues, name).peek(n=1, raw=True)
        if not isinstance(result, list):
            raise RuntimeError(f"{name} status queue peek returned an invalid response")
    return {"success": "passed", "failure": "passed"}
