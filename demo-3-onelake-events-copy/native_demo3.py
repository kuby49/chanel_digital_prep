"""Native producer hooks for the deployment-pinned Demo 3 event path."""

import ast
import base64
import copy
import hashlib
import json
import re
import uuid
from datetime import date, datetime, timedelta

import audit
import batch_gate
import generator
import guard_spark
import native_guard
import native_stop
from fabric_runtime import run_config


_FIELDS = {
    "version", "workspace_id", "lakehouse_id", "guard_notebook_id", "pipeline_id", "reflex_id",
    "connection_id", "marker", "native_policy", "generator_sha256", "guard_sha256",
    "pipeline_sha256", "graph_disabled_sha256",
}
_SHA = re.compile(r"[0-9a-f]{64}\Z")
_TERMINAL = {"Completed", "Failed", "Cancelled", "Canceled", "Deduped"}


def _digest(value):
    return hashlib.sha256(generator.canonical_bytes(value)).hexdigest()


def _guid(value):
    result = str(uuid.UUID(value))
    if result != value or uuid.UUID(result).int == 0:
        raise ValueError("native proof contains an invalid identity")
    return result


def _deadline(context, field):
    deadline = datetime.fromisoformat(context[field].replace("Z", "+00:00"))
    if deadline.utcoffset() != timedelta(0):
        raise ValueError("native hook deadline must be UTC")
    return deadline


def _remaining(context, deadline):
    now = context["clock"]()
    if not isinstance(now, datetime) or now.utcoffset() != timedelta(0):
        raise ValueError("native hook clock must be UTC")
    remaining = (deadline - now).total_seconds()
    if remaining <= 0:
        raise TimeoutError("native Demo 3 hook deadline expired")
    if context["stop_requested"]():
        raise RuntimeError("native run was stopped")
    return remaining


def _definition(context, client, path, part, deadline):
    status, headers, body = client.request("POST", path, {}, timeout=min(30, _remaining(context, deadline)))
    if status == 202:
        operation = _guid(next((value for key, value in headers.items()
                                if key.lower() == "x-ms-operation-id"), None))
        while True:
            _remaining(context, deadline)
            status, _, state = client.request("GET", "/v1/operations/" + operation,
                                              timeout=min(30, _remaining(context, deadline)))
            if status != 200 or not isinstance(state, dict):
                raise RuntimeError("definition operation status is unavailable")
            if state.get("status") == "Succeeded":
                break
            if state.get("status") not in ("NotStarted", "Running"):
                raise RuntimeError("definition operation did not succeed")
            context["sleep"](min(5, _remaining(context, deadline)))
        status, _, body = client.request("GET", "/v1/operations/" + operation + "/result",
                                        timeout=min(30, _remaining(context, deadline)))
    if status != 200 or not isinstance(body, dict):
        raise RuntimeError("native definition read was not confirmed")
    parts = [item for item in body["definition"]["parts"]
             if item.get("path") == part and item.get("payloadType") == "InlineBase64"]
    if len(parts) != 1:
        raise RuntimeError("native definition part is missing or ambiguous")
    return generator.strict_json_loads(base64.b64decode(parts[0]["payload"], validate=True).decode("utf-8"))


def _graph(graph, *, enabled):
    graph = copy.deepcopy(graph)
    if not isinstance(graph, list):
        raise ValueError("native graph is invalid")
    identities = [item["uniqueIdentifier"] for item in graph]
    if len(set(identities)) != len(identities):
        raise ValueError("native graph has duplicate identities")
    rules = [item for item in graph if item.get("type") == "timeSeriesView-v1"
             and item["payload"].get("definition", {}).get("type") == "Rule"]
    if len(rules) != 1:
        raise ValueError("native graph must contain exactly one rule")
    settings = rules[0]["payload"]["definition"]["settings"]
    if settings != {"shouldRun": enabled, "shouldApplyRuleOnUpdate": False}:
        raise ValueError("native rule state is not ready")
    settings["shouldRun"] = False
    for item in graph:
        definition = item.get("payload", {}).get("definition", {})
        if isinstance(definition.get("instance"), str):
            definition["instance"] = generator.strict_json_loads(definition["instance"])
    return sorted(graph, key=lambda item: item["uniqueIdentifier"])


def _guard(notebook):
    return {"cells": [item["source"] for item in notebook["cells"]],
            "lakehouse": notebook["metadata"]["trident"]["lakehouse"]}


def _guard_staging(notebook):
    try:
        cells = notebook["cells"]
        if len(cells) != 2 or not isinstance(cells[1]["source"], list):
            raise ValueError("unexpected guard notebook cells")
        tree = ast.parse("".join(cells[1]["source"]))
        calls = [node for node in ast.walk(tree) if isinstance(node, ast.Call)
                 and isinstance(node.func, ast.Name) and node.func.id == "execute"]
        if len(calls) != 1 or len(calls[0].args) != 4:
            raise ValueError("unexpected guard entry point")
        settings = ast.literal_eval(calls[0].args[2])
        return _guid(settings["producer_workspace_id"]), _guid(settings["producer_item_id"])
    except (KeyError, TypeError, ValueError, SyntaxError) as exc:
        raise ValueError("native guard staging binding is unavailable") from exc


def _proof(document):
    proof = document["demo-3"]["native_proof"]
    if (not isinstance(proof, dict) or set(proof) != _FIELDS
            or type(proof["version"]) is not int or proof["version"] != 1
            or not isinstance(proof["marker"], str) or len(proof["marker"]) < 8):
        raise ValueError("deployment-pinned native proof is incomplete")
    for field in ("workspace_id", "lakehouse_id", "guard_notebook_id", "pipeline_id",
                  "reflex_id", "connection_id"):
        _guid(proof[field])
    for field in ("generator_sha256", "guard_sha256", "pipeline_sha256", "graph_disabled_sha256"):
        if not isinstance(proof[field], str) or not _SHA.fullmatch(proof[field]):
            raise ValueError("native proof fingerprint is invalid")
    target = document["shared"]["target_bindings"]["demo-3"]
    if (target != {"workspace_id": proof["workspace_id"], "item_id": proof["lakehouse_id"],
                   "fact": "dbo.factsales"}
            or proof["generator_sha256"] != generator.generator_source_sha256()):
        raise ValueError("native target or generator differs from the deployment proof")
    config, _ = run_config(document)
    native_guard.validate_run_policy(config, proof["native_policy"])
    return proof, config


def _binding(context, config):
    document, binding, store = context["document"], context["binding"], context["store"]
    digest = _digest(document)
    if (document["data"]["run_id"] != config.run_id or binding["run_id"] != config.run_id
            or binding["config_sha256"] != digest or binding["nonce"] != "native-" + digest[:32]
            or store.workspace_id != binding["producer"]["workspace_id"]
            or store.lakehouse_id != binding["producer"]["staging_lakehouse_id"]
            or document["shared"]["workspace_id"] != store.workspace_id
            or document["shared"]["staging_lakehouse_id"] != store.lakehouse_id):
        raise ValueError("native producer binding differs from the runtime document")


def _dimensions(context, proof, config):
    from pyspark.sql import functions as F, types as T

    schemas = {
        "DimDate": (T.StructType([T.StructField("Date", T.DateType()),
                                  T.StructField("Year", T.IntegerType()),
                                  T.StructField("MonthNumber", T.IntegerType()),
                                  T.StructField("MonthName", T.StringType())]), "Date"),
        "DimProduct": (T.StructType([T.StructField("ProductID", T.IntegerType()),
                                     T.StructField("ProductName", T.StringType()),
                                     T.StructField("ProductCategory", T.StringType())]), "ProductID"),
        "DimCustomer": (T.StructType([T.StructField("CustomerID", T.IntegerType()),
                                      T.StructField("CustomerName", T.StringType()),
                                      T.StructField("CustomerRegion", T.StringType())]), "CustomerID"),
    }
    spark, utilities = context["spark"], context["notebookutils"]
    endpoint = guard_spark.onelake_endpoint(utilities.conf.get("trident.onelake.endpoint"))
    root = f"abfss://{proof['workspace_id']}@{endpoint}/{proof['lakehouse_id']}/Tables/dbo"
    expected = generator.dimensions(config)
    for name, (schema, key) in schemas.items():
        rows = expected[name]
        if name == "DimDate":
            rows = [{**row, "Date": date.fromisoformat(row["Date"])} for row in rows]
        required = spark.createDataFrame(rows, schema)
        actual = spark.read.format("delta").load(f"{root}/{name.lower()}").select(*schema.fieldNames())
        if (actual.schema.simpleString() != required.schema.simpleString()
                or actual.filter(F.col(key).isNull()).limit(1).count()
                or actual.select(key).distinct().count() != actual.count()
                or required.exceptAll(actual).limit(1).count()):
            raise RuntimeError(f"native {name} rows or keys are not ready")


def prepare(context):
    document = context["document"]
    proof, config = _proof(document)
    _binding(context, config)
    deadline = _deadline(context, "preparation_deadline_utc")
    _remaining(context, deadline)
    workspace = proof["workspace_id"]
    base = "/v1/workspaces/" + workspace
    spark_client = context["fabric_factory"]("spark-cli")
    activator = context["fabric_factory"]("activator-cli")
    for client, path, identity in (
        (spark_client, base + "/lakehouses", proof["lakehouse_id"]),
        (spark_client, base + "/items?type=Notebook", proof["guard_notebook_id"]),
        (spark_client, base + "/items?type=DataPipeline", proof["pipeline_id"]),
        (activator, base + "/reflexes", proof["reflex_id"]),
    ):
        _remaining(context, deadline)
        matches = [item for item in client.list_all(path) if item.get("id") == identity]
        if len(matches) != 1 or proof["marker"] not in matches[0].get("description", ""):
            raise RuntimeError("native deployment ownership or identity differs")
    connection = spark_client.get("/v1/connections/" + proof["connection_id"])[1]
    if (connection.get("id") != proof["connection_id"]
            or connection.get("connectionDetails", {}).get("type") != "Lakehouse"):
        raise RuntimeError("native Lakehouse connection is not ready")
    notebook = _definition(context, spark_client,
                           base + "/items/" + proof["guard_notebook_id"] + "/getDefinition?format=ipynb",
                           "notebook-content.ipynb", deadline)
    pipeline = _definition(context, spark_client, base + "/items/" + proof["pipeline_id"] + "/getDefinition",
                           "pipeline-content.json", deadline)
    graph = _definition(context, activator, base + "/reflexes/" + proof["reflex_id"] + "/getDefinition",
                        "ReflexEntities.json", deadline)
    if (_digest(_guard(notebook)) != proof["guard_sha256"]
            or _digest(pipeline["properties"]) != proof["pipeline_sha256"]
            or _digest(_graph(graph, enabled=True)) != proof["graph_disabled_sha256"]):
        raise RuntimeError("native guard, pipeline or enabled event rule differs from deployment proof")
    if _guard_staging(notebook) != (document["shared"]["workspace_id"],
                                    document["shared"]["staging_lakehouse_id"]):
        raise RuntimeError("native guard and producer staging bindings differ")
    for item, kind in ((proof["pipeline_id"], "Pipeline"), (proof["guard_notebook_id"], "RunNotebook")):
        _remaining(context, deadline)
        jobs = spark_client.list_all(base + f"/items/{item}/jobs/instances?jobType={kind}")
        if any(job.get("status") not in _TERMINAL for job in jobs):
            raise RuntimeError("an earlier native guard or pipeline job is not terminal")
    store = context["store_factory"](workspace, proof["lakehouse_id"])
    audit.BatchAudit(store, workspace, proof["lakehouse_id"])
    try:
        store.read(audit.LOCK_PATH)
    except FileNotFoundError:
        pass
    else:
        raise RuntimeError("native writer lock exists; reconcile before launch")
    _dimensions(context, proof, config)
    _remaining(context, deadline)
    return {"status": "ready", "run_id": config.run_id, "proof_sha256": _digest(proof)}


def verify(context, prepared):
    document = context["document"]
    proof, config = _proof(document)
    _binding(context, config)
    if prepared != {"status": "ready", "run_id": config.run_id, "proof_sha256": _digest(proof)}:
        raise ValueError("native preparation evidence differs")
    deadline = _deadline(context, "verification_deadline_utc")
    target = context["store_factory"](proof["workspace_id"], proof["lakehouse_id"])
    batch_audit = audit.BatchAudit(target, proof["workspace_id"], proof["lakehouse_id"])
    client = context["fabric_factory"]("spark-cli")
    base = f"/v1/workspaces/{proof['workspace_id']}/items/{proof['pipeline_id']}/jobs/instances/"
    settings = {"workspace_id": proof["workspace_id"], "item_id": proof["lakehouse_id"]}
    pending = set(range(1, config.file_count + 1))
    while pending:
        _remaining(context, deadline)
        manifest = context["read_manifest"]()
        manifest_bytes = generator.canonical_bytes(manifest)
        for number in sorted(pending):
            _remaining(context, deadline)
            batch_id = f"{config.run_id}_{number:04d}"
            subject = f"/Files/incoming/sales/sales_{batch_id}.csv"
            if str(number) not in manifest.get("batches", {}):
                continue
            native_guard.admitted_config(
                generator, {"native_policy": proof["native_policy"], "workspace_id": proof["workspace_id"],
                            "item_id": proof["lakehouse_id"]}, subject, manifest_bytes, now=context["clock"]())
            try:
                source, _ = target.read(subject[1:])
                evidence = batch_gate.validate_batch(generator, config, manifest_bytes, subject, source,
                                                     origin="recovery")
                record, _ = batch_audit._record(batch_id)
            except FileNotFoundError:
                continue
            if batch_audit.batch_state(evidence) != "completed":
                continue
            attempt = record["copy_attempt"]["pipeline_run_id"]
            if not any(item["pipeline_run_id"] == attempt and item["origin"] == "event"
                       for item in record["observations"]):
                raise RuntimeError("native batch has no matching event-triggered Copy")
            status, _, job = client.request("GET", base + attempt,
                                            timeout=min(30, _remaining(context, deadline)))
            if status != 200 or not isinstance(job, dict):
                raise RuntimeError("native pipeline job status is unavailable")
            if job.get("status") in ("Failed", "Cancelled", "Canceled"):
                raise RuntimeError("native pipeline job failed")
            if job.get("status") != "Completed":
                continue
            current = guard_spark.query_target(
                context["spark"], context["notebookutils"], settings, config, batch_id)
            if not audit._proof_matches(record["batch"], current):
                raise RuntimeError("native fact or dimension readback differs")
            pending.remove(number)
        if pending:
            context["sleep"](min(5, _remaining(context, deadline)))
    return {"status": "verified", "run_id": config.run_id, "batch_count": config.file_count}


def stop(context, prepared):
    return native_stop.stop(context, prepared)
