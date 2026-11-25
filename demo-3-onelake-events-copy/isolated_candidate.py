"""Build an offline-only candidate for an isolated Demo 3 target."""

import hashlib
import json
import re
import subprocess
import sys
import tempfile
from pathlib import Path

import activator
import customer_preflight as original
import guarded_notebook
import guarded_pipeline
from deploy import _guid
from native_guard import validate_policy


def _digest(value):
    data = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                      allow_nan=False).encode("utf-8")
    return hashlib.sha256(data).hexdigest()


def _producer_ids(producer_dir, config_path, prefix, state_dir):
    helper, config_path, state_dir = (Path(producer_dir) / "deploy.py", Path(config_path),
                                      Path(state_dir))
    if not all(path.is_absolute() for path in (helper, config_path, state_dir)):
        raise ValueError("producer helper, config and state directory must be absolute")
    with tempfile.TemporaryDirectory(prefix="demo3-staging-candidate-") as directory:
        result = Path(directory) / "result.json"
        command = [sys.executable, str(helper), "--mode", "resolve-candidate-ids",
                   "--config", str(config_path), "--prefix", prefix,
                   "--state-dir", str(state_dir), "--result", str(result)]
        try:
            completed = subprocess.run(command, capture_output=True, timeout=120, check=False)
        except subprocess.TimeoutExpired as exc:
            raise RuntimeError("Producer ownership journal resolution timed out") from exc
        if completed.returncode != 0:
            if result.is_file():
                failure = json.loads(result.read_text(encoding="utf-8"))
                diagnostics = failure.get("diagnostics", [])
                if isinstance(diagnostics, list) and all(isinstance(item, str) for item in diagnostics):
                    raise ValueError("Producer ownership journal resolution failed: " + "; ".join(diagnostics))
            raise ValueError(f"Producer ownership journal resolver exited with code {completed.returncode}")
        document = json.loads(result.read_text(encoding="utf-8"))
        sidecar = json.loads((result.parent / "shared-producer-candidate-ids.json").read_text(encoding="utf-8"))
        if (document.get("contract_version") != 1 or document.get("track") != "shared"
                or document.get("mode") != "resolve-candidate-ids" or document.get("status") != "pass"
                or document.get("ready") is not False or set(sidecar) != {
                    "version", "workspace_id", "staging_lakehouse_id"} or type(sidecar["version"]) is not int
                or sidecar["version"] != 1):
            raise ValueError("Producer ownership journal resolver returned an invalid result")
        return _guid(sidecar["workspace_id"]), _guid(sidecar["staging_lakehouse_id"])


def build_candidate(spec, producer_dir, copy_activity, source_sample, *,
                    producer_config_path, producer_prefix, producer_state_dir):
    """Package definitions, not ownership, live readback, publication or readiness."""
    required = {
        "tenant_id", "prefix", "owner_id", "workspace_id", "lakehouse_id", "lakehouse_name",
        "notebook_id", "pipeline_id", "reflex_id", "connection_id",
        "producer_workspace_id", "producer_lakehouse_id", "native_policy",
    }
    if not isinstance(spec, dict) or set(spec) != required:
        raise ValueError("isolated candidate must specify exactly the required bindings")
    ids = {key: _guid(spec[key]) for key in required if key.endswith("_id")}
    if ids["tenant_id"] != original.TENANT:
        raise ValueError("isolated candidate must remain in the approved tenant")
    if ids["producer_lakehouse_id"] == original.STAGING_LAKEHOUSE:
        raise ValueError("isolated candidate must not reuse the original producer staging Lakehouse")
    target_items = ("lakehouse_id", "notebook_id", "pipeline_id", "reflex_id", "connection_id")
    old_items = {original.LAKEHOUSE, original.NOTEBOOK, original.PIPELINE,
                 original.REFLEX, original.CONNECTION}
    if any(ids[key] in old_items and not (key == "connection_id" and ids[key] == original.CONNECTION)
           for key in target_items):
        raise ValueError("isolated candidate must not reuse original target artifacts")
    if len({ids[k] for k in (*target_items, "producer_lakehouse_id")}) != 6:
        raise ValueError("target artifacts and producer Lakehouse must be distinct")
    prefix = spec["prefix"]
    if not isinstance(prefix, str) or not re.fullmatch(r"[a-z0-9][a-z0-9-]{0,40}", prefix):
        raise ValueError("invalid owner marker prefix")
    marker = f"{prefix}-demo3-{ids['owner_id']}"
    if marker == original.MARKER:
        raise ValueError("isolated candidate must use a distinct owner marker")
    producer_dir = Path(producer_dir)
    if _producer_ids(producer_dir, producer_config_path, producer_prefix, producer_state_dir) != (
            ids["producer_workspace_id"], ids["producer_lakehouse_id"]):
        raise ValueError("producer staging IDs differ from the owned journal candidate")
    policy = validate_policy(spec["native_policy"])
    notebook = guarded_notebook.build_native_notebook(
        producer_dir, policy, ids["workspace_id"], ids["lakehouse_id"], spec["lakehouse_name"],
        ids["producer_workspace_id"], ids["producer_lakehouse_id"])
    pipeline = guarded_pipeline.build_pipeline(
        ids["workspace_id"], ids["lakehouse_id"], ids["connection_id"], ids["notebook_id"],
        copy_activity)
    graph = activator.build_native_definition(
        ids["tenant_id"], ids["workspace_id"], ids["lakehouse_id"], ids["pipeline_id"],
        source_sample)
    return {
        "contract_version": 1,
        "status": "offline-candidate-only",
        "ready": False,
        "owner_marker": marker,
        "target": {"workspace_id": ids["workspace_id"], "item_id": ids["lakehouse_id"],
                   "fact": "dbo.factsales"},
        "identities": ids,
        "producer_source_sha256": {
            name: hashlib.sha256((producer_dir / name).read_bytes()).hexdigest()
            for name in ("generator.py", "fabric_storage.py")
        },
        "definitions": {"guard_notebook": notebook, "pipeline": pipeline, "activator_graph": graph},
        "definition_sha256": {"guard_notebook": _digest(notebook), "pipeline": _digest(pipeline),
                              "activator_graph": _digest(graph)},
        "required_live_gates": [
            "Original parent and writer outcomes independently reconciled; no old evidence altered.",
            "New target ownership, intent receipts, parentage and marker independently read back.",
            "Compatible fact protocol and dimensions verified; connection type, path and identity read back "
            "and runtime access to the new target independently verified.",
            "New target's representative FileCreated event observed; rule remains disabled.",
            "Notebook, pipeline and complete Activator definitions independently read back.",
            "Jobs, lock, writer exclusion and shared producer lifecycle separately assessed.",
            "New producer binding, proof, journal and coordinated enable explicitly approved.",
        ],
    }
