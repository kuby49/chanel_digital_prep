"""Drain cooperating Demo 3 Copy intents after an immutable per-run stop."""

import hashlib
import re
import uuid
from datetime import datetime, timedelta, timezone

from audit import BatchAudit, LOCK_PATH, WriterToken, _encode, _time


def _utc(value):
    if not isinstance(value, datetime) or value.utcoffset() != timedelta(0):
        raise ValueError("stop barrier requires a timezone-aware UTC clock")
    return value


def stop(context, prepared):
    import generator
    from fabric_storage import StorageConflict

    document, binding, producer = context["document"], context["binding"], context["store"]
    target = document["shared"]["target_bindings"]["demo-3"]
    run_id = document["data"]["run_id"]
    digest = hashlib.sha256(generator.canonical_bytes(document)).hexdigest()
    if (not isinstance(run_id, str)
            or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}", run_id, re.ASCII)
            or binding["run_id"] != run_id or binding["config_sha256"] != digest
            or binding["nonce"] != "native-" + digest[:32]
            or target["fact"] != "dbo.factsales"
            or producer.workspace_id != binding["producer"]["workspace_id"]
            or producer.lakehouse_id != binding["producer"]["staging_lakehouse_id"]):
        raise ValueError("stop barrier binding differs from the native run")
    deadline = _utc(datetime.fromisoformat(context["cleanup_deadline_utc"].replace("Z", "+00:00")))
    now = lambda: _utc(context["clock"]())
    if now() >= deadline:
        raise TimeoutError("stop barrier deadline expired")

    marker_path = f"Files/producer/runs/{run_id}/stop.json"
    marker, marker_etag = producer.read(marker_path)
    expected = {"version": 1, "run_id": run_id, "nonce": binding["nonce"], "config_sha256": digest}
    actual = generator.strict_json_loads(marker.decode("utf-8"))
    if not isinstance(actual, dict) or type(actual.get("version")) is not int or actual != expected:
        raise ValueError("stop marker does not match the immutable native run")

    store = context["store_factory"](target["workspace_id"], target["item_id"])
    audit = BatchAudit(store, target["workspace_id"], target["item_id"])
    batch = run_id + "_0001"
    job_id = str(uuid.uuid4())
    body = _encode({"contract_version": 1, "target": audit.target, "pipeline_run_id": job_id,
                    "batch_id": batch, "nonce": str(uuid.uuid4()), "acquired_at_utc": _time()})
    while True:
        if now() >= deadline:
            raise TimeoutError("stop barrier could not acquire the writer before its deadline")
        try:
            etag = store.create(LOCK_PATH, body)
            break
        except StorageConflict:
            remaining = (deadline - now()).total_seconds()
            if remaining > 0:
                context["sleep"](min(5, remaining))

    token = WriterToken(job_id, batch, body, etag)
    audit._lock(token, batch)
    if now() >= deadline or producer.read(marker_path) != (marker, marker_etag):
        raise RuntimeError("stop marker or deadline changed while holding the writer; reconcile")
    audit._lock(token, batch)
    store.delete(LOCK_PATH, token.lock_etag)
    return {"status": "stopped", "scope": "cooperating guarded Copy intents"}
