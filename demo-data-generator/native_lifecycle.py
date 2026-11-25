"""Reusable native run admission and track-owned lifecycle coordination."""

import copy
import hashlib
import importlib
from datetime import datetime, timedelta, timezone

from fabric_ledger import FabricLedger
from fabric_runtime import run_config
from generator import canonical_bytes, strict_json_loads


LOCK_PATH = "Files/producer/native/writer.lock"
LATEST_PATH = "Files/producer/native/latest.json"
HOOK_MODULES = {"demo-1": "native_demo1", "demo-2": "native_demo2", "demo-3": "native_demo3"}


def load_hooks(selected):
    hooks = {}
    for name in selected:
        try:
            module = importlib.import_module(HOOK_MODULES[name])
        except ImportError:
            raise RuntimeError(f"{name} native lifecycle module is unavailable; no sales were produced") from None
        required = ("prepare", "verify", "stop") + (("check_ready",) if name == "demo-1" else ())
        if any(not callable(getattr(module, key, None)) for key in required):
            raise RuntimeError(f"{name} native lifecycle is incomplete; no sales were produced")
        hooks[name] = module
    return hooks


def run_native_lifecycle(store, document, selected, binding, expiry, profile, context, produce, *,
                         hooks=None, clock=None):
    """Keep global exclusion until every selected track verifies and stops safely."""
    clock = clock or (lambda: datetime.now(timezone.utc))
    hooks = load_hooks(selected) if hooks is None else hooks
    if set(hooks) != set(selected):
        raise ValueError("Native hooks must match exactly the selected tracks")
    run, duration = run_config(document)
    if (binding["run_id"] != run.run_id or binding["config_sha256"] !=
            hashlib.sha256(canonical_bytes(document)).hexdigest()):
        raise ValueError("Native lifecycle document differs from the immutable run binding")
    directory = f"Files/producer/runs/{run.run_id}"
    journal_path = f"{directory}/native-lifecycle.json"
    manifest_path = f"{directory}/manifest.json"
    receipt = {"version": 1, "run_id": run.run_id, "config_sha256": binding["config_sha256"],
               "manifest_path": manifest_path, "expires_at_utc": expiry}
    lock_bytes = canonical_bytes(receipt)
    # An ambiguous create is never retried, deleted, or replaced.
    lock_etag = store.create(LOCK_PATH, lock_bytes)
    unsafe = False

    def assert_lock():
        nonlocal unsafe
        actual, etag = store.read(LOCK_PATH)
        if actual != lock_bytes or etag != lock_etag:
            unsafe = True
            raise RuntimeError("Native launcher exclusion changed; manual reconciliation required")

    def write(path, value, etag=None):
        nonlocal unsafe
        assert_lock()
        payload = canonical_bytes(value)
        try:
            written = store.create(path, payload) if etag is None else store.replace(path, payload, etag)
            actual, observed = store.read(path)
            if actual != payload or observed != written:
                raise RuntimeError("Native lifecycle write readback differs")
            return written
        except BaseException:
            unsafe = True
            raise

    started = datetime.fromisoformat(document["shared"]["native_started_at_utc"])
    expiry_time = datetime.fromisoformat(expiry)
    preparation_deadline = started + timedelta(seconds=profile["preparation_timeout_seconds"])
    verification_deadline = expiry_time - timedelta(seconds=profile["cleanup_timeout_seconds"])
    journal = {**receipt, "document": copy.deepcopy(document), "binding": copy.deepcopy(binding),
               "status": "preparing", "tracks": {}, "errors": [], "producer": None,
               "preparation_deadline_utc": preparation_deadline.isoformat(),
               "verification_deadline_utc": verification_deadline.isoformat()}
    # Collision or an uncertain create consumes this ID; never overwrite another run.
    journal_etag = write(journal_path, journal)
    try:
        _, latest_etag = store.read(LATEST_PATH)
    except FileNotFoundError:
        latest_etag = None
    write(LATEST_PATH, receipt, latest_etag)
    print(f"Native producer run {run.run_id}; lifecycle {journal_path}", flush=True)

    def save():
        nonlocal journal_etag
        journal_etag = write(journal_path, journal, journal_etag)

    def read_manifest():
        raw, _ = store.read(manifest_path)
        return strict_json_loads(raw.decode("utf-8"))

    expected_document = canonical_bytes(document)
    context = {**context, "document": document, "binding": binding, "store": store, "clock": clock,
               "manifest_path": manifest_path, "read_manifest": read_manifest,
               "expires_at_utc": expiry, "preparation_deadline_utc": preparation_deadline.isoformat(),
               "verification_deadline_utc": verification_deadline.isoformat(),
               "cleanup_deadline_utc": expiry}
    attempted = []

    def invoke(name, action, prepared=None):
        assert_lock()
        hook_context = {**context, "document": copy.deepcopy(document), "binding": copy.deepcopy(binding)}
        method = getattr(hooks[name], action)
        result = method(hook_context) if action == "prepare" else method(hook_context, copy.deepcopy(prepared))
        if (canonical_bytes(hook_context["document"]) != expected_document
                or canonical_bytes(hook_context["binding"]) != canonical_bytes(binding)):
            raise RuntimeError(f"{name} {action} changed the immutable runtime configuration")
        status = {"prepare": "ready", "check_ready": "ready", "verify": "verified", "stop": "stopped"}[action]
        if not isinstance(result, dict) or result.get("status") != status:
            raise RuntimeError(f"{name} {action} did not prove {status}")
        canonical_bytes(result)
        return result

    def error(label, exc):
        journal["errors"].append(f"{label}: {type(exc).__name__}: {str(exc)[:1000]}")

    def prepare():
        for name in sorted(selected, key=lambda value: value == "demo-1"):
            if clock() >= preparation_deadline or context["stop_requested"]():
                raise RuntimeError("Native preparation stopped or exceeded its fixed deadline")
            attempted.append(name)
            journal["tracks"][name] = {"prepare": None}
            save()
            journal["tracks"][name]["prepare"] = invoke(name, "prepare")
            save()
        if "demo-1" in selected:
            journal["tracks"]["demo-1"]["source_readiness"] = invoke(
                "demo-1", "check_ready", journal["tracks"]["demo-1"]["prepare"])
            save()
        if (clock() >= preparation_deadline or context["stop_requested"]()
                or clock() + timedelta(seconds=duration) >= verification_deadline):
            raise RuntimeError("Native source no longer fits its prepared admission window")
        journal["status"] = "producing"
        save()

    try:
        result = produce(prepare)
        journal["producer"] = result
        if not isinstance(result, dict) or result.get("verified") is not True:
            raise RuntimeError("Native source or delivery is not verified; reconcile the producer manifest")
        manifest = read_manifest()
        if manifest.get("scheduler", {}).get("status") != "completed":
            raise RuntimeError("Native producer completed without a durable successful scheduler")
        journal["status"] = "verifying"
        save()
        for name in selected:
            if clock() >= verification_deadline or context["stop_requested"]():
                raise RuntimeError("Native verification stopped or exceeded its fixed deadline")
            journal["tracks"][name]["verify"] = invoke(name, "verify", journal["tracks"][name]["prepare"])
            save()
            if clock() >= verification_deadline:
                raise RuntimeError(f"{name} verification exceeded the fixed native deadline")
    except BaseException as exc:
        error("native execution", exc)
    finally:
        # Revocation fences queued per-run admissions before track-specific drain.
        journal["status"] = "stopping"
        try:
            save()
            marker = {"version": 1, "run_id": run.run_id, "nonce": binding["nonce"],
                      "config_sha256": binding["config_sha256"]}
            marker_path = f"{directory}/stop.json"
            try:
                raw, _ = store.read(marker_path)
            except FileNotFoundError:
                write(marker_path, marker)
            else:
                if raw != canonical_bytes(marker):
                    raise RuntimeError("Native stop marker differs; no overwrite was attempted")
            if "demo-3" in selected:
                # Only after produce has returned and released its own ledger.
                with FabricLedger(store, run, selected, selected_targets=document["shared"]["target_bindings"],
                                  fresh_only=False) as ledger:
                    ledger.stop_demo3_admission(native_owner=True)
        except Exception as exc:
            error("native revocation", exc)
        for name in reversed(attempted):
            try:
                journal["tracks"][name]["stop"] = invoke(name, "stop", journal["tracks"][name]["prepare"])
                if clock() >= expiry_time:
                    raise RuntimeError("Native cleanup exceeded its fixed expiry")
                save()
            except Exception as exc:
                error(f"{name} cleanup", exc)
        journal["status"] = "completed" if not journal["errors"] else "blocked"
        try:
            save()
        except Exception as exc:
            error("native journal", exc)
        if not journal["errors"] and not unsafe:
            assert_lock()
            try:
                store.delete(LOCK_PATH, lock_etag)
            except Exception:
                raise RuntimeError("Native outputs verified but launcher lock release is unconfirmed; "
                                   "inspect its exact receipt before another launch") from None
    if journal["errors"] or unsafe:
        raise RuntimeError("Native run blocked; launcher lock retained for reconciliation: " +
                           "; ".join(journal["errors"]))
    return {"run_id": run.run_id, "status": "verified", "lifecycle_path": journal_path,
            "manifest_path": manifest_path, "verification_scope": "selected track hooks and producer delivery"}
