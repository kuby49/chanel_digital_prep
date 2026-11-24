"""Validate candidate journals and independently check Fabric metadata."""

import hashlib
import json
import os
import re
import shutil
import subprocess
import tempfile
import uuid
from contextlib import contextmanager


STATE_FILE = "demo-1-state.json"
LOCK_FILE = "demo-1-state.lock"
CANDIDATE_FILE = "demo-1-target-candidate.json"
FACT = "silver.sales_joined"


class CandidateBlocked(Exception):
    pass


FABRIC_ORIGIN = "https://api.fabric.microsoft.com"


def _az_json(*args):
    executable = shutil.which("az")
    if not executable:
        raise CandidateBlocked("Azure CLI is required for live Demo 1 ownership readback.")
    try:
        result = subprocess.run(
            [executable, *args, "--output", "json", "--only-show-errors"],
            capture_output=True, text=True, encoding="utf-8", timeout=45, check=False,
        )
    except (OSError, UnicodeError, subprocess.TimeoutExpired) as exc:
        raise CandidateBlocked(f"Azure CLI {args[0]} could not complete live readback.") from exc
    if result.returncode:
        raise CandidateBlocked(f"Azure CLI {args[0]} failed during live ownership readback.")
    try:
        value = json.loads(result.stdout, object_pairs_hook=_unique_pairs)
    except ValueError as exc:
        raise CandidateBlocked("Azure CLI did not return valid ownership metadata.") from exc
    if not isinstance(value, dict):
        raise CandidateBlocked("Azure CLI did not return an ownership metadata object.")
    return value


def _verify_live_provenance(config, tenant, workspace_id, item_id, workspace_name,
                            lakehouse_name, marker):
    account = _az_json("account", "show")
    if account.get("tenantId") != tenant:
        raise CandidateBlocked("Azure CLI tenant does not match the Demo 1 ownership journal.")
    try:
        subscription = _uuid(account.get("id"))
    except ValueError as exc:
        raise CandidateBlocked("Azure CLI account has no usable subscription context.") from exc
    resources = (
        ("Workspace", f"workspaces/{workspace_id}", {
            "id": workspace_id, "displayName": workspace_name,
            "capacityId": config["capacity"]["id"], "capacityAssignmentProgress": "Completed",
        }),
        ("Lakehouse", f"workspaces/{workspace_id}/lakehouses/{item_id}", {
            "id": item_id, "displayName": lakehouse_name, "workspaceId": workspace_id,
        }),
    )
    for kind, path, expected in resources:
        live = _az_json("rest", "--method", "get", "--url", f"{FABRIC_ORIGIN}/v1/{path}",
                        "--subscription", subscription, "--resource", FABRIC_ORIGIN,
                        "--headers", "x-ms-fabric-skill=spark-cli")
        for field, value in {**expected, "type": kind, "description": marker}.items():
            if live.get(field) != value:
                raise CandidateBlocked(f"Live {kind} {field} is absent or mismatched.")
    raise CandidateBlocked(
        "Fabric workspace metadata does not attest the creator or creation operation; "
        "matching descriptions are mutable, and no verified creation provenance source is configured."
    )


def _uuid(value):
    if not isinstance(value, str):
        raise ValueError("expected a canonical nonzero UUID")
    try:
        parsed = uuid.UUID(value)
    except ValueError as exc:
        raise ValueError("expected a canonical nonzero UUID") from exc
    if str(parsed) != value or not parsed.int:
        raise ValueError("expected a canonical nonzero UUID")
    return value


def _unique_pairs(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate key in ownership journal")
        result[key] = value
    return result


def _config_fingerprint(config, prefix):
    demo = config.get("demo-1")
    capacity = config.get("capacity")
    if not isinstance(demo, dict) or not isinstance(capacity, dict):
        raise ValueError("demo-1 and capacity configuration objects are required")
    _uuid(capacity.get("id"))
    if (demo.get("silver_schema"), demo.get("silver_view")) != ("silver", "sales_joined"):
        raise ValueError("Demo 1 target must be silver.sales_joined")
    for field in ("workspace_name", "lakehouse_name"):
        if not isinstance(demo.get(field), str) or not demo[field].strip():
            raise ValueError(f"demo-1.{field} must be a nonempty string")
    if config.get("prefix") not in (None, prefix):
        raise ValueError("config prefix conflicts with --prefix")
    data = {"capacity": capacity, "demo-1": demo, "prefix": prefix}
    encoded = json.dumps(data, sort_keys=True, separators=(",", ":"), ensure_ascii=True,
                         allow_nan=False).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


@contextmanager
def _state_lock(state_dir):
    lock = state_dir / LOCK_FILE
    token = str(uuid.uuid4())
    try:
        with lock.open("x", encoding="utf-8") as file:
            file.write(token + "\n")
            file.flush()
            os.fsync(file.fileno())
    except FileExistsError as exc:
        raise CandidateBlocked("Demo 1 state is locked; stale locks require manual recovery.") from exc
    except OSError as exc:
        raise CandidateBlocked(f"Cannot acquire Demo 1 state lock: {exc}") from exc
    try:
        yield
    finally:
        try:
            if lock.read_text(encoding="utf-8") != token + "\n":
                raise OSError("lock ownership changed")
            lock.unlink()
        except OSError as exc:
            raise CandidateBlocked(f"Cannot safely release Demo 1 state lock: {exc}") from exc


def _resource(record, kind, owner_id, name, marker):
    if not isinstance(record, dict) or record.get("status") != "confirmed":
        raise CandidateBlocked(f"{kind} creation is not confirmed.")
    if record.get("type") != kind or record.get("created_by_owner") != owner_id:
        raise CandidateBlocked(f"{kind} ownership does not match the journal owner.")
    try:
        resource_id = _uuid(record.get("id"))
        _uuid(record.get("create_intent_id"))
    except ValueError as exc:
        raise CandidateBlocked(f"{kind} creation identifiers are invalid.") from exc
    response = record.get("create_response")
    readback = record.get("readback")
    if not isinstance(response, dict) or response.get("id") != resource_id:
        raise CandidateBlocked(f"{kind} creation response is missing or inconsistent.")
    if (not isinstance(readback, dict) or readback.get("id") != resource_id
            or readback.get("type") != kind or readback.get("name") != name
            or readback.get("owner_marker") != marker):
        raise CandidateBlocked(f"{kind} ownership readback is missing or inconsistent.")
    return resource_id


def _publish_candidate(path, candidate):
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=path.parent,
                                         prefix=".demo-1-candidate-", delete=False) as file:
            temporary = file.name
            json.dump(candidate, file, indent=2)
            file.write("\n")
            file.flush()
            os.fsync(file.fileno())
        os.link(temporary, path)
    except FileExistsError as exc:
        raise CandidateBlocked("Target candidate already exists; it was not overwritten.") from exc
    except OSError as exc:
        raise CandidateBlocked(f"Cannot atomically publish target candidate: {exc}") from exc
    finally:
        if temporary is not None:
            os.unlink(temporary)


def resolve_target_candidate(config, prefix, state_dir, result_path):
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,62}", prefix):
        raise ValueError("prefix must be a nonempty safe name")
    tenant = _uuid(config.get("tenant_id"))
    fingerprint = _config_fingerprint(config, prefix)
    if not state_dir.is_dir():
        raise CandidateBlocked("Demo 1 ownership journal directory does not exist.")
    with _state_lock(state_dir):
        try:
            with (state_dir / STATE_FILE).open(encoding="utf-8") as file:
                journal = json.load(file, object_pairs_hook=_unique_pairs)
        except (OSError, ValueError) as exc:
            raise CandidateBlocked(f"Demo 1 ownership journal is missing or invalid: {exc}") from exc
        if (not isinstance(journal, dict) or type(journal.get("contract_version")) is not int
                or journal["contract_version"] != 1 or journal.get("track") != "demo-1"
                or journal.get("tenant_id") != tenant or journal.get("prefix") != prefix
                or journal.get("config_sha256") != fingerprint):
            raise CandidateBlocked("Demo 1 ownership journal identity or configuration does not match.")
        if journal.get("pending_operations") != [] or journal.get("uncertain_operations") != []:
            raise CandidateBlocked("Demo 1 ownership journal has pending or uncertain operations.")
        owner = journal.get("owner")
        try:
            owner_id = _uuid(owner.get("id")) if isinstance(owner, dict) else _uuid(None)
        except ValueError as exc:
            raise CandidateBlocked("Demo 1 ownership identity is missing or invalid.") from exc
        marker = f"{prefix}-demo1-{owner_id}"
        if owner.get("marker") != marker:
            raise CandidateBlocked("Demo 1 ownership marker does not match its owner.")
        demo = config["demo-1"]
        workspace_name = demo["workspace_name"].replace("{prefix}", prefix)
        workspace = journal.get("workspace")
        lakehouse = journal.get("lakehouse")
        workspace_id = _resource(workspace, "Workspace", owner_id, workspace_name, marker)
        item_id = _resource(lakehouse, "Lakehouse", owner_id, demo["lakehouse_name"], marker)
        if (lakehouse.get("workspace_id") != workspace_id
                or lakehouse["readback"].get("workspace_id") != workspace_id):
            raise CandidateBlocked("Lakehouse parent does not match the owned workspace.")
        fact = journal.get("fact")
        if (not isinstance(fact, dict) or fact.get("status") != "confirmed"
                or fact.get("workspace_id") != workspace_id or fact.get("item_id") != item_id
                or fact.get("name") != FACT or fact.get("type") != "MATERIALIZED_LAKE_VIEW"):
            raise CandidateBlocked("Demo 1 fact binding is missing or inconsistent.")
        for field, expected in (("workspace_id", workspace_id), ("lakehouse_id", item_id)):
            if demo.get(field) is not None and demo[field] != expected:
                raise CandidateBlocked(f"Configured {field} conflicts with the owned target.")
        target = {"workspace_id": workspace_id, "item_id": item_id, "fact": FACT}
        shared = config.get("shared", {})
        if not isinstance(shared, dict) or not isinstance(shared.get("target_bindings", {}), dict):
            raise ValueError("shared.target_bindings must be an object")
        bindings = shared.get("target_bindings", {})
        if "demo-1" in bindings and bindings["demo-1"] != target:
            raise CandidateBlocked("Configured shared target binding conflicts with the owned target.")
        _verify_live_provenance(
            config, tenant, workspace_id, item_id, workspace_name, demo["lakehouse_name"], marker,
        )
        candidate = {"contract_version": 1, "track": "demo-1", "tenant_id": tenant,
                     "prefix": prefix, "target": target}
        _publish_candidate(result_path.parent / CANDIDATE_FILE, candidate)
