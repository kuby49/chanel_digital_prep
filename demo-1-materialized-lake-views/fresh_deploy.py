"""Fail-closed journal transitions for fresh Demo 1 resource creation."""

import json
import os
import re
import tempfile
import uuid

from ownership import CandidateBlocked, STATE_FILE, _config_fingerprint, _uuid


def _persist(path, journal):
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=path.parent,
                                         prefix=".demo-1-state-", delete=False) as file:
            temporary = file.name
            json.dump(journal, file, indent=2)
            file.write("\n")
            file.flush()
            os.fsync(file.fileno())
        if os.name == "nt":
            import ctypes

            move = ctypes.WinDLL("kernel32", use_last_error=True).MoveFileExW
            move.argtypes = (ctypes.c_wchar_p, ctypes.c_wchar_p, ctypes.c_uint)
            if not move(temporary, str(path), 0x9):  # REPLACE_EXISTING | WRITE_THROUGH
                raise ctypes.WinError(ctypes.get_last_error())
        else:
            os.replace(temporary, path)
            directory = os.open(path.parent, os.O_RDONLY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
        temporary = None
    finally:
        if temporary is not None:
            os.unlink(temporary)


def binding(config, prefix, principal_id, owner):
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,62}", prefix):
        raise ValueError("prefix must be a nonempty safe name")
    tenant = _uuid(config.get("tenant_id"))
    principal_id = _uuid(principal_id)
    fingerprint = _config_fingerprint(config, prefix)
    demo = config["demo-1"]
    if demo.get("workspace_id") is not None or demo.get("lakehouse_id") is not None:
        raise CandidateBlocked("Fresh deployment cannot adopt configured resource IDs.")
    shared = config.get("shared", {})
    if (not isinstance(shared, dict) or not isinstance(shared.get("target_bindings", {}), dict)
            or "demo-1" in shared.get("target_bindings", {})):
        raise CandidateBlocked("Fresh deployment cannot use a configured Demo 1 target binding.")
    name = demo["workspace_name"].replace("{prefix}", prefix) + f"-synthetic-{owner}"
    if len(name) > 256:
        raise ValueError("Generated synthetic Workspace name exceeds 256 characters.")
    return {
        "contract_version": 1, "track": "demo-1", "tenant_id": tenant,
        "prefix": prefix, "config_sha256": fingerprint,
        "owner": {"id": owner, "marker": f"{prefix}-demo1-{owner}",
                  "principal_id": principal_id},
        "expected": {
            "workspace_name": name,
            "lakehouse_name": demo["lakehouse_name"],
            "capacity_id": _uuid(config["capacity"]["id"]),
        },
    }


def begin(config, prefix, state_dir, principal_id):
    """Reserve a new journal exclusively; never reuse an existing journal."""
    journal = {**binding(config, prefix, principal_id, str(uuid.uuid4())),
               "pending_operations": [], "uncertain_operations": []}
    state_dir.mkdir(parents=True, exist_ok=True)
    path = state_dir / STATE_FILE
    try:
        with path.open("x", encoding="utf-8") as file:
            json.dump(journal, file, indent=2)
            file.write("\n")
            file.flush()
            os.fsync(file.fileno())
    except FileExistsError as exc:
        raise CandidateBlocked("Existing Demo 1 journal requires manual recovery.") from exc
    return journal


def record_intent(state_dir, journal, kind, request, *, path):
    """Persist the exact create request before a caller is allowed to POST it."""
    if kind not in ("Workspace", "Lakehouse"):
        raise ValueError("Unsupported resource kind")
    if journal.get("pending_operations") != [] or journal.get("uncertain_operations") != []:
        raise CandidateBlocked("Unresolved operations require manual recovery.")
    if not isinstance(request, dict) or not request:
        raise ValueError("Create request must be a nonempty object")
    expected = journal["expected"]
    expected_name = expected["workspace_name" if kind == "Workspace" else "lakehouse_name"]
    if request.get("displayName") != expected_name or request.get("description") != journal["owner"]["marker"]:
        raise CandidateBlocked("Create request does not match the journal identity.")
    if kind == "Workspace" and request.get("capacityId") != expected["capacity_id"]:
        raise CandidateBlocked("Create request does not match the configured capacity.")
    if kind == "Lakehouse" and journal.get("workspace", {}).get("status") != "confirmed":
        raise CandidateBlocked("Workspace must be confirmed before Lakehouse creation.")
    if kind == "Lakehouse" and request.get("creationPayload") != {"enableSchemas": True}:
        raise CandidateBlocked("Synthetic Lakehouse creation must request schema support.")
    if kind.lower() in journal:
        raise CandidateBlocked(f"{kind} already exists in the journal.")
    intent = {"id": str(uuid.uuid4()), "kind": kind, "request": request, "path": path}
    journal["pending_operations"] = [intent]
    _persist(state_dir / STATE_FILE, journal)
    return intent["id"]


def pending_intent(journal, intent_id):
    pending = journal.get("pending_operations")
    if not isinstance(pending, list) or len(pending) != 1 or pending[0].get("id") != intent_id:
        raise CandidateBlocked("Evidence does not match the pending create intent.")
    return pending[0]


def record_response(state_dir, journal, intent_id, status, body, operation_id=None,
                    location=None, retry_after=None, body_read=False):
    """Durably retain the response while leaving its intent unresolved."""
    intent = pending_intent(journal, intent_id)
    if "response" in intent:
        raise CandidateBlocked("Retained create response cannot be replaced.")
    intent["response"] = {"status": status, "body": body, "operation_id": operation_id,
                          "location": location, "retry_after": retry_after, "body_read": body_read}
    _persist(state_dir / STATE_FILE, journal)


def record_lro_result(state_dir, journal, intent_id, result, operation_state):
    intent = pending_intent(journal, intent_id)
    if "lro_result" in intent:
        raise CandidateBlocked("Retained operation result cannot be replaced.")
    intent["lro_result"] = {"body": result, "operation_state": operation_state}
    _persist(state_dir / STATE_FILE, journal)


def verify_readback(journal, kind, resource_id, readback):
    expected = {
        "id": resource_id, "type": kind, "description": journal["owner"]["marker"],
        "displayName": journal["expected"]["workspace_name" if kind == "Workspace" else "lakehouse_name"],
    }
    if kind == "Workspace":
        expected.update(capacityId=journal["expected"]["capacity_id"],
                        capacityAssignmentProgress="Completed")
    else:
        expected["workspaceId"] = journal["workspace"]["id"]
    if not isinstance(readback, dict) or any(readback.get(key) != value for key, value in expected.items()):
        raise CandidateBlocked(f"{kind} live readback does not match the intent.")
    if kind == "Lakehouse":
        properties = readback.get("properties")
        if not isinstance(properties, dict) or properties.get("defaultSchema") != "dbo":
            raise CandidateBlocked("Lakehouse live readback did not confirm schema support.")


def confirm(state_dir, journal, intent_id, result, readback, operation_state=None):
    """Confirm only an exact create result and independent resource readback."""
    intent = pending_intent(journal, intent_id)
    response = intent.get("response")
    if not isinstance(response, dict) or response.get("status") not in (201, 202):
        raise CandidateBlocked("A successful create response is required.")
    if not isinstance(result, dict) or not isinstance(readback, dict):
        raise CandidateBlocked("Create result and independent readback are required.")
    resource_id = _uuid(result.get("id"))
    if response["status"] == 201 and response.get("body") != result:
        raise CandidateBlocked("Create result differs from the retained response.")
    if response["status"] == 202:
        if (not isinstance(operation_state, dict)
                or operation_state.get("operation_id") != response.get("operation_id")
                or operation_state.get("status") != "Succeeded"
                or intent.get("lro_result") != {"body": result, "operation_state": operation_state}):
            raise CandidateBlocked("Successful matching LRO state is required.")
    kind = intent["kind"]
    request = intent["request"]
    marker = journal["owner"]["marker"]
    if request.get("description") != marker:
        raise CandidateBlocked("Create intent lacks the deployment marker.")
    verify_readback(journal, kind, resource_id, readback)
    if result.get("type") != kind or result.get("displayName") != request.get("displayName"):
        raise CandidateBlocked(f"{kind} creation result is inconsistent.")
    if kind == "Lakehouse" and result.get("workspaceId") != journal["workspace"]["id"]:
        raise CandidateBlocked("Lakehouse creation result has a mismatched parent.")
    record = {
        "status": "confirmed", "type": kind, "id": resource_id,
        "created_by_owner": journal["owner"]["id"], "create_intent_id": intent_id,
        "create_response": result, "accepted_response": response,
        "operation_state": operation_state,
        "create_request": request, "create_path": intent["path"], "service_readback": readback,
        "readback": {"id": resource_id, "type": kind, "name": readback["displayName"],
                     "owner_marker": marker},
    }
    if kind == "Lakehouse":
        record["workspace_id"] = record["readback"]["workspace_id"] = journal["workspace"]["id"]
    journal[kind.lower()] = record
    journal["pending_operations"] = []
    _persist(state_dir / STATE_FILE, journal)
