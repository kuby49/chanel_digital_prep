"""Journaled creation of isolated item shells; no jobs or rule updates.

Nothing runs on import. The caller supplies the authenticated API and explicit
approval; reconciliation never submits a create request.
"""

import hashlib
import json
import uuid
from pathlib import Path

import activator
import customer_preflight as original
import isolated_preflight
import native_binding
import ownership
from deploy import CandidateBlocked, _guid, _load_json, _state_lock


JOURNAL = "demo3-isolated-items-v1.json"
KINDS = {"setup_notebook": "Notebook", "guard_notebook": "Notebook",
         "pipeline": "DataPipeline", "reflex": "Reflex"}
ORDER = tuple(KINDS)
ORIGINAL_IDS = {original.LAKEHOUSE, original.NOTEBOOK, original.PIPELINE,
                original.REFLEX, original.CONNECTION, original.STAGING_LAKEHOUSE,
                isolated_preflight._PIPELINE, isolated_preflight._NOTEBOOK}


def event_sample_sha256(sample):
    """SHA-256 of UTF-8 JSON with sorted keys, compact separators and no NaN."""
    data = json.dumps(sample, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False, allow_nan=False).encode("utf-8")
    return hashlib.sha256(data).hexdigest()


def _skill(key):
    return "activator-cli" if key == "reflex" else "spark-cli"


def _context(config, prefix, state_dir, api):
    state = ownership.provision(config, prefix, state_dir, api, create=False)
    workspace = _guid(state["workspace"]["id"])
    lakehouse = _guid(state["lakehouse"]["id"])
    owner = _guid(state["owner"]["id"])
    marker = state["owner"]["marker"]
    principal = _guid(api.principal_id)
    capacity = _guid(state["deployment_spec"]["capacity_id"])
    if (lakehouse in ORIGINAL_IDS or workspace in {original.WORKSPACE, original.STAGING_WORKSPACE,
                                                    isolated_preflight._WORKSPACE}
            or marker == original.MARKER or marker != f"{prefix}-demo3-{owner}"):
        raise CandidateBlocked("Original or inconsistent target ownership is prohibited.")
    isolated_preflight.original_parent(api)
    isolated_preflight.require_f32(api, capacity)
    return {"version": 1, "target_workspace_id": workspace, "target_lakehouse_id": lakehouse,
            "owner_id": owner, "marker": marker, "principal_id": principal}, capacity


def _load(path, header):
    if not path.exists():
        return {**header, "pending": None, "assets": {}}
    state = _load_json(path)
    if (not isinstance(state, dict) or type(state.get("version")) is not int
            or any(state.get(k) != v for k, v in header.items())
            or not isinstance(state.get("assets"), dict)
            or set(state["assets"]) - set(KINDS)
            or "pending" not in state):
        raise CandidateBlocked("Isolated item journal does not match the owned target.")
    ids, names = set(), set()
    for key, record in state["assets"].items():
        if not isinstance(record, dict):
            raise CandidateBlocked("Invalid item receipt.")
        item_id, intent = _guid(record.get("id")), _guid(record.get("intent_id"))
        name = record.get("name")
        if (item_id in ORIGINAL_IDS or item_id in {header["target_lakehouse_id"],
                                                     header["target_workspace_id"]}
                or item_id in ids or not isinstance(name, str) or not name.strip()
                or name.casefold() in names or record.get("type") != KINDS[key]
                or record.get("status") != "confirmed"
                or record.get("description") != f"{header['marker']}; create-intent={intent}"):
            raise CandidateBlocked("Invalid or conflicting item receipt.")
        ids.add(item_id)
        names.add(name.casefold())
        if key == "reflex":
            digest = record.get("event_sample_sha256")
            if not isinstance(digest, str) or len(digest) != 64 or any(c not in "0123456789abcdef" for c in digest):
                raise CandidateBlocked("Reflex event evidence is missing.")
    if any(key in state["assets"] and any(p not in state["assets"] for p in ORDER[:ORDER.index(key)])
           for key in ORDER):
        raise CandidateBlocked("Item creation receipts are out of order.")
    return state


def _collection(workspace, key):
    kind = "reflexes" if key == "reflex" else "items"
    return f"/v1/workspaces/{workspace}/{kind}"


def _inventory(api, workspace, key):
    return list(original._inventory(api, _collection(workspace, key), _skill(key)))


def _verify(api, workspace, record, key):
    items = _inventory(api, workspace, key)
    matches = [item for item in items if isinstance(item, dict)
               and isinstance(item.get("displayName"), str)
               and item["displayName"].casefold() == record["name"].casefold()]
    if len(matches) != 1 or matches[0].get("id") != record["id"]:
        raise CandidateBlocked("Item name does not resolve uniquely to its receipt.")
    item = api.get(f"{_collection(workspace, key)}/{record['id']}", skill=_skill(key))
    expected = {"id": record["id"], "displayName": record["name"],
                "type": record["type"], "description": record["description"]}
    if (not isinstance(item, dict) or any(item.get(k) != v for k, v in expected.items())
            or item.get("workspaceId", workspace) != workspace):
        raise CandidateBlocked("Item authoritative readback differs from its receipt.")


def _pending(state):
    record = state.get("pending")
    if not isinstance(record, dict) or record.get("key") not in KINDS:
        raise CandidateBlocked("No valid pending creation intent.")
    key = record["key"]
    intent = _guid(record.get("intent_id"))
    name = record.get("name")
    if (key in state["assets"] or not isinstance(name, str) or not name.strip()
            or record.get("type") != KINDS[key]
            or record.get("description") != f"{state['marker']}; create-intent={intent}"
            or any(previous not in state["assets"] for previous in ORDER[:ORDER.index(key)])
            or any(name.casefold() == r["name"].casefold() for r in state["assets"].values())):
        raise CandidateBlocked("Pending creation intent is inconsistent.")
    if key == "reflex":
        digest = record.get("event_sample_sha256")
        if not isinstance(digest, str) or len(digest) != 64 or any(c not in "0123456789abcdef" for c in digest):
            raise CandidateBlocked("Pending Reflex event evidence is missing.")
    return record


def _persist(path, state, capacity, api):
    # Once Fabric accepts a request, preserve its receipt even if capacity changes.
    (native_binding._save if path.exists() else native_binding._create)(path, state)


def _finish(api, path, state, capacity):
    pending = _pending(state)
    key, skill = pending["key"], _skill(pending["key"])
    workspace = state["target_workspace_id"]
    operation = pending.get("operation_id")
    location = pending.get("operation_location")
    if operation is not None or location is not None:
        operation, location = native_binding._operation_location(
            {"x-ms-operation-id": operation, "location": location})
        native_binding._poll(api, location, operation, skill)
        status, _, body = native_binding._operation_get(api, location + "/result", skill, 20)
        if status != 200 or not isinstance(body, dict):
            raise CandidateBlocked("Create operation result unavailable; reconcile without retry.")
        result_id = _guid(body.get("id"))
        if pending.get("response_id") not in (None, result_id):
            raise CandidateBlocked("Create operation identity changed.")
        pending["response_id"] = result_id
        _persist(path, state, capacity, api)
    if pending.get("response_id") is None:
        matches = [item for item in _inventory(api, workspace, key)
                   if isinstance(item, dict) and item.get("displayName") == pending["name"]
                   and item.get("type") == pending["type"]
                   and item.get("description") == pending["description"]]
        if len(matches) != 1:
            raise CandidateBlocked("Creation outcome remains ambiguous; never resubmit.")
        pending["response_id"] = _guid(matches[0].get("id"))
        _persist(path, state, capacity, api)
    item_id = _guid(pending["response_id"])
    if (item_id in ORIGINAL_IDS or item_id in {state["target_lakehouse_id"],
                                               state["target_workspace_id"]}
            or any(r["id"] == item_id for r in state["assets"].values())):
        raise CandidateBlocked("Create result is not a distinct new item.")
    record = {k: pending[k] for k in ("name", "type", "intent_id", "description")}
    record.update(id=item_id, status="confirmed")
    if key == "reflex":
        record["event_sample_sha256"] = pending["event_sample_sha256"]
    _verify(api, workspace, record, key)
    state["assets"][key] = record
    state["pending"] = None
    _persist(path, state, capacity, api)
    return record


def create_item(config, prefix, state_dir, api, key, name, *, approved=False,
                event_sample=None, event_observed=False):
    """Create one shell in order; Reflex needs an actually observed target event."""
    if approved is not True:
        raise CandidateBlocked("Explicit approval is required.")
    if key not in KINDS or not isinstance(name, str) or not name.strip() or name != name.strip():
        raise ValueError("Invalid asset key or name.")
    state_dir = Path(state_dir)
    header, capacity = _context(config, prefix, state_dir, api)
    path = state_dir / JOURNAL
    with _state_lock(state_dir):
        state = _load(path, header)
        if state["pending"] is not None:
            raise CandidateBlocked("Pending create must be reconciled, never retried.")
        if key in state["assets"]:
            raise CandidateBlocked("Asset already has an immutable receipt.")
        if any(previous not in state["assets"] for previous in ORDER[:ORDER.index(key)]):
            raise CandidateBlocked("Earlier isolated shells must be confirmed first.")
        for existing_key, record in state["assets"].items():
            _verify(api, header["target_workspace_id"], record, existing_key)
        if any(record["name"].casefold() == name.casefold() for record in state["assets"].values()):
            raise CandidateBlocked("Item names must be distinct.")
        if key == "reflex":
            if event_observed is not True:
                raise CandidateBlocked("A real target event must be explicitly confirmed observed.")
            activator.validate_event(event_sample)
            activator.build_native_definition(config["tenant_id"], header["target_workspace_id"],
                                              header["target_lakehouse_id"],
                                              state["assets"]["pipeline"]["id"], event_sample)
        skill = _skill(key)
        if any(isinstance(item, dict) and isinstance(item.get("displayName"), str)
               and item["displayName"].casefold() == name.casefold()
               for item in _inventory(api, header["target_workspace_id"], key)):
            raise CandidateBlocked("Existing unjournaled item cannot be adopted.")
        intent = str(uuid.uuid4())
        pending = {"key": key, "name": name, "type": KINDS[key], "intent_id": intent,
                   "description": f"{header['marker']}; create-intent={intent}"}
        if key == "reflex":
            pending["event_sample_sha256"] = event_sample_sha256(event_sample)
        state["pending"] = pending
        _persist(path, state, capacity, api)

    # provision() takes the same lock, so the fresh check must run outside it.
    fresh_header, fresh_capacity = _context(config, prefix, state_dir, api)
    if fresh_header != header or fresh_capacity != capacity:
        raise CandidateBlocked("Owned target changed after the creation intent.")
    with _state_lock(state_dir):
        current = _load(path, header)
        if current != state:
            raise CandidateBlocked("Creation journal changed during ownership recheck.")
        state = current
        pending = _pending(state)
        isolated_preflight.require_f32(api, capacity)
        body = {"displayName": name, "description": pending["description"]}
        if key != "reflex":
            body["type"] = KINDS[key]
        try:
            status, headers, body = api.request(
                "POST", _collection(header["target_workspace_id"], key), body,
                skill=skill)
            if status == 202:
                operation, location = native_binding._operation_location(headers)
                pending.update(operation_id=operation, operation_location=location)
            elif status == 201 and isinstance(body, dict):
                pending["response_id"] = _guid(body.get("id"))
            else:
                raise ValueError("Create response is not an authoritative acceptance.")
        except (CandidateBlocked, OSError, TimeoutError, ValueError, TypeError, KeyError) as exc:
            raise CandidateBlocked("Create outcome uncertain; reconcile without retry.") from exc
        _persist(path, state, capacity, api)
        return _finish(api, path, state, capacity)


def reconcile(config, prefix, state_dir, api, *, approved=False):
    """Resolve a durable pending intent using only reads; never submit a create."""
    if approved is not True:
        raise CandidateBlocked("Explicit approval is required to update the journal.")
    state_dir = Path(state_dir)
    header, capacity = _context(config, prefix, state_dir, api)
    path = state_dir / JOURNAL
    with _state_lock(state_dir):
        if not path.exists():
            raise CandidateBlocked("No item journal to reconcile.")
        state = _load(path, header)
        for key, record in state["assets"].items():
            _verify(api, header["target_workspace_id"], record, key)
        return _finish(api, path, state, capacity)
