"""Owned-only Fabric shell provisioning; never creates or adopts a fact table."""

import base64
import json
import os
import shutil
import subprocess
import tempfile
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener

from deploy import CandidateBlocked, STATE_FILE, TRACK, _guid, _load_json, _state_lock


_API = "https://api.fabric.microsoft.com"
_TERMINAL = {"Completed", "Failed", "Cancelled", "Canceled", "Deduped"}


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


class FabricControl:
    def __init__(self, token, principal_id):
        self.token = token
        self.principal_id = _guid(principal_id)
        self.opener = build_opener(_NoRedirect())

    @classmethod
    def from_cli(cls, tenant_id, *, timeout=30):
        try:
            executable = shutil.which("az")
            if executable is None:
                raise OSError("Azure CLI is unavailable")
            response = subprocess.run(
                [executable, "account", "get-access-token", "--resource", _API, "--output", "json"],
                check=True, capture_output=True, timeout=timeout, text=True,
            )
            credential = json.loads(response.stdout)
            token = credential["accessToken"]
            payload = token.split(".")[1]
            claims = json.loads(base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4)))
            if credential["tenant"] != tenant_id or claims["tid"] != tenant_id:
                raise ValueError("tenant mismatch")
            return cls(token, claims["oid"])
        except (OSError, ValueError, KeyError, TypeError, IndexError, subprocess.SubprocessError) as exc:
            raise CandidateBlocked("Active Azure CLI Fabric identity could not be established.") from exc

    def request(self, method, url, body=None, *, timeout=20, skill="spark-cli"):
        if url.startswith("/v1/"):
            url = _API + url
        parsed = urlsplit(url)
        if parsed.scheme != "https" or parsed.netloc != "api.fabric.microsoft.com" or not parsed.path.startswith("/v1/"):
            raise CandidateBlocked("Unexpected Fabric continuation or operation endpoint.")
        payload = None if body is None else json.dumps(body).encode("utf-8")
        request = Request(url, method=method, data=payload, headers={
            "Authorization": "Bearer " + self.token, "Content-Type": "application/json",
            "x-ms-fabric-skill": skill,
        })
        try:
            with self.opener.open(request, timeout=timeout) as response:
                content = response.read()
                return response.status, response.headers, json.loads(content) if content else None
        except HTTPError as exc:
            raise CandidateBlocked(f"Fabric {method} returned HTTP {exc.code}; no automatic retry.") from exc
        except (URLError, OSError, ValueError) as exc:
            raise CandidateBlocked(f"Fabric {method} outcome is unavailable; no automatic retry.") from exc

    def get(self, url, *, skill="spark-cli"):
        status, _, doc = self.request("GET", url, skill=skill)
        if status != 200 or not isinstance(doc, dict):
            raise CandidateBlocked("Fabric GET response is incomplete.")
        return doc

    def list(self, url):
        visited = set()
        while url:
            if not isinstance(url, str) or url in visited or len(visited) >= 100:
                raise CandidateBlocked("Fabric pagination is incomplete.")
            visited.add(url)
            page = self.get(url)
            values = page.get("value")
            if not isinstance(values, list) or not all(isinstance(item, dict) for item in values):
                raise CandidateBlocked("Fabric inventory response is malformed.")
            yield from values
            url = page.get("continuationUri")
            if page.get("continuationToken") and not url:
                raise CandidateBlocked("Fabric inventory pagination is incomplete.")

    def _operation(self, status, headers, doc, expected, *, skill="spark-cli"):
        if status == 202:
            try:
                operation = _guid(headers.get("x-ms-operation-id"))
            except ValueError as exc:
                raise CandidateBlocked("Fabric create did not identify its accepted operation.") from exc
            location = f"/v1/operations/{operation}"
            deadline = time.monotonic() + 180
            while time.monotonic() + 5 < deadline:
                time.sleep(5)
                state = self.get(location, skill=skill).get("status")
                if state == "Succeeded":
                    return self.get(location + "/result", skill=skill)
                if state not in ("NotStarted", "Running"):
                    raise CandidateBlocked("Fabric create operation is not confirmed successful.")
            raise CandidateBlocked("Fabric create operation did not complete within its budget.")
        if status != expected or not isinstance(doc, dict):
            raise CandidateBlocked("Fabric response did not confirm resource identity.")
        return doc

    def create(self, url, body):
        return self._operation(*self.request("POST", url, body), 201)

    def reflex_definition(self, workspace_id, item_id):
        url = f"/v1/workspaces/{workspace_id}/reflexes/{item_id}/getDefinition"
        skill = "activator-cli"
        doc = self._operation(*self.request("POST", url, {}, skill=skill), 200, skill=skill)
        parts = [part for part in doc["definition"]["parts"] if part["path"] == "ReflexEntities.json"]
        if len(parts) != 1 or parts[0].get("payloadType") != "InlineBase64":
            raise CandidateBlocked("Activator rule definition is missing or ambiguous.")
        return json.loads(base64.b64decode(parts[0]["payload"], validate=True))

    def run_recovery_job(self, workspace_id, pipeline_id, subject, event_id, origin, remaining):
        """Submit once to a caller-verified guarded pipeline and wait for terminal success."""
        workspace_id, pipeline_id = _guid(workspace_id), _guid(pipeline_id)
        if origin != "recovery" or event_id != "" or not isinstance(subject, str) or remaining <= 1:
            raise ValueError("invalid recovery job request or remaining budget")
        base = f"/v1/workspaces/{workspace_id}/items/{pipeline_id}/jobs"
        deadline = time.monotonic() + remaining
        status, headers, _ = self.request("POST", f"{base}/Pipeline/instances", {
            "executionData": {"parameters": {"sourceSubject": subject, "eventId": event_id,
                                             "deliveryOrigin": origin}},
        }, timeout=min(20, remaining))
        if status != 202:
            raise CandidateBlocked("Recovery job acceptance is unconfirmed; do not resubmit.")
        expected = f"{_API}{base}/instances/"
        location = headers.get("Location", "")
        if not location.startswith(expected):
            raise CandidateBlocked("Recovery job Location does not identify this pipeline.")
        job_id = _guid(location[len(expected):])
        try:
            interval = max(1, int(headers.get("Retry-After", "5")))
        except ValueError as exc:
            raise CandidateBlocked(f"Recovery job {job_id} has invalid polling metadata.") from exc
        while time.monotonic() + interval < deadline:
            time.sleep(interval)
            budget = deadline - time.monotonic()
            if budget <= 0:
                break
            http, _, job = self.request("GET", location, timeout=min(20, budget))
            if http != 200 or not isinstance(job, dict) or job.get("id") != job_id:
                raise CandidateBlocked(f"Recovery job {job_id} status could not be verified.")
            if job.get("status") == "Completed":
                return job_id
            if job.get("status") not in ("NotStarted", "InProgress"):
                raise CandidateBlocked(f"Recovery job {job_id} is failed or in an unknown state.")
        raise CandidateBlocked(f"Recovery job {job_id} was not terminal before the deadline.")


def _save(path, value):
    fd, temp = tempfile.mkstemp(prefix=".demo3-state-", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as file:
            json.dump(value, file, indent=2)
            file.write("\n")
            file.flush()
            os.fsync(file.fileno())
        os.replace(temp, path)
        if os.name != "nt":
            directory = os.open(path.parent, os.O_RDONLY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
    finally:
        if Path(temp).exists():
            Path(temp).unlink()


def _spec(config, prefix):
    demo = config[TRACK]
    template = demo.get("workspace_name") or "{prefix}-demo3-copy-events"
    if not isinstance(template, str):
        raise ValueError("workspace_name must be a string")
    workspace_name = template.replace("{prefix}", prefix)
    lakehouse_name = demo.get("lakehouse_name") or "Demo3Lakehouse"
    if not isinstance(lakehouse_name, str) or not workspace_name or "{" in workspace_name or "}" in workspace_name:
        raise ValueError("invalid deployment names")
    capacity = config.get("capacity")
    if not isinstance(capacity, dict):
        raise ValueError("capacity.id is required")
    return {
        "tenant_id": _guid(config.get("tenant_id")), "prefix": prefix,
        "capacity_id": _guid(capacity.get("id")),
        "workspace_name": workspace_name, "lakehouse_name": lakehouse_name,
    }


def _readback(api, spec, record, marker, intent_id, parent=None):
    kind, resource_id = record["type"], _guid(record["id"])
    collection = "/v1/workspaces" if kind == "Workspace" else f"/v1/workspaces/{parent}/lakehouses"
    wanted_name = spec["workspace_name" if kind == "Workspace" else "lakehouse_name"]
    expected = {"id": resource_id, "displayName": wanted_name, "type": kind,
                "description": f"{marker}; create-intent={intent_id}"}
    expected.update({"capacityId": spec["capacity_id"]} if kind == "Workspace" else {"workspaceId": parent})
    matches = [item for item in api.list(collection)
               if item.get("displayName", "").casefold() == wanted_name.casefold()]
    if len(matches) != 1 or matches[0].get("id") != resource_id:
        raise CandidateBlocked(f"{kind} does not resolve uniquely to the journaled identity.")
    value = api.get(f"{collection}/{resource_id}")
    if any(value.get(key) != wanted for key, wanted in expected.items()):
        raise CandidateBlocked(f"{kind} authoritative identity, scope, or creation marker changed.")
    evidence = {"id": resource_id, "type": kind, "owner_marker": marker, "create_intent_id": intent_id}
    if parent is not None:
        evidence["workspace_id"] = parent
    return evidence


def _validate_header(state, spec, principal_id):
    if (not isinstance(state, dict) or type(state.get("contract_version")) is not int
            or state["contract_version"] != 1 or state.get("track") != TRACK
            or state.get("owned_creation_version") != 1 or state.get("deployment_spec") != spec
            or state.get("tenant_id") != spec["tenant_id"] or state.get("prefix") != spec["prefix"]
            or state.get("principal_id") != principal_id
            or state.get("pending_operations") != [] or state.get("uncertain_operations") != []):
        raise CandidateBlocked("Journal provenance is missing, pending, uncertain, or does not match this owner.")
    owner = state.get("owner")
    if not isinstance(owner, dict):
        raise CandidateBlocked("Journal owner is missing.")
    owner_id = _guid(owner.get("id"))
    if owner.get("marker") != f"{spec['prefix']}-demo3-{owner_id}":
        raise CandidateBlocked("Journal owner marker differs.")
    return owner_id


def provision(config, prefix, state_dir, api, *, create=False):
    """Create only a new workspace and Lakehouse, or freshly verify their receipts."""
    spec = _spec(config, prefix)
    settings = config[TRACK]
    path = state_dir / STATE_FILE
    with _state_lock(state_dir):
        if path.exists():
            state = _load_json(path)
        else:
            if not create or any(settings.get(key) is not None for key in ("workspace_id", "lakehouse_id")):
                raise CandidateBlocked("No owned journal; explicit or manual targets cannot be adopted.")
            owner_id = str(uuid.uuid4())
            state = {"contract_version": 1, "track": TRACK, "owned_creation_version": 1,
                     "deployment_spec": spec, "tenant_id": spec["tenant_id"], "prefix": prefix,
                     "principal_id": api.principal_id,
                     "owner": {"id": owner_id, "marker": f"{prefix}-demo3-{owner_id}"},
                     "pending_operations": [], "uncertain_operations": []}
        owner_id = _validate_header(state, spec, api.principal_id)
        marker = state["owner"]["marker"]
        parent = None
        capacities = [item for item in api.list("/v1/capacities")
                      if item.get("id") == spec["capacity_id"] and item.get("state") == "Active"]
        if len(capacities) != 1:
            raise CandidateBlocked("Owned provisioning requires the configured capacity to be visible and active.")
        for key, kind, supplied in (("workspace", "Workspace", "workspace_id"),
                                     ("lakehouse", "Lakehouse", "lakehouse_id")):
            record = state.get(key)
            name = spec[key + "_name"]
            collection = "/v1/workspaces" if parent is None else f"/v1/workspaces/{parent}/lakehouses"
            if record is None:
                if not create or settings.get(supplied) is not None:
                    raise CandidateBlocked(f"{kind} is not recorded as owned.")
                if any(item.get("displayName", "").casefold() == name.casefold()
                       for item in api.list(collection)):
                    raise CandidateBlocked(f"{kind} name already exists; adoption is prohibited.")
                intent = str(uuid.uuid4())
                state["pending_operations"] = [{"type": kind, "intent_id": intent, "parent": parent,
                                                "display_name": name}]
                _save(path, state)
                body = {"displayName": name, "description": f"{marker}; create-intent={intent}"}
                if parent is None:
                    body["capacityId"] = spec["capacity_id"]
                else:
                    body["creationPayload"] = {"enableSchemas": True}
                response = api.create(collection, body)
                resource_id = _guid(response.get("id"))
                state["pending_operations"][0]["response_id"] = resource_id
                _save(path, state)
                record = {"id": resource_id, "type": kind, "created_by_owner": owner_id,
                          "create_intent_id": intent, "status": "confirmed"}
                if parent is not None:
                    record["workspace_id"] = parent
                record["readback"] = _readback(api, spec, record, marker, intent, parent)
                state[key] = record
                state["pending_operations"] = []
                _save(path, state)
            else:
                intent = _guid(record.get("create_intent_id"))
                if (record.get("created_by_owner") != owner_id or record.get("status") != "confirmed"
                        or record.get("type") != kind or record.get("workspace_id") != parent):
                    raise CandidateBlocked(f"{kind} has no owned creation receipt.")
                fresh = _readback(api, spec, record, marker, intent, parent)
                if fresh != record.get("readback"):
                    raise CandidateBlocked(f"{kind} journal receipt differs from authoritative readback.")
            if settings.get(supplied) not in (None, record["id"]):
                raise CandidateBlocked(f"Configured {supplied} conflicts with the owned target.")
            parent = record["id"]
        return state


def observe_quiescence(api, workspace_id, probe_items, *, window_seconds, sleep=time.sleep,
                       now=lambda: datetime.now(timezone.utc)):
    """Observe known items and role assignments twice, never claim a writer fence."""
    workspace_id = _guid(workspace_id)
    if type(window_seconds) is not int or not 1 <= window_seconds <= 900:
        raise ValueError("invalid inventory observation window")
    permitted = {key: _guid(value) for key, value in probe_items.items()}
    if len(set(permitted.values())) != len(permitted):
        raise ValueError("inventory item IDs must be distinct")
    base = f"/v1/workspaces/{workspace_id}"
    started = now()

    def snapshot():
        items = list(api.list(f"{base}/items?recursive=true"))
        actual = {(item["type"], item["displayName"]): item["id"] for item in items}
        if len(actual) != len(items) or actual != permitted:
            raise CandidateBlocked("Workspace inventory includes missing, unknown, or ambiguous items.")
        roles = list(api.list(f"{base}/roleAssignments"))
        unexamined = []
        for item in items:
            kind, item_id = item["type"], item["id"]
            if kind == "Reflex":
                entities = api.reflex_definition(workspace_id, item_id)
                if not isinstance(entities, list):
                    raise CandidateBlocked("Activator rule inventory is malformed.")
                for entity in entities:
                    if entity["type"] == "timeSeriesView-v1":
                        definition = entity["payload"]["definition"]
                        if definition["type"] == "Rule" and definition["settings"]["shouldRun"] is not False:
                            raise CandidateBlocked("Activator rule is not disabled.")
                continue
            if kind in ("Lakehouse", "SQLEndpoint"):
                continue
            job_type = {"DataPipeline": "Pipeline", "Notebook": "RunNotebook"}.get(kind)
            if job_type is None:
                unexamined.append(kind)
                continue
            root = f"{base}/items/{item_id}/jobs"
            for schedule in api.list(f"{root}/{job_type}/schedules"):
                if schedule.get("enabled") is not False:
                    raise CandidateBlocked("An owned job schedule is enabled or cannot be verified.")
            for job in api.list(f"{root}/instances?jobType={job_type}"):
                if job.get("status") not in _TERMINAL:
                    raise CandidateBlocked("An owned job has not reached a known terminal state.")
                if not isinstance(job.get("endTimeUtc"), str):
                    raise CandidateBlocked("Terminal job does not include an end time.")
                finished = datetime.fromisoformat(job["endTimeUtc"].replace("Z", "+00:00"))
                # Fabric's endTimeUtc response can omit its UTC offset.
                if finished.tzinfo is None:
                    finished = finished.replace(tzinfo=timezone.utc)
                if finished >= started:
                    raise CandidateBlocked("Owned job activity overlaps the observation window.")
        return (sorted(json.dumps(item, sort_keys=True) for item in roles), sorted(unexamined))

    before = snapshot()
    sleep(window_seconds)
    if snapshot() != before:
        raise CandidateBlocked("Workspace inventory or role assignments changed during the observation window.")
    ended = now()
    if (ended - started).total_seconds() < window_seconds:
        raise CandidateBlocked("Inventory observation window was incomplete.")
    return {"start_utc": started.isoformat(), "end_utc": ended.isoformat(),
            "observed_quiescent": not before[1], "checked_jobs_and_rules_quiescent": True,
            "unexamined_item_types": before[1], "writer_fence": False}
