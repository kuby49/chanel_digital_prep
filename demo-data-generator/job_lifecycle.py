"""Durable, fail-closed primitives for a single owned Fabric pipeline job.

The measured caller must supply a fresh admission check and reconcile deliveries.
"""

import hashlib
import json
import os
import re
import time
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlsplit

from fabric_runtime import MAX_ACTIVITY_SECONDS, STARTUP_HEADROOM_SECONDS
from generator import strict_json_loads


TERMINAL = frozenset(("Completed", "Failed", "Cancelled", "Deduped"))
ACTIVE = frozenset(("NotStarted", "InProgress"))
API = "https://api.fabric.microsoft.com"
JOURNAL_DIR_NAME = "producer-jobs-v1"


class LifecycleBlocked(RuntimeError):
    pass


def _uuid(value):
    try:
        parsed = uuid.UUID(value)
    except (ValueError, TypeError, AttributeError):
        raise LifecycleBlocked("Invalid job identity; termination unconfirmed.") from None
    if not parsed.int or str(parsed) != value:
        raise LifecycleBlocked("Invalid job identity; termination unconfirmed.")
    return value


def _canonical(value):
    try:
        return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                          allow_nan=False).encode("utf-8")
    except (TypeError, ValueError, UnicodeError):
        raise LifecycleBlocked("Invalid immutable job binding.") from None


def _utc():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


@contextmanager
def _lock(path):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+b") as file:
        file.seek(0, os.SEEK_END)
        if file.tell() == 0:
            file.write(b"\0")
            file.flush()
        file.seek(0)
        try:
            if os.name == "nt":
                import msvcrt
                msvcrt.locking(file.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            raise LifecycleBlocked("Another lifecycle operation owns this run.") from None
        try:
            yield
        finally:
            file.seek(0)
            if os.name == "nt":
                msvcrt.locking(file.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                fcntl.flock(file.fileno(), fcntl.LOCK_UN)


def _write(path, value):
    sibling = path.with_name("." + path.name + "." + uuid.uuid4().hex)
    try:
        with sibling.open("x", encoding="utf-8", newline="\n") as file:
            json.dump(value, file, sort_keys=True, ensure_ascii=False, allow_nan=False)
            file.write("\n")
            file.flush()
            os.fsync(file.fileno())
        os.replace(sibling, path)
        if os.name != "nt":
            fd = os.open(str(path.parent), os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
    finally:
        sibling.unlink(missing_ok=True)


def _location(headers, workspace, item):
    location = next((v for k, v in headers.items() if k.lower() == "location"), None)
    if not isinstance(location, str):
        raise LifecycleBlocked("Submission identity missing; acceptance is ambiguous.")
    parts = urlsplit(location)
    prefix = f"/v1/workspaces/{workspace}/items/{item}/jobs/instances/"
    if (parts.scheme != "https" or parts.netloc != "api.fabric.microsoft.com"
            or parts.query or parts.fragment or not parts.path.startswith(prefix)):
        raise LifecycleBlocked("Submission identity does not match the owned pipeline; acceptance is ambiguous.")
    return _uuid(parts.path[len(prefix):])


def _delay(headers):
    value = next((v for k, v in headers.items() if k.lower() == "retry-after"), "1")
    if not isinstance(value, str) or not re.fullmatch(r"[0-9]{1,9}", value):
        raise LifecycleBlocked("Invalid Fabric Retry-After; termination unconfirmed.")
    return max(1, int(value))


def _read_journal(path):
    try:
        with path.open("rb") as file:
            raw = file.read(1024 * 1024 + 1)
        if len(raw) > 1024 * 1024:
            raise ValueError("job journal too large")
        return strict_json_loads(raw.decode("utf-8"))
    except (OSError, UnicodeError, ValueError, RecursionError):
        raise LifecycleBlocked("Owned job journal missing or invalid; termination unconfirmed.") from None


def _journal_time(value):
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.utcoffset() is None or parsed.utcoffset().total_seconds() != 0:
            raise ValueError
        return parsed
    except (ValueError, TypeError, AttributeError):
        raise LifecycleBlocked("Owned job journal timestamp is invalid; termination unconfirmed.") from None


def _service_time(value):
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.utcoffset() not in (None, timedelta(0)):
            raise ValueError
        return parsed.replace(tzinfo=timezone.utc)
    except (ValueError, TypeError, AttributeError):
        raise LifecycleBlocked("Pipeline readback lacks a valid UTC activity window.") from None


def activity_timeout(max_duration_seconds):
    if (type(max_duration_seconds) is not int or max_duration_seconds <= 0
            or max_duration_seconds + STARTUP_HEADROOM_SECONDS > MAX_ACTIVITY_SECONDS):
        raise LifecycleBlocked("Producer duration exceeds the supported pipeline activity timeout.")
    total = max_duration_seconds + STARTUP_HEADROOM_SECONDS
    days, remainder = divmod(total, 86400)
    hours, remainder = divmod(remainder, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{days}.{hours:02}:{minutes:02}:{seconds:02}"


class PipelineJob:
    """Journal submission/cancellation and read the exact pipeline job.

    The caller must independently verify deployment ownership and readiness.
    No pipeline status implies child-job termination or reconciled deliveries.
    """

    def __init__(self, directory, binding, client, *, monotonic=time.monotonic, sleep=time.sleep):
        if not isinstance(binding, dict):
            raise LifecycleBlocked("Invalid immutable job binding.")
        expected = {"run_id", "nonce", "tenant_id", "prefix", "config_sha256", "selected_demos",
                    "workspace_id", "pipeline_id", "notebook_id", "manifest_uri", "deadline_utc"}
        if set(binding) != expected:
            raise LifecycleBlocked("Invalid immutable job binding.")
        for field in ("tenant_id", "workspace_id", "pipeline_id", "notebook_id"):
            _uuid(binding[field])
        if (not isinstance(binding["run_id"], str)
                or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}", binding["run_id"])
                or not isinstance(binding["nonce"], str) or not binding["nonce"]
                or not isinstance(binding["prefix"], str) or not binding["prefix"]
                or not isinstance(binding["manifest_uri"], str) or not binding["manifest_uri"]
                or not isinstance(binding["config_sha256"], str)
                or not re.fullmatch(r"[0-9a-f]{64}", binding["config_sha256"])
                or not isinstance(binding["selected_demos"], list)
                or not binding["selected_demos"]
                or any(not isinstance(d, str) for d in binding["selected_demos"])
                or binding["selected_demos"] != sorted(set(binding["selected_demos"]))
                or any(d not in ("demo-1", "demo-2", "demo-3") for d in binding["selected_demos"])):
            raise LifecycleBlocked("Invalid immutable job binding.")
        try:
            parsed = datetime.fromisoformat(binding["deadline_utc"].replace("Z", "+00:00"))
            if parsed.utcoffset() is None or parsed.utcoffset().total_seconds() != 0:
                raise ValueError
        except (ValueError, TypeError, AttributeError):
            raise LifecycleBlocked("Invalid immutable job deadline.") from None
        self.binding = json.loads(_canonical(binding))
        digest = hashlib.sha256(binding["run_id"].encode("utf-8")).hexdigest()
        self.directory = Path(directory) / JOURNAL_DIR_NAME
        self.path = self.directory / (digest + ".json")
        self.lock_path = self.directory / (digest + ".lock")
        self.client = client
        self.monotonic = monotonic
        self.sleep = sleep

    def _read(self):
        value = _read_journal(self.path)
        if (not isinstance(value, dict) or type(value.get("version")) is not int or value["version"] != 1
                or _canonical(value.get("binding")) != _canonical(self.binding) or value.get("phase") not in
                ("submitting", "submitted", "cancelling", "cancel_requested")):
            raise LifecycleBlocked("Owned job journal binding or phase mismatch; termination unconfirmed.")
        expected = {"version", "binding", "phase", "payload_sha256", "intent_at_utc"}
        if "payload_binding_version" in value:
            if type(value["payload_binding_version"]) is not int or value["payload_binding_version"] != 1:
                raise LifecycleBlocked("Unsupported job payload binding; termination unconfirmed.")
            expected.add("payload_binding_version")
        if value["phase"] != "submitting":
            expected.update(("job_id", "submission_location", "submitted_at_utc", "poll_not_before_utc"))
        if value["phase"] in ("cancelling", "cancel_requested"):
            expected.add("cancel_intent_at_utc")
        if (set(value) != expected or not isinstance(value.get("payload_sha256"), str)
                or not re.fullmatch(r"[0-9a-f]{64}", value["payload_sha256"])):
            raise LifecycleBlocked("Owned job journal is incomplete or invalid; termination unconfirmed.")
        intent = _journal_time(value["intent_at_utc"])
        if value["phase"] != "submitting":
            submitted = _journal_time(value["submitted_at_utc"])
            if submitted < intent:
                raise LifecycleBlocked("Owned job journal timestamps disagree; termination unconfirmed.")
            if value["poll_not_before_utc"] is not None:
                _journal_time(value["poll_not_before_utc"])
        if value["phase"] in ("cancelling", "cancel_requested"):
            _journal_time(value["cancel_intent_at_utc"])
        return value

    @classmethod
    def recover(cls, directory, expected, client=None):
        """Recover a journal only when all caller-known immutable fields match."""
        fields = {"run_id", "tenant_id", "prefix", "config_sha256", "selected_demos",
                  "workspace_id", "pipeline_id", "notebook_id", "manifest_uri"}
        if (not isinstance(expected, dict) or set(expected) != fields
                or not isinstance(expected.get("run_id"), str)
                or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}", expected["run_id"])):
            raise LifecycleBlocked("Invalid recovery binding; termination unconfirmed.")
        name = hashlib.sha256(expected["run_id"].encode("ascii")).hexdigest()
        root = Path(directory) / JOURNAL_DIR_NAME
        with _lock(root / (name + ".lock")):
            state = _read_journal(root / (name + ".json"))
            if not isinstance(state, dict) or not isinstance(state.get("binding"), dict):
                raise LifecycleBlocked("Owned job journal binding is invalid; termination unconfirmed.")
            binding = state["binding"]
            if any(_canonical(binding.get(field)) != _canonical(value) for field, value in expected.items()):
                raise LifecycleBlocked("Job journal differs from the original run or configuration; termination unconfirmed.")
            job = cls(directory, binding, client)
            state = job._read()
            if state["phase"] == "submitting":
                raise LifecycleBlocked("Submission outcome unknown; termination unconfirmed.")
            if state.get("payload_binding_version") != 1:
                raise LifecycleBlocked("Legacy job parameters were not bound to the journal; termination unconfirmed.")
            job._job_path(state)
            return job

    def _validate_payload(self, payload):
        parameters = payload.get("executionData") if isinstance(payload, dict) else None
        if not isinstance(payload, dict) or set(payload) != {"executionData"} or not isinstance(parameters, dict):
            raise LifecycleBlocked("Only explicitly selected no-sales preflight jobs are supported.")
        if set(parameters) != {"parameters"} or not isinstance(parameters["parameters"], dict):
            raise LifecycleBlocked("Only explicitly selected no-sales preflight jobs are supported.")
        parameters = parameters["parameters"]
        keys = {"operation", "probe_id", "runtime_evidence_json", "run_config_json", "selected_demos_json"}
        operation = parameters.get("operation")
        if operation == "run-data":
            keys.add("activity_timeout")
        if (set(parameters) != keys or any(not isinstance(value, str) for value in parameters.values())
                or operation not in ("preflight", "run-data")
                or not re.fullmatch(r"[a-z0-9][a-z0-9-]{15,63}", parameters["probe_id"])
                or parameters["probe_id"] != self.binding["nonce"]):
            raise LifecycleBlocked("Only bound no-sales preflight or measured run jobs are supported.")
        try:
            config = strict_json_loads(parameters["run_config_json"])
            selected = strict_json_loads(parameters["selected_demos_json"])
            request = strict_json_loads(parameters["runtime_evidence_json"])
        except (ValueError, RecursionError):
            raise LifecycleBlocked("Pipeline parameters contain invalid JSON.") from None
        if (not isinstance(config, dict) or not isinstance(config.get("data"), dict)
                or config["data"].get("run_id") != self.binding["run_id"]
                or hashlib.sha256(_canonical(config)).hexdigest() != self.binding["config_sha256"]
                or not isinstance(selected, list) or any(not isinstance(item, str) for item in selected)
                or len(set(selected)) != len(selected)
                or sorted(selected) != self.binding["selected_demos"]
                or not isinstance(request, dict)):
            raise LifecycleBlocked("Pipeline parameters differ from the immutable job binding.")
        if request:
            producer = request.get("producer")
            if (request.get("nonce") != self.binding["nonce"] or request.get("run_id") != self.binding["run_id"]
                    or request.get("tenant_id") != self.binding["tenant_id"] or not isinstance(producer, dict)
                    or producer.get("workspace_id") != self.binding["workspace_id"]
                    or producer.get("notebook_id") != self.binding["notebook_id"]):
                raise LifecycleBlocked("Runtime evidence request differs from the immutable job binding.")
        if operation == "run-data":
            shared = config.get("shared")
            producer = request.get("producer")
            fields = {"nonce", "run_id", "config_sha256", "tenant_id", "identity_id", "producer"}
            if (set(request) != fields or not isinstance(shared, dict)
                    or not isinstance(producer, dict)
                    or set(producer) != {"workspace_id", "staging_lakehouse_id", "notebook_id"}
                    or request["config_sha256"] != self.binding["config_sha256"]
                    or request["identity_id"] != shared.get("identity_id")
                    or producer["workspace_id"] != shared.get("workspace_id")
                    or producer["staging_lakehouse_id"] != shared.get("staging_lakehouse_id")
                    or (shared.get("producer_notebook_id") is not None
                        and producer["notebook_id"] != shared["producer_notebook_id"])
                    or parameters["activity_timeout"] != activity_timeout(config["data"].get("max_duration_seconds"))):
                raise LifecycleBlocked("Measured pipeline parameters differ from the immutable job binding.")
            lakehouse = _uuid(producer["staging_lakehouse_id"])
            _uuid(request["identity_id"])
            expected_uri = (f"https://onelake.blob.fabric.microsoft.com/{self.binding['workspace_id']}/{lakehouse}/"
                            f"Files/producer/runs/{self.binding['run_id']}/manifest.json")
            if self.binding["manifest_uri"] != expected_uri:
                raise LifecycleBlocked("Measured manifest URI differs from the owned producer binding.")
        return operation

    def submit(self, payload, *, admission=None):
        """Submit once; an uncertain POST or failed journal write is never retried."""
        with _lock(self.lock_path):
            if self.path.exists():
                state = self._read()
                if state["phase"] == "submitting":
                    raise LifecycleBlocked("Submission outcome unknown; reconcile without retrying.")
                raise LifecycleBlocked("Run already submitted; resume its exact owned job instead.")
            operation = self._validate_payload(payload)
            if datetime.now(timezone.utc) >= datetime.fromisoformat(
                    self.binding["deadline_utc"].replace("Z", "+00:00")):
                raise LifecycleBlocked("Run deadline already expired; no job was submitted.")
            if operation == "run-data":
                if not callable(admission):
                    raise LifecycleBlocked("Measured submission requires fresh producer and destination admission.")
                admission()
                if datetime.now(timezone.utc) >= datetime.fromisoformat(
                        self.binding["deadline_utc"].replace("Z", "+00:00")):
                    raise LifecycleBlocked("Run deadline expired during admission; no job was submitted.")
            state = {"version": 1, "payload_binding_version": 1, "binding": self.binding, "phase": "submitting",
                     "payload_sha256": hashlib.sha256(_canonical(payload)).hexdigest(),
                     "intent_at_utc": _utc()}
            _write(self.path, state)
            workspace, item = self.binding["workspace_id"], self.binding["pipeline_id"]
            remaining = (datetime.fromisoformat(self.binding["deadline_utc"].replace("Z", "+00:00"))
                         - datetime.now(timezone.utc)).total_seconds()
            if remaining <= 0:
                raise LifecycleBlocked("Run deadline expired before submission; reconcile the persisted intent.")
            status, headers, _ = self.client.request(
                "POST", f"/v1/workspaces/{workspace}/dataPipelines/{item}/jobs/execute/instances", payload,
                timeout=min(30, remaining))
            if status != 202:
                raise LifecycleBlocked("Unexpected submission response; acceptance is ambiguous.")
            job_id = _location(headers, workspace, item)
            submitted = datetime.now(timezone.utc)
            state.update(phase="submitted", job_id=job_id,
                         submission_location=next(v for k, v in headers.items() if k.lower() == "location"),
                         submitted_at_utc=submitted.isoformat().replace("+00:00", "Z"),
                         poll_not_before_utc=None)
            _write(self.path, state)
            state["poll_not_before_utc"] = (submitted + timedelta(seconds=_delay(headers))).isoformat().replace("+00:00", "Z")
            _write(self.path, state)
            return job_id

    def matches_payload(self, payload):
        with _lock(self.lock_path):
            state = self._read()
            self._validate_payload(payload)
            return state["payload_sha256"] == hashlib.sha256(_canonical(payload)).hexdigest()

    def _job_path(self, state):
        job = _uuid(state.get("job_id"))
        workspace, item = self.binding["workspace_id"], self.binding["pipeline_id"]
        if _location({"Location": state.get("submission_location")}, workspace, item) != job:
            raise LifecycleBlocked("Persisted pipeline identity differs from submission; termination unconfirmed.")
        return f"/v1/workspaces/{workspace}/items/{item}/jobs/instances/{job}"

    def _parent(self, state, deadline):
        try:
            not_before = datetime.fromisoformat(state["poll_not_before_utc"].replace("Z", "+00:00"))
            if not_before.utcoffset() is None or not_before.utcoffset().total_seconds() != 0:
                raise ValueError
        except (KeyError, AttributeError, ValueError, TypeError):
            raise LifecycleBlocked("Initial poll time unknown; termination unconfirmed.") from None
        if datetime.now(timezone.utc) < not_before:
            raise LifecycleBlocked("Fabric initial polling delay has not elapsed; termination unconfirmed.")
        _, body = self.client.get(self._job_path(state), deadline)
        if (not isinstance(body, dict) or body.get("id") != state.get("job_id")
                or body.get("itemId") != self.binding["pipeline_id"]
                or not isinstance(body.get("status"), str)
                or body.get("status") not in ACTIVE | TERMINAL):
            raise LifecycleBlocked("Exact owned pipeline status is unproven; termination unconfirmed.")
        return body

    def _status(self, state, deadline):
        return self._parent(state, deadline)["status"]

    def status_detail(self, timeout=30, *, include_notebook=False, wait_for_terminal_child=False):
        with _lock(self.lock_path):
            state = self._read()
            if state["phase"] == "submitting":
                raise LifecycleBlocked("Submission outcome unknown; termination unconfirmed.")
            if state.get("payload_binding_version") != 1:
                raise LifecycleBlocked("Legacy job parameters were not bound to the journal; termination unconfirmed.")
            deadline = self.monotonic() + timeout
            parent = self._parent(state, deadline)
            snapshot = {"job_id": state["job_id"], "status": parent["status"]}
            if include_notebook:
                try:
                    snapshot["notebook"] = self._notebook_detail(parent, deadline)
                except LifecycleBlocked as exc:
                    snapshot["notebook_diagnostic"] = str(exc)
                if (wait_for_terminal_child and parent["status"] in TERMINAL - {"Deduped"}
                        and snapshot.get("notebook", {}).get("status") in ACTIVE):
                    child_id = snapshot["notebook"]["job_id"]
                    parent_status = parent["status"]
                    while snapshot["notebook"]["status"] in ACTIVE:
                        remaining = deadline - self.monotonic()
                        if remaining <= 0:
                            snapshot["notebook_diagnostic"] = (
                                "Bounded wait expired while the linked notebook remained active.")
                            break
                        self.sleep(min(5, remaining))
                        if self.monotonic() >= deadline:
                            snapshot["notebook_diagnostic"] = (
                                "Bounded wait expired while the linked notebook remained active.")
                            break
                        parent = self._parent(state, deadline)
                        if parent["status"] != parent_status:
                            raise LifecycleBlocked("Parent terminal state changed; termination unconfirmed.")
                        notebook = self._notebook_detail(parent, deadline)
                        if notebook["job_id"] != child_id:
                            raise LifecycleBlocked("Linked notebook identity changed; termination unconfirmed.")
                        snapshot.update(status=parent["status"], notebook=notebook)
            return snapshot

    def _notebook_detail(self, parent, deadline):
        if parent.get("jobType") != "Pipeline" or parent["status"] in ("NotStarted", "Deduped"):
            raise LifecycleBlocked("Pipeline scheduler readback cannot establish notebook linkage.")
        start = _service_time(parent.get("startTimeUtc"))
        end = (_service_time(parent.get("endTimeUtc")) if parent["status"] in TERMINAL
               else datetime.now(timezone.utc))
        if end < start or end - start > timedelta(days=1):
            raise LifecycleBlocked("Pipeline activity window is invalid or exceeds one day.")
        base = f"/v1/workspaces/{self.binding['workspace_id']}"
        job_id = parent["id"]
        margin = timedelta(minutes=3)
        payload = {"filters": [],
                   "lastUpdatedAfter": (start - margin).isoformat().replace("+00:00", "Z"),
                   "lastUpdatedBefore": (end + margin).isoformat().replace("+00:00", "Z")}
        remaining = deadline - self.monotonic()
        if remaining <= 0:
            raise LifecycleBlocked("Notebook activity readback deadline expired.")
        code, headers, body = self.client.request(
            "POST", f"{base}/datapipelines/pipelineruns/{job_id}/queryactivityruns", payload,
            timeout=min(30, remaining))
        paged = any(k.lower() in ("x-ms-continuation-token", "continuationtoken", "link")
                    and v for k, v in headers.items())
        if isinstance(body, dict):
            paged = paged or bool(body.get("continuationToken") or body.get("continuationUri"))
            body = body.get("value")
        if code != 200 or paged or not isinstance(body, list) or len(body) != 1:
            raise LifecycleBlocked("Pipeline activity query did not yield one unambiguous producer activity.")
        activity = body[0]
        inputs = activity.get("input") if isinstance(activity, dict) else None
        output = activity.get("output") if isinstance(activity, dict) else None
        workspace, notebook = self.binding["workspace_id"], self.binding["notebook_id"]
        if (not isinstance(inputs, dict) or not isinstance(output, dict)
                or activity.get("pipelineRunId") != job_id or activity.get("activityName") != "RunProducer"
                or activity.get("activityType") != "TridentNotebook"
                or activity.get("retryAttempt") not in (None, 0)
                or inputs.get("workspaceId") != workspace or inputs.get("notebookId") != notebook):
            raise LifecycleBlocked("Pipeline activity does not bind the exact producer notebook.")
        result = output.get("result")
        if not isinstance(result, dict) or not result.get("runId"):
            raise LifecycleBlocked("Pipeline activity does not yet expose a notebook job ID.")
        child = _uuid(result["runId"])
        if child != activity.get("activityRunId") or child == job_id:
            raise LifecycleBlocked("Notebook job ID does not match the observed activity identity.")
        _, body = self.client.get(f"{base}/items/{notebook}/jobs/instances/{child}", deadline)
        if (not isinstance(body, dict) or body.get("id") != child
                or body.get("itemId") != notebook or body.get("jobType") != "PipelineRunNotebook"
                or not isinstance(body.get("status"), str)
                or body.get("status") not in (ACTIVE | TERMINAL) - {"Deduped"}):
            raise LifecycleBlocked("Notebook scheduler did not confirm the linked job identity or status.")
        return {"job_id": child, "status": body["status"]}

    def status(self, timeout=30):
        return self.status_detail(timeout)["status"]

    def cancel(self, timeout=120):
        """Cancel only the persisted pipeline identity, then read its terminal state."""
        with _lock(self.lock_path):
            state = self._read()
            if state["phase"] == "submitting":
                raise LifecycleBlocked("Submission outcome unknown; termination unconfirmed.")
            if state.get("payload_binding_version") != 1:
                raise LifecycleBlocked("Legacy job parameters were not bound to the journal; termination unconfirmed.")
            deadline = self.monotonic() + timeout
            status = self._status(state, deadline)
            if status in TERMINAL:
                return status
            if state["phase"] in ("cancelling", "cancel_requested"):
                raise LifecycleBlocked("Prior cancellation outcome uncertain; termination unconfirmed.")
            state["phase"] = "cancelling"
            state["cancel_intent_at_utc"] = _utc()
            _write(self.path, state)
            remaining = deadline - self.monotonic()
            if remaining <= 0:
                raise LifecycleBlocked("Cancellation deadline expired; termination unconfirmed.")
            code, headers, _ = self.client.request("POST", self._job_path(state) + "/cancel",
                                                   timeout=min(30, remaining))
            if code != 202:
                raise LifecycleBlocked("Cancellation response uncertain; termination unconfirmed.")
            state["phase"] = "cancel_requested"
            state["poll_not_before_utc"] = None
            _write(self.path, state)
            if any(k.lower() == "location" for k in headers):
                if _location(headers, self.binding["workspace_id"], self.binding["pipeline_id"]) != state["job_id"]:
                    raise LifecycleBlocked("Cancellation location differs from the owned job; termination unconfirmed.")
            delay = _delay(headers)
            state["poll_not_before_utc"] = (datetime.now(timezone.utc) + timedelta(seconds=delay)).isoformat().replace("+00:00", "Z")
            _write(self.path, state)
            while True:
                if delay >= deadline - self.monotonic():
                    raise LifecycleBlocked("Cancellation did not reach terminal state; termination unconfirmed.")
                self.sleep(delay)
                status = self._status(state, deadline)
                if status in TERMINAL:
                    return status
                delay = 1
