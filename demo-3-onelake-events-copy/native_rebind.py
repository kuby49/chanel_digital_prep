"""Separately journaled staging-access proof and native guard staging rebind."""

import argparse
import base64
import copy
import hashlib
import json
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlsplit

import customer_preflight as check
import guarded_notebook
import native_binding as binding
import native_guard
import native_rule
from deploy import CandidateBlocked, _guid, _load_json, _state_lock
from ownership import FabricControl


_SHA = re.compile(r"[0-9a-f]{64}\Z")
_PATH = re.compile(r"Files/[A-Za-z0-9._/-]+\Z")
_LEGACY_ACCESS = "demo3-staging-access-state.json"
_ACCESS_NAME = "Demo3StagingAccessA9bbae81"
_ACCESS_PIPELINE_NAME = "Demo3StagingAccessPipelineA9bbae81"


def _fingerprint(definition):
    parts, notebook = binding._notebook_parts(definition)
    entries = []
    for name in sorted(parts):
        part = copy.deepcopy(parts[name][0])
        if name == "notebook-content.ipynb":
            part["payload"] = notebook
        entries.append(part)
    return check.config_sha256({**{key: value for key, value in definition.items() if key != "parts"},
                                "parts": entries})


def _probe_object(value):
    if not isinstance(value, dict) or set(value) != {"path", "sha256", "etag"}:
        raise ValueError("Probe object must contain exactly path, sha256 and etag.")
    path, digest, etag = value["path"], value["sha256"], value["etag"]
    if (not isinstance(path, str) or not _PATH.fullmatch(path)
            or any(part in ("", ".", "..") for part in path.split("/"))
            or not isinstance(digest, str) or not _SHA.fullmatch(digest)
            or not isinstance(etag, str) or not etag or len(etag) > 256
            or any(ord(char) < 32 or ord(char) == 127 for char in etag)):
        raise ValueError("Probe path, SHA-256 or ETag is invalid.")
    return value


def _spec(value, *, allow_missing_baseline=False):
    fields = {"version", "producer_workspace_id", "producer_lakehouse_id", "principal_id",
              "access_baseline_sha256", "producer_probe", "target_probe"}
    allowed = fields - {"access_baseline_sha256"} if allow_missing_baseline else fields
    if (not isinstance(value, dict) or set(value) not in (fields, allowed)
            or type(value.get("version")) is not int or value["version"] != 1):
        raise ValueError("Staging rebind specification has unexpected or missing fields.")
    for key in ("producer_workspace_id", "producer_lakehouse_id", "principal_id"):
        _guid(value[key])
    if (value["producer_lakehouse_id"] in (check.STAGING_LAKEHOUSE, check.LAKEHOUSE)
            or (value["producer_workspace_id"], value["producer_lakehouse_id"]) ==
            (check.WORKSPACE, check.LAKEHOUSE)):
        raise ValueError("Rebound staging must be a distinct Lakehouse.")
    baseline = value.get("access_baseline_sha256")
    if baseline is not None and (not isinstance(baseline, str) or not _SHA.fullmatch(baseline)):
        raise ValueError("Access notebook baseline fingerprint is invalid.")
    producer, target = _probe_object(value["producer_probe"]), _probe_object(value["target_probe"])
    if producer["path"].startswith("Files/producer/runs/"):
        raise ValueError("The staging probe must not be a run manifest or admission.")
    return copy.deepcopy(value)


def _probe_pipeline(notebook_id):
    return {"properties": {
        "concurrency": 1,
        "activities": [{
            "name": "ReadExistingManifest", "type": "TridentNotebook", "dependsOn": [],
            "policy": {"timeout": "0.00:10:00", "retry": 0, "secureOutput": True, "secureInput": True},
            "typeProperties": {"workspaceId": check.WORKSPACE, "notebookId": _guid(notebook_id),
                               "sessionTag": "demo3_guarded_copy", "parameters": {}},
        }],
    }}


def _probe_notebook(before, producer, spec, notebook_id):
    generated = guarded_notebook.build_notebook(
        producer, {}, check.WORKSPACE, check.LAKEHOUSE, check.HOUSE_NAME,
        spec["producer_workspace_id"], spec["producer_lakehouse_id"])
    if (not isinstance(before.get("cells"), list) or len(before["cells"]) != 2
            or before["cells"][0].get("source") != generated["cells"][0]["source"]
            or before["metadata"]["trident"]["lakehouse"] != generated["metadata"]["trident"]["lakehouse"]
            or before["metadata"]["dependencies"]["lakehouse"] !=
            generated["metadata"]["dependencies"]["lakehouse"]):
        raise CandidateBlocked("Access notebook baseline or target Lakehouse binding differs.")
    probe = f'''\
import base64
import hashlib
import json

from guard_spark import OneLakeStore


def execute(spark, notebookutils, settings, arguments):
    context = notebookutils.runtime.context
    if (context["isForPipeline"] is not True
            or context["currentWorkspaceId"] != settings["workspace_id"]
            or context["currentNotebookId"] != {notebook_id!r}
            or context["defaultLakehouseId"] != settings["item_id"]):
        raise RuntimeError("Probe is not in the expected pipeline notebook and Lakehouse")

    def token():
        value = notebookutils.credentials.getToken("storage")
        try:
            encoded = value.split(".")[1]
            claims = json.loads(base64.urlsafe_b64decode(encoded + "=" * (-len(encoded) % 4)))
        except (AttributeError, IndexError, TypeError, ValueError):
            raise RuntimeError("Storage token identity could not be established") from None
        if claims.get("tid") != {check.TENANT!r} or claims.get("oid") != {spec["principal_id"]!r}:
            raise RuntimeError("Pipeline storage identity differs from the expected guard principal")
        return value

    producer = OneLakeStore(settings["producer_workspace_id"], settings["producer_item_id"], token)
    target = OneLakeStore(settings["workspace_id"], settings["item_id"], token)
    for store, expected in ((producer, {spec["producer_probe"]!r}), (target, {spec["target_probe"]!r})):
        content, etag = store.read(expected["path"])
        if hashlib.sha256(content).hexdigest() != expected["sha256"] or etag != expected["etag"]:
            raise RuntimeError("Probe object bytes or ETag differ")
    return json.dumps({{"pipeline_context": True, "token_identity_matches": True}})
'''
    loader = "".join(generated["cells"][1]["source"])
    prefix = "_guard_inputs = dict("
    original = "        from guard_spark import execute\n"
    if loader.count(prefix) != 1 or loader.count(original) != 1:
        raise CandidateBlocked("Packaged access notebook loader changed.")
    loader = loader.replace(prefix, f"_guard_modules['staging_probe.py'] = {probe.encode()!r}\n" + prefix, 1)
    loader = loader.replace(original, "        from staging_probe import execute\n", 1)
    compile(probe, "staging_probe.py", "exec")
    compile(loader, "staging_access_notebook", "exec")
    result = copy.deepcopy(before)
    result["cells"][1]["source"] = loader.splitlines(keepends=True)
    return result


def _job_location(location, item_id):
    prefix = "https://api.fabric.microsoft.com" + binding._BASE + "/items/" + _guid(item_id) + "/jobs/instances/"
    if not isinstance(location, str) or not location.startswith(prefix):
        raise CandidateBlocked("Probe job Location does not identify the owned pipeline.")
    parsed = urlsplit(location)
    if parsed.query or parsed.fragment or parsed.username or parsed.password or parsed.port is not None:
        raise CandidateBlocked("Probe job Location is invalid.")
    job_id = _guid(location[len(prefix):])
    return job_id, location


def _when(value):
    if not isinstance(value, str):
        raise CandidateBlocked("Job timestamp is unavailable.")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise CandidateBlocked("Job timestamp is invalid.") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    if parsed.utcoffset() != timedelta(0):
        raise CandidateBlocked("Job timestamp is not UTC.")
    return parsed


class NativeRebind:
    rebound = True

    def __init__(self, publisher, spec_path, previous_proof_path, *, allow_missing_baseline=False):
        self.publisher = publisher
        self.root, self.api, self.poll, self.graph = publisher.root, publisher.api, publisher.poll, publisher.graph
        self.spec_path, self.previous_proof_path = Path(spec_path), Path(previous_proof_path)
        self.journal = self.root / binding.REBIND_JOURNAL
        self.access_journal = self.root / binding.ACCESS_REBIND_JOURNAL
        protected = (self.journal, self.access_journal, publisher.journal,
                     self.root / binding.HC_JOURNAL, self.root / binding.RULE_JOURNAL,
                     self.root / binding.RULE_SECOND_JOURNAL, self.root / _LEGACY_ACCESS)
        if (not self.spec_path.is_absolute() or not self.previous_proof_path.is_absolute()
                or self.spec_path.resolve() == self.previous_proof_path.resolve()
                or any(item.resolve() in {path.resolve() for path in protected}
                       for item in (self.spec_path, self.previous_proof_path))):
            raise ValueError("Rebind inputs must be distinct absolute paths outside the journals.")
        self.paths = {
            "spec_sha256": self.spec_path, "previous_proof_sha256": self.previous_proof_path,
            "hc_journal_sha256": self.root / binding.HC_JOURNAL,
            "first_rule_journal_sha256": self.root / binding.RULE_JOURNAL,
            "legacy_access_journal_sha256": self.root / _LEGACY_ACCESS,
        }
        inputs = {key: binding._bytes(path) for key, path in self.paths.items()}
        self.pinned = {key: binding._sha(raw) for key, raw in inputs.items()}
        self.spec = _spec(binding._json_bytes(inputs["spec_sha256"]),
                          allow_missing_baseline=allow_missing_baseline)
        self.previous_proof = binding._json_bytes(inputs["previous_proof_sha256"])
        self.legacy_access = binding._json_bytes(inputs["legacy_access_journal_sha256"])
        self.notebook_id, self.pipeline_id = self._legacy_ids()
        self._lineage()

    def _unchanged(self):
        self.publisher._unchanged()
        if any(binding._sha(binding._bytes(path)) != self.pinned[key] for key, path in self.paths.items()):
            raise CandidateBlocked("A pinned staging rebind input or predecessor changed.")

    def _legacy_ids(self):
        state = self.legacy_access
        notebook, pipeline = _guid(state.get("notebook_id")), _guid(state.get("pipeline_id"))
        actions, result = state.get("actions", {}), state.get("result", {})
        notebook_create = actions.get("notebook_create", {})
        if notebook_create.get("http") == 202:
            binding._operation_location({"x-ms-operation-id": notebook_create.get("operation_id"),
                                         "location": notebook_create.get("location")})
        if (state.get("workspace_id") != check.WORKSPACE or state.get("lakehouse_id") != check.LAKEHOUSE
                or state.get("producer_workspace_id") != check.STAGING_WORKSPACE
                or state.get("producer_lakehouse_id") != check.STAGING_LAKEHOUSE
                or notebook_create.get("id") != notebook or notebook_create.get("http") not in (201, 202)
                or notebook_create.get("status") != "confirmed"
                or actions.get("pipeline_create", {}).get("id") != pipeline
                or actions.get("pipeline_create", {}).get("status") != "confirmed"
                or actions.get("pipeline_create", {}).get("http") not in (201, 202)
                or result.get("notebook_job_status") != "Completed"
                or result.get("activity_status") != "Succeeded" or notebook == pipeline):
            raise CandidateBlocked("Original staging access ownership and receipt are not confirmed.")
        if actions["pipeline_create"]["http"] == 202:
            binding._operation_location({"x-ms-operation-id": actions["pipeline_create"].get("operation_id"),
                                         "location": actions["pipeline_create"].get("location")})
        return notebook, pipeline

    def _lineage(self):
        self._unchanged()
        published = self.publisher._published()
        correction = self.publisher._correction(published)
        if correction["update"]["status"] != "confirmed-readback":
            raise CandidateBlocked("Original guard correction is not confirmed.")
        fingerprint = check.config_sha256(published)
        rule = native_rule.NativeRule(self.publisher)
        previous = rule._validate_state(_load_json(self.root / binding.RULE_JOURNAL), fingerprint)
        if (any(previous["actions"].get(key, {}).get("status") != "confirmed-readback"
                for key in ("enable", "disable"))
                or previous["proof_sha256"] != check.config_sha256(self.previous_proof)):
            raise CandidateBlocked("First rule cycle or its predecessor proof is not confirmed.")
        expected = {
            "version": 1, "workspace_id": check.WORKSPACE, "lakehouse_id": check.LAKEHOUSE,
            "guard_notebook_id": check.NOTEBOOK, "pipeline_id": check.PIPELINE,
            "reflex_id": check.REFLEX, "connection_id": check.CONNECTION, "marker": check.MARKER,
            "native_policy": published["native_policy"], "generator_sha256": check.GENERATOR_SHA,
            "guard_sha256": check.config_sha256(binding._notebook(correction["target"])),
            "graph_disabled_sha256": check.config_sha256(check._canonical(self.graph)),
        }
        if (not isinstance(self.previous_proof, dict)
                or set(self.previous_proof) != set(expected) | {"pipeline_sha256"}
                or any(self.previous_proof.get(key) != value for key, value in expected.items())
                or not isinstance(self.previous_proof.get("pipeline_sha256"), str)
                or not _SHA.fullmatch(self.previous_proof["pipeline_sha256"])):
            raise CandidateBlocked("The first-cycle proof does not match the confirmed guard lineage.")
        self._unchanged()
        return published, correction

    def predecessor_binding_hash(self):
        return check.config_sha256(self._lineage()[0])

    def _control(self):
        self._lineage()
        pipeline = binding._quiescent(self.api, self.root)
        graph = binding._graph(self.api)
        binding._stopped(graph, self.graph)
        if (check.config_sha256(pipeline["properties"]) != self.previous_proof["pipeline_sha256"]
                or check.config_sha256(check._canonical(graph)) != self.previous_proof["graph_disabled_sha256"]):
            raise CandidateBlocked("Retained pipeline or stopped graph changed since the first proof.")
        self._unchanged()
        return pipeline, graph

    def _stage_and_objects(self):
        workspace, lakehouse = self.spec["producer_workspace_id"], self.spec["producer_lakehouse_id"]
        workspaces = [item for item in check._inventory(self.api, "/v1/workspaces")
                      if item.get("id") == workspace]
        houses = [item for item in check._inventory(self.api, "/v1/workspaces/" + workspace + "/lakehouses")
                  if item.get("id") == lakehouse]
        if len(workspaces) != 1 or len(houses) != 1:
            raise CandidateBlocked("New staging workspace or Lakehouse is not uniquely visible.")
        _, _, storage = check._producer(self.root)
        token = check._storage_token()
        for workspace_id, item_id, expected in (
                (workspace, lakehouse, self.spec["producer_probe"]),
                (check.WORKSPACE, check.LAKEHOUSE, self.spec["target_probe"])):
            store = storage.OneLakeStore(workspace_id, item_id, lambda: token)
            content, etag = store.read(expected["path"])
            if hashlib.sha256(content).hexdigest() != expected["sha256"] or etag != expected["etag"]:
                raise CandidateBlocked("Probe object bytes or ETag changed.")

    def _probe_items(self):
        check._owned(self.api, binding._BASE + "/items?type=Notebook", _ACCESS_NAME, self.notebook_id)
        check._owned(self.api, binding._BASE + "/items?type=DataPipeline",
                     _ACCESS_PIPELINE_NAME, self.pipeline_id)
        pipeline = binding._definition(self.api, "/items/" + self.pipeline_id, "pipeline-content.json")
        if pipeline["properties"] != _probe_pipeline(self.notebook_id)["properties"]:
            raise CandidateBlocked("Owned access pipeline changed.")
        return pipeline

    def _old_guard(self):
        _, correction = self._lineage()
        _, notebook = binding._notebook_parts(binding._notebook_definition(self.api, check.NOTEBOOK))
        if notebook != correction["target"]:
            raise CandidateBlocked("Native guard changed before the access probe.")

    def _access_target(self, before):
        producer, _, _ = check._producer(self.root)
        _, notebook = binding._notebook_parts(before)
        return binding._replace_notebook(before, _probe_notebook(notebook, producer, self.spec, self.notebook_id))

    def inspect_access(self):
        self._control()
        self._old_guard()
        self._probe_items()
        current = binding._notebook_definition(self.api, self.notebook_id)
        self._lineage()
        return {"access_notebook_id": self.notebook_id, "access_pipeline_id": self.pipeline_id,
                "access_baseline_sha256": _fingerprint(current)}

    def _receipt(self, record, label):
        if not isinstance(record, dict) or not record.get("time_utc"):
            raise CandidateBlocked(label + " intent is missing.")
        if record.get("http") == 202:
            return binding._operation_location({"x-ms-operation-id": record.get("operation_id"),
                                                "location": record.get("location")})
        if record.get("http") != 200:
            raise CandidateBlocked(label + " acceptance is unknown; never resubmit.")
        return None

    def _access_state(self):
        state = _load_json(self.access_journal)
        actions = state.get("actions", {})
        if (state.get("version") != 1 or state.get("inputs") != self.pinned
                or state.get("notebook_id") != self.notebook_id or state.get("pipeline_id") != self.pipeline_id
                or not isinstance(actions, dict) or set(actions) not in ({"update"}, {"update", "run"})
                or _fingerprint(state["before"]) != self.spec.get("access_baseline_sha256")
                or not binding._same_notebook_definition(state["target"], self._access_target(state["before"]))):
            raise CandidateBlocked("Access rebind journal or its complete definitions differ.")
        update = actions["update"]
        if (not isinstance(update, dict) or update.get("status") not in ("in-flight", "confirmed-readback")
                or not update.get("time_utc")):
            raise CandidateBlocked("Access notebook update journal is invalid.")
        if update["status"] == "confirmed-readback":
            self._receipt(update, "Access notebook update")
            if not update.get("confirmed_at_utc"):
                raise CandidateBlocked("Access notebook readback receipt is missing.")
        if "run" in actions and update["status"] != "confirmed-readback":
            raise CandidateBlocked("Access run precedes its confirmed notebook update.")
        if "run" in actions:
            run = actions["run"]
            if (not isinstance(run, dict) or run.get("status") not in
                    ("in-flight", "accepted", "confirmed-readback") or not run.get("time_utc")
                    or not isinstance(run.get("previous_jobs"), dict)
                    or set(run["previous_jobs"]) != {"RunNotebook", "Pipeline"}
                    or any(not isinstance(ids, list) or any(not isinstance(item, str) for item in ids)
                           or len(ids) != len(set(ids))
                           for ids in run["previous_jobs"].values())):
                raise CandidateBlocked("Access probe run journal is invalid.")
            for ids in run["previous_jobs"].values():
                for item in ids:
                    _guid(item)
            if run["status"] != "in-flight":
                if run.get("http") != 202:
                    raise CandidateBlocked("Accepted access probe has no HTTP receipt.")
                job_id, _ = _job_location(run.get("location"), self.pipeline_id)
                if run.get("job_id") != job_id:
                    raise CandidateBlocked("Accepted access probe job identity differs.")
            if run["status"] == "confirmed-readback" and (
                    not run.get("confirmed_at_utc") or not isinstance(run.get("result"), dict)):
                raise CandidateBlocked("Confirmed access probe is missing its readback.")
        return state

    def _confirm_access_update(self, state):
        record = state["actions"]["update"]
        operation = self._receipt(record, "Access notebook update")
        if operation:
            self.poll(self.api, operation[1], operation[0], "spark-cli")
        if not binding._same_notebook_definition(
                binding._notebook_definition(self.api, self.notebook_id), state["target"]):
            raise CandidateBlocked("Complete access notebook readback differs; do not retry.")
        self._control()
        self._old_guard()
        self._probe_items()
        self._unchanged()
        record.update(status="confirmed-readback", confirmed_at_utc=binding._utc())
        binding._save(self.access_journal, state)

    def access_update(self):
        with _state_lock(self.root):
            if self.access_journal.exists() or self.journal.exists() or (
                    self.root / binding.RULE_SECOND_JOURNAL).exists():
                raise CandidateBlocked("Staging access or subsequent journal already exists; do not resubmit.")
            if not self.spec.get("access_baseline_sha256"):
                raise ValueError("A confirmed complete access notebook baseline fingerprint is required.")
            self._control()
            self._old_guard()
            self._probe_items()
            self._stage_and_objects()
            before = binding._notebook_definition(self.api, self.notebook_id)
            if _fingerprint(before) != self.spec["access_baseline_sha256"]:
                raise CandidateBlocked("Access notebook baseline changed.")
            target = self._access_target(before)
            self._unchanged()
            state = {"version": 1, "inputs": copy.deepcopy(self.pinned), "notebook_id": self.notebook_id,
                     "pipeline_id": self.pipeline_id, "before": before, "target": target,
                     "actions": {"update": {"status": "in-flight", "time_utc": binding._utc()}}}
            binding._create(self.access_journal, state)
            status, headers, _ = self.api.request(
                "POST", binding._BASE + "/items/" + self.notebook_id + "/updateDefinition",
                binding._notebook_update_body(target), timeout=120, skill="spark-cli")
            record = state["actions"]["update"]
            record["http"] = status
            if status == 202:
                values = {key.lower(): value for key, value in headers.items()}
                record.update(operation_id=values.get("x-ms-operation-id"), location=values.get("location"))
            binding._save(self.access_journal, state)
            self._confirm_access_update(state)
            return state

    def _recent_jobs(self):
        snapshots = {}
        now = datetime.now(timezone.utc)
        for item, kind in ((self.notebook_id, "RunNotebook"), (self.pipeline_id, "Pipeline")):
            jobs = list(check._inventory(
                self.api, binding._BASE + "/items/" + item + "/jobs/instances?jobType=" + kind))
            ids = []
            for job in jobs:
                job_id = _guid(job.get("id"))
                if job.get("status") not in check.TERMINAL or _when(job.get("startTimeUtc")) > now - timedelta(minutes=5):
                    raise CandidateBlocked("A recent or nonterminal access job must be reconciled first.")
                ids.append(job_id)
            if len(ids) != len(set(ids)):
                raise CandidateBlocked("Access job inventory contains duplicate IDs.")
            snapshots[kind] = sorted(ids)
        return snapshots

    def access_run(self):
        with _state_lock(self.root):
            state = self._access_state()
            if "run" in state["actions"] or self.journal.exists() or (
                    self.root / binding.RULE_SECOND_JOURNAL).exists():
                raise CandidateBlocked("Access run already attempted or rebind started; never resubmit.")
            self._control()
            self._old_guard()
            self._probe_items()
            self._stage_and_objects()
            if not binding._same_notebook_definition(
                    binding._notebook_definition(self.api, self.notebook_id), state["target"]):
                raise CandidateBlocked("Access notebook changed before the probe run.")
            before = self._recent_jobs()
            self._unchanged()
            record = {"status": "in-flight", "time_utc": binding._utc(), "previous_jobs": before}
            state["actions"]["run"] = record
            binding._save(self.access_journal, state)
            status, headers, _ = self.api.request(
                "POST", binding._BASE + "/items/" + self.pipeline_id + "/jobs/Pipeline/instances",
                {"executionData": {"parameters": {}}}, timeout=120, skill="spark-cli")
            record["http"] = status
            if status == 202:
                values = {key.lower(): value for key, value in headers.items()}
                record["location"] = values.get("location")
            binding._save(self.access_journal, state)
            if status != 202:
                raise CandidateBlocked("Access job acceptance is unconfirmed; never resubmit.")
            record["job_id"], _ = _job_location(record["location"], self.pipeline_id)
            record["status"] = "accepted"
            binding._save(self.access_journal, state)
            return state

    def _job_evidence(self, record):
        if record.get("http") != 202:
            raise CandidateBlocked("Access job acceptance is unknown; never resubmit.")
        job_id, location = _job_location(record.get("location"), self.pipeline_id)
        if record.get("job_id") not in (None, job_id):
            raise CandidateBlocked("Access job receipt identity differs.")
        job = self.api.get(location)
        if job.get("id") != job_id:
            raise CandidateBlocked("Access pipeline job identity differs.")
        if job.get("status") in ("NotStarted", "InProgress"):
            return None
        if job.get("status") != "Completed":
            raise CandidateBlocked("Access pipeline job failed or has an unknown state.")
        start, end = _when(job.get("startTimeUtc")), _when(job.get("endTimeUtc"))
        if end < start or start < _when(record["time_utc"]) - timedelta(minutes=1):
            raise CandidateBlocked("Access pipeline job timing differs from the recorded intent.")
        query = {"filters": [], "lastUpdatedAfter": (start - timedelta(minutes=5)).isoformat(),
                 "lastUpdatedBefore": (end + timedelta(minutes=5)).isoformat()}
        status, _, page = self.api.request(
            "POST", binding._BASE + "/datapipelines/pipelineruns/" + job_id + "/queryactivityruns",
            query, skill="spark-cli")
        rows = page.get("value") if status == 200 and isinstance(page, dict) else None
        if (not isinstance(rows, list) or len(rows) != 1 or not isinstance(rows[0], dict)
                or page.get("continuationToken") or page.get("continuationUri")
                or rows[0].get("activityName") != "ReadExistingManifest"
                or rows[0].get("activityType") != "TridentNotebook"
                or rows[0].get("status") != "Succeeded"):
            raise CandidateBlocked("Access pipeline activity is not independently confirmed.")
        activity_id = _guid(rows[0].get("activityRunId"))
        jobs = list(check._inventory(
            self.api, binding._BASE + "/items/" + self.notebook_id + "/jobs/instances?jobType=RunNotebook"))
        prior = record.get("previous_jobs", {}).get("RunNotebook")
        if not isinstance(prior, list) or len(prior) != len(set(prior)):
            raise CandidateBlocked("Access notebook job baseline is missing.")
        if not set(prior).issubset({item.get("id") for item in jobs}):
            raise CandidateBlocked("Access notebook job history is incomplete.")
        matches = [item for item in jobs if item.get("id") not in prior]
        if (len(matches) != 1 or matches[0].get("id") != activity_id
                or matches[0].get("status") != "Completed"
                or matches[0].get("jobType") != "PipelineRunNotebook"
                or _when(matches[0].get("startTimeUtc")) < start - timedelta(minutes=1)
                or _when(matches[0].get("startTimeUtc")) > end + timedelta(minutes=1)):
            raise CandidateBlocked("Access activity does not identify exactly one new completed notebook job.")
        exact = self.api.get(binding._BASE + "/items/" + self.notebook_id +
                             "/jobs/instances/" + activity_id, skill="spark-cli")
        notebook_start, notebook_end = _when(exact.get("startTimeUtc")), _when(exact.get("endTimeUtc"))
        if (exact.get("id") != activity_id or exact.get("status") != "Completed"
                or exact.get("jobType") != "PipelineRunNotebook" or notebook_end < notebook_start
                or notebook_start < start - timedelta(minutes=1)
                or notebook_start > end + timedelta(minutes=1)):
            raise CandidateBlocked("Exact activity-linked notebook job is not confirmed.")
        return {"pipeline_job_id": job_id, "activity_run_id": activity_id, "notebook_job_id": activity_id,
                "activity_status": "Succeeded", "notebook_job_status": "Completed"}

    def _confirm_access_run(self, state):
        record = state["actions"]["run"]
        result = self._job_evidence(record)
        if result is None:
            return False
        self._control()
        self._old_guard()
        self._probe_items()
        self._stage_and_objects()
        if not binding._same_notebook_definition(
                binding._notebook_definition(self.api, self.notebook_id), state["target"]):
            raise CandidateBlocked("Access notebook changed during the probe.")
        self._unchanged()
        record.update(status="confirmed-readback", job_id=result["pipeline_job_id"],
                      result=result, confirmed_at_utc=binding._utc())
        binding._save(self.access_journal, state)
        return True

    def access_reconcile(self):
        with _state_lock(self.root):
            self._lineage()
            state = self._access_state()
            update = state["actions"]["update"]
            if update["status"] == "in-flight":
                self._confirm_access_update(state)
            elif "run" in state["actions"] and state["actions"]["run"].get("status") != "confirmed-readback":
                self._confirm_access_run(state)
            elif "run" in state["actions"]:
                self._control()
                self._access_confirmed()
            else:
                self._control()
                self._old_guard()
                self._probe_items()
                if not binding._same_notebook_definition(
                        binding._notebook_definition(self.api, self.notebook_id), state["target"]):
                    raise CandidateBlocked("Confirmed access notebook changed.")
            return state

    def _access_confirmed(self):
        state = self._access_state()
        record = state["actions"].get("run", {})
        if (record.get("status") != "confirmed-readback" or not record.get("confirmed_at_utc")
                or not isinstance(record.get("result"), dict)):
            raise CandidateBlocked("New pipeline-context staging access is not confirmed.")
        if self._job_evidence(record) != record["result"]:
            raise CandidateBlocked("New pipeline-context access job evidence changed.")
        self._probe_items()
        self._stage_and_objects()
        if not binding._same_notebook_definition(
                binding._notebook_definition(self.api, self.notebook_id), state["target"]):
            raise CandidateBlocked("Confirmed access notebook changed.")
        self._unchanged()
        return state

    def _guard_target(self, before):
        _, correction = self._lineage()
        _, notebook = binding._notebook_parts(before)
        if notebook != correction["target"]:
            raise CandidateBlocked("Guard baseline differs from the confirmed correction.")
        producer, _, _ = check._producer(self.root)
        policy = self.publisher.template["native_policy"]
        old = guarded_notebook.build_native_notebook(
            producer, policy, check.WORKSPACE, check.LAKEHOUSE, check.HOUSE_NAME,
            check.STAGING_WORKSPACE, check.STAGING_LAKEHOUSE)
        new = guarded_notebook.build_native_notebook(
            producer, policy, check.WORKSPACE, check.LAKEHOUSE, check.HOUSE_NAME,
            self.spec["producer_workspace_id"], self.spec["producer_lakehouse_id"])
        old_lines, new_lines = old["cells"][1]["source"], new["cells"][1]["source"]
        changed = [index for index, (left, right) in enumerate(zip(old_lines, new_lines)) if left != right]
        if (binding._notebook(notebook) != binding._notebook(old) or len(old_lines) != len(new_lines)
                or len(changed) != 1 or not old_lines[changed[0]].lstrip().startswith("_guard_result = execute(")):
            raise CandidateBlocked("Staging rebinding is not the expected single guard settings change.")
        target = copy.deepcopy(notebook)
        target["cells"][1]["source"][changed[0]] = new_lines[changed[0]]
        if binding._notebook(target) != binding._notebook(new):
            raise CandidateBlocked("Rebound guard differs from the pinned native builder.")
        return binding._replace_notebook(before, target)

    def _state(self):
        state = _load_json(self.journal)
        access_raw = binding._bytes(self.access_journal)
        if (state.get("version") != 1 or state.get("inputs") != self.pinned
                or state.get("original_binding_sha256") != self.predecessor_binding_hash()
                or state.get("access_journal_sha256") != binding._sha(access_raw)
                or not binding._same_notebook_definition(state["target"], self._guard_target(state["before"]))):
            raise CandidateBlocked("Staging rebind journal, predecessor or complete definitions differ.")
        record = state.get("update")
        if (not isinstance(record, dict) or record.get("status") not in ("in-flight", "confirmed-readback")
                or not record.get("time_utc")):
            raise CandidateBlocked("Staging guard update journal is invalid.")
        if record["status"] == "confirmed-readback":
            self._receipt(record, "Staging guard update")
            if not record.get("confirmed_at_utc"):
                raise CandidateBlocked("Staging guard update readback receipt is missing.")
        return state

    def _confirm_guard(self, state):
        record = state["update"]
        operation = self._receipt(record, "Staging guard update")
        if operation:
            self.poll(self.api, operation[1], operation[0], "spark-cli")
        if not binding._same_notebook_definition(
                binding._notebook_definition(self.api, check.NOTEBOOK), state["target"]):
            raise CandidateBlocked("Complete rebound guard readback differs; do not retry.")
        self._control()
        self._access_confirmed()
        self._unchanged()
        if state["access_journal_sha256"] != binding._sha(binding._bytes(self.access_journal)):
            raise CandidateBlocked("Access proof changed during guard update.")
        record.update(status="confirmed-readback", confirmed_at_utc=binding._utc())
        binding._save(self.journal, state)

    def update(self):
        with _state_lock(self.root):
            if self.journal.exists() or (self.root / binding.RULE_SECOND_JOURNAL).exists():
                raise CandidateBlocked("Staging rebind or second rule cycle already attempted; never resubmit.")
            self._control()
            self._access_confirmed()
            before = binding._notebook_definition(self.api, check.NOTEBOOK)
            target = self._guard_target(before)
            access_hash = binding._sha(binding._bytes(self.access_journal))
            self._unchanged()
            state = {"version": 1, "inputs": copy.deepcopy(self.pinned),
                     "original_binding_sha256": self.predecessor_binding_hash(),
                     "access_journal_sha256": access_hash, "before": before, "target": target,
                     "update": {"status": "in-flight", "time_utc": binding._utc()}}
            binding._create(self.journal, state)
            status, headers, _ = self.api.request(
                "POST", binding._BASE + "/items/" + check.NOTEBOOK + "/updateDefinition",
                binding._notebook_update_body(target), timeout=120, skill="spark-cli")
            state["update"]["http"] = status
            if status == 202:
                values = {key.lower(): value for key, value in headers.items()}
                state["update"].update(operation_id=values.get("x-ms-operation-id"),
                                       location=values.get("location"))
            binding._save(self.journal, state)
            self._confirm_guard(state)
            return state

    def reconcile(self):
        with _state_lock(self.root):
            state = self._state()
            if state["update"]["status"] == "in-flight":
                self._confirm_guard(state)
            elif not binding._same_notebook_definition(
                    binding._notebook_definition(self.api, check.NOTEBOOK), state["target"]):
                raise CandidateBlocked("Rebound guard changed after confirmation.")
            return state

    def _published(self):
        self._lineage()
        state = self._state()
        if state["update"]["status"] != "confirmed-readback":
            raise CandidateBlocked("Staging guard update is not confirmed.")
        access = self._access_state()
        if access["actions"].get("run", {}).get("status") != "confirmed-readback":
            raise CandidateBlocked("New pipeline-context staging access is not confirmed.")
        self._unchanged()
        return state

    def _read_proof(self, state):
        return self._proof_readback(state, allow_running=False)

    def _proof_readback(self, state, *, allow_running):
        if state != self._published():
            raise CandidateBlocked("Staging rebind changed before proof.")
        self._lineage()
        pipeline = binding._quiescent(self.api, self.root)
        graph = binding._graph(self.api)
        running = check._rule_state(graph, {"definition_disabled": self.graph}, reusable=True)
        if running and not allow_running:
            raise CandidateBlocked("New proof requires the exact stopped rule.")
        cycle = None
        if running:
            controller = native_rule.NativeRule(self, cycle=2)
            cycle = controller._state(check.config_sha256(state))
            if (set(cycle["actions"]) != {"enable"}
                    or cycle["actions"]["enable"]["status"] != "confirmed-readback"):
                raise CandidateBlocked("Enabled rebound rule has no confirmed second-cycle enable.")
            controller._readback(True, cycle["actions"]["enable"]["target"])
        elif allow_running and (self.root / binding.RULE_SECOND_JOURNAL).exists():
            raise CandidateBlocked("A stopped second rule cycle cannot be admitted again.")
        disabled = copy.deepcopy(graph)
        for entity in disabled:
            definition = entity.get("payload", {}).get("definition", {})
            if entity.get("type") == "timeSeriesView-v1" and definition.get("type") == "Rule":
                definition["settings"]["shouldRun"] = False
        if (check.config_sha256(pipeline["properties"]) != self.previous_proof["pipeline_sha256"]
                or check.config_sha256(check._canonical(disabled)) !=
                self.previous_proof["graph_disabled_sha256"]):
            raise CandidateBlocked("Retained pipeline or event graph changed since the first proof.")
        self._access_confirmed()
        definition = binding._notebook_definition(self.api, check.NOTEBOOK)
        if not binding._same_notebook_definition(definition, state["target"]):
            raise CandidateBlocked("Rebound guard or an unrelated notebook part changed before proof.")
        _, notebook = binding._notebook_parts(definition)
        self._unchanged()
        proof = {**self.previous_proof,
                 "guard_sha256": check.config_sha256(binding._notebook(notebook)),
                 "pipeline_sha256": check.config_sha256(pipeline["properties"]),
                 "graph_disabled_sha256": check.config_sha256(check._canonical(disabled))}
        if cycle is not None and cycle["proof_sha256"] != check.config_sha256(proof):
            raise CandidateBlocked("Second-cycle enable is bound to a different proof.")
        return proof

    def proof(self, output):
        output = Path(output)
        protected = (*self.paths.values(), self.journal, self.access_journal, self.publisher.journal,
                     self.root / binding.RULE_SECOND_JOURNAL, self.publisher.candidate_path,
                     self.publisher.native_config_path, self.publisher.previous_config_path,
                     self.root / binding._PREVIOUS, self.root / check.JOURNAL)
        if not output.is_absolute() or any(output.resolve() == item.resolve() for item in protected):
            raise ValueError("Proof output must be a distinct absolute path.")
        with _state_lock(self.root):
            result = self._read_proof(self._published())
            binding._create(output, result)
            return result


def validate_rebound(config, prefix, state_dir, *, api=None):
    demo, shared, data = config["demo-3"], config["shared"], config["data"]
    refs = demo.get("native_rebind")
    fields = {"candidate", "native_config", "previous_config", "spec", "previous_proof", "proof"}
    if (prefix != "chanel" or config.get("tenant_id") != check.TENANT
            or type(config.get("contract_version")) is not int or config["contract_version"] != 1
            or demo.get("native_preflight") is not True or demo.get("customer_preflight") is True
            or demo.get("native_binding_journal") != check.NATIVE_JOURNAL
            or not isinstance(refs, dict) or set(refs) != fields
            or any(not isinstance(value, str) or not Path(value).is_absolute() for value in refs.values())):
        raise CandidateBlocked("Rebound native preflight inputs are incomplete or not absolute.")
    api = api or FabricControl.from_cli(check.TENANT, timeout=120)
    publisher = binding.NativeBinding(state_dir, Path(refs["candidate"]), Path(refs["native_config"]),
                                      Path(refs["previous_config"]), api)
    rebind = NativeRebind(publisher, Path(refs["spec"]), Path(refs["previous_proof"]))
    expected_target = {"workspace_id": check.WORKSPACE, "item_id": check.LAKEHOUSE, "fact": "dbo.factsales"}
    if (any(demo.get(key) != value for key, value in (
            ("workspace_id", check.WORKSPACE), ("lakehouse_id", check.LAKEHOUSE),
            ("lakehouse_name", check.HOUSE_NAME), ("destination_table", "dbo.factsales")))
            or demo.get("landing_path") not in ("Files/incoming/sales", check.LANDING)
            or shared.get("workspace_id") != rebind.spec["producer_workspace_id"]
            or shared.get("staging_lakehouse_id") != rebind.spec["producer_lakehouse_id"]
            or shared.get("target_bindings", {}).get("demo-3") != expected_target
            or type(data.get("max_duration_seconds")) is not int
            or not 0 < data["max_duration_seconds"] <= 1800 or data.get("currency") != "USD"):
        raise CandidateBlocked("Rebound native staging, target or bounded run configuration differs.")
    policy = native_guard.validate_policy(demo["native_policy"])
    if policy != publisher.template["native_policy"]:
        raise CandidateBlocked("Rebound native policy changed.")
    _, generator, _ = check._producer(state_dir)
    projected = {**config, "data": {**data, "run_id": "native-validation"}}
    native_guard.validate_run_policy(
        generator.RunConfig(**guarded_notebook.run_config_from_shared(generator, projected)), policy)
    saved = _load_json(Path(refs["proof"]))
    if saved != demo.get("native_proof") or saved != rebind._proof_readback(
            rebind._published(), allow_running=True):
        raise CandidateBlocked("Rebound native proof differs from the profile or fresh readback.")
    return [
        "Rebound native target, staging, access proof and confirmed rule lineage verified.",
        "Readiness is point-in-time; it does not prove event-queue drainage, an external-writer fence, "
        "or successful delivery.",
    ]


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", required=True, choices=(
        "inspect-access", "access-update", "access-run", "access-reconcile",
        "update", "reconcile", "proof"))
    for name in ("candidate", "native-config", "previous-config", "state-dir", "rebind-spec", "previous-proof"):
        parser.add_argument("--" + name, required=True, type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--yes", action="store_true")
    args = parser.parse_args(argv)
    if args.mode in ("access-update", "access-run", "update") and not args.yes:
        parser.error("Each requested update or probe run requires explicit --yes")
    if (args.mode == "proof") != (args.output is not None):
        parser.error("--output is required only for proof")
    api = FabricControl.from_cli(check.TENANT, timeout=120)
    publisher = binding.NativeBinding(args.state_dir, args.candidate, args.native_config, args.previous_config, api)
    rebind = NativeRebind(publisher, args.rebind_spec, args.previous_proof,
                          allow_missing_baseline=args.mode == "inspect-access")
    if args.mode == "inspect-access":
        result = rebind.inspect_access()
    elif args.mode == "proof":
        proof = rebind.proof(args.output)
        result = {"proof_path": str(args.output), "proof_sha256": check.config_sha256(proof)}
    else:
        method = {"access-update": rebind.access_update, "access-run": rebind.access_run,
                  "access-reconcile": rebind.access_reconcile, "update": rebind.update,
                  "reconcile": rebind.reconcile}[args.mode]
        state = method()
        result = {"journal": str(rebind.access_journal if args.mode.startswith("access-") else rebind.journal),
                  "actions": state.get("actions", {"update": state.get("update")})}
    print(json.dumps({"mode": args.mode, **result}, sort_keys=True))


if __name__ == "__main__":
    main()
