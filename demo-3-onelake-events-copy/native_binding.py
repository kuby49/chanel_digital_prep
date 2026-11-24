"""Fail-closed, journaled publication of the owned reusable native binding."""

import argparse
import base64
import copy
import hashlib
import json
import os
import re
import tempfile
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import Request

import activator
import customer_preflight as check
import guarded_notebook
import guarded_pipeline
import native_guard
from deploy import CandidateBlocked, _guid, _load_json, _state_lock, _unique_pairs
from ownership import FabricControl


_PREVIOUS = "demo3-customer-" + check.FRESH_RUN_ID + "-binding-state.json"
_BASE = "/v1/workspaces/" + check.WORKSPACE
_ACTIONS = ("guard_update", "activator_update")


def _utc():
    return datetime.now(timezone.utc).isoformat()


def _bytes(path):
    with path.open("rb") as file:
        return file.read()


def _sha(data):
    return hashlib.sha256(data).hexdigest()


def _invalid_constant(value):
    raise ValueError(f"invalid JSON constant: {value}")


def _json_bytes(raw):
    return json.loads(raw.decode("utf-8"), object_pairs_hook=_unique_pairs, parse_constant=_invalid_constant)


def _decode_part(body, path):
    parts = body["definition"]["parts"]
    if len(parts) != 1 or parts[0].get("path") != path or parts[0].get("payloadType") != "InlineBase64":
        raise ValueError("candidate definition envelope differs")
    return _json_bytes(base64.b64decode(parts[0]["payload"], validate=True))


def _notebook(notebook):
    return {"cells": [item["source"] for item in notebook["cells"]],
            "lakehouse": notebook["metadata"]["trident"]["lakehouse"]}


def _graph_ids(graph):
    if not isinstance(graph, list) or len(graph) != 5:
        raise ValueError("candidate native graph shape differs")
    event = json.loads(graph[2]["payload"]["definition"]["instance"])
    rule = json.loads(graph[3]["payload"]["definition"]["instance"])
    if len(event["steps"]) != 1 or len(rule["steps"]) != 3:
        raise ValueError("candidate native event steps differ")
    result = [item["uniqueIdentifier"] for item in graph]
    result.extend(item["id"] for item in event["steps"] + rule["steps"])
    if len(set(result)) != 9:
        raise ValueError("candidate native graph identifiers are not unique")
    for item in result:
        _guid(item)
    return result


def _normalized_graph(graph):
    mapping = {value: f"entity-{index}" for index, value in enumerate(_graph_ids(graph))}

    def replace(value):
        if isinstance(value, dict):
            return {key: replace(json.loads(item) if key == "instance" and isinstance(item, str) else item)
                    for key, item in value.items()}
        if isinstance(value, list):
            return [replace(item) for item in value]
        if isinstance(value, str):
            return mapping.get(value, value)
        return value

    return replace(graph)


def candidate_inputs(candidate, config, state_dir):
    if (candidate.get("contract_version") != 1 or candidate.get("status") != "offline-candidate-only"
            or candidate.get("ready") is not False):
        raise ValueError("native candidate is not an offline-only v1 bundle")
    state = copy.deepcopy(candidate["binding_journal_candidate"])
    expected_ids = {
        "workspace_id": check.WORKSPACE, "lakehouse_id": check.LAKEHOUSE,
        "guard_notebook_id": check.NOTEBOOK, "pipeline_id": check.PIPELINE, "reflex_id": check.REFLEX,
        "producer_workspace_id": check.STAGING_WORKSPACE,
        "producer_lakehouse_id": check.STAGING_LAKEHOUSE, "generator_sha256": check.GENERATOR_SHA,
    }
    if (any(state.get(key) != value for key, value in expected_ids.items())
            or state.get("actions") != {} or state.get("previous_journal") != _PREVIOUS):
        raise ValueError("candidate journal is not the expected unconfirmed owned binding")
    previous = _load_json(state_dir / _PREVIOUS)
    if (state.get("previous_journal_sha256") != check.config_sha256(previous)
            or set(previous.get("actions", {})) != set(_ACTIONS)
            or any(previous["actions"][action].get("status") != "confirmed-readback" for action in _ACTIONS)):
        raise ValueError("candidate predecessor differs or is not confirmed")
    demo, shared = config["demo-3"], config["shared"]
    policy = native_guard.validate_policy(demo["native_policy"])
    if (demo.get("native_preflight") is not True or demo.get("customer_preflight") is True
            or demo.get("native_binding_journal") != check.NATIVE_JOURNAL
            or demo.get("workspace_id") != check.WORKSPACE or demo.get("lakehouse_id") != check.LAKEHOUSE
            or demo.get("destination_table") != "dbo.factsales"
            or shared.get("workspace_id") != check.STAGING_WORKSPACE
            or shared.get("staging_lakehouse_id") != check.STAGING_LAKEHOUSE
            or shared.get("target_bindings", {}).get("demo-3") != {
                "workspace_id": check.WORKSPACE, "item_id": check.LAKEHOUSE, "fact": "dbo.factsales"}
            or config.get("tenant_id") != check.TENANT
            or policy["run_template"] != {key: check.RUN[key] for key in native_guard._TEMPLATE}
            or policy["max_file_count"] > 10 or policy["max_rows_per_file"] > 1000
            or policy["max_admission_seconds"] > 86400
            or state.get("native_policy") != policy
            or state.get("native_policy_sha256") != check.config_sha256(policy)):
        raise ValueError("candidate policy or configured native target differs")
    producer, generator, _ = check._producer(state_dir)
    projected = {**config, "data": {**config["data"], "run_id": "native-publication-preview"}}
    native_guard.validate_run_policy(
        generator.RunConfig(**guarded_notebook.run_config_from_shared(generator, projected)), policy)
    if candidate["guard_update_body"]["definition"].get("format") != "ipynb":
        raise ValueError("candidate notebook format differs")
    notebook = _decode_part(candidate["guard_update_body"], "notebook-content.ipynb")
    expected = guarded_notebook.build_native_notebook(
        producer, policy, check.WORKSPACE, check.LAKEHOUSE, check.HOUSE_NAME,
        check.STAGING_WORKSPACE, check.STAGING_LAKEHOUSE)
    if _notebook(notebook) != _notebook(expected):
        raise ValueError("candidate guard source or Lakehouse binding differs")
    graph = _decode_part(candidate["reflex_update_body"], "ReflexEntities.json")
    sample = _load_json(state_dir / "demo3-event-e2e-sample.json")
    expected_graph = activator.build_native_definition(
        check.TENANT, check.WORKSPACE, check.LAKEHOUSE, check.PIPELINE, sample)
    if graph != state.get("definition_disabled") or _normalized_graph(graph) != _normalized_graph(expected_graph):
        raise ValueError("candidate stopped event graph or references differ")
    return state, notebook, graph


def _save(path, value):
    fd, temporary = tempfile.mkstemp(prefix=".native-binding-", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as file:
            json.dump(value, file, indent=2, allow_nan=False)
            file.write("\n")
            file.flush()
            os.fsync(file.fileno())
        os.replace(temporary, path)
        if os.name != "nt":
            directory = os.open(path.parent, os.O_RDONLY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
    finally:
        if Path(temporary).exists():
            Path(temporary).unlink()


def _create(path, value):
    with path.open("x", encoding="utf-8") as file:
        json.dump(value, file, indent=2, allow_nan=False)
        file.write("\n")
        file.flush()
        os.fsync(file.fileno())


def _operation_location(headers):
    values = {key.lower(): value for key, value in headers.items()}
    operation = _guid(values.get("x-ms-operation-id"))
    location = values.get("location")
    if not isinstance(location, str):
        raise ValueError("accepted operation has no Location; reconcile without retry")
    parsed = urlsplit(location)
    if (parsed.scheme != "https" or parsed.username is not None or parsed.password is not None
            or parsed.port is not None or parsed.query or parsed.fragment
            or parsed.path != "/v1/operations/" + operation
            or not (parsed.hostname == "api.fabric.microsoft.com" or re.fullmatch(
                r"wabi-[a-z0-9-]+\.analysis\.windows\.net", parsed.hostname or ""))):
        raise ValueError("accepted operation Location does not identify a trusted Fabric operation")
    return operation, location


def _operation_get(api, location, skill, timeout):
    if urlsplit(location).hostname == "api.fabric.microsoft.com":
        return api.request("GET", location, timeout=timeout, skill=skill)
    request = Request(location, headers={"Authorization": "Bearer " + api.token,
                                         "Accept": "application/json",
                                         "x-ms-fabric-skill": skill}, method="GET")
    try:
        with api.opener.open(request, timeout=timeout) as response:
            status, headers, raw = response.status, response.headers, response.read()
        return status, headers, _json_bytes(raw)
    except (HTTPError, URLError, OSError, UnicodeError, ValueError) as exc:
        raise CandidateBlocked("Fabric operation read was unavailable; do not retry the update.") from exc


def _poll(api, location, operation, skill, *, seconds=180, clock=time.monotonic, sleep=time.sleep):
    if _operation_location({"x-ms-operation-id": operation, "location": location}) != (operation, location):
        raise ValueError("operation identity differs")
    deadline = clock() + seconds
    while True:
        remaining = deadline - clock()
        if remaining <= 0:
            raise TimeoutError("definition operation remains unconfirmed")
        status, headers, body = _operation_get(api, location, skill, min(20, remaining))
        if status != 200 or not isinstance(body, dict):
            raise CandidateBlocked("Fabric operation status is unavailable; do not retry the update.")
        if body.get("status") == "Succeeded":
            return
        if body.get("status") not in ("NotStarted", "Running"):
            raise CandidateBlocked("Fabric operation did not succeed; do not retry the update.")
        retry = headers.get("Retry-After", "5")
        if not isinstance(retry, str) or not re.fullmatch(r"[0-9]{1,6}", retry):
            raise CandidateBlocked("Fabric operation retry interval is invalid.")
        sleep(min(max(1, int(retry)), max(0, deadline - clock())))


def _definition(api, resource, path, *, skill="spark-cli", suffix=""):
    status, headers, document = api.request(
        "POST", _BASE + resource + "/getDefinition" + suffix, {}, skill=skill)
    if status == 202:
        operation, location = _operation_location(headers)
        _poll(api, location, operation, skill)
        status, _, document = _operation_get(api, location + "/result", skill, 20)
    if status != 200 or not isinstance(document, dict):
        raise CandidateBlocked("Fabric definition read was not confirmed.")
    parts = [part for part in document["definition"]["parts"]
             if part.get("path") == path and part.get("payloadType") == "InlineBase64"]
    if len(parts) != 1:
        raise CandidateBlocked("Fabric definition part is missing or ambiguous.")
    return _json_bytes(base64.b64decode(parts[0]["payload"], validate=True))


def _graph(api):
    return _definition(api, "/reflexes/" + check.REFLEX, "ReflexEntities.json", skill="activator-cli")


def _guard(api):
    return _definition(api, "/items/" + check.NOTEBOOK, "notebook-content.ipynb", suffix="?format=ipynb")


def _stopped(graph, expected):
    state = {"definition_disabled": expected, "actions": {}}
    if check._rule_state(graph, state, reusable=True):
        raise CandidateBlocked("Native publication requires the exact stopped rule.")


def _quiescent(api, state_dir):
    workspace = check._owned(api, "/v1/workspaces", "chanel-demo3-copy-events", check.WORKSPACE)
    if workspace.get("capacityId") != "de12195c-40d2-4bee-af60-f0d6dedfbb0f":
        raise CandidateBlocked("Owned workspace capacity changed.")
    check._owned(api, _BASE + "/lakehouses", check.HOUSE_NAME, check.LAKEHOUSE)
    check._owned(api, _BASE + "/items?type=Notebook", "Demo3EventE2eGuardA9bbae81", check.NOTEBOOK)
    check._owned(api, _BASE + "/items?type=DataPipeline", "Demo3EventE2ePipelineA9bbae81", check.PIPELINE)
    check._owned(api, _BASE + "/reflexes", "Demo3EventE2eActivatorA9bbae81", check.REFLEX, "activator-cli")
    pipeline = _definition(api, "/items/" + check.PIPELINE, "pipeline-content.json")
    activities = pipeline["properties"]["activities"]
    if len(activities) != 2 or activities[1].get("name") != "CopyOnlyWhenAdmitted":
        raise CandidateBlocked("Guarded pipeline graph differs.")
    copy_activity = activities[1]["typeProperties"]["ifTrueActivities"][0]
    if pipeline["properties"] != guarded_pipeline.build_pipeline(
            check.WORKSPACE, check.LAKEHOUSE, check.CONNECTION, check.NOTEBOOK, copy_activity)["properties"]:
        raise CandidateBlocked("Exact-file Copy or pipeline binding changed.")
    connection = api.get("/v1/connections/" + check.CONNECTION)
    if (connection.get("id") != check.CONNECTION
            or connection.get("connectionDetails", {}).get("type") != "Lakehouse"):
        raise CandidateBlocked("Lakehouse connection is unavailable.")
    items = list(check._inventory(api, _BASE + "/items"))
    for item_id, item_type in ((check.NOTEBOOK, "Notebook"), (check.PIPELINE, "DataPipeline")):
        if not any(item.get("id") == item_id and item.get("type") == item_type
                   and check.MARKER in item.get("description", "") for item in items):
            raise CandidateBlocked("Owned target job inventory is incomplete.")
    for item in items:
        if item.get("type") not in ("Notebook", "DataPipeline") or check.MARKER not in item.get("description", ""):
            continue
        kind = "RunNotebook" if item["type"] == "Notebook" else "Pipeline"
        if any(job.get("status") not in check.TERMINAL for job in check._inventory(
                api, _BASE + "/items/" + item["id"] + "/jobs/instances?jobType=" + kind)):
            raise CandidateBlocked("An owned job is not terminal.")
    _, _, storage = check._producer(state_dir)
    token = check._storage_token()
    store = storage.OneLakeStore(check.WORKSPACE, check.LAKEHOUSE, lambda: token)
    check._absent(store, check.LOCK_PATH)
    dimensions = json.loads(store.read("Files/demo3-audit/setup/common-dimensions-a9bbae81.json")[0])
    if (dimensions.get("workspace_id") != check.WORKSPACE or dimensions.get("lakehouse_id") != check.LAKEHOUSE
            or dimensions.get("generator_sha256") != check.GENERATOR_SHA
            or dimensions.get("result", {}).get("dimensions") != {
                "DimDate": 369, "DimProduct": 20, "DimCustomer": 100}
            or dimensions["result"].get("fact_unchanged") is not True):
        raise CandidateBlocked("Dimension preparation proof changed.")
    return pipeline


class NativeBinding:
    def __init__(self, state_dir, candidate_path, native_config_path, previous_config_path, api, *, poll=_poll):
        self.root = Path(state_dir)
        self.candidate_path = Path(candidate_path)
        self.native_config_path = Path(native_config_path)
        self.previous_config_path = Path(previous_config_path)
        self.journal = self.root / check.NATIVE_JOURNAL
        if (not self.root.is_absolute() or not self.root.is_dir()
                or any(not item.is_absolute() for item in (
                    self.candidate_path, self.native_config_path, self.previous_config_path))
                or any(item.resolve() == self.journal.resolve() for item in (
                    self.candidate_path, self.native_config_path, self.previous_config_path))):
            raise ValueError("publication requires distinct absolute existing input paths")
        self.api, self.poll = api, poll
        paths = {
            "candidate_sha256": self.candidate_path, "native_config_sha256": self.native_config_path,
            "previous_config_sha256": self.previous_config_path,
            "previous_journal_sha256": self.root / _PREVIOUS,
            "original_journal_sha256": self.root / check.JOURNAL,
            "event_sample_sha256": self.root / "demo3-event-e2e-sample.json",
        }
        inputs = {key: _bytes(path) for key, path in paths.items()}
        self.pinned = {key: _sha(raw) for key, raw in inputs.items()}
        self.candidate = _json_bytes(inputs["candidate_sha256"])
        self.native_config = _json_bytes(inputs["native_config_sha256"])
        self.previous_config = _json_bytes(inputs["previous_config_sha256"])
        self.template, self.notebook, self.graph = candidate_inputs(self.candidate, self.native_config, self.root)
        self.previous = _json_bytes(inputs["previous_journal_sha256"])
        original = _json_bytes(inputs["original_journal_sha256"])
        if (check.config_sha256(self.previous_config) != self.previous.get("config_sha256")
                or self.previous.get("previous_journal") != check.JOURNAL
                or self.previous.get("previous_journal_sha256") != check.config_sha256(original)
                or original.get("runtime_stop") != "rule-disabled-jobs-terminal-lock-absent"
                or original.get("actions", {}).get("disable", {}).get("status") != "confirmed-readback"):
            raise ValueError("previous configuration or stopped predecessor chain differs")
        self._unchanged()

    def _unchanged(self):
        paths = {
            "candidate_sha256": self.candidate_path, "native_config_sha256": self.native_config_path,
            "previous_config_sha256": self.previous_config_path,
            "previous_journal_sha256": self.root / _PREVIOUS,
            "original_journal_sha256": self.root / check.JOURNAL,
            "event_sample_sha256": self.root / "demo3-event-e2e-sample.json",
        }
        if any(_sha(_bytes(path)) != self.pinned[key] for key, path in paths.items()):
            raise CandidateBlocked("A pinned publication input changed; reconcile before continuing.")

    def _state(self):
        state = _load_json(self.journal)
        if (state.get("publisher") != self.pinned or not isinstance(state.get("actions"), dict)
                or any(state.get(key) != value for key, value in self.template.items() if key != "actions")
                or any(key not in _ACTIONS for key in state["actions"])
                or any(not isinstance(value, dict) or value.get("status") not in
                       ("in-flight", "confirmed-readback") for value in state["actions"].values())
                or any(record.get("status") == "confirmed-readback" and
                       (record.get("http") not in (200, 202) or not record.get("time_utc")
                        or not record.get("confirmed_at_utc"))
                       for record in state["actions"].values())
                or ("activator_update" in state["actions"] and
                    state["actions"].get("guard_update", {}).get("status") != "confirmed-readback")):
            raise CandidateBlocked("Native publication journal differs; reconcile manually.")
        for record in state["actions"].values():
            if record.get("status") == "confirmed-readback" and record["http"] == 202:
                _operation_location({"x-ms-operation-id": record.get("operation_id"),
                                     "location": record.get("location")})
        return state

    def _old_guard(self):
        producer, _, _ = check._producer(self.root)
        return guarded_notebook.build_notebook(
            producer, self.previous["run_config"], check.WORKSPACE, check.LAKEHOUSE, check.HOUSE_NAME,
            check.STAGING_WORKSPACE, check.STAGING_LAKEHOUSE)

    def _verify_before(self, action):
        self._unchanged()
        _quiescent(self.api, self.root)
        _stopped(_graph(self.api), self.previous["definition_disabled"])
        expected = self._old_guard() if action == "guard_update" else self.notebook
        if _notebook(_guard(self.api)) != _notebook(expected):
            raise CandidateBlocked("Guard notebook changed before native publication.")
        self._unchanged()

    def _readback(self, action):
        if action == "guard_update":
            if _notebook(_guard(self.api)) != _notebook(self.notebook):
                raise CandidateBlocked("Native guard readback differs; do not retry.")
        else:
            _stopped(_graph(self.api), self.graph)

    def _confirm(self, state, action):
        record = state["actions"][action]
        if record.get("http") == 202:
            operation, location = _operation_location({
                "x-ms-operation-id": record.get("operation_id"), "location": record.get("location")})
            self.poll(self.api, location, operation,
                      "spark-cli" if action == "guard_update" else "activator-cli")
        elif record.get("http") != 200:
            raise CandidateBlocked("Update acceptance is ambiguous; do not retry.")
        self._readback(action)
        self._unchanged()
        record.update(status="confirmed-readback", confirmed_at_utc=_utc())
        _save(self.journal, state)

    def _submit(self, state, action):
        if action in state["actions"]:
            raise CandidateBlocked("An update was already attempted; reconcile, never resubmit.")
        if action == "activator_update" and state["actions"].get(
                "guard_update", {}).get("status") != "confirmed-readback":
            raise CandidateBlocked("Native guard must be confirmed before updating the rule.")
        self._verify_before(action)
        state["actions"][action] = {"status": "in-flight", "time_utc": _utc()}
        _save(self.journal, state)
        resource = ("/items/" + check.NOTEBOOK if action == "guard_update" else "/reflexes/" + check.REFLEX)
        skill = "spark-cli" if action == "guard_update" else "activator-cli"
        body = self.candidate["guard_update_body" if action == "guard_update" else "reflex_update_body"]
        status, headers, _ = self.api.request("POST", _BASE + resource + "/updateDefinition", body,
                                              timeout=120, skill=skill)
        record = state["actions"][action]
        record["http"] = status
        values = {key.lower(): value for key, value in headers.items()}
        if status == 202:
            record.update(operation_id=values.get("x-ms-operation-id"), location=values.get("location"))
        _save(self.journal, state)
        if status not in (200, 202):
            raise CandidateBlocked("Update response was not confirmed; do not retry.")
        self._confirm(state, action)

    def publish(self):
        with _state_lock(self.root):
            if self.journal.exists():
                raise CandidateBlocked("Native binding journal exists; reconcile instead of publishing.")
            self._unchanged()
            check.validate(self.previous_config, "chanel", self.root, api=self.api)
            self._verify_before("guard_update")
            state = copy.deepcopy(self.template)
            state["publisher"] = copy.deepcopy(self.pinned)
            state["prepared_at_utc"] = _utc()
            state["source_assumption"] = (
                "The reusable rule targets future producer files; event fields were observed in the supplied sample.")
            _create(self.journal, state)
            self._submit(state, "guard_update")
            self._submit(state, "activator_update")
            return state

    def resume(self):
        with _state_lock(self.root):
            self._unchanged()
            state = self._state()
            if (state["actions"].get("guard_update", {}).get("status") != "confirmed-readback"
                    or "activator_update" in state["actions"]):
                raise CandidateBlocked("Only an unattempted rule update after confirmed guard may resume.")
            self._submit(state, "activator_update")
            return state

    def reconcile(self):
        with _state_lock(self.root):
            self._unchanged()
            state = self._state()
            for action in _ACTIONS:
                record = state["actions"].get(action)
                if record is None or record.get("status") == "confirmed-readback":
                    continue
                if record.get("status") != "in-flight":
                    raise CandidateBlocked("Unknown native update state; reconcile manually.")
                self._confirm(state, action)
            return state

    def proof(self, output):
        output = Path(output)
        protected = (self.candidate_path, self.native_config_path, self.previous_config_path,
                     self.journal, self.root / _PREVIOUS, self.root / check.JOURNAL)
        if not output.is_absolute() or any(output.resolve() == path.resolve() for path in protected):
            raise ValueError("proof output must be a distinct absolute path")
        with _state_lock(self.root):
            self._unchanged()
            state = self._state()
            if any(state["actions"].get(action, {}).get("status") != "confirmed-readback" for action in _ACTIONS):
                raise CandidateBlocked("Both native updates require confirmed readbacks before proof.")
            check.validate(self.native_config, "chanel", self.root, api=self.api)
            pipeline = _quiescent(self.api, self.root)
            notebook, graph = _guard(self.api), _graph(self.api)
            if _notebook(notebook) != _notebook(self.notebook):
                raise CandidateBlocked("Native guard changed before proof.")
            _stopped(graph, self.graph)
            self._unchanged()
            proof = {
                "version": 1, "workspace_id": check.WORKSPACE, "lakehouse_id": check.LAKEHOUSE,
                "guard_notebook_id": check.NOTEBOOK, "pipeline_id": check.PIPELINE,
                "reflex_id": check.REFLEX, "connection_id": check.CONNECTION, "marker": check.MARKER,
                "native_policy": state["native_policy"], "generator_sha256": check.GENERATOR_SHA,
                "guard_sha256": check.config_sha256(_notebook(notebook)),
                "pipeline_sha256": check.config_sha256(pipeline["properties"]),
                "graph_disabled_sha256": check.config_sha256(check._canonical(graph)),
            }
            _create(output, proof)
            return proof


def main(argv=None):
    parser = argparse.ArgumentParser(description="Publish or reconcile one stopped, owned native binding.")
    parser.add_argument("--mode", required=True, choices=("publish", "resume", "reconcile", "proof"))
    parser.add_argument("--candidate", required=True, type=Path)
    parser.add_argument("--native-config", required=True, type=Path)
    parser.add_argument("--previous-config", required=True, type=Path)
    parser.add_argument("--state-dir", required=True, type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--yes", action="store_true")
    args = parser.parse_args(argv)
    if args.mode in ("publish", "resume") and not args.yes:
        parser.error("publication or resume requires explicit --yes")
    if (args.mode == "proof") != (args.output is not None):
        parser.error("--output is required only for proof")
    api = FabricControl.from_cli(check.TENANT, timeout=120)
    binding = NativeBinding(args.state_dir, args.candidate, args.native_config, args.previous_config, api)
    if args.mode == "proof":
        proof = binding.proof(args.output)
        result = {"mode": "proof", "proof_path": str(args.output), "proof_sha256": check.config_sha256(proof)}
    else:
        state = getattr(binding, args.mode)()
        result = {"mode": args.mode, "journal": str(binding.journal),
                  "actions": {key: value["status"] for key, value in state["actions"].items()}}
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
