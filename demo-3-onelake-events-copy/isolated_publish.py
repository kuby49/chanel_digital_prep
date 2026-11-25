"""Journaled definition updates for independently owned replacement items.

The controller does not create items, run jobs, enable rules, or assert readiness.
Activator publication remains blocked until independent runtime access is proven.
"""

import base64
import copy
import importlib.util
import json
from pathlib import Path

import customer_preflight as check
import isolated_candidate
import isolated_preflight
import isolated_setup
import native_binding as binding
import ownership
from deploy import CandidateBlocked, _guid, _state_lock


_ACTIONS = ("setup_notebook", "guard_notebook", "pipeline", "reflex")
_TYPES = {"setup_notebook": "Notebook", "guard_notebook": "Notebook",
          "pipeline": "DataPipeline", "reflex": "Reflex"}
_PARTS = {"setup_notebook": "notebook-content.ipynb", "guard_notebook": "notebook-content.ipynb",
          "pipeline": "pipeline-content.json", "reflex": "ReflexEntities.json"}
_TERMINAL = {"Completed", "Failed", "Cancelled", "Canceled", "Deduped"}


def _read_definition(api, base, item, action):
    skill = "activator-cli" if action == "reflex" else "spark-cli"
    resource = "/reflexes/" if action == "reflex" else "/items/"
    suffix = "?format=ipynb" if action.endswith("notebook") else ""
    status, headers, document = api.request(
        "POST", base + resource + item + "/getDefinition" + suffix, {}, skill=skill)
    if status == 202:
        operation, location = binding._operation_location(headers)
        binding._poll(api, location, operation, skill)
        status, _, document = binding._operation_get(api, location + "/result", skill, 20)
    if status != 200 or not isinstance(document, dict) or not isinstance(document.get("definition"), dict):
        raise CandidateBlocked("Replacement definition readback is unavailable.")
    definition = document["definition"]
    if action.endswith("notebook"):
        definition = {**definition, "format": definition.get("format", "ipynb")}
        binding._notebook_parts(definition)
    else:
        binding._item_parts(definition, _PARTS[action], _TYPES[action])
    return definition


def _replace(definition, action, content):
    result = copy.deepcopy(definition)
    part = next(part for part in result["parts"] if part["path"] == _PARTS[action])
    part["payload"] = base64.b64encode(json.dumps(
        content, ensure_ascii=False, allow_nan=False).encode("utf-8")).decode("ascii")
    binding._item_parts(result, _PARTS[action], _TYPES[action])
    return result


def _same_definition(actual, expected, action):
    actual_parts, actual_content = binding._item_parts(actual, _PARTS[action], _TYPES[action])
    expected_parts, expected_content = binding._item_parts(expected, _PARTS[action], _TYPES[action])
    if ({key: value for key, value in actual.items() if key != "parts"} !=
            {key: value for key, value in expected.items() if key != "parts"}
            or set(actual_parts) != set(expected_parts) or actual_content != expected_content):
        return False
    for name in actual_parts:
        left, right = actual_parts[name][0], expected_parts[name][0]
        if name == _PARTS[action]:
            left = {key: value for key, value in left.items() if key != "payload"}
            right = {key: value for key, value in right.items() if key != "payload"}
        if left != right:
            return False
    return True


def _disabled(graph):
    if not isinstance(graph, list):
        raise CandidateBlocked("Replacement Activator graph is invalid.")
    for entity in graph:
        if not isinstance(entity, dict) or not isinstance(entity.get("payload"), dict):
            raise CandidateBlocked("Replacement Activator entity is invalid.")
        definition = entity["payload"].get("definition", {})
        if definition.get("type") == "Rule" and definition.get("settings", {}).get("shouldRun") is not False:
            raise CandidateBlocked("Replacement Activator must stay disabled.")


class Publisher:
    """Publish only exact candidate definitions to confirmed owned replacement items."""

    def __init__(self, *, state_dir, config_path, prefix, candidate_path, spec_path, copy_path,
                 sample_path, producer_dir, producer_config_path, producer_prefix,
                 producer_state_dir, api):
        self.root = Path(state_dir)
        self.producer_dir, self.producer_state_dir = Path(producer_dir), Path(producer_state_dir)
        self.paths = {key: Path(value) for key, value in {
            "config": config_path, "candidate": candidate_path, "spec": spec_path, "copy": copy_path,
            "sample": sample_path, "producer_config": producer_config_path}.items()}
        if (not self.root.is_absolute() or not self.root.is_dir()
                or not self.producer_dir.is_absolute() or not self.producer_state_dir.is_absolute()
                or any(not path.is_absolute() for path in self.paths.values())):
            raise ValueError("Publisher requires existing absolute paths.")
        self.prefix, self.producer_prefix, self.api = prefix, producer_prefix, api
        self.items_path = self.root / "demo3-isolated-items-v1.json"
        self.journal = self.root / "demo3-isolated-publication-v1.json"
        if any(path.resolve() in (self.journal.resolve(), self.items_path.resolve())
               for path in self.paths.values()):
            raise ValueError("Publisher inputs must not overwrite owned journals.")
        self.pinned = {key: binding._sha(binding._bytes(path)) for key, path in self.paths.items()}
        self.pinned["items"] = binding._sha(binding._bytes(self.items_path))
        self.pinned["generator"] = binding._sha(binding._bytes(self.producer_dir / "generator.py"))
        self.pinned["storage"] = binding._sha(binding._bytes(self.producer_dir / "fabric_storage.py"))
        self.pinned["producer_deploy"] = binding._sha(binding._bytes(self.producer_dir / "deploy.py"))
        self.documents = {key: binding._json_bytes(binding._bytes(path)) for key, path in self.paths.items()
                          if key != "producer_config"}
        self.items = binding._json_bytes(binding._bytes(self.items_path))
        self._unchanged()

    def _unchanged(self):
        current = {key: binding._sha(binding._bytes(path)) for key, path in self.paths.items()}
        current["items"] = binding._sha(binding._bytes(self.items_path))
        current["generator"] = binding._sha(binding._bytes(self.producer_dir / "generator.py"))
        current["storage"] = binding._sha(binding._bytes(self.producer_dir / "fabric_storage.py"))
        current["producer_deploy"] = binding._sha(binding._bytes(self.producer_dir / "deploy.py"))
        if current != self.pinned:
            raise CandidateBlocked("Pinned replacement publication inputs changed.")

    def _prepare(self):
        self._unchanged()
        config, spec = self.documents["config"], self.documents["spec"]
        owned = ownership.provision(config, self.prefix, self.root, self.api, create=False)
        isolated_preflight.original_parent(self.api)
        isolated_preflight.require_f32(self.api, config["capacity"]["id"])
        isolated_preflight.connection(self.api, spec["connection_id"])
        staging = isolated_preflight.staging(
            self.api, self.producer_dir, self.paths["producer_config"],
            self.producer_prefix, self.producer_state_dir)
        if (staging["workspace_id"], staging["lakehouse_id"]) != (
                spec["producer_workspace_id"], spec["producer_lakehouse_id"]):
            raise CandidateBlocked("Replacement candidate staging differs from live ownership.")
        items = self.items
        if (items.get("version") != 1 or items.get("pending") is not None
                or items.get("target_workspace_id") != owned["workspace"]["id"]
                or items.get("target_lakehouse_id") != owned["lakehouse"]["id"]
                or items.get("owner_id") != owned["owner"]["id"]
                or items.get("marker") != owned["owner"]["marker"]
                or items.get("principal_id") != self.api.principal_id
                or not isinstance(items.get("assets"), dict)
                or set(items["assets"]) != set(_ACTIONS)):
            raise CandidateBlocked("Replacement item ownership journal is incomplete or differs.")
        assets = items["assets"]
        ids = []
        for action in _ACTIONS:
            record = assets[action]
            if (not isinstance(record, dict) or record.get("status") != "confirmed"
                    or record.get("type") != _TYPES[action] or not isinstance(record.get("name"), str)
                    or record.get("description") !=
                       f"{items['marker']}; create-intent={_guid(record.get('intent_id'))}"):
                raise CandidateBlocked("Replacement item ownership receipt is invalid.")
            ids.append(_guid(record.get("id")))
        if len(set(ids)) != len(ids):
            raise CandidateBlocked("Replacement item IDs are not distinct.")
        if (spec.get("workspace_id") != items["target_workspace_id"]
                or spec.get("lakehouse_id") != items["target_lakehouse_id"]
                or spec.get("owner_id") != items["owner_id"]
                or spec.get("lakehouse_name") != owned["deployment_spec"]["lakehouse_name"]
                or any(spec.get(key) != assets[action]["id"] for key, action in (
                    ("notebook_id", "guard_notebook"), ("pipeline_id", "pipeline"), ("reflex_id", "reflex")))):
            raise CandidateBlocked("Candidate does not match the owned replacement items.")
        if assets["reflex"].get("event_sample_sha256") != isolated_candidate._digest(self.documents["sample"]):
            raise CandidateBlocked("Observed replacement event does not match the Reflex creation receipt.")
        expected = isolated_candidate.build_candidate(
            spec, self.producer_dir, self.documents["copy"], self.documents["sample"],
            producer_config_path=self.paths["producer_config"], producer_prefix=self.producer_prefix,
            producer_state_dir=self.producer_state_dir)
        candidate = self.documents["candidate"]
        if (candidate.get("status") != "offline-candidate-only" or candidate.get("ready") is not False
                or candidate.get("identities") != expected["identities"]
                or candidate.get("owner_marker") != expected["owner_marker"]
                or candidate.get("target") != expected["target"]
                or candidate.get("producer_source_sha256") != expected["producer_source_sha256"]):
            raise CandidateBlocked("Replacement candidate identity or source differs.")
        definitions = candidate.get("definitions", {})
        if (set(definitions) != {"guard_notebook", "pipeline", "activator_graph"}
                or definitions.get("guard_notebook") != expected["definitions"]["guard_notebook"]
                or definitions.get("pipeline") != expected["definitions"]["pipeline"]
                or binding._normalized_graph(definitions.get("activator_graph")) !=
                   binding._normalized_graph(expected["definitions"]["activator_graph"])
                or candidate.get("definition_sha256") != {
                    key: isolated_candidate._digest(value) for key, value in definitions.items()}):
            raise CandidateBlocked("Replacement candidate definitions differ from their builders.")
        _disabled(definitions["activator_graph"])
        setup = isolated_setup.prepare(config, self.prefix, self.root, self.api, self.producer_dir)
        contents = {"setup_notebook": setup["notebook"], "guard_notebook": definitions["guard_notebook"],
                    "pipeline": definitions["pipeline"], "reflex": definitions["activator_graph"]}
        self._unchanged()
        return owned, assets, contents

    def _live_items(self, owned, assets):
        base = f"/v1/workspaces/{owned['workspace']['id']}"
        for action, record in assets.items():
            skill = "activator-cli" if action == "reflex" else "spark-cli"
            collection = base + ("/reflexes" if action == "reflex" else "/items?type=" + record["type"])
            rows = [item for item in check._inventory(self.api, collection, skill)
                    if item.get("displayName") == record["name"]]
            if (len(rows) != 1 or rows[0].get("id") != record["id"]
                    or rows[0].get("description") != record["description"]
                    or rows[0].get("type") != record["type"]):
                raise CandidateBlocked("Replacement item live ownership readback differs.")
            if action != "reflex":
                kind = "Pipeline" if action == "pipeline" else "RunNotebook"
                jobs = base + "/items/" + record["id"] + "/jobs"
                if any(job.get("status") not in _TERMINAL for job in
                       check._inventory(self.api, jobs + "/instances?jobType=" + kind)):
                    raise CandidateBlocked("Replacement item has an active or unknown job.")
                if any(schedule.get("enabled") is not False for schedule in
                       check._inventory(self.api, jobs + "/" + kind + "/schedules")):
                    raise CandidateBlocked("Replacement item has an enabled or unknown schedule.")
        return base

    def _state(self, contents):
        state = binding._json_bytes(binding._bytes(self.journal))
        if (state.get("version") != 1 or state.get("ready") is not False
                or state.get("publisher") != self.pinned
                or not isinstance(state.get("actions"), dict)
                or any(key not in _ACTIONS for key in state["actions"])
                or any(not isinstance(record, dict) or record.get("status") not in
                       ("in-flight", "confirmed-readback") for record in state["actions"].values())):
            raise CandidateBlocked("Replacement publication journal differs.")
        for index, action in enumerate(_ACTIONS):
            if action in state["actions"] and any(
                    state["actions"].get(previous, {}).get("status") != "confirmed-readback"
                    for previous in _ACTIONS[:index]):
                raise CandidateBlocked("Replacement definition journal order differs.")
            if action in state["actions"]:
                record = state["actions"][action]
                _, content = binding._item_parts(record.get("expected_definition"), _PARTS[action], _TYPES[action])
                if content != contents[action]:
                    raise CandidateBlocked("Journaled replacement content differs from the reviewed candidate.")
        return state

    def _target_lock_absent(self, owned):
        source = self.producer_dir / "fabric_storage.py"
        module_spec = importlib.util.spec_from_file_location("_isolated_publisher_storage", source)
        if module_spec is None or module_spec.loader is None:
            raise CandidateBlocked("Replacement storage client cannot be loaded.")
        module = importlib.util.module_from_spec(module_spec)
        module_spec.loader.exec_module(module)
        def storage_token():
            credential = ownership.cli_credential("https://storage.azure.com/", timeout=120)
            try:
                token = credential["accessToken"]
                payload = token.split(".")[1]
                claims = json.loads(base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4)))
                if (credential["tenant"] != check.TENANT or claims["tid"] != check.TENANT
                        or claims["oid"] != self.api.principal_id):
                    raise ValueError("identity mismatch")
            except (KeyError, ValueError, TypeError, IndexError) as exc:
                raise CandidateBlocked("Storage token identity differs from the publication caller.") from exc
            return token
        store = module.OneLakeStore(owned["workspace"]["id"], owned["lakehouse"]["id"],
                                    storage_token)
        check._absent(store, check.LOCK_PATH)

    def _live_target(self, owned):
        spec = owned["deployment_spec"]
        marker = owned["owner"]["marker"]
        parent = None
        for key in ("workspace", "lakehouse"):
            record = owned[key]
            fresh = ownership._readback(
                self.api, spec, record, marker, record["create_intent_id"], parent)
            if fresh != record.get("readback"):
                raise CandidateBlocked("Replacement target ownership readback changed.")
            parent = record["id"]

    def _gate_locked(self, owned, assets):
        self._unchanged()
        self._live_target(owned)
        isolated_preflight.original_parent(self.api)
        isolated_preflight.require_f32(self.api, owned["deployment_spec"]["capacity_id"])
        isolated_preflight.connection(self.api, self.documents["spec"]["connection_id"])
        staging = isolated_preflight.staging(
            self.api, self.producer_dir, self.paths["producer_config"],
            self.producer_prefix, self.producer_state_dir)
        if (staging["workspace_id"], staging["lakehouse_id"]) != (
                self.documents["spec"]["producer_workspace_id"],
                self.documents["spec"]["producer_lakehouse_id"]):
            raise CandidateBlocked("Replacement staging live readback changed.")
        base = self._live_items(owned, assets)
        self._target_lock_absent(owned)
        current_rule = _read_definition(self.api, base, assets["reflex"]["id"], "reflex")
        _, graph = binding._item_parts(current_rule, _PARTS["reflex"], "Reflex")
        _disabled(graph)
        self._unchanged()
        return base

    def _confirm(self, state, action, base, assets):
        record = state["actions"][action]
        if record.get("http") == 202:
            operation, location = binding._operation_location({
                "x-ms-operation-id": record.get("operation_id"), "location": record.get("location")})
            binding._poll(self.api, location, operation,
                          "activator-cli" if action == "reflex" else "spark-cli")
        elif record.get("http") != 200:
            raise CandidateBlocked("Replacement update acceptance is unknown; never resubmit.")
        actual = _read_definition(self.api, base, assets[action]["id"], action)
        if not _same_definition(actual, record["expected_definition"], action):
            raise CandidateBlocked("Replacement definition readback differs; do not retry.")
        if action == "reflex":
            _, graph = binding._item_parts(actual, _PARTS[action], _TYPES[action])
            _disabled(graph)
        self._unchanged()
        record["status"] = "confirmed-readback"
        record["confirmed_at_utc"] = binding._utc()
        binding._save(self.journal, state)

    def _submit(self, state, action, base, assets, contents):
        if action in state["actions"]:
            raise CandidateBlocked("Replacement update was already attempted; reconcile without resubmitting.")
        index = _ACTIONS.index(action)
        if any(state["actions"].get(previous, {}).get("status") != "confirmed-readback"
               for previous in _ACTIONS[:index]):
            raise CandidateBlocked("Earlier replacement definition readback is not confirmed.")
        before = _read_definition(self.api, base, assets[action]["id"], action)
        expected = _replace(before, action, contents[action])
        if action == "reflex":
            _, graph = binding._item_parts(expected, _PARTS[action], _TYPES[action])
            _disabled(graph)
        payload = copy.deepcopy(expected)
        payload["parts"] = [part for part in payload["parts"] if part["path"] != ".platform"]
        record = {"status": "in-flight", "time_utc": binding._utc(),
                  "expected_definition": expected}
        state["actions"][action] = record
        binding._save(self.journal, state)
        resource = "/reflexes/" if action == "reflex" else "/items/"
        skill = "activator-cli" if action == "reflex" else "spark-cli"
        staging = isolated_preflight.staging(
            self.api, self.producer_dir, self.paths["producer_config"],
            self.producer_prefix, self.producer_state_dir)
        if (staging["workspace_id"], staging["lakehouse_id"]) != (
                self.documents["spec"]["producer_workspace_id"],
                self.documents["spec"]["producer_lakehouse_id"]):
            raise CandidateBlocked("Replacement staging live readback changed.")
        isolated_preflight.require_f32(self.api, self.documents["config"]["capacity"]["id"])
        status, headers, _ = self.api.request(
            "POST", base + resource + assets[action]["id"] + "/updateDefinition",
            {"definition": payload}, timeout=120, skill=skill)
        record["http"] = status
        if status == 202:
            values = {key.lower(): value for key, value in headers.items()}
            record.update(operation_id=values.get("x-ms-operation-id"), location=values.get("location"))
        binding._save(self.journal, state)
        if status not in (200, 202):
            raise CandidateBlocked("Replacement update acceptance is unknown; never resubmit.")
        self._confirm(state, action, base, assets)

    def publish(self, action, *, approved=False):
        if approved is not True or action not in _ACTIONS:
            raise CandidateBlocked("An explicitly approved replacement definition update is required.")
        if action == "reflex":
            raise CandidateBlocked("A separately approved controlled access job and authoritative "
                                   "runtime identity, target/staging read-write and connection proof "
                                   "are required before publishing the disabled Activator.")
        owned, assets, contents = self._prepare()
        with _state_lock(self.root):
            base = self._gate_locked(owned, assets)
            if self.journal.exists():
                state = self._state(contents)
            else:
                state = {"version": 1, "ready": False, "publisher": self.pinned, "actions": {}}
                binding._create(self.journal, state)
            self._submit(state, action, base, assets, contents)
            return state

    def reconcile(self):
        """Read back recorded operations only; never submit an update."""
        owned, assets, contents = self._prepare()
        with _state_lock(self.root):
            state = self._state(contents)
            base = self._gate_locked(owned, assets)
            for action in _ACTIONS:
                record = state["actions"].get(action)
                if record and record["status"] == "in-flight":
                    self._confirm(state, action, base, assets)
            return state
