import base64
import copy
import json
import sys
import tempfile
import unittest
import uuid
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import isolated_publish as publish
from deploy import CandidateBlocked


def definition(content, item_type, part, notebook=False):
    def encoded(name, value):
        return {"path": name, "payloadType": "InlineBase64",
                "payload": base64.b64encode(json.dumps(value).encode()).decode()}
    result = {"parts": [encoded(".platform", {"metadata": {"type": item_type}}),
                        encoded(part, content)]}
    if notebook:
        result["format"] = "ipynb"
    return result


class PublisherTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        self.controller = publish.Publisher.__new__(publish.Publisher)
        self.controller.root = self.root
        self.controller.journal = self.root / "publication.json"
        self.controller.pinned = {"candidate": "fixed"}
        self.controller.documents = {"config": {"capacity": {"id": str(uuid.uuid4())}},
                                     "spec": {"producer_workspace_id": str(uuid.uuid4()),
                                              "producer_lakehouse_id": str(uuid.uuid4())}}
        self.controller.producer_dir = self.root
        self.controller.producer_state_dir = self.root
        self.controller.producer_prefix = "new"
        self.controller.paths = {"producer_config": self.root / "producer.json"}
        self.controller.api = mock.Mock()
        self.controller._unchanged = mock.Mock()
        self.assets = {"setup_notebook": {"id": str(uuid.uuid4())}}
        self.before = definition({"cells": []}, "Notebook", "notebook-content.ipynb", notebook=True)
        self.content = {"cells": [{"source": ["print('safe')"]}]}
        self.expected = publish._replace(self.before, "setup_notebook", self.content)
        self.state = {"version": 1, "ready": False, "publisher": self.controller.pinned, "actions": {}}
        publish.binding._create(self.controller.journal, self.state)
        self.capacity = mock.patch.object(publish.isolated_preflight, "require_f32")
        self.capacity.start()
        self.addCleanup(self.capacity.stop)
        self.staging = mock.patch.object(publish.isolated_preflight, "staging", return_value={
            "workspace_id": self.controller.documents["spec"]["producer_workspace_id"],
            "lakehouse_id": self.controller.documents["spec"]["producer_lakehouse_id"]})
        self.staging.start()
        self.addCleanup(self.staging.stop)

    def test_submit_records_intent_before_post_and_readback(self):
        def update(method, url, body, **kwargs):
            saved = json.loads(self.controller.journal.read_text(encoding="utf-8"))
            self.assertEqual(saved["actions"]["setup_notebook"]["status"], "in-flight")
            self.assertEqual(kwargs["skill"], "spark-cli")
            self.assertFalse(any(part["path"] == ".platform" for part in body["definition"]["parts"]))
            return 200, {}, None
        self.controller.api.request.side_effect = update
        with mock.patch.object(publish, "_read_definition", side_effect=[self.before, self.expected]):
            self.controller._submit(self.state, "setup_notebook", "/v1/workspaces/new", self.assets,
                                    {"setup_notebook": self.content})
        record = json.loads(self.controller.journal.read_text(encoding="utf-8"))["actions"]["setup_notebook"]
        self.assertEqual(record["status"], "confirmed-readback")
        self.controller.api.request.assert_called_once()

    def test_uncertain_update_is_never_retried(self):
        self.controller.api.request.side_effect = TimeoutError("unknown result")
        with mock.patch.object(publish, "_read_definition", return_value=self.before), \
                self.assertRaises(TimeoutError):
            self.controller._submit(self.state, "setup_notebook", "/v1/workspaces/new", self.assets,
                                    {"setup_notebook": self.content})
        self.assertEqual(self.state["actions"]["setup_notebook"]["status"], "in-flight")
        with self.assertRaisesRegex(CandidateBlocked, "already attempted"):
            self.controller._submit(self.state, "setup_notebook", "/v1/workspaces/new", self.assets,
                                    {"setup_notebook": self.content})
        self.controller.api.request.assert_called_once()
        with self.assertRaisesRegex(CandidateBlocked, "acceptance is unknown"):
            self.controller._confirm(self.state, "setup_notebook", "/v1/workspaces/new", self.assets)
        self.controller.api.request.assert_called_once()

    def test_fresh_staging_failure_prevents_update(self):
        publish.isolated_preflight.staging.side_effect = CandidateBlocked("staging changed")
        with mock.patch.object(publish, "_read_definition", return_value=self.before), \
                self.assertRaisesRegex(CandidateBlocked, "staging changed"):
            self.controller._submit(self.state, "setup_notebook", "/v1/workspaces/new", self.assets,
                                    {"setup_notebook": self.content})
        self.controller.api.request.assert_not_called()
        self.assertEqual(self.state["actions"]["setup_notebook"]["status"], "in-flight")

    def test_changed_staging_or_capacity_prevents_update(self):
        publish.isolated_preflight.staging.return_value = {
            "workspace_id": str(uuid.uuid4()),
            "lakehouse_id": self.controller.documents["spec"]["producer_lakehouse_id"]}
        with mock.patch.object(publish, "_read_definition", return_value=self.before), \
                self.assertRaisesRegex(CandidateBlocked, "staging live readback changed"):
            self.controller._submit(self.state, "setup_notebook", "/v1/workspaces/new", self.assets,
                                    {"setup_notebook": self.content})
        self.controller.api.request.assert_not_called()

        self.state["actions"].clear()
        publish.isolated_preflight.staging.return_value = {
            "workspace_id": self.controller.documents["spec"]["producer_workspace_id"],
            "lakehouse_id": self.controller.documents["spec"]["producer_lakehouse_id"]}
        publish.isolated_preflight.require_f32.side_effect = CandidateBlocked("capacity changed")
        with mock.patch.object(publish, "_read_definition", return_value=self.before), \
                self.assertRaisesRegex(CandidateBlocked, "capacity changed"):
            self.controller._submit(self.state, "setup_notebook", "/v1/workspaces/new", self.assets,
                                    {"setup_notebook": self.content})
        self.controller.api.request.assert_not_called()

    def test_recorded_lro_is_polled_and_read_back_without_resubmitting(self):
        operation = str(uuid.uuid4())
        location = "https://api.fabric.microsoft.com/v1/operations/" + operation
        record = {"status": "in-flight", "http": 202, "operation_id": operation,
                  "location": location, "expected_definition": self.expected}
        self.state["actions"]["setup_notebook"] = record
        publish.binding._save(self.controller.journal, self.state)
        with mock.patch.object(publish.binding, "_poll") as poll, \
                mock.patch.object(publish, "_read_definition", return_value=self.expected):
            self.controller._confirm(self.state, "setup_notebook", "/v1/workspaces/new", self.assets)
        poll.assert_called_once_with(self.controller.api, location, operation, "spark-cli")
        self.controller.api.request.assert_not_called()
        self.assertEqual(self.state["actions"]["setup_notebook"]["status"], "confirmed-readback")

    def test_definition_drift_and_enabled_rule_block(self):
        altered = definition({"cells": ["different"]}, "Notebook", "notebook-content.ipynb", notebook=True)
        self.assertFalse(publish._same_definition(altered, self.expected, "setup_notebook"))
        with self.assertRaisesRegex(CandidateBlocked, "disabled"):
            publish._disabled([{"payload": {"definition": {"type": "Rule",
                                "settings": {"shouldRun": True}}}}])

    def test_reflex_stays_blocked_without_runtime_access_proof(self):
        self.controller._prepare = mock.Mock(return_value=({}, {}, {}))
        with self.assertRaisesRegex(CandidateBlocked, "controlled access job"):
            self.controller.publish("reflex", approved=True)
        self.controller.api.request.assert_not_called()
        with self.assertRaisesRegex(CandidateBlocked, "approved"):
            self.controller.publish("setup_notebook")

    def test_publication_requires_gate_and_stays_unready(self):
        contents = {"setup_notebook": self.content}
        self.controller._prepare = mock.Mock(return_value=({}, self.assets, contents))
        self.controller._gate_locked = mock.Mock(return_value="/v1/workspaces/new")
        self.controller._submit = mock.Mock()
        state = self.controller.publish("setup_notebook", approved=True)
        self.assertFalse(state["ready"])
        self.controller._gate_locked.assert_called_once()
        self.controller._submit.assert_called_once()

    def test_prepare_cross_checks_item_event_and_rebuilt_definitions(self):
        import activator
        import customer_preflight as original

        uid = lambda: str(uuid.uuid4())
        workspace, lakehouse, owner = uid(), uid(), uid()
        staging_workspace, staging_lakehouse = uid(), uid()
        marker = "new-demo3-" + owner
        assets = {}
        for action, kind in publish._TYPES.items():
            intent = uid()
            assets[action] = {"id": uid(), "name": action, "type": kind, "intent_id": intent,
                              "description": f"{marker}; create-intent={intent}", "status": "confirmed"}
        sample = {"type": activator.EVENT_TYPE, "id": "test-event",
                  "subject": "/Files/incoming/sales/sales_bootstrap_0001.csv",
                  "source": f"/tenants/{original.TENANT}/workspaces/{workspace}/items/{lakehouse}"}
        assets["reflex"]["event_sample_sha256"] = publish.isolated_candidate._digest(sample)
        graph = activator.build_native_definition(original.TENANT, workspace, lakehouse,
                                                  assets["pipeline"]["id"], sample)
        spec = {"workspace_id": workspace, "lakehouse_id": lakehouse, "owner_id": owner,
                "lakehouse_name": "Replacement", "notebook_id": assets["guard_notebook"]["id"],
                "pipeline_id": assets["pipeline"]["id"], "reflex_id": assets["reflex"]["id"],
                "producer_workspace_id": staging_workspace, "producer_lakehouse_id": staging_lakehouse,
                "connection_id": uid()}
        contents = {"guard_notebook": {"cells": []}, "pipeline": {"properties": {}},
                    "activator_graph": graph}
        candidate = {"status": "offline-candidate-only", "ready": False, "identities": spec,
                     "owner_marker": marker, "target": {"workspace_id": workspace, "item_id": lakehouse},
                     "producer_source_sha256": {"generator.py": "test"}, "definitions": contents,
                     "definition_sha256": {k: publish.isolated_candidate._digest(v) for k, v in contents.items()}}
        owned = {"workspace": {"id": workspace}, "lakehouse": {"id": lakehouse},
                 "owner": {"id": owner, "marker": marker},
                 "deployment_spec": {"lakehouse_name": "Replacement"}}
        self.controller.api.principal_id = uid()
        self.controller.items = {"version": 1, "pending": None, "assets": assets,
                                 "target_workspace_id": workspace, "target_lakehouse_id": lakehouse,
                                 "owner_id": owner, "marker": marker,
                                 "principal_id": self.controller.api.principal_id}
        self.controller.documents.update(spec=spec, candidate=copy.deepcopy(candidate), copy={}, sample=sample)
        self.controller.prefix = "new"
        self.controller.producer_dir, self.controller.producer_state_dir = self.root, self.root
        self.controller.producer_prefix = "new"
        self.controller.paths = {"producer_config": self.root / "producer.json"}
        with mock.patch.object(publish.ownership, "provision", return_value=owned), \
                mock.patch.object(publish.isolated_preflight, "original_parent"), \
                mock.patch.object(publish.isolated_preflight, "connection"), \
                mock.patch.object(publish.isolated_preflight, "staging", return_value={
                    "workspace_id": staging_workspace, "lakehouse_id": staging_lakehouse}), \
                mock.patch.object(publish.isolated_candidate, "build_candidate", return_value=candidate), \
                mock.patch.object(publish.isolated_setup, "prepare", return_value={"notebook": {"cells": []}}):
            _, result_assets, result_contents = self.controller._prepare()
            self.assertEqual(result_assets, assets)
            self.assertEqual(result_contents["reflex"], graph)
            self.controller.items["assets"]["reflex"]["event_sample_sha256"] = "0" * 64
            with self.assertRaisesRegex(CandidateBlocked, "event"):
                self.controller._prepare()
