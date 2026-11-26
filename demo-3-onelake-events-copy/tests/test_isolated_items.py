import hashlib
import copy
import json
import shutil
import sys
import unittest
import uuid
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import activator
import customer_preflight as original
import isolated_items as subject
import isolated_publish as publish


def uid():
    return str(uuid.uuid4())


class API:
    def __init__(self, root, workspace, capacity):
        self.root, self.workspace, self.capacity = root, workspace, capacity
        self.principal_id = uid()
        self.items = []
        self.calls = []
        self.create_bodies = []
        self.mode = "sync"
        self.sku, self.state = "F32", "Active"
        self.operation = uid()

    def list(self, path):
        assert path == "/v1/capacities"
        return [{"id": self.capacity, "sku": self.sku, "state": self.state}]

    def get(self, path, *, skill):
        self.calls.append(("GET", path, skill))
        if path.endswith("/items"):
            return {"value": list(self.items)}
        if path.endswith("/reflexes"):
            return {"value": [item for item in self.items if item["type"] == "Reflex"]}
        item = next(item for item in self.items if path.endswith("/" + item["id"]))
        expected = "reflexes" if item["type"] == "Reflex" else "items"
        assert path == f"/v1/workspaces/{self.workspace}/{expected}/{item['id']}"
        assert skill == ("activator-cli" if expected == "reflexes" else "spark-cli")
        return item

    def request(self, method, path, body=None, *, skill, timeout=None):
        self.calls.append((method, path, skill))
        if method == "POST":
            self.create_bodies.append((path, copy.deepcopy(body), skill))
            journal = json.loads((self.root / subject.JOURNAL).read_text(encoding="utf-8"))
            assert journal["pending"]["description"] == body["description"]
            if self.mode == "lost":
                raise OSError("response lost")
            kind = "Reflex" if path.endswith("/reflexes") else body["type"]
            item = {"id": uid(), "displayName": body["displayName"], "type": kind,
                    "description": body["description"], "workspaceId": self.workspace}
            self.items.append(item)
            if self.mode == "lost_after_create":
                raise OSError("response lost after acceptance")
            if self.mode == "lro":
                return 202, {"x-ms-operation-id": self.operation,
                             "location": "https://api.fabric.microsoft.com/v1/operations/" + self.operation}, None
            return 201, {}, item
        if path.endswith("/result"):
            return 200, {}, self.items[-1]
        return 200, {}, {"status": "Succeeded"}


class IsolatedItemTests(unittest.TestCase):
    def setUp(self):
        self.root = Path(__file__).resolve().parent / ("isolated-items-test-" + uid())
        self.root.mkdir()
        self.addCleanup(shutil.rmtree, self.root)
        self.workspace, self.lakehouse, self.owner, self.capacity = (uid() for _ in range(4))
        self.api = API(self.root, self.workspace, self.capacity)
        self.config = {"tenant_id": original.TENANT}
        self.owned = {"workspace": {"id": self.workspace}, "lakehouse": {"id": self.lakehouse},
                      "owner": {"id": self.owner, "marker": "test-demo3-" + self.owner},
                      "deployment_spec": {"capacity_id": self.capacity}}
        patch = mock.patch.object(subject.ownership, "provision", return_value=self.owned)
        self.provision = patch.start()
        self.addCleanup(patch.stop)
        patch = mock.patch.object(subject.isolated_preflight, "original_parent", return_value={})
        patch.start()
        self.addCleanup(patch.stop)

    def create(self, key, **kwargs):
        return subject.create_item(self.config, "test", self.root, self.api, key, key,
                                   approved=True, **kwargs)

    def journal(self):
        return json.loads((self.root / subject.JOURNAL).read_text(encoding="utf-8"))

    def sample(self):
        return {"type": activator.EVENT_TYPE, "id": "event",
                "subject": "/Files/incoming/sales/sales_batch_1.csv",
                "source": f"/tenants/{original.TENANT}/workspaces/{self.workspace}/items/{self.lakehouse}"}

    def test_prewrite_intent_receipt_and_distinct_order(self):
        record = self.create("setup_notebook")
        self.assertEqual(record["status"], "confirmed")
        self.assertEqual(self.journal()["assets"]["setup_notebook"], record)
        self.assertIsNone(self.journal()["pending"])
        self.provision.assert_called_with(self.config, "test", self.root, self.api, create=False)
        with self.assertRaises(subject.CandidateBlocked):
            self.create("pipeline")
        with self.assertRaises(subject.CandidateBlocked):
            self.create("setup_notebook")
        self.assertEqual(len([c for c in self.api.calls if c[0] == "POST"]), 1)
        path, body, skill = self.api.create_bodies[0]
        self.assertEqual(path, f"/v1/workspaces/{self.workspace}/items")
        self.assertEqual((body["type"], skill), ("Notebook", "spark-cli"))

    def test_target_rechecked_after_intent_without_nested_lock(self):
        def verify(*args, **kwargs):
            with subject._state_lock(self.root):
                if (self.root / subject.JOURNAL).exists():
                    self.assertIsNotNone(self.journal()["pending"])
            return self.owned
        self.provision.side_effect = verify
        self.create("setup_notebook")
        self.assertEqual(self.provision.call_count, 2)

    def test_changed_target_after_intent_blocks_write(self):
        changed = copy.deepcopy(self.owned)
        changed["lakehouse"]["id"] = uid()
        self.provision.side_effect = [self.owned, changed]
        with self.assertRaises(subject.CandidateBlocked):
            self.create("setup_notebook")
        self.assertIsNotNone(self.journal()["pending"])
        self.assertFalse(any(c[0] == "POST" for c in self.api.calls))

    def test_changed_journal_during_ownership_recheck_blocks_write(self):
        def verify(*args, **kwargs):
            if (self.root / subject.JOURNAL).exists():
                state = self.journal()
                state["pending"]["name"] = "changed"
                (self.root / subject.JOURNAL).write_text(json.dumps(state), encoding="utf-8")
            return self.owned
        self.provision.side_effect = verify
        with self.assertRaises(subject.CandidateBlocked):
            self.create("setup_notebook")
        self.assertFalse(any(c[0] == "POST" for c in self.api.calls))

    def test_approval_and_capacity_fail_before_create(self):
        with self.assertRaises(subject.CandidateBlocked):
            subject.create_item(self.config, "test", self.root, self.api, "setup_notebook", "s")
        for sku, state in [("F16", "Active"), ("F32", "Paused")]:
            self.api.sku, self.api.state = sku, state
            with self.assertRaises(subject.CandidateBlocked):
                self.create("setup_notebook")
            self.assertFalse((self.root / subject.JOURNAL).exists())
        self.assertFalse(any(c[0] == "POST" for c in self.api.calls))

    def test_capacity_checked_again_immediately_before_mutation(self):
        real = subject.isolated_preflight.require_f32
        calls = []
        def capacity(api, value):
            calls.append(value)
            if len(calls) == 2:
                api.sku = "F16"
            return real(api, value)
        with mock.patch.object(subject.isolated_preflight, "require_f32", side_effect=capacity):
            with self.assertRaises(subject.CandidateBlocked):
                self.create("setup_notebook")
        self.assertIsNotNone(self.journal()["pending"])
        self.assertFalse(any(c[0] == "POST" for c in self.api.calls))

    def test_lost_response_never_retries(self):
        self.api.mode = "lost"
        with self.assertRaises(subject.CandidateBlocked):
            self.create("setup_notebook")
        with self.assertRaises(subject.CandidateBlocked):
            self.create("setup_notebook")
        with self.assertRaises(subject.CandidateBlocked):
            subject.reconcile(self.config, "test", self.root, self.api, approved=True)
        self.assertEqual(len([c for c in self.api.calls if c[0] == "POST"]), 1)

    def test_lost_acceptance_reconciles_exact_intent(self):
        self.api.mode = "lost_after_create"
        with self.assertRaises(subject.CandidateBlocked):
            self.create("setup_notebook")
        result = subject.reconcile(self.config, "test", self.root, self.api, approved=True)
        self.assertEqual(result["id"], self.api.items[0]["id"])
        self.assertEqual(len([c for c in self.api.calls if c[0] == "POST"]), 1)

    def test_lro_receipt_persisted_before_poll_and_reconciled_without_post(self):
        self.api.mode = "lro"
        def interrupted(*args, **kwargs):
            pending = self.journal()["pending"]
            self.assertEqual(pending["operation_id"], self.api.operation)
            self.assertTrue(pending["operation_location"].endswith(self.api.operation))
            raise TimeoutError("interrupted")
        with mock.patch.object(subject.native_binding, "_poll", side_effect=interrupted):
            with self.assertRaises(TimeoutError):
                self.create("setup_notebook")
        record = subject.reconcile(self.config, "test", self.root, self.api, approved=True)
        self.assertEqual(record["id"], self.api.items[0]["id"])
        self.assertEqual(len([c for c in self.api.calls if c[0] == "POST"]), 1)
        self.assertTrue(any(c[0] == "GET" and c[1].endswith("/result") and c[2] == "spark-cli"
                            for c in self.api.calls))

    def test_original_target_and_ownership_changes_block(self):
        self.owned["lakehouse"]["id"] = original.LAKEHOUSE
        with self.assertRaises(subject.CandidateBlocked):
            self.create("setup_notebook")
        self.owned["lakehouse"]["id"] = self.lakehouse
        self.create("setup_notebook")
        self.owned["lakehouse"]["id"] = uid()
        with self.assertRaises(subject.CandidateBlocked):
            self.create("guard_notebook")
        self.assertEqual(len([c for c in self.api.calls if c[0] == "POST"]), 1)

    def test_reflex_requires_real_observation_and_actual_source(self):
        for key in ("setup_notebook", "guard_notebook", "pipeline"):
            self.create(key)
        sample = self.sample()
        with self.assertRaises(subject.CandidateBlocked):
            self.create("reflex", event_sample=sample)
        sample["source"] = sample["source"].replace(self.lakehouse, original.LAKEHOUSE)
        with self.assertRaises(ValueError):
            self.create("reflex", event_sample=sample, event_observed=True)
        sample = self.sample()
        record = self.create("reflex", event_sample=sample, event_observed=True)
        canonical = json.dumps(sample, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                               allow_nan=False).encode("utf-8")
        self.assertEqual(record["event_sample_sha256"], hashlib.sha256(canonical).hexdigest())
        reflex_post = [c for c in self.api.calls if c[0] == "POST"][-1]
        self.assertEqual(reflex_post[2], "activator-cli")
        self.assertEqual(self.api.items[-1]["type"], "Reflex")
        path, body, skill = self.api.create_bodies[-1]
        self.assertEqual(path, f"/v1/workspaces/{self.workspace}/reflexes")
        self.assertEqual(set(body), {"displayName", "description"})
        self.assertEqual(skill, "activator-cli")
        self.assertIn(("GET", path, "activator-cli"), self.api.calls)
        self.assertIn(("GET", path + "/" + record["id"], "activator-cli"), self.api.calls)

    def test_created_item_journal_is_accepted_by_publisher(self):
        for key in ("setup_notebook", "guard_notebook", "pipeline"):
            self.create(key)
        sample = self.sample()
        self.create("reflex", event_sample=sample, event_observed=True)
        assets = self.journal()["assets"]
        staging_workspace, staging_lakehouse = uid(), uid()
        spec = {"workspace_id": self.workspace, "lakehouse_id": self.lakehouse,
                "owner_id": self.owner, "lakehouse_name": "Replacement",
                "notebook_id": assets["guard_notebook"]["id"], "pipeline_id": assets["pipeline"]["id"],
                "reflex_id": assets["reflex"]["id"], "producer_workspace_id": staging_workspace,
                "producer_lakehouse_id": staging_lakehouse, "connection_id": uid()}
        graph = activator.build_native_definition(original.TENANT, self.workspace, self.lakehouse,
                                                  assets["pipeline"]["id"], sample)
        definitions = {"guard_notebook": {"cells": []}, "pipeline": {"properties": {}},
                       "activator_graph": graph}
        candidate = {"status": "offline-candidate-only", "ready": False, "identities": spec,
                     "owner_marker": self.owned["owner"]["marker"],
                     "target": {"workspace_id": self.workspace, "item_id": self.lakehouse},
                     "producer_source_sha256": {"generator.py": "test"}, "definitions": definitions,
                     "definition_sha256": {k: publish.isolated_candidate._digest(v)
                                           for k, v in definitions.items()}}
        self.owned["deployment_spec"]["lakehouse_name"] = "Replacement"
        controller = publish.Publisher.__new__(publish.Publisher)
        controller.root, controller.api, controller.prefix = self.root, self.api, "test"
        controller.items = self.journal()
        controller.documents = {"config": {"capacity": {"id": self.capacity}}, "spec": spec,
                                "candidate": candidate, "copy": {}, "sample": sample}
        controller.producer_dir, controller.producer_state_dir = self.root, self.root
        controller.producer_prefix = "test"
        controller.paths = {"producer_config": self.root / "producer.json"}
        controller._unchanged = mock.Mock()
        with mock.patch.object(publish.isolated_preflight, "connection"), \
                mock.patch.object(publish.isolated_preflight, "staging", return_value={
                    "workspace_id": staging_workspace, "lakehouse_id": staging_lakehouse}), \
                mock.patch.object(publish.isolated_candidate, "build_candidate", return_value=candidate), \
                mock.patch.object(publish.isolated_setup, "prepare", return_value={"notebook": {"cells": []}}):
            _, actual_assets, contents = controller._prepare()
        self.assertEqual(actual_assets, assets)
        self.assertEqual(contents["reflex"], graph)

    def test_reflex_lro_uses_activator_telemetry(self):
        for key in ("setup_notebook", "guard_notebook", "pipeline"):
            self.create(key)
        path, body, skill = self.api.create_bodies[-1]
        self.assertEqual((path, body["type"], skill),
                         (f"/v1/workspaces/{self.workspace}/items", "DataPipeline", "spark-cli"))
        self.api.mode = "lro"
        with mock.patch.object(subject.native_binding, "_poll", side_effect=TimeoutError):
            with self.assertRaises(TimeoutError):
                self.create("reflex", event_sample=self.sample(), event_observed=True)
        result = subject.reconcile(self.config, "test", self.root, self.api, approved=True)
        reads = [c for c in self.api.calls if c[0] == "GET" and "/v1/operations/" in c[1]]
        self.assertEqual(len(reads), 2)
        self.assertTrue(all(c[2] == "activator-cli" for c in reads))
        collection = f"/v1/workspaces/{self.workspace}/reflexes"
        self.assertIn(("GET", collection + "/" + result["id"], "activator-cli"), self.api.calls)
        self.assertEqual(len([c for c in self.api.calls if c[0] == "POST"]), 4)

    def test_reflex_lost_acceptance_reconciles_through_reflex_collection(self):
        for key in ("setup_notebook", "guard_notebook", "pipeline"):
            self.create(key)
        self.api.mode = "lost_after_create"
        with self.assertRaises(subject.CandidateBlocked):
            self.create("reflex", event_sample=self.sample(), event_observed=True)
        self.api.calls.clear()
        result = subject.reconcile(self.config, "test", self.root, self.api, approved=True)
        collection = f"/v1/workspaces/{self.workspace}/reflexes"
        self.assertIn(("GET", collection, "activator-cli"), self.api.calls)
        self.assertIn(("GET", collection + "/" + result["id"], "activator-cli"), self.api.calls)
        self.assertFalse(any(c[0] == "POST" for c in self.api.calls))

    def test_unjournaled_name_and_changed_readback_block(self):
        self.api.items.append({"id": uid(), "displayName": "setup_notebook", "type": "Notebook"})
        with self.assertRaises(subject.CandidateBlocked):
            self.create("setup_notebook")
        self.api.items.clear()
        self.create("setup_notebook")
        self.api.items[0]["description"] = "changed"
        with self.assertRaises(subject.CandidateBlocked):
            self.create("guard_notebook")


if __name__ == "__main__":
    unittest.main()
