import base64
import copy
import importlib.util
import json
import os
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import activator
import customer_preflight as check
import guarded_notebook
import native_binding
import native_rule
from deploy import CandidateBlocked


@unittest.skipUnless(
    os.environ.get("DEMO3_PRODUCER_PATH") or
    (ROOT.parent / "demo-data-generator" / "generator.py").is_file(),
    "producer fixture not supplied",
)
class NativeBindingTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.producer = Path(os.environ.get("DEMO3_PRODUCER_PATH") or
                            ROOT.parent / "demo-data-generator" / "generator.py").resolve().parent
        spec = importlib.util.spec_from_file_location("binding_producer_fixture", cls.producer / "generator.py")
        cls.generator = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = cls.generator
        cls.addClassCleanup(sys.modules.pop, spec.name, None)
        spec.loader.exec_module(cls.generator)

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.original = {"runtime_stop": "rule-disabled-jobs-terminal-lock-absent",
                         "actions": {"disable": {"status": "confirmed-readback"}}}
        self.old_config = {"old": "frozen"}
        self.previous = {
            "run_config": check.RUN, "config_sha256": check.config_sha256(self.old_config),
            "actions": {name: {"status": "confirmed-readback"} for name in native_binding._ACTIONS},
            "previous_journal": check.JOURNAL,
            "previous_journal_sha256": check.config_sha256(self.original),
            "definition_disabled": [],
        }
        self.write(check.JOURNAL, self.original)
        self.write(native_binding._PREVIOUS, self.previous)
        self.write("old.json", self.old_config)
        sample = {"type": activator.EVENT_TYPE, "id": "observed",
                  "subject": "/Files/incoming/sales/sales_bootstrap_0001.csv",
                  "source": f"/tenants/{check.TENANT}/workspaces/{check.WORKSPACE}/items/{check.LAKEHOUSE}"}
        self.write("demo3-event-e2e-sample.json", sample)
        policy = {"version": 1, "max_file_count": 10, "max_rows_per_file": 1000,
                  "max_admission_seconds": 86400, "writer_wait_seconds": 300,
                  "run_template": {key: value for key, value in check.RUN.items()
                                   if key not in ("run_id", "file_count", "rows_per_file")}}
        config = {"contract_version": 1, "tenant_id": check.TENANT,
                  "demo-3": {"workspace_id": check.WORKSPACE, "lakehouse_id": check.LAKEHOUSE,
                             "destination_table": "dbo.factsales", "native_preflight": True,
                             "native_binding_journal": check.NATIVE_JOURNAL, "native_policy": policy},
                  "shared": {"workspace_id": check.STAGING_WORKSPACE,
                             "staging_lakehouse_id": check.STAGING_LAKEHOUSE,
                             "target_bindings": {"demo-3": {"workspace_id": check.WORKSPACE,
                                                             "item_id": check.LAKEHOUSE,
                                                             "fact": "dbo.factsales"}}},
                  "data": {"run_id": "original", "start_date": "2026-01-01", "seed": 42,
                           "date_count": 365, "product_count": 20, "customer_count": 100,
                           "file_count": 2, "rows_per_file": 10, "interval_seconds": 60}}
        self.write("native.json", config)
        graph = activator.build_native_definition(check.TENANT, check.WORKSPACE, check.LAKEHOUSE,
                                                   check.PIPELINE, sample)
        notebook = guarded_notebook.build_native_notebook(
            self.producer, policy, check.WORKSPACE, check.LAKEHOUSE, check.HOUSE_NAME,
            check.STAGING_WORKSPACE, check.STAGING_LAKEHOUSE)
        self.template = {"workspace_id": check.WORKSPACE, "lakehouse_id": check.LAKEHOUSE,
                         "guard_notebook_id": check.NOTEBOOK, "pipeline_id": check.PIPELINE,
                         "reflex_id": check.REFLEX, "producer_workspace_id": check.STAGING_WORKSPACE,
                         "producer_lakehouse_id": check.STAGING_LAKEHOUSE,
                         "generator_sha256": check.GENERATOR_SHA, "native_policy": policy,
                         "native_policy_sha256": check.config_sha256(policy), "definition_disabled": graph,
                         "previous_journal": native_binding._PREVIOUS,
                         "previous_journal_sha256": check.config_sha256(self.previous), "actions": {}}
        part = lambda name, value: {"path": name, "payloadType": "InlineBase64",
                                    "payload": base64.b64encode(json.dumps(value).encode()).decode()}
        self.candidate = {
            "contract_version": 1, "status": "offline-candidate-only", "ready": False,
            "binding_journal_candidate": self.template,
            "guard_update_body": {"definition": {"format": "ipynb", "parts": [
                part("notebook-content.ipynb", notebook)]}},
            "reflex_update_body": {"definition": {"parts": [part("ReflexEntities.json", graph)]}},
        }
        self.write("candidate.json", self.candidate)
        self.producer_patch = mock.patch.object(check, "_producer", return_value=(self.producer, self.generator, None))
        self.producer_patch.start()
        self.addCleanup(self.producer_patch.stop)
        self.api = mock.Mock()
        self.poll = mock.Mock()
        self.binding = native_binding.NativeBinding(
            self.root, self.root / "candidate.json", self.root / "native.json", self.root / "old.json",
            self.api, poll=self.poll)
        self.before = mock.patch.object(self.binding, "_verify_before")
        self.readback = mock.patch.object(self.binding, "_readback")
        self.validation = mock.patch.object(check, "validate")
        self.before.start()
        self.readback.start()
        self.validation.start()
        self.addCleanup(self.before.stop)
        self.addCleanup(self.readback.stop)
        self.addCleanup(self.validation.stop)

    def write(self, name, document):
        (self.root / name).write_text(json.dumps(document), encoding="utf-8")

    def test_candidate_rejects_changed_graph_or_guard(self):
        changed = copy.deepcopy(self.candidate)
        graph = changed["binding_journal_candidate"]["definition_disabled"]
        graph[1]["payload"]["filterSettings"]["filters"][0]["values"] = ["/Files/other/"]
        encoded = base64.b64encode(json.dumps(graph).encode()).decode()
        changed["reflex_update_body"]["definition"]["parts"][0]["payload"] = encoded
        with self.assertRaisesRegex(ValueError, "graph or references"):
            native_binding.candidate_inputs(changed, self.binding.native_config, self.root)
        changed = copy.deepcopy(self.candidate)
        part = changed["guard_update_body"]["definition"]["parts"][0]
        notebook = json.loads(base64.b64decode(part["payload"]))
        notebook["cells"][1]["source"].append("unsafe()\n")
        part["payload"] = base64.b64encode(json.dumps(notebook).encode()).decode()
        with self.assertRaisesRegex(ValueError, "guard source"):
            native_binding.candidate_inputs(changed, self.binding.native_config, self.root)

    def test_each_update_has_durable_intent_and_confirmed_receipt(self):
        operation = "468567aa-fc56-4b9f-b61a-0e39ae31fe51"
        location = "https://wabi-west-us3-a-primary-redirect.analysis.windows.net/v1/operations/" + operation

        def request(method, url, body, **kwargs):
            state = json.loads(self.binding.journal.read_text(encoding="utf-8"))
            action = "guard_update" if "/items/" in url else "activator_update"
            self.assertEqual(state["actions"][action]["status"], "in-flight")
            return (202, {"x-ms-operation-id": operation, "Location": location}, None) if action == "guard_update" \
                else (200, {}, None)

        self.api.request.side_effect = request
        state = self.binding.publish()
        self.assertEqual([state["actions"][name]["status"] for name in native_binding._ACTIONS],
                         ["confirmed-readback", "confirmed-readback"])
        self.assertEqual(self.api.request.call_count, 2)
        self.poll.assert_called_once_with(self.api, location, operation, "spark-cli")
        self.assertEqual(self.binding._state()["publisher"], self.binding.pinned)
        with self.assertRaisesRegex(CandidateBlocked, "journal exists"):
            self.binding.publish()
        self.assertEqual(self.api.request.call_count, 2)

    def test_uncertain_submission_cannot_be_retried_or_confirmed_by_guess(self):
        self.api.request.side_effect = RuntimeError("response lost")
        with self.assertRaisesRegex(RuntimeError, "response lost"):
            self.binding.publish()
        self.assertEqual(self.binding._state()["actions"]["guard_update"]["status"], "in-flight")
        with self.assertRaisesRegex(CandidateBlocked, "ambiguous"):
            self.binding.reconcile()
        with self.assertRaises(CandidateBlocked):
            self.binding.resume()
        self.assertEqual(self.api.request.call_count, 1)

    def test_lro_reconciliation_never_resubmits_and_can_resume_only_unattempted_rule(self):
        operation = "468567aa-fc56-4b9f-b61a-0e39ae31fe51"
        location = "https://api.fabric.microsoft.com/v1/operations/" + operation
        self.api.request.side_effect = [(202, {"x-ms-operation-id": operation, "Location": location}, None),
                                        (200, {}, None)]
        self.poll.side_effect = [TimeoutError("operation uncertain"), None]
        with self.assertRaisesRegex(TimeoutError, "uncertain"):
            self.binding.publish()
        self.assertEqual(self.binding._state()["actions"]["guard_update"]["status"], "in-flight")
        state = self.binding.reconcile()
        self.assertEqual(state["actions"]["guard_update"]["status"], "confirmed-readback")
        self.assertEqual(self.api.request.call_count, 1)
        state = self.binding.resume()
        self.assertEqual(state["actions"]["activator_update"]["status"], "confirmed-readback")
        self.assertEqual(self.api.request.call_count, 2)
        with self.assertRaises(CandidateBlocked):
            self.binding.resume()

    def test_changed_input_or_untrusted_operation_location_blocks(self):
        self.write("native.json", {"changed": True})
        with self.assertRaisesRegex(CandidateBlocked, "input changed"):
            self.binding.publish()
        self.api.request.assert_not_called()
        operation = "468567aa-fc56-4b9f-b61a-0e39ae31fe51"
        for location in ("https://attacker.example/v1/operations/" + operation,
                         "https://api.fabric.microsoft.com/v1/operations/" + operation + "/result",
                         "https://api.fabric.microsoft.com/v1/operations/" + operation + "?x=1"):
            with self.subTest(location=location), self.assertRaises(ValueError):
                native_binding._operation_location({"x-ms-operation-id": operation, "Location": location})

    def test_pre_mutation_check_rejects_changed_guard_or_enabled_graph(self):
        self.before.stop()
        old = self.binding._old_guard()
        with mock.patch.object(native_binding, "_quiescent"), \
             mock.patch.object(native_binding, "_graph", return_value=self.binding.graph), \
             mock.patch.object(native_binding, "_stopped") as stopped, \
             mock.patch.object(native_binding, "_guard", return_value=old):
            self.binding._verify_before("guard_update")
            stopped.assert_called_once()
        changed = copy.deepcopy(old)
        changed["cells"][1]["source"].append("different\n")
        with mock.patch.object(native_binding, "_quiescent"), \
             mock.patch.object(native_binding, "_graph", return_value=self.binding.graph), \
             mock.patch.object(native_binding, "_stopped"), \
             mock.patch.object(native_binding, "_guard", return_value=changed):
            with self.assertRaisesRegex(CandidateBlocked, "Guard notebook changed"):
                self.binding._verify_before("guard_update")
        enabled = copy.deepcopy(self.binding.graph)
        enabled[3]["payload"]["definition"]["settings"]["shouldRun"] = True
        with self.assertRaisesRegex(CandidateBlocked, "requires the exact stopped rule"):
            native_binding._stopped(enabled, self.binding.graph)
        self.api.request.assert_not_called()

    def test_quiescence_checks_jobs_writer_connection_and_dimensions(self):
        pipeline = {"properties": {"activities": [
            {"name": "PrepareCopy"}, {"name": "CopyOnlyWhenAdmitted",
             "typeProperties": {"ifTrueActivities": [{"type": "Copy"}]}}]}}
        dimensions = {"workspace_id": check.WORKSPACE, "lakehouse_id": check.LAKEHOUSE,
                      "generator_sha256": check.GENERATOR_SHA,
                      "result": {"dimensions": {"DimDate": 369, "DimProduct": 20, "DimCustomer": 100},
                                 "fact_unchanged": True}}
        store = mock.Mock()
        store.read.side_effect = lambda path: (
            (_ for _ in ()).throw(FileNotFoundError()) if path == check.LOCK_PATH
            else (json.dumps(dimensions).encode(), "etag"))
        storage = types.SimpleNamespace(OneLakeStore=lambda *args: store)
        self.api.get.return_value = {"id": check.CONNECTION, "connectionDetails": {"type": "Lakehouse"}}
        owned = lambda api, path, *args: ({"capacityId": "de12195c-40d2-4bee-af60-f0d6dedfbb0f"}
                                           if path == "/v1/workspaces" else {})
        jobs = [{"status": "Completed"}]

        def inventory(api, path, **kwargs):
            if path == native_binding._BASE + "/items":
                return [{"type": "Notebook", "id": check.NOTEBOOK, "description": check.MARKER},
                        {"type": "DataPipeline", "id": check.PIPELINE, "description": check.MARKER}]
            return jobs

        with mock.patch.object(check, "_owned", side_effect=owned), \
             mock.patch.object(check, "_inventory", side_effect=inventory), \
             mock.patch.object(check, "_producer", return_value=(self.producer, self.generator, storage)), \
             mock.patch.object(check, "_storage_token", return_value="token"), \
             mock.patch.object(native_binding, "_definition", return_value=pipeline), \
             mock.patch.object(native_binding.guarded_pipeline, "build_pipeline", return_value=pipeline):
            self.assertEqual(native_binding._quiescent(self.api, self.root), pipeline)
            jobs[0]["status"] = "InProgress"
            with self.assertRaisesRegex(CandidateBlocked, "not terminal"):
                native_binding._quiescent(self.api, self.root)
            jobs[0]["status"] = "Completed"
            store.read.side_effect = lambda path: (b"writer", "etag")
            with self.assertRaisesRegex(CandidateBlocked, "writer lock"):
                native_binding._quiescent(self.api, self.root)
            self.api.get.return_value = {"id": check.CONNECTION, "connectionDetails": {"type": "Other"}}
            with self.assertRaisesRegex(CandidateBlocked, "connection"):
                native_binding._quiescent(self.api, self.root)

    def test_proof_requires_confirmed_actions_and_fresh_matching_readbacks(self):
        state = copy.deepcopy(self.template)
        state["publisher"] = self.binding.pinned
        state["actions"] = {name: {"status": "confirmed-readback", "http": 200,
                                    "time_utc": "2026-01-01T00:00:00+00:00",
                                    "confirmed_at_utc": "2026-01-01T00:00:01+00:00"}
                            for name in native_binding._ACTIONS}
        native_binding._create(self.binding.journal, state)
        pipeline = {"properties": {"verified": True}}
        output = self.root / "proof.json"
        with mock.patch.object(native_binding, "_quiescent", return_value=pipeline), \
             mock.patch.object(native_binding, "_guard", return_value=self.binding.notebook), \
             mock.patch.object(native_binding, "_graph", return_value=self.binding.graph):
            proof = self.binding.proof(output)
            self.assertEqual(proof["guard_sha256"], check.config_sha256(native_binding._notebook(self.binding.notebook)))
            self.assertEqual(proof["pipeline_sha256"], check.config_sha256(pipeline["properties"]))
            self.assertEqual(proof["graph_disabled_sha256"], check.config_sha256(
                check._canonical(self.binding.graph)))
            with self.assertRaises(FileExistsError):
                self.binding.proof(output)
        self.assertEqual(json.loads(output.read_text(encoding="utf-8")), proof)

    def test_poll_reads_exact_operation_location_without_update_retry(self):
        operation = "468567aa-fc56-4b9f-b61a-0e39ae31fe51"
        location = "https://api.fabric.microsoft.com/v1/operations/" + operation
        self.api.request.side_effect = [(200, {"Retry-After": "1"}, {"status": "Running"}),
                                        (200, {}, {"status": "Succeeded"})]
        elapsed = [0]
        native_binding._poll(self.api, location, operation, "activator-cli", seconds=10,
                             clock=lambda: elapsed[0], sleep=lambda n: elapsed.__setitem__(0, elapsed[0] + n))
        self.assertEqual(self.api.request.call_count, 2)
        for call in self.api.request.call_args_list:
            self.assertEqual(call.args[:2], ("GET", location))
            self.assertEqual(call.kwargs["skill"], "activator-cli")

    def test_definition_uses_recorded_operation_location_for_poll_and_result(self):
        operation = "468567aa-fc56-4b9f-b61a-0e39ae31fe51"
        location = "https://api.fabric.microsoft.com/v1/operations/" + operation
        value = {"entities": ["verified"]}
        part = {"path": "ReflexEntities.json", "payloadType": "InlineBase64",
                "payload": base64.b64encode(json.dumps(value).encode()).decode()}
        self.api.request.side_effect = [(202, {"x-ms-operation-id": operation, "Location": location}, None),
                                        (200, {}, {"status": "Succeeded"}),
                                        (200, {}, {"definition": {"parts": [part]}})]
        result = native_binding._definition(self.api, "/reflexes/" + check.REFLEX,
                                            "ReflexEntities.json", skill="activator-cli")
        self.assertEqual(result, value)
        calls = self.api.request.call_args_list
        self.assertEqual([call.args[0] for call in calls], ["POST", "GET", "GET"])
        self.assertEqual([calls[1].args[1], calls[2].args[1]], [location, location + "/result"])
        self.assertTrue(all(call.kwargs["skill"] == "activator-cli" for call in calls))

    def test_changed_predecessor_cannot_be_accepted_on_new_process(self):
        self.write(check.JOURNAL, {"runtime_stop": "different", "actions": {}})
        with self.assertRaisesRegex(ValueError, "predecessor chain"):
            native_binding.NativeBinding(self.root, self.root / "candidate.json", self.root / "native.json",
                                          self.root / "old.json", self.api, poll=self.poll)

    def test_journal_cannot_confirm_without_a_recorded_receipt(self):
        state = copy.deepcopy(self.template)
        state["publisher"] = self.binding.pinned
        state["actions"] = {"guard_update": {"status": "confirmed-readback"}}
        native_binding._create(self.binding.journal, state)
        with self.assertRaisesRegex(CandidateBlocked, "journal differs"):
            self.binding.reconcile()
        self.api.request.assert_not_called()

    def rule_controller(self):
        state = copy.deepcopy(self.template)
        state["publisher"] = self.binding.pinned
        state["actions"] = {name: {"status": "confirmed-readback", "http": 200,
                                    "time_utc": "start", "confirmed_at_utc": "end"}
                            for name in native_binding._ACTIONS}
        native_binding._create(self.binding.journal, state)
        return native_rule.NativeRule(self.binding)

    def test_native_rule_one_shot_enable_disable_and_point_in_time_drain(self):
        controller = self.rule_controller()
        operation = "468567aa-fc56-4b9f-b61a-0e39ae31fe51"
        location = "https://wabi-west-us3-a-primary-redirect.analysis.windows.net/v1/operations/" + operation
        current = copy.deepcopy(self.binding.graph)

        def request(method, url, body, **kwargs):
            self.assertEqual((method, url, kwargs["skill"]),
                             ("POST", native_rule._RESOURCE + "/updateDefinition", "activator-cli"))
            state = json.loads(controller.journal.read_text(encoding="utf-8"))
            action = "disable" if "disable" in state["actions"] else "enable"
            self.assertEqual(state["actions"][action]["status"], "in-flight")
            current[:] = native_binding._decode_part(body, "ReflexEntities.json")
            if action == "enable":
                return 202, {"x-ms-operation-id": operation, "Location": location}, None
            return 200, {}, None

        self.api.request.side_effect = request
        with mock.patch.object(controller, "_verify_proof", return_value="a" * 64), \
             mock.patch.object(check, "_owned"), \
             mock.patch.object(native_binding, "_graph", return_value=current), \
             mock.patch.object(native_binding, "_quiescent") as quiescent:
            controller.change("enable", self.root / "proof.json")
            with self.assertRaisesRegex(CandidateBlocked, "already attempted"):
                controller.change("enable", self.root / "proof.json")
            self.poll.assert_called_once_with(self.api, location, operation, "activator-cli")
            controller.change("disable")
            with self.assertRaisesRegex(CandidateBlocked, "already attempted"):
                controller.change("disable")
            self.assertEqual(current, self.binding.graph)
            self.assertEqual(controller.drain()["event_queue"], "unverified")
            quiescent.assert_called_once_with(self.api, self.root)
        self.assertEqual(self.api.request.call_count, 2)

    def test_native_rule_reconciles_exact_receipt_without_resubmitting(self):
        controller = self.rule_controller()
        operation = "468567aa-fc56-4b9f-b61a-0e39ae31fe51"
        location = "https://api.fabric.microsoft.com/v1/operations/" + operation
        self.api.request.return_value = (202, {"x-ms-operation-id": operation, "Location": location}, None)
        self.poll.side_effect = [TimeoutError("still running"), None]
        with mock.patch.object(controller, "_verify_proof", return_value="a" * 64), \
             mock.patch.object(controller, "_readback"):
            with self.assertRaises(TimeoutError):
                controller.change("enable", self.root / "proof.json")
            with self.assertRaisesRegex(CandidateBlocked, "enable unconfirmed"):
                controller.change("disable")
            state = controller.reconcile()
        self.assertEqual(state["actions"]["enable"]["status"], "confirmed-readback")
        self.assertEqual(self.api.request.call_count, 1)
        self.assertEqual(self.poll.call_args_list, [mock.call(self.api, location, operation, "activator-cli")] * 2)

    def test_native_rule_lost_receipt_blocks_reconcile_and_disable(self):
        controller = self.rule_controller()
        self.api.request.side_effect = RuntimeError("response lost")
        with mock.patch.object(controller, "_verify_proof", return_value="a" * 64), \
             mock.patch.object(controller, "_readback") as readback:
            with self.assertRaisesRegex(RuntimeError, "response lost"):
                controller.change("enable", self.root / "proof.json")
            with self.assertRaisesRegex(CandidateBlocked, "acceptance is unknown"):
                controller.reconcile()
            with self.assertRaisesRegex(CandidateBlocked, "enable unconfirmed"):
                controller.change("disable")
            readback.assert_called_once_with(False)
        self.assertEqual(self.api.request.call_count, 1)

    def test_native_rule_enable_rejects_changed_proof_without_creating_intent(self):
        controller = self.rule_controller()
        self.write("proof.json", {"guard_sha256": "stale"})
        with mock.patch.object(self.binding, "_read_proof", return_value={"guard_sha256": "fresh"}):
            with self.assertRaisesRegex(CandidateBlocked, "proof differs"):
                controller.change("enable", self.root / "proof.json")
        self.assertFalse(controller.journal.exists())
        self.api.request.assert_not_called()


if __name__ == "__main__":
    unittest.main()
