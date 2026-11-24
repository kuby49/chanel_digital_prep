import ast
import base64
import copy
import hashlib
import importlib.util
import json
import os
import sys
import tempfile
import types
import unittest
import uuid
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import activator
import customer_preflight as check
import guarded_notebook
import native_binding
import native_guard_hc
import native_rebind
import native_rule
from deploy import CandidateBlocked


JOB_EVIDENCE = native_rebind.NativeRebind._job_evidence


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
            check.STAGING_WORKSPACE, check.STAGING_LAKEHOUSE, high_concurrency=False)
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
        corrected = copy.deepcopy(self.binding.notebook)
        corrected["cells"][1]["source"].insert(0, native_binding.HC_FLAG)
        correction = {"binding_sha256": check.config_sha256(state), "before": self.binding.notebook,
                      "old_sha256": check.config_sha256(self.binding.notebook), "target": corrected,
                      "target_sha256": check.config_sha256(corrected),
                      "update": {"status": "confirmed-readback", "http": 200,
                                 "time_utc": "start", "confirmed_at_utc": "end"}}
        native_binding._create(self.root / native_binding.HC_JOURNAL, correction)
        with mock.patch.object(native_binding, "_quiescent", return_value=pipeline), \
             mock.patch.object(native_binding, "_guard", return_value=corrected), \
             mock.patch.object(native_binding, "_graph", return_value=self.binding.graph):
            proof = self.binding.proof(output)
            self.assertEqual(proof["guard_sha256"], check.config_sha256(native_binding._notebook(corrected)))
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

    def test_notebook_definition_accepts_absent_format_from_ipynb_read(self):
        part = lambda name, value: {"path": name, "payloadType": "InlineBase64",
                                    "payload": base64.b64encode(json.dumps(value).encode()).decode()}
        definition = {"parts": [part("notebook-content.ipynb", self.binding.notebook),
                                part(".platform", {"metadata": {"type": "Notebook"}})]}
        self.api.request.return_value = (200, {}, {"definition": definition})
        actual = native_binding._notebook_definition(self.api, check.NOTEBOOK)
        self.assertEqual(actual, {**definition, "format": "ipynb"})
        self.assertEqual(self.api.request.call_args.args[1],
                         native_binding._BASE + "/items/" + check.NOTEBOOK + "/getDefinition?format=ipynb")
        self.assertEqual(self.api.request.call_args.kwargs["skill"], "spark-cli")
        self.api.request.return_value = (200, {}, {"definition": {**definition, "format": "other"}})
        with self.assertRaisesRegex(CandidateBlocked, "format differs"):
            native_binding._notebook_definition(self.api, check.NOTEBOOK)

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

    def completed_first_cycle(self):
        first = self.rule_controller()
        state = {"binding_sha256": first._binding_hash(), "proof_sha256": "a" * 64,
                 "actions": {action: {"status": "confirmed-readback", "http": 200,
                                      "time_utc": "start", "confirmed_at_utc": "end"}
                             for action in ("enable", "disable")}}
        native_binding._create(first.journal, state)
        return first, state, native_rule.NativeRule(self.binding, cycle=2)

    def test_second_rule_cycle_preserves_and_pins_first_cycle(self):
        first, _, second = self.completed_first_cycle()
        before = first.journal.read_bytes()
        operation = "468567aa-fc56-4b9f-b61a-0e39ae31fe51"
        location = "https://api.fabric.microsoft.com/v1/operations/" + operation
        self.api.request.side_effect = [(202, {"x-ms-operation-id": operation, "Location": location}, None),
                                        (200, {}, None)]
        with mock.patch.object(second, "_verify_proof", return_value="b" * 64), \
             mock.patch.object(second, "_readback"), mock.patch.object(native_binding, "_quiescent"):
            result = second.change("enable", self.root / "proof.json")
            self.assertEqual(result["previous_journal"], native_binding.RULE_JOURNAL)
            self.assertEqual(result["previous_journal_sha256"], native_binding._sha(before))
            second.change("disable")
            self.assertEqual(second.drain()["event_queue"], "unverified")
            with self.assertRaisesRegex(CandidateBlocked, "already attempted"):
                second.change("enable", self.root / "proof.json")
        self.assertEqual(first.journal.read_bytes(), before)
        self.poll.assert_called_once_with(self.api, location, operation, "activator-cli")
        self.assertEqual(self.api.request.call_count, 2)

    def test_second_rule_cycle_requires_confirmed_predecessor_and_stopped_readback(self):
        first, state, second = self.completed_first_cycle()
        for value in (None, {"status": "in-flight", "time_utc": "start"},
                      {"status": "confirmed-readback", "time_utc": "start"}):
            modified = copy.deepcopy(state)
            if value is None:
                del modified["actions"]["disable"]
            else:
                modified["actions"]["disable"] = value
            native_binding._save(first.journal, modified)
            with mock.patch.object(second, "_verify_proof", return_value="b" * 64), \
                 self.assertRaises(CandidateBlocked):
                second.change("enable", self.root / "proof.json")
            self.assertFalse(second.journal.exists())
        native_binding._save(first.journal, state)
        with mock.patch.object(second, "_verify_proof", return_value="b" * 64), \
             mock.patch.object(second, "_readback", side_effect=CandidateBlocked("rule running")), \
             self.assertRaisesRegex(CandidateBlocked, "rule running"):
            second.change("enable", self.root / "proof.json")
        self.assertFalse(second.journal.exists())
        self.api.request.assert_not_called()

    def test_second_rule_cycle_rejects_missing_or_malformed_predecessor(self):
        first = self.rule_controller()
        second = native_rule.NativeRule(self.binding, cycle=2)
        with mock.patch.object(second, "_verify_proof", return_value="b" * 64):
            with self.assertRaises(FileNotFoundError):
                second.change("enable", self.root / "proof.json")
            first.journal.write_text("{", encoding="utf-8")
            with self.assertRaises(json.JSONDecodeError):
                second.change("enable", self.root / "proof.json")
        self.assertFalse(second.journal.exists())
        self.api.request.assert_not_called()

    def test_proof_cannot_overwrite_second_rule_journal(self):
        with self.assertRaisesRegex(ValueError, "distinct absolute path"):
            self.binding.proof(self.root / native_binding.RULE_SECOND_JOURNAL)
        self.api.request.assert_not_called()

    def test_second_rule_cycle_blocks_changed_predecessor_and_unknown_receipt(self):
        first, _, second = self.completed_first_cycle()
        self.api.request.side_effect = RuntimeError("response lost")
        with mock.patch.object(second, "_verify_proof", return_value="b" * 64), \
             mock.patch.object(second, "_readback"):
            with self.assertRaisesRegex(RuntimeError, "response lost"):
                second.change("enable", self.root / "proof.json")
            with self.assertRaisesRegex(CandidateBlocked, "acceptance is unknown"):
                second.reconcile()
            with first.journal.open("a", encoding="utf-8") as file:
                file.write(" ")
            with self.assertRaisesRegex(CandidateBlocked, "journal changed"):
                second.reconcile()
            with self.assertRaisesRegex(CandidateBlocked, "journal changed"):
                second.change("disable")
        self.assertEqual(self.api.request.call_count, 1)

    def test_second_rule_cycle_reconciles_exact_recorded_operation(self):
        _, _, second = self.completed_first_cycle()
        operation = "468567aa-fc56-4b9f-b61a-0e39ae31fe51"
        location = "https://api.fabric.microsoft.com/v1/operations/" + operation
        self.api.request.return_value = (202, {"x-ms-operation-id": operation, "Location": location}, None)
        self.poll.side_effect = [TimeoutError("pending"), None]
        with mock.patch.object(second, "_verify_proof", return_value="b" * 64), \
             mock.patch.object(second, "_readback"):
            with self.assertRaises(TimeoutError):
                second.change("enable", self.root / "proof.json")
            state = second.reconcile()
            self.assertEqual(state["actions"]["enable"]["status"], "confirmed-readback")
        self.assertEqual(self.api.request.call_count, 1)
        self.assertEqual(self.poll.call_args_list, [mock.call(self.api, location, operation, "activator-cli")] * 2)

    def test_second_rule_cycle_rechecks_predecessor_after_receipt(self):
        first, _, second = self.completed_first_cycle()

        def request(*args, **kwargs):
            with first.journal.open("a", encoding="utf-8") as file:
                file.write(" ")
            return 200, {}, None

        self.api.request.side_effect = request
        with mock.patch.object(second, "_verify_proof", return_value="b" * 64), \
             mock.patch.object(second, "_readback"), \
             self.assertRaisesRegex(CandidateBlocked, "journal changed"):
            second.change("enable", self.root / "proof.json")
        self.assertEqual(json.loads(second.journal.read_text(encoding="utf-8"))["actions"]["enable"]["status"],
                         "in-flight")
        self.assertEqual(self.api.request.call_count, 1)

    def test_hc_correction_preserves_metadata_and_rule_admits_new_proof(self):
        controller = self.rule_controller()
        correction = native_guard_hc.GuardCorrection(self.binding)
        old = copy.deepcopy(self.binding.notebook)
        old["metadata"]["unrelated"] = {"preserve": "yes"}
        old["cells"][1]["metadata"]["unrelated"] = True
        current = copy.deepcopy(old)
        pipeline = {"properties": {"verified": True}}
        published = self.binding._published()
        self.write("old-proof.json", {
            "version": 1, "workspace_id": check.WORKSPACE, "lakehouse_id": check.LAKEHOUSE,
            "guard_notebook_id": check.NOTEBOOK, "pipeline_id": check.PIPELINE, "reflex_id": check.REFLEX,
            "connection_id": check.CONNECTION, "marker": check.MARKER,
            "native_policy": published["native_policy"], "generator_sha256": check.GENERATOR_SHA,
            "guard_sha256": check.config_sha256(native_binding._notebook(old)),
            "pipeline_sha256": check.config_sha256(pipeline["properties"]),
            "graph_disabled_sha256": check.config_sha256(check._canonical(self.binding.graph))})
        operation = "468567aa-fc56-4b9f-b61a-0e39ae31fe51"
        location = "https://api.fabric.microsoft.com/v1/operations/" + operation

        def request(method, url, body, **kwargs):
            state = json.loads(correction.journal.read_text(encoding="utf-8"))
            self.assertEqual(state["update"]["status"], "in-flight")
            current.clear()
            current.update(native_binding._decode_part(body, "notebook-content.ipynb"))
            return 202, {"x-ms-operation-id": operation, "Location": location}, None

        self.api.request.side_effect = request
        with mock.patch.object(native_binding, "_quiescent", return_value=pipeline), \
             mock.patch.object(native_binding, "_guard", return_value=current), \
             mock.patch.object(native_binding, "_graph", return_value=self.binding.graph):
            correction.update(self.root / "old-proof.json")
            self.poll.assert_called_once_with(self.api, location, operation, "spark-cli")
            self.assertEqual(current["metadata"], old["metadata"])
            self.assertEqual(current["cells"][1]["metadata"], old["cells"][1]["metadata"])
            self.assertEqual(current["cells"][1]["source"][0], native_binding.HC_FLAG)
            new_proof = self.binding.proof(self.root / "new-proof.json")
            self.assertEqual(controller._verify_proof(self.root / "new-proof.json"),
                             check.config_sha256(new_proof))
            with self.assertRaisesRegex(CandidateBlocked, "proof differs"):
                controller._verify_proof(self.root / "old-proof.json")
            with self.assertRaisesRegex(CandidateBlocked, "already attempted"):
                correction.update(self.root / "old-proof.json")
        self.assertEqual(self.api.request.call_count, 1)
        with mock.patch.object(native_binding, "_quiescent", return_value=pipeline), \
             mock.patch.object(native_binding, "_guard", return_value=current), \
             mock.patch.object(native_binding, "_graph", return_value=self.binding.graph), \
             mock.patch.object(controller, "_readback"), \
             mock.patch.object(self.api, "request", return_value=(200, {}, None)):
            state = controller.change("enable", self.root / "new-proof.json")
            self.assertEqual(state["actions"]["enable"]["status"], "confirmed-readback")
            self.assertEqual(state["proof_sha256"], check.config_sha256(new_proof))

    def test_hc_uncertain_submission_never_retries(self):
        self.rule_controller()
        correction = native_guard_hc.GuardCorrection(self.binding)
        old = self.binding.notebook
        target = copy.deepcopy(old)
        target["cells"][1]["source"].insert(0, native_binding.HC_FLAG)
        self.write("old-proof.json", {"proof": "test"})
        self.api.request.side_effect = RuntimeError("response lost")
        with mock.patch.object(correction, "_baseline", return_value=(old, target)):
            with self.assertRaisesRegex(RuntimeError, "response lost"):
                correction.update(self.root / "old-proof.json")
            with self.assertRaisesRegex(CandidateBlocked, "acceptance unknown"):
                correction.reconcile()
            with self.assertRaisesRegex(CandidateBlocked, "already attempted"):
                correction.update(self.root / "old-proof.json")
        self.assertEqual(self.api.request.call_count, 1)

    def test_hc_lro_reconcile_polls_recorded_operation_only(self):
        self.rule_controller()
        correction = native_guard_hc.GuardCorrection(self.binding)
        old = copy.deepcopy(self.binding.notebook)
        target = copy.deepcopy(old)
        target["cells"][1]["source"].insert(0, native_binding.HC_FLAG)
        self.write("old-proof.json", {"proof": "test"})
        operation = "468567aa-fc56-4b9f-b61a-0e39ae31fe51"
        location = "https://api.fabric.microsoft.com/v1/operations/" + operation
        self.api.request.return_value = (202, {"x-ms-operation-id": operation, "Location": location}, None)
        self.poll.side_effect = [TimeoutError("pending"), None]
        with mock.patch.object(correction, "_baseline", return_value=(old, target)), \
             mock.patch.object(native_binding, "_guard", return_value=target), \
             mock.patch.object(native_binding, "_graph", return_value=self.binding.graph):
            with self.assertRaises(TimeoutError):
                correction.update(self.root / "old-proof.json")
            state = correction.reconcile()
            self.assertEqual(state["update"]["status"], "confirmed-readback")
        self.assertEqual(self.api.request.call_count, 1)
        self.assertEqual(self.poll.call_args_list, [mock.call(self.api, location, operation, "spark-cli")] * 2)

    def rebound_setup(self):
        first, first_state, _ = self.completed_first_cycle()
        published = self.binding._published()
        corrected = copy.deepcopy(self.binding.notebook)
        corrected["cells"][1]["source"].insert(0, native_binding.HC_FLAG)
        correction = {"binding_sha256": check.config_sha256(published), "before": self.binding.notebook,
                      "old_sha256": check.config_sha256(self.binding.notebook), "target": corrected,
                      "target_sha256": check.config_sha256(corrected),
                      "update": {"status": "confirmed-readback", "http": 200,
                                 "time_utc": "start", "confirmed_at_utc": "end"}}
        self.write(native_binding.HC_JOURNAL, correction)
        pipeline = {"properties": {"verified": True}}
        old_proof = {
            "version": 1, "workspace_id": check.WORKSPACE, "lakehouse_id": check.LAKEHOUSE,
            "guard_notebook_id": check.NOTEBOOK, "pipeline_id": check.PIPELINE, "reflex_id": check.REFLEX,
            "connection_id": check.CONNECTION, "marker": check.MARKER,
            "native_policy": published["native_policy"], "generator_sha256": check.GENERATOR_SHA,
            "guard_sha256": check.config_sha256(native_binding._notebook(corrected)),
            "pipeline_sha256": check.config_sha256(pipeline["properties"]),
            "graph_disabled_sha256": check.config_sha256(check._canonical(self.binding.graph)),
        }
        self.write("old-proof.json", old_proof)
        first_state["proof_sha256"] = check.config_sha256(old_proof)
        native_binding._save(first.journal, first_state)
        access_id, access_pipeline = str(uuid.uuid4()), str(uuid.uuid4())
        self.write("demo3-staging-access-state.json", {
            "workspace_id": check.WORKSPACE, "lakehouse_id": check.LAKEHOUSE,
            "producer_workspace_id": check.STAGING_WORKSPACE,
            "producer_lakehouse_id": check.STAGING_LAKEHOUSE,
            "notebook_id": access_id, "pipeline_id": access_pipeline,
            "actions": {"notebook_create": {"id": access_id, "status": "confirmed", "http": 201},
                        "pipeline_create": {"id": access_pipeline, "status": "confirmed", "http": 201}},
            "result": {"notebook_job_status": "Completed", "activity_status": "Succeeded"},
        })

        def part(name, content):
            return {"path": name, "payloadType": "InlineBase64",
                    "payload": base64.b64encode(content).decode("ascii")}

        def envelope(notebook):
            return {"format": "ipynb", "parts": [
                part("notebook-content.ipynb", json.dumps(notebook).encode()),
                part(".platform", b'{"metadata":{"type":"Notebook","displayName":"Owned"}}'),
                part("builtin/settings.bin", b"unrelated-resource"),
            ]}

        access_before = copy.deepcopy(corrected)
        access_before["metadata"]["unrelated"] = {"must": "remain"}
        access_before["cells"][1]["metadata"]["retained"] = True
        access_definition = envelope(access_before)
        plan = {"version": 1, "producer_workspace_id": str(uuid.uuid4()),
                "producer_lakehouse_id": str(uuid.uuid4()), "principal_id": str(uuid.uuid4()),
                "access_baseline_sha256": native_rebind._fingerprint(access_definition),
                "producer_probe": {"path": "Files/probe/staging.json", "sha256": "a" * 64, "etag": '"stage"'},
                "target_probe": {"path": "Files/probe/target.csv", "sha256": "b" * 64, "etag": '"target"'}}
        self.write("rebind.json", plan)
        rebound = native_rebind.NativeRebind(self.binding, self.root / "rebind.json",
                                             self.root / "old-proof.json")
        live = {check.NOTEBOOK: envelope(corrected), access_id: access_definition}
        graph = copy.deepcopy(self.binding.graph)
        reflex_definition = {"parts": [
            part("ReflexEntities.json", json.dumps(graph).encode()),
            part(".platform", b'{"metadata":{"type":"Reflex","displayName":"Owned"}}'),
            part("resources/keep.bin", b"unrelated-reflex-resource"),
        ]}
        probe_job, notebook_job = str(uuid.uuid4()), str(uuid.uuid4())
        job_result = {"pipeline_job_id": probe_job, "activity_run_id": notebook_job,
                      "notebook_job_id": notebook_job,
                      "activity_status": "Succeeded", "notebook_job_status": "Completed"}
        patches = [
            mock.patch.object(native_binding, "_quiescent", return_value=pipeline),
            mock.patch.object(native_binding, "_graph", return_value=graph),
            mock.patch.object(native_binding, "_notebook_definition", side_effect=lambda api, item: copy.deepcopy(live[item])),
            mock.patch.object(native_binding, "_reflex_definition", side_effect=lambda api: copy.deepcopy(reflex_definition)),
            mock.patch.object(check, "_owned"),
            mock.patch.object(native_rebind.NativeRebind, "_probe_items"),
            mock.patch.object(native_rebind.NativeRebind, "_stage_and_objects"),
            mock.patch.object(native_rebind.NativeRebind, "_recent_jobs",
                              return_value={"RunNotebook": [str(uuid.uuid4())], "Pipeline": [str(uuid.uuid4())]}),
            mock.patch.object(native_rebind.NativeRebind, "_job_evidence", return_value=job_result),
        ]
        for patch in patches:
            patch.start()
            self.addCleanup(patch.stop)

        def request(method, url, body, **kwargs):
            if url.endswith("/updateDefinition") and "/items/" in url:
                item_id = url.split("/items/")[1].split("/")[0]
                self.assertNotIn(".platform", [part["path"] for part in body["definition"]["parts"]])
                platform = next(part for part in live[item_id]["parts"] if part["path"] == ".platform")
                live[item_id] = copy.deepcopy(body["definition"])
                live[item_id]["parts"].append(platform)
                return 200, {}, None
            if "/jobs/Pipeline/instances" in url:
                location = ("https://api.fabric.microsoft.com" + native_binding._BASE + "/items/" +
                            access_pipeline + "/jobs/instances/" + probe_job)
                return 202, {"Location": location}, None
            if "/reflexes/" in url and url.endswith("/updateDefinition"):
                self.assertNotIn(".platform", [part["path"] for part in body["definition"]["parts"]])
                platform = next(part for part in reflex_definition["parts"] if part["path"] == ".platform")
                reflex_definition.clear()
                reflex_definition.update(copy.deepcopy(body["definition"]))
                reflex_definition["parts"].append(platform)
                graph[:] = native_binding._reflex_parts(reflex_definition)[1]
                return 200, {}, None
            raise AssertionError((method, url))

        self.api.request.side_effect = request
        return rebound, live, graph, pipeline, old_proof, job_result

    def finish_access(self, rebound):
        rebound.access_update()
        rebound.access_run()
        state = rebound.access_reconcile()
        self.assertEqual(state["actions"]["run"]["status"], "confirmed-readback")
        return state

    def test_rebound_proof_admits_only_second_cycle_and_preserves_every_part(self):
        rebound, live, graph, _, old_proof, _ = self.rebound_setup()
        first_path = self.root / native_binding.RULE_JOURNAL
        first_bytes = first_path.read_bytes()
        original_guard = copy.deepcopy(live[check.NOTEBOOK])
        access_before = copy.deepcopy(live[rebound.notebook_id])
        self.finish_access(rebound)
        rebound.update()
        for name in (".platform", "builtin/settings.bin"):
            for item, original in ((check.NOTEBOOK, original_guard), (rebound.notebook_id, access_before)):
                before = next(part for part in original["parts"] if part["path"] == name)
                after = next(part for part in live[item]["parts"] if part["path"] == name)
                self.assertEqual(after, before)
        _, before_nb = native_binding._notebook_parts(original_guard)
        _, after_nb = native_binding._notebook_parts(live[check.NOTEBOOK])
        self.assertEqual(before_nb["metadata"], after_nb["metadata"])
        self.assertEqual(before_nb["cells"][1]["metadata"], after_nb["cells"][1]["metadata"])
        self.assertNotEqual(before_nb["cells"][1]["source"], after_nb["cells"][1]["source"])
        proof = rebound.proof(self.root / "new-proof.json")
        self.assertNotEqual(proof["guard_sha256"], old_proof["guard_sha256"])
        with self.assertRaisesRegex(ValueError, "only for rule cycle 2"):
            native_rule.NativeRule(rebound)
        rule = native_rule.NativeRule(rebound, cycle=2)
        with self.assertRaisesRegex(CandidateBlocked, "proof differs"):
            rule._verify_proof(self.root / "old-proof.json")
        state = rule.change("enable", self.root / "new-proof.json")
        self.assertEqual(state["proof_sha256"], check.config_sha256(proof))
        self.assertEqual(state["previous_journal_sha256"], native_binding._sha(first_bytes))
        self.assertNotEqual(state["binding_sha256"], check.config_sha256(self.binding._published()))
        self.assertEqual(first_path.read_bytes(), first_bytes)
        before_rule = state["actions"]["enable"]["before"]
        target_rule = state["actions"]["enable"]["target"]
        for name in (".platform", "resources/keep.bin"):
            self.assertEqual(next(part for part in before_rule["parts"] if part["path"] == name),
                             next(part for part in target_rule["parts"] if part["path"] == name))
        self.assertTrue(check._rule_state(graph, {"definition_disabled": self.binding.graph}, reusable=True))
        self.assertEqual(rebound._proof_readback(rebound._published(), allow_running=True), proof)
        rule.change("disable")
        with self.assertRaisesRegex(CandidateBlocked, "cannot be admitted again"):
            rebound._proof_readback(rebound._published(), allow_running=True)

    def test_definition_parts_are_strict_and_unrelated_payloads_are_compared(self):
        _, live, _, _, _, _ = self.rebound_setup()
        definition = live[check.NOTEBOOK]
        duplicate = copy.deepcopy(definition)
        duplicate["parts"].append(copy.deepcopy(duplicate["parts"][0]))
        missing = copy.deepcopy(definition)
        missing["parts"] = [part for part in missing["parts"] if part["path"] != ".platform"]
        unsafe = copy.deepcopy(definition)
        unsafe["parts"][2]["path"] = "../foreign"
        invalid = copy.deepcopy(definition)
        invalid["parts"][2]["payload"] = "not-base64!"
        for value in (duplicate, missing, unsafe, invalid):
            with self.subTest(value=value), self.assertRaises(CandidateBlocked):
                native_binding._notebook_parts(value)
        altered = copy.deepcopy(definition)
        altered["parts"][2]["payload"] = base64.b64encode(b"changed").decode()
        self.assertFalse(native_binding._same_notebook_definition(definition, altered))
        reordered = copy.deepcopy(definition)
        reordered["parts"].reverse()
        self.assertTrue(native_binding._same_notebook_definition(definition, reordered))

    def test_access_update_rejects_changed_baseline_and_protected_proof_paths(self):
        rebound, live, _, _, _, _ = self.rebound_setup()
        extra = next(part for part in live[rebound.notebook_id]["parts"] if part["path"] == "builtin/settings.bin")
        extra["payload"] = base64.b64encode(b"changed").decode()
        with self.assertRaisesRegex(CandidateBlocked, "baseline changed"):
            rebound.access_update()
        self.assertFalse(rebound.access_journal.exists())
        self.api.request.assert_not_called()
        for path in (rebound.spec_path, rebound.previous_proof_path, rebound.access_journal, rebound.journal,
                     self.root / native_binding.RULE_JOURNAL,
                     self.root / "demo3-staging-access-state.json"):
            with self.subTest(path=path), self.assertRaisesRegex(ValueError, "distinct absolute path"):
                rebound.proof(path)

    def test_access_probe_checks_pipeline_context_identity_hashes_and_etags(self):
        rebound, live, _, _, _, _ = self.rebound_setup()
        _, before = native_binding._notebook_parts(live[rebound.notebook_id])
        spec = copy.deepcopy(rebound.spec)
        objects = {
            (spec["producer_workspace_id"], spec["producer_lakehouse_id"]): b"staging",
            (check.WORKSPACE, check.LAKEHOUSE): b"target",
        }
        spec["producer_probe"]["sha256"] = hashlib.sha256(b"staging").hexdigest()
        spec["target_probe"]["sha256"] = hashlib.sha256(b"target").hexdigest()
        notebook = native_rebind._probe_notebook(before, self.producer, spec, rebound.notebook_id)
        tree = ast.parse("".join(notebook["cells"][1]["source"]))
        statements = [item for item in ast.walk(tree) if isinstance(item, ast.Assign)
                      and isinstance(item.targets[0], ast.Subscript)
                      and isinstance(item.targets[0].value, ast.Name)
                      and item.targets[0].value.id == "_guard_modules"
                      and ast.literal_eval(item.targets[0].slice) == "staging_probe.py"]
        self.assertEqual(len(statements), 1)
        source = ast.literal_eval(statements[0].value)
        etags = {spec["producer_probe"]["path"]: spec["producer_probe"]["etag"],
                 spec["target_probe"]["path"]: spec["target_probe"]["etag"]}

        class Store:
            def __init__(self, workspace, lakehouse, token):
                self.identity, self.token = (workspace, lakehouse), token

            def read(self, path):
                self.token()
                return objects[self.identity], etags[path]

        claims = {"tid": check.TENANT, "oid": spec["principal_id"]}
        token = lambda: "header." + base64.urlsafe_b64encode(
            json.dumps(claims).encode()).decode().rstrip("=") + ".signature"
        context = {"isForPipeline": True, "currentWorkspaceId": check.WORKSPACE,
                   "currentNotebookId": rebound.notebook_id, "defaultLakehouseId": check.LAKEHOUSE}
        utilities = types.SimpleNamespace(runtime=types.SimpleNamespace(context=context),
                                          credentials=types.SimpleNamespace(getToken=lambda audience: token()))
        settings = {"workspace_id": check.WORKSPACE, "item_id": check.LAKEHOUSE,
                    "producer_workspace_id": spec["producer_workspace_id"],
                    "producer_item_id": spec["producer_lakehouse_id"]}
        scope = {}
        with mock.patch.dict(sys.modules, {"guard_spark": types.SimpleNamespace(OneLakeStore=Store)}):
            exec(compile(source, "staging_probe.py", "exec"), scope)
            self.assertEqual(json.loads(scope["execute"](None, utilities, settings, {})),
                             {"pipeline_context": True, "token_identity_matches": True})
            context["isForPipeline"] = False
            with self.assertRaisesRegex(RuntimeError, "pipeline notebook"):
                scope["execute"](None, utilities, settings, {})
            context["isForPipeline"] = True
            claims["oid"] = str(uuid.uuid4())
            with self.assertRaisesRegex(RuntimeError, "identity differs"):
                scope["execute"](None, utilities, settings, {})
            claims["oid"] = spec["principal_id"]
            etags[spec["producer_probe"]["path"]] = '"changed"'
            with self.assertRaisesRegex(RuntimeError, "bytes or ETag"):
                scope["execute"](None, utilities, settings, {})
            etags[spec["producer_probe"]["path"]] = spec["producer_probe"]["etag"]
            objects[(check.WORKSPACE, check.LAKEHOUSE)] = b"changed"
            with self.assertRaisesRegex(RuntimeError, "bytes or ETag"):
                scope["execute"](None, utilities, settings, {})

    def test_access_update_rejects_platform_drift_and_only_reconciles_recorded_write(self):
        rebound, live, _, _, _, _ = self.rebound_setup()
        original_request = self.api.request.side_effect

        def drift(method, url, body, **kwargs):
            result = original_request(method, url, body, **kwargs)
            platform = next(part for part in live[rebound.notebook_id]["parts"] if part["path"] == ".platform")
            platform["payload"] = base64.b64encode(b'{"metadata":{"type":"Notebook","changed":true}}').decode()
            return result

        self.api.request.side_effect = drift
        with self.assertRaisesRegex(CandidateBlocked, "readback differs"):
            rebound.access_update()
        self.assertEqual(self.api.request.call_count, 1)
        with self.assertRaisesRegex(CandidateBlocked, "already exists"):
            rebound.access_update()
        state = rebound._access_state()
        live[rebound.notebook_id] = copy.deepcopy(state["target"])
        rebound.access_reconcile()
        self.assertEqual(self.api.request.call_count, 1)

    def test_rebound_rule_rejects_unrelated_part_drift_after_single_enable(self):
        rebound, _, _, _, _, _ = self.rebound_setup()
        self.finish_access(rebound)
        rebound.update()
        rebound.proof(self.root / "new-proof.json")
        controller = native_rule.NativeRule(rebound, cycle=2)
        original_read = native_binding._reflex_definition.side_effect

        def changed_read(api):
            definition = original_read(api)
            graph = native_binding._reflex_parts(definition)[1]
            if check._rule_state(graph, {"definition_disabled": self.binding.graph}, reusable=True):
                part = next(item for item in definition["parts"] if item["path"] == "resources/keep.bin")
                part["payload"] = base64.b64encode(b"unrelated-change").decode()
            return definition

        native_binding._reflex_definition.side_effect = changed_read
        with self.assertRaisesRegex(CandidateBlocked, "Complete native rule definition changed"):
            controller.change("enable", self.root / "new-proof.json")
        with self.assertRaisesRegex(CandidateBlocked, "Complete native rule definition changed"):
            controller.reconcile()
        with self.assertRaisesRegex(CandidateBlocked, "enable unconfirmed"):
            controller.change("disable")
        self.assertEqual(self.api.request.call_count, 4)

    def test_rebound_profile_requires_new_staging_and_fresh_proof(self):
        rebound, _, _, _, old_proof, _ = self.rebound_setup()
        self.finish_access(rebound)
        rebound.update()
        proof_path = self.root / "new-proof.json"
        proof = rebound.proof(proof_path)
        profile = copy.deepcopy(self.binding.native_config)
        demo = profile["demo-3"]
        demo.update(lakehouse_name=check.HOUSE_NAME, landing_path="Files/incoming/sales",
                    native_proof=proof, native_rebind={
                        "candidate": str(self.binding.candidate_path),
                        "native_config": str(self.binding.native_config_path),
                        "previous_config": str(self.binding.previous_config_path),
                        "spec": str(rebound.spec_path), "previous_proof": str(rebound.previous_proof_path),
                        "proof": str(proof_path),
                    })
        profile["shared"].update(workspace_id=rebound.spec["producer_workspace_id"],
                                 staging_lakehouse_id=rebound.spec["producer_lakehouse_id"])
        profile["data"].update(max_duration_seconds=600, currency="USD")
        self.validation.stop()
        with mock.patch.object(native_binding, "NativeBinding", return_value=self.binding), \
             mock.patch.object(native_rebind, "NativeRebind", return_value=rebound):
            self.assertIn("verified", check.validate(profile, "chanel", self.root, api=self.api)[0])
            profile["shared"]["staging_lakehouse_id"] = check.STAGING_LAKEHOUSE
            with self.assertRaisesRegex(CandidateBlocked, "staging, target or bounded"):
                check.validate(profile, "chanel", self.root, api=self.api)
            profile["shared"]["staging_lakehouse_id"] = rebound.spec["producer_lakehouse_id"]
            demo["native_proof"] = old_proof
            with self.assertRaisesRegex(CandidateBlocked, "proof differs"):
                check.validate(profile, "chanel", self.root, api=self.api)

    def test_unknown_access_run_or_guard_update_never_resubmits(self):
        rebound, _, _, _, _, _ = self.rebound_setup()
        rebound.access_update()
        self.api.request.side_effect = RuntimeError("response lost")
        with self.assertRaisesRegex(RuntimeError, "response lost"):
            rebound.access_run()
        with mock.patch.object(rebound, "_job_evidence", side_effect=lambda record: JOB_EVIDENCE(rebound, record)):
            with self.assertRaisesRegex(CandidateBlocked, "acceptance is unknown"):
                rebound.access_reconcile()
        with self.assertRaisesRegex(CandidateBlocked, "already attempted"):
            rebound.access_run()
        self.assertEqual(self.api.request.call_count, 2)

    def test_guard_lost_receipt_blocks_reconcile_without_second_update(self):
        rebound, _, _, _, _, _ = self.rebound_setup()
        self.finish_access(rebound)
        self.api.request.side_effect = RuntimeError("guard response lost")
        with self.assertRaisesRegex(RuntimeError, "guard response lost"):
            rebound.update()
        with self.assertRaisesRegex(CandidateBlocked, "acceptance is unknown"):
            rebound.reconcile()
        with self.assertRaisesRegex(CandidateBlocked, "already attempted"):
            rebound.update()
        self.assertEqual(self.api.request.call_count, 3)

    def test_guard_lro_reconcile_uses_exact_recorded_operation(self):
        rebound, _, _, _, _, _ = self.rebound_setup()
        self.finish_access(rebound)
        operation = str(uuid.uuid4())
        location = "https://wabi-west-us3-a-primary-redirect.analysis.windows.net/v1/operations/" + operation
        original_request = self.api.request.side_effect

        def request(method, url, body, **kwargs):
            result = original_request(method, url, body, **kwargs)
            if "/items/" + check.NOTEBOOK + "/updateDefinition" in url:
                return 202, {"x-ms-operation-id": operation, "Location": location}, None
            return result

        self.api.request.side_effect = request
        self.poll.side_effect = [TimeoutError("pending"), None]
        with self.assertRaises(TimeoutError):
            rebound.update()
        rebound.reconcile()
        self.assertEqual(self.api.request.call_count, 3)
        self.assertEqual(self.poll.call_args_list,
                         [mock.call(self.api, location, operation, "spark-cli")] * 2)

    def test_rebind_rejects_changed_predecessor_or_access_receipt(self):
        rebound, _, _, _, _, _ = self.rebound_setup()
        self.finish_access(rebound)
        first = self.root / native_binding.RULE_JOURNAL
        with first.open("a", encoding="utf-8") as file:
            file.write(" ")
        with self.assertRaisesRegex(CandidateBlocked, "predecessor changed"):
            rebound.update()
        self.assertEqual(self.api.request.call_count, 2)

    def test_access_job_requires_exact_activity_link_and_completed_notebook(self):
        rebound, _, _, _, _, job_result = self.rebound_setup()
        job_id, notebook_id = job_result["pipeline_job_id"], job_result["notebook_job_id"]
        old_id = str(uuid.uuid4())
        location = ("https://api.fabric.microsoft.com" + native_binding._BASE + "/items/" +
                    rebound.pipeline_id + "/jobs/instances/" + job_id)
        record = {"http": 202, "location": location, "job_id": job_id,
                  "time_utc": "2026-11-24T21:00:00+00:00",
                  "previous_jobs": {"RunNotebook": [old_id], "Pipeline": []}}
        pipeline_job = {"id": job_id, "status": "Completed",
                        "startTimeUtc": "2026-11-24T21:00:01Z",
                        "endTimeUtc": "2026-11-24T21:04:00Z"}
        notebook_job = {"id": notebook_id, "status": "Completed", "jobType": "PipelineRunNotebook",
                        "startTimeUtc": "2026-11-24T21:00:02Z",
                        "endTimeUtc": "2026-11-24T21:05:12Z"}
        self.api.get.side_effect = lambda url, **kwargs: (
            pipeline_job if url == location else notebook_job)
        self.api.request.side_effect = None
        activity = {"activityName": "ReadExistingManifest", "activityType": "TridentNotebook",
                    "activityRunId": notebook_id, "status": "Succeeded"}
        self.api.request.return_value = (200, {}, {"value": [activity]})
        jobs = [{"id": old_id}, {"id": notebook_id, "status": "Completed",
                                "jobType": "PipelineRunNotebook", "startTimeUtc": "2026-11-24T21:00:02Z"}]
        with mock.patch.object(rebound, "_job_evidence", side_effect=lambda record: JOB_EVIDENCE(rebound, record)), \
             mock.patch.object(check, "_inventory", return_value=jobs):
            self.assertEqual(rebound._job_evidence(record), job_result)
            self.api.request.return_value = (200, {}, {"value": [activity], "continuationToken": "more"})
            with self.assertRaisesRegex(CandidateBlocked, "activity"):
                rebound._job_evidence(record)
            self.api.request.return_value = (200, {}, {"value": [{**activity, "activityRunId": str(uuid.uuid4())}]})
            with self.assertRaisesRegex(CandidateBlocked, "does not identify"):
                rebound._job_evidence(record)
            self.api.request.return_value = (200, {}, {"value": [activity]})
            notebook_job["status"] = "Failed"
            with self.assertRaisesRegex(CandidateBlocked, "notebook job is not confirmed"):
                rebound._job_evidence(record)
            notebook_job["status"] = "Completed"
            notebook_job["endTimeUtc"] = "2026-11-24T21:00:00Z"
            with self.assertRaisesRegex(CandidateBlocked, "notebook job is not confirmed"):
                rebound._job_evidence(record)
            notebook_job["endTimeUtc"] = "2026-11-24T21:05:12Z"
            jobs.append({**jobs[-1], "id": str(uuid.uuid4())})
            with self.assertRaisesRegex(CandidateBlocked, "does not identify"):
                rebound._job_evidence(record)


if __name__ == "__main__":
    unittest.main()
