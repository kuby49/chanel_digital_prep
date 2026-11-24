import copy
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import activator
import customer_preflight as preflight
import deploy


class CustomerPreflightTests(unittest.TestCase):
    def setUp(self):
        sample = {
            "type": activator.EVENT_TYPE, "id": "observed-source",
            "subject": "/Files/incoming/sales/sales_bootstrap_0001.csv",
            "source": f"/tenants/{preflight.TENANT}/workspaces/{preflight.WORKSPACE}/items/{preflight.LAKEHOUSE}",
        }
        self.graph = activator.build_definition(
            preflight.TENANT, preflight.WORKSPACE, preflight.LAKEHOUSE,
            preflight.PIPELINE, sample, run_id=preflight.RUN["run_id"])
        self.state = {"definition_disabled": copy.deepcopy(self.graph), "actions": {}}
        self.config = {
            "contract_version": 1, "tenant_id": preflight.TENANT,
            "demo-3": {
                "customer_preflight": True, "workspace_id": preflight.WORKSPACE,
                "lakehouse_id": preflight.LAKEHOUSE, "lakehouse_name": preflight.HOUSE_NAME,
                "destination_table": "dbo.factsales", "landing_path": "Files/incoming/sales",
            },
            "data": {"max_duration_seconds": 1800, "currency": "USD"},
            "shared": {
                "workspace_id": preflight.STAGING_WORKSPACE,
                "staging_lakehouse_id": preflight.STAGING_LAKEHOUSE,
                "target_bindings": {"demo-3": {
                    "workspace_id": preflight.WORKSPACE, "item_id": preflight.LAKEHOUSE,
                    "fact": "dbo.factsales",
                }},
            },
        }

    def enable_graph(self):
        rule = next(entity for entity in self.graph
                    if entity["payload"].get("definition", {}).get("type") == "Rule")
        rule["payload"]["definition"]["settings"]["shouldRun"] = True

    def test_accepts_stopped_and_confirmed_enabled_rule_without_changing_definition(self):
        self.assertFalse(preflight._rule_state(self.graph, self.state))
        self.enable_graph()
        with self.assertRaisesRegex(deploy.CandidateBlocked, "coordinated enable"):
            preflight._rule_state(self.graph, self.state)
        self.state["actions"]["enable"] = {"status": "confirmed-readback"}
        before = copy.deepcopy(self.graph)
        self.assertTrue(preflight._rule_state(self.graph, self.state))
        self.assertEqual(self.graph, before)

    def test_stopped_or_uncertain_runtime_cannot_be_readmitted(self):
        for action in ({"enable": {"status": "in-flight"}},
                       {"enable": {"status": "confirmed-readback"}, "disable": {"status": "in-flight"}}):
            with self.subTest(actions=action), self.assertRaises(deploy.CandidateBlocked):
                preflight._rule_state(self.graph, {**self.state, "actions": action})
        for field in ("runtime_failure", "runtime_stop", "runtime_stop_failure"):
            with self.subTest(field=field), self.assertRaises(deploy.CandidateBlocked):
                preflight._rule_state(self.graph, {**self.state, field: "recorded"})

    def test_changed_event_filter_or_action_is_not_admitted(self):
        for kind in ("realTimeHubSource-v1", "fabricItemAction-v1"):
            changed = copy.deepcopy(self.graph)
            entity = next(item for item in changed if item["type"] == kind)
            if kind == "realTimeHubSource-v1":
                entity["payload"]["filterSettings"]["filters"][0]["values"] = ["/Files/other/"]
            else:
                entity["payload"]["fabricItem"]["itemId"] = preflight.NOTEBOOK
            with self.subTest(kind=kind), self.assertRaisesRegex(deploy.CandidateBlocked, "binding changed"):
                preflight._rule_state(changed, self.state)

    def test_conflicting_target_or_staging_blocks_before_cloud_calls(self):
        cases = []
        for section, key, value in (
            ("demo-3", "lakehouse_id", preflight.STAGING_LAKEHOUSE),
            ("demo-3", "landing_path", "Files/other"),
            ("shared", "staging_lakehouse_id", preflight.LAKEHOUSE),
            ("data", "max_duration_seconds", 1801),
        ):
            config = copy.deepcopy(self.config)
            config[section][key] = value
            cases.append(config)
        api = mock.Mock()
        with mock.patch.object(preflight, "_producer") as producer:
            for config in cases:
                with self.subTest(config=config), self.assertRaises(deploy.CandidateBlocked):
                    preflight.validate(config, "chanel", Path("."), api=api)
            producer.assert_not_called()
        api.assert_not_called()
        self.assertEqual(api.method_calls, [])

    def test_original_journal_with_different_run_blocks_before_cloud_calls(self):
        with tempfile.TemporaryDirectory() as directory:
            folder = Path(directory)
            (folder / preflight.JOURNAL).write_text(json.dumps({"run_config": {"run_id": "other"}}))
            api = mock.Mock()
            with self.assertRaisesRegex(deploy.CandidateBlocked, "journal differs"):
                preflight.validate(self.config, "chanel", folder, api=api)
            self.assertEqual(api.method_calls, [])

    def test_absence_does_not_hide_storage_errors_or_existing_writer(self):
        store = mock.Mock()
        store.read.side_effect = FileNotFoundError
        preflight._absent(store, preflight.LOCK_PATH)
        store.read.side_effect = None
        store.read.return_value = (b"writer", '"etag"')
        with self.assertRaises(deploy.CandidateBlocked):
            preflight._absent(store, preflight.LOCK_PATH)
        store.read.side_effect = RuntimeError("access denied")
        with self.assertRaisesRegex(RuntimeError, "access denied"):
            preflight._absent(store, preflight.LOCK_PATH)

    def test_entrypoint_reports_ready_only_after_successful_customer_validation(self):
        with tempfile.TemporaryDirectory() as directory:
            folder = Path(directory)
            config = folder / "config.json"
            config.write_text(json.dumps(self.config))
            result = folder / "result.json"
            arguments = [
                "--mode", "validate-only", "--config", str(config), "--prefix", "chanel",
                "--state-dir", str(folder), "--result", str(result),
            ]
            with mock.patch.object(preflight, "validate", return_value=["Admission only."]):
                self.assertEqual(deploy.main(arguments), 0)
            payload = json.loads(result.read_text())
            self.assertEqual((payload["status"], payload["ready"]), ("pass", True))
            with mock.patch.object(preflight, "validate", side_effect=deploy.CandidateBlocked("Unsafe writer.")):
                self.assertEqual(deploy.main(arguments), 1)
            payload = json.loads(result.read_text())
            self.assertEqual((payload["status"], payload["ready"]), ("blocked", False))

    def test_result_cannot_overwrite_original_customer_journal(self):
        with tempfile.TemporaryDirectory() as directory:
            folder = Path(directory)
            config = folder / "config.json"
            config.write_text(json.dumps(self.config))
            journal = folder / preflight.JOURNAL
            journal.write_text('{"original":true}')
            self.assertEqual(deploy.main([
                "--mode", "validate-only", "--config", str(config), "--prefix", "chanel",
                "--state-dir", str(folder), "--result", str(journal),
            ]), 1)
            self.assertEqual(journal.read_text(), '{"original":true}')


if __name__ == "__main__":
    unittest.main()
