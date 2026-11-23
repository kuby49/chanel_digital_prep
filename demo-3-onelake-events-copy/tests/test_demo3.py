import importlib.util
import json
import subprocess
import sys
import tempfile
import unittest
import uuid
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("demo3_activator", ROOT / "activator.py")
activator = importlib.util.module_from_spec(spec)
spec.loader.exec_module(activator)


class EventTests(unittest.TestCase):
    def setUp(self):
        self.event = {"type": activator.EVENT_TYPE, "id": "event-1",
                      "subject": "/Files/incoming/sales/sales_run_1.csv"}

    def test_only_immutable_sales_files_accepted(self):
        self.assertEqual(activator.validate_event(self.event), self.event["subject"])
        for subject in ("/Files/incoming/sales/sales_run_1.csv.tmp",
                        "/Files/incoming/sales/nested/sales_run_1.csv",
                        "/Files/incoming/sales/../sales_run_1.csv",
                        "/Files/incoming/sales/DimCustomer.csv"):
            with self.subTest(subject=subject), self.assertRaises(ValueError):
                activator.validate_event({**self.event, "subject": subject})

    def test_definition_has_connected_disabled_rule_and_dynamic_parameters(self):
        ids = [str(uuid.uuid4()) for _ in range(4)]
        entities = activator.build_definition(*ids, self.event)
        by_type = {entity["type"]: entity for entity in entities if entity["type"] != "timeSeriesView-v1"}
        source = by_type["realTimeHubSource-v1"]
        self.assertEqual(source["payload"]["connection"]["artifactId"], ids[2])
        self.assertEqual(source["payload"]["filterSettings"]["eventTypes"], [{"name": activator.EVENT_TYPE}])
        views = [e for e in entities if e["type"] == "timeSeriesView-v1"]
        event = next(e for e in views if e["payload"]["definition"]["type"] == "Event")
        rule = next(e for e in views if e["payload"]["definition"]["type"] == "Rule")
        self.assertFalse(rule["payload"]["definition"]["settings"]["shouldRun"])
        event_template = json.loads(event["payload"]["definition"]["instance"])
        self.assertEqual(event_template["steps"][0]["rows"][0]["arguments"][0]["value"], source["uniqueIdentifier"])
        rule_template = json.loads(rule["payload"]["definition"]["instance"])
        selector = rule_template["steps"][0]["rows"][0]["arguments"][0]["arguments"][0]["value"]
        self.assertEqual(selector, event["uniqueIdentifier"])
        binding = rule_template["steps"][-1]["rows"][0]["arguments"]
        action_id = next(x["value"] for x in binding if x["name"] == "fabricJobConnectionDocumentId")
        self.assertEqual(action_id, by_type["fabricItemAction-v1"]["uniqueIdentifier"])
        params = next(x["values"] for x in binding if x["name"] == "parameters")
        self.assertEqual([p["arguments"][2]["values"][0]["arguments"][0]["value"] for p in params],
                         ["subject", "id"])

    def test_sample_is_required_and_invalid_ids_rejected(self):
        ids = [str(uuid.uuid4()) for _ in range(4)]
        with self.assertRaises(ValueError):
            activator.build_definition(*ids, {})
        with self.assertRaises(ValueError):
            activator.build_definition("not-a-guid", *ids[1:], self.event)


class EntrypointTests(unittest.TestCase):
    def test_contract_fails_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config = root / "config.json"
            config.write_text('{"contract_version":1,"demo-3":{}}', encoding="utf-8")
            for mode in ("dry-run", "validate-only", "deploy", "cleanup"):
                result = root / f"{mode}.json"
                cmd = [sys.executable, str(ROOT / "deploy.py"), "--mode", mode,
                       "--config", str(config), "--prefix", "chanel", "--state-dir", str(root),
                       "--result", str(result)]
                if mode == "cleanup":
                    cmd.append("--yes")
                proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
                payload = json.loads(result.read_text(encoding="utf-8"))
                self.assertEqual(payload["track"], "demo-3")
                self.assertFalse(payload["ready"])
                self.assertEqual(payload["status"], "pass" if mode == "dry-run" else "blocked")
                self.assertEqual(proc.returncode, 0 if mode == "dry-run" else 1)

    def test_cleanup_requires_confirmation(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config = root / "config.json"
            config.write_text('{"contract_version":1,"demo-3":{}}', encoding="utf-8")
            result = root / "result.json"
            proc = subprocess.run(
                [sys.executable, str(ROOT / "deploy.py"), "--mode", "cleanup",
                 "--config", str(config), "--prefix", "chanel", "--state-dir", str(root),
                 "--result", str(result)],
                capture_output=True, text=True, check=False,
            )
            self.assertNotEqual(proc.returncode, 0)
            self.assertEqual(json.loads(result.read_text(encoding="utf-8"))["status"], "fail")


if __name__ == "__main__":
    unittest.main()
