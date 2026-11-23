import importlib.util
import json
import os
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
spec = importlib.util.spec_from_file_location("demo3_reconcile", ROOT / "reconcile.py")
reconcile = importlib.util.module_from_spec(spec)
spec.loader.exec_module(reconcile)


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


class ReconciliationTests(unittest.TestCase):
    def test_exact_results_and_excluded_batch_classes(self):
        entry = {"batch_id": "run_0001", "batch_class": "measured",
                 "row_count": 1, "sales_amount_minor_units": 42,
                 "amount_by_date": {"2024-01-01": 42},
                 "amount_by_product_category": {"Beauty": 42},
                 "amount_by_customer_region": {"Europe": 42}}
        manifest = {"contract_version": 1, "batches": {
            "1": entry, "2": {"batch_class": "bootstrap"},
            "3": {"batch_class": "negative-test"}}}
        observed = {"duplicate_sales_line_ids": 0, "orphan_rows": 0,
                    "batches": {"run_0001": dict(entry)}}
        self.assertEqual(reconcile.reconcile(manifest, observed), [])
        observed["batches"]["run_0001"]["amount_by_date"] = {"2024-01-01": 41}
        observed["duplicate_sales_line_ids"] = 1
        self.assertEqual(len(reconcile.reconcile(manifest, observed)), 2)
        observed["batches"]["unexpected"] = {}
        self.assertTrue(any("unexpected" in error for error in reconcile.reconcile(manifest, observed)))

    def test_missing_measured_batch_and_bad_integer_fail(self):
        entry = {"batch_id": "run_0001", "batch_class": "measured", "row_count": 1,
                 "sales_amount_minor_units": 42, **{name: {"x": 42} for name in reconcile.GROUPS}}
        manifest = {"contract_version": 1, "batches": {"1": entry}}
        observed = {"duplicate_sales_line_ids": 0, "orphan_rows": 0, "batches": {}}
        self.assertTrue(any("missing" in error for error in reconcile.reconcile(manifest, observed)))
        observed["batches"]["run_0001"] = {**entry, "row_count": True}
        self.assertTrue(any("integer" in error for error in reconcile.reconcile(manifest, observed)))

    @unittest.skipUnless(os.environ.get("DEMO3_PRODUCER_PATH"), "producer fixture not supplied")
    def test_actual_producer_manifest(self):
        import csv
        import io
        from collections import defaultdict
        producer_spec = importlib.util.spec_from_file_location("producer_fixture", os.environ["DEMO3_PRODUCER_PATH"])
        producer = importlib.util.module_from_spec(producer_spec)
        sys.modules[producer_spec.name] = producer
        producer_spec.loader.exec_module(producer)
        config = producer.RunConfig("integration", date_count=4, products=3, customers=5,
                                    file_count=1, rows_per_file=12)
        with tempfile.TemporaryDirectory() as directory:
            entry = producer.stage_batch(directory, config, 1, ["demo-3"])
            manifest = json.loads((Path(directory) / "integration" / "manifest.json").read_text(encoding="utf-8"))
            payload = (Path(directory) / "integration" / entry["filename"]).read_text(encoding="utf-8")
            dims = producer.dimensions(config)
            products = {str(x["ProductID"]): x["ProductCategory"] for x in dims["DimProduct"]}
            customers = {str(x["CustomerID"]): x["CustomerRegion"] for x in dims["DimCustomer"]}
            groups = {name: defaultdict(int) for name in reconcile.GROUPS}
            total = count = 0
            seen = set()
            duplicates = orphans = 0
            dates = {x["Date"] for x in dims["DimDate"]}
            for row in csv.DictReader(io.StringIO(payload)):
                amount = int(row["SalesAmountMinorUnits"])
                total += amount
                count += 1
                duplicates += row["SalesLineID"] in seen
                seen.add(row["SalesLineID"])
                orphans += row["Date"] not in dates or row["ProductID"] not in products or row["CustomerID"] not in customers
                groups["amount_by_date"][row["Date"]] += amount
                groups["amount_by_product_category"][products[row["ProductID"]]] += amount
                groups["amount_by_customer_region"][customers[row["CustomerID"]]] += amount
            result = {"row_count": count, "sales_amount_minor_units": total,
                      **{name: dict(values) for name, values in groups.items()}}
            observed = {"duplicate_sales_line_ids": duplicates, "orphan_rows": orphans,
                        "batches": {entry["batch_id"]: result}}
            self.assertEqual(reconcile.reconcile(manifest, observed), [])


if __name__ == "__main__":
    unittest.main()
