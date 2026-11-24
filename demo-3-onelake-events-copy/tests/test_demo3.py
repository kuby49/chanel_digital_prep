import copy
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
spec = importlib.util.spec_from_file_location("demo3_setup", ROOT / "setup_notebook.py")
setup = importlib.util.module_from_spec(spec)
spec.loader.exec_module(setup)
spec = importlib.util.spec_from_file_location("demo3_view_validation", ROOT / "view_validation.py")
view_validation = importlib.util.module_from_spec(spec)
spec.loader.exec_module(view_validation)
spec = importlib.util.spec_from_file_location("demo3_deploy", ROOT / "deploy.py")
deploy = importlib.util.module_from_spec(spec)
spec.loader.exec_module(deploy)


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
        sample = {**self.event, "source": f"/tenants/{ids[0]}/workspaces/{ids[1]}/items/{ids[2]}"}
        entities = activator.build_definition(*ids, sample)
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
                         ["___subject", "___id"])

    def test_sample_is_required_and_invalid_ids_rejected(self):
        ids = [str(uuid.uuid4()) for _ in range(4)]
        with self.assertRaises(ValueError):
            activator.build_definition(*ids, {})
        with self.assertRaises(ValueError):
            activator.build_definition("not-a-guid", *ids[1:], self.event)
        with self.assertRaises(ValueError):
            activator.build_definition(*ids, {**self.event, "source": "/other-lakehouse"})


class EntrypointTests(unittest.TestCase):
    def test_verification_timeout_precedence_and_validation(self):
        self.assertEqual(deploy.verification_timeout({"demo-3": {}}), 900)
        self.assertEqual(deploy.verification_timeout(
            {"execution": {"verification_timeout_seconds": 25}, "demo-3": {}}), 25)
        self.assertEqual(deploy.verification_timeout(
            {"execution": {"verification_timeout_seconds": 25},
             "demo-3": {"verification_timeout_seconds": 12}}), 12)
        for invalid in (0, -1, True, None, 1.5, "12"):
            for section in ("execution", "demo-3"):
                config = {"demo-3": {}, "execution": {}}
                config[section]["verification_timeout_seconds"] = invalid
                with self.subTest(section=section, invalid=invalid), self.assertRaises(ValueError):
                    deploy.verification_timeout(config)

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


class TargetCandidateTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.tenant = str(uuid.uuid4())
        self.owner = str(uuid.uuid4())
        self.workspace = str(uuid.uuid4())
        self.lakehouse = str(uuid.uuid4())
        self.marker = f"chanel-demo3-{self.owner}"
        self.config = {"contract_version": 1, "tenant_id": self.tenant, "demo-3": {}}
        self.journal = {
            "contract_version": 1, "track": "demo-3", "tenant_id": self.tenant,
            "prefix": "chanel", "owner": {"id": self.owner, "marker": self.marker},
            "pending_operations": [], "uncertain_operations": [],
            "workspace": {
                "id": self.workspace, "type": "Workspace", "status": "confirmed",
                "created_by_owner": self.owner, "create_intent_id": str(uuid.uuid4()),
                "readback": {"id": self.workspace, "type": "Workspace", "owner_marker": self.marker},
            },
            "lakehouse": {
                "id": self.lakehouse, "type": "Lakehouse", "status": "confirmed",
                "workspace_id": self.workspace, "created_by_owner": self.owner,
                "create_intent_id": str(uuid.uuid4()),
                "readback": {"id": self.lakehouse, "type": "Lakehouse",
                             "workspace_id": self.workspace, "owner_marker": self.marker},
            },
            "fact": {"status": "confirmed", "workspace_id": self.workspace,
                     "item_id": self.lakehouse, "name": "dbo.factsales"},
        }

    def run_resolver(self, journal=None, config=None, result_name="result.json"):
        config_path = self.root / "config.json"
        config_path.write_text(json.dumps(config or self.config), encoding="utf-8")
        if journal is not None:
            (self.root / deploy.STATE_FILE).write_text(json.dumps(journal), encoding="utf-8")
        result = self.root / result_name
        proc = subprocess.run(
            [sys.executable, str(ROOT / "deploy.py"), "--mode", "resolve-target-candidate",
             "--config", str(config_path), "--prefix", "chanel", "--state-dir", str(self.root),
             "--result", str(result)],
            capture_output=True, text=True, check=False,
        )
        return proc, json.loads(result.read_text(encoding="utf-8"))

    def test_confirmed_journal_emits_one_candidate_without_claiming_readiness(self):
        proc, result = self.run_resolver(self.journal)
        self.assertEqual(proc.returncode, 0)
        self.assertEqual((result["status"], result["ready"]), ("pass", False))
        candidate_path = self.root / deploy.CANDIDATE_FILE
        candidate = json.loads(candidate_path.read_text(encoding="utf-8"))
        self.assertEqual(candidate, {
            "contract_version": 1, "track": "demo-3", "tenant_id": self.tenant,
            "prefix": "chanel", "target": {
                "workspace_id": self.workspace, "item_id": self.lakehouse, "fact": "dbo.factsales",
            },
        })
        self.assertFalse((self.root / deploy.LOCK_FILE).exists())
        before = candidate_path.read_bytes()
        proc, result = self.run_resolver(result_name="second-result.json")
        self.assertNotEqual(proc.returncode, 0)
        self.assertEqual(result["status"], "blocked")
        self.assertEqual(candidate_path.read_bytes(), before)

    def test_missing_incomplete_or_conflicting_ownership_blocks(self):
        proc, result = self.run_resolver()
        self.assertNotEqual(proc.returncode, 0)
        self.assertEqual(result["status"], "blocked")
        cases = []
        for field in ("pending_operations", "uncertain_operations"):
            journal = copy.deepcopy(self.journal)
            journal[field] = ["unresolved"]
            cases.append((journal, self.config))
        for field, value in (("status", "pending"), ("created_by_owner", str(uuid.uuid4())),
                             ("create_intent_id", str(uuid.UUID(int=0)))):
            journal = copy.deepcopy(self.journal)
            journal["workspace"][field] = value
            cases.append((journal, self.config))
        journal = copy.deepcopy(self.journal)
        journal["lakehouse"]["readback"]["workspace_id"] = str(uuid.uuid4())
        cases.append((journal, self.config))
        journal = copy.deepcopy(self.journal)
        journal["lakehouse"]["readback"]["owner_marker"] = "different-owner"
        cases.append((journal, self.config))
        journal = copy.deepcopy(self.journal)
        journal["fact"]["name"] = "dbo.OtherFact"
        cases.append((journal, self.config))
        for field, value in (("workspace_id", str(uuid.uuid4())),
                             ("lakehouse_id", str(uuid.uuid4())),
                             ("destination_table", "dbo.OtherFact")):
            config = copy.deepcopy(self.config)
            config["demo-3"][field] = value
            cases.append((self.journal, config))
        for index, (journal, config) in enumerate(cases):
            with self.subTest(case=index):
                proc, result = self.run_resolver(journal, config, f"invalid-{index}.json")
                self.assertNotEqual(proc.returncode, 0)
                self.assertEqual(result["status"], "blocked")
                self.assertFalse((self.root / deploy.CANDIDATE_FILE).exists())
                self.assertFalse((self.root / deploy.LOCK_FILE).exists())

    def test_stale_lock_and_journal_are_never_overwritten(self):
        lock = self.root / deploy.LOCK_FILE
        lock.write_text("existing lock\n", encoding="utf-8")
        proc, result = self.run_resolver(self.journal)
        self.assertNotEqual(proc.returncode, 0)
        self.assertEqual(result["status"], "blocked")
        self.assertEqual(lock.read_text(encoding="utf-8"), "existing lock\n")
        self.assertFalse((self.root / deploy.CANDIDATE_FILE).exists())
        journal_path = self.root / deploy.STATE_FILE
        before = journal_path.read_bytes()
        proc, _ = self.run_resolver(result_name=deploy.STATE_FILE)
        self.assertNotEqual(proc.returncode, 0)
        self.assertEqual(journal_path.read_bytes(), before)


class SetupNotebookTests(unittest.TestCase):
    def setUp(self):
        self.generator = (
            "from dataclasses import dataclass\n"
            "from pathlib import Path\n"
            "assert Path(__file__).is_file()\n"
            "@dataclass\n"
            "class RunConfig:\n    pass\n"
            "def dimensions(config):\n    return {}\n"
        )
        self.config = {"contract_version": 1, "data": {
            "start_date": "2026-01-01", "date_count": 365, "product_count": 20,
            "customer_count": 100, "seed": 42,
        }}
        self.workspace_id = str(uuid.uuid4())
        self.lakehouse_id = str(uuid.uuid4())

    def test_notebook_uses_shared_source_and_binds_the_lakehouse(self):
        notebook = setup.build_notebook(
            self.generator, self.config, self.workspace_id, self.lakehouse_id, "DemoLakehouse")
        binding = notebook["metadata"]["trident"]["lakehouse"]
        self.assertEqual(binding["default_lakehouse"], self.lakehouse_id)
        self.assertEqual(binding["default_lakehouse_workspace_id"], self.workspace_id)
        parameters = "".join(notebook["cells"][0]["source"])
        self.assertIn("setup_start_date = '2026-01-01'", parameters)
        self.assertIn("setup_customer_count = 100", parameters)
        for cell in notebook["cells"]:
            compile("".join(cell["source"]), cell["id"], "exec")
        namespace = {}
        exec("".join(notebook["cells"][1]["source"]), namespace)
        self.addCleanup(namespace["_setup_generator_dir"].cleanup)
        self.addCleanup(sys.modules.pop, namespace["_setup_generator_name"], None)
        self.assertEqual(namespace["_setup_generator_path"].read_bytes(), self.generator.encode("utf-8"))
        self.assertEqual(namespace["dimensions"](namespace["RunConfig"]()), {})

    def test_invalid_dimension_inputs_are_rejected(self):
        for field, value in (("date_count", 0), ("product_count", True),
                             ("customer_count", 2147483648), ("seed", None),
                             ("start_date", "invalid")):
            config = {**self.config, "data": {**self.config["data"], field: value}}
            with self.subTest(field=field), self.assertRaises(ValueError):
                setup.build_notebook(
                    self.generator, config, self.workspace_id, self.lakehouse_id, "DemoLakehouse")
        with self.assertRaises(ValueError):
            setup.build_notebook(
                self.generator, self.config, self.workspace_id, self.lakehouse_id,
                "DemoLakehouse`; DROP TABLE x")

    def test_spark_view_definition_allows_only_the_same_workspace_qualifier(self):
        view = "`DemoLakehouse`.`dbo`.`FactSalesAmounts`"
        table = "`DemoLakehouse`.`dbo`.`FactSales`"
        query = f"SELECT *, CAST(`SalesAmountMinorUnits` / 100 AS DECIMAL(18,2)) AS `SalesAmount` FROM {table}"
        ddl = (
            f"CREATE VIEW `demo-workspace`.{view} (`SalesAmount`) "
            f"WITH SCHEMA COMPENSATION AS {query.replace(f'FROM {table}', f'FROM `demo-workspace`.{table}')}"
        )
        self.assertTrue(view_validation.matches_amount_view(ddl, query, view, table))
        self.assertTrue(view_validation.matches_amount_view(
            f"CREATE VIEW {view} AS {query}", query, view, table))
        self.assertFalse(view_validation.matches_amount_view(
            ddl.replace(f"FROM `demo-workspace`.{table}", f"FROM `other-workspace`.{table}"),
            query, view, table))
        self.assertFalse(view_validation.matches_amount_view(
            ddl + " WHERE Quantity > 0", query, view, table))
        self.assertFalse(view_validation.matches_amount_view(
            ddl.replace("/ 100", "/ 10"), query, view, table))


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
        producer_path = Path(os.environ["DEMO3_PRODUCER_PATH"]).resolve()
        sys.path.insert(0, str(producer_path.parent))
        self.addCleanup(sys.path.remove, str(producer_path.parent))
        producer_spec = importlib.util.spec_from_file_location("producer_fixture", producer_path)
        producer = importlib.util.module_from_spec(producer_spec)
        sys.modules[producer_spec.name] = producer
        self.addCleanup(sys.modules.pop, producer_spec.name, None)
        producer_spec.loader.exec_module(producer)
        config = producer.RunConfig("integration", date_count=4, products=3, customers=5,
                                    file_count=1, rows_per_file=12)
        notebook = setup.build_notebook(
            producer_path.read_text(encoding="utf-8"),
            {"contract_version": 1, "data": {"start_date": "2024-01-01", "date_count": 4,
             "product_count": 3, "customer_count": 5, "seed": 42}},
            str(uuid.uuid4()), str(uuid.uuid4()), "DemoLakehouse")
        namespace = {}
        exec("".join(notebook["cells"][1]["source"]), namespace)
        self.addCleanup(namespace["_setup_generator_dir"].cleanup)
        self.addCleanup(sys.modules.pop, namespace["_setup_generator_name"], None)
        self.assertEqual(namespace["dimensions"](namespace["RunConfig"](
            "integration", date_count=4, products=3, customers=5,
            file_count=1, rows_per_file=12)), producer.dimensions(config))
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
