import base64
import copy
import importlib.util
import json
import os
import subprocess
import sys
import tempfile
import types
import unittest
from datetime import datetime, timedelta, timezone
from unittest import mock
import uuid
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import delivery_runtime
import guarded_notebook
import audit

spec = importlib.util.spec_from_file_location("demo3_activator", ROOT / "activator.py")
activator = importlib.util.module_from_spec(spec)
spec.loader.exec_module(activator)
spec = importlib.util.spec_from_file_location("demo3_reconcile", ROOT / "reconcile.py")
reconcile = importlib.util.module_from_spec(spec)
spec.loader.exec_module(reconcile)
spec = importlib.util.spec_from_file_location("demo3_batch_gate", ROOT / "batch_gate.py")
batch_gate = importlib.util.module_from_spec(spec)
spec.loader.exec_module(batch_gate)
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

    def test_native_source_is_reusable_but_stopped_and_keeps_dynamic_exact_file_action(self):
        ids = [str(uuid.uuid4()) for _ in range(4)]
        sample = {**self.event, "source": f"/tenants/{ids[0]}/workspaces/{ids[1]}/items/{ids[2]}"}
        graph = activator.build_native_definition(*ids, sample)
        source = next(item for item in graph if item["type"] == "realTimeHubSource-v1")
        self.assertEqual(source["payload"]["filterSettings"]["filters"][0]["values"],
                         ["/Files/incoming/sales/sales_"])
        rule = next(item for item in graph if item["payload"].get("definition", {}).get("type") == "Rule")
        self.assertFalse(rule["payload"]["definition"]["settings"]["shouldRun"])
        self.assertIn("___subject", rule["payload"]["definition"]["instance"])
        self.assertIn("___id", rule["payload"]["definition"]["instance"])

    def test_definition_has_connected_disabled_rule_and_dynamic_parameters(self):
        ids = [str(uuid.uuid4()) for _ in range(4)]
        sample = {**self.event, "source": f"/tenants/{ids[0]}/workspaces/{ids[1]}/items/{ids[2]}"}
        entities = activator.build_definition(*ids, sample, run_id="run")
        by_type = {entity["type"]: entity for entity in entities if entity["type"] != "timeSeriesView-v1"}
        source = by_type["realTimeHubSource-v1"]
        self.assertEqual(source["payload"]["connection"]["artifactId"], ids[2])
        self.assertEqual(source["payload"]["filterSettings"]["eventTypes"], [{"name": activator.EVENT_TYPE}])
        self.assertEqual(source["payload"]["filterSettings"]["filters"][0]["values"],
                         ["/Files/incoming/sales/sales_run_"])
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
            activator.build_definition(*ids, {}, run_id="run")
        with self.assertRaises(ValueError):
            activator.build_definition("not-a-guid", *ids[1:], self.event, run_id="run")
        with self.assertRaises(ValueError):
            activator.build_definition(*ids, {**self.event, "source": "/other-lakehouse"}, run_id="run")

    def test_fixed_source_remains_bound_to_its_run(self):
        ids = [str(uuid.uuid4()) for _ in range(4)]
        sample = {**self.event, "source": f"/tenants/{ids[0]}/workspaces/{ids[1]}/items/{ids[2]}"}
        graph = activator.build_definition(*ids, sample, run_id="new_run-2026")
        source = next(item for item in graph if item["type"] == "realTimeHubSource-v1")
        self.assertEqual(source["payload"]["filterSettings"]["filters"][0]["values"],
                         ["/Files/incoming/sales/sales_new_run-2026_"])
        for invalid in ("", "../run", "x/y", "x" * 65, None):
            with self.subTest(run_id=invalid), self.assertRaises(ValueError):
                activator.build_definition(*ids, sample, run_id=invalid)


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
                if mode == "deploy":
                    diagnostics = " ".join(payload["diagnostics"])
                    self.assertIn("no-missed-notification assumption is unmeasured", diagnostics)
                    self.assertIn("missed-notification recovery is not a prerequisite", diagnostics)
                    self.assertIn("safe stopping", diagnostics)

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


class GuardSparkTests(unittest.TestCase):
    def test_onelake_endpoint_normalizes_https_and_rejects_other_hosts(self):
        producer_path = str(ROOT.parent / "demo-data-generator")
        sys.path.insert(0, producer_path)
        self.addCleanup(sys.path.remove, producer_path)
        self.addCleanup(sys.modules.pop, "generator", None)
        import guard_spark

        self.assertEqual(guard_spark.onelake_endpoint(
            "https://onelake.dfs.fabric.microsoft.com/"), "onelake.dfs.fabric.microsoft.com")
        self.assertEqual(guard_spark.onelake_endpoint("onelake.dfs.fabric.microsoft.com"),
                         "onelake.dfs.fabric.microsoft.com")
        with self.assertRaises(RuntimeError):
            guard_spark.onelake_endpoint("https://other.invalid/")


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

    def test_incompatible_fact_protocol_is_rejected(self):
        view_validation.validate_copy_protocol({"minReaderVersion": 1, "minWriterVersion": 2})
        for detail in ({"minReaderVersion": 3, "minWriterVersion": 7}, {},
                       {"minReaderVersion": True, "minWriterVersion": 2}):
            with self.subTest(detail=detail), self.assertRaises(RuntimeError):
                view_validation.validate_copy_protocol(detail)


class ConditionalStore:
    class Conflict(RuntimeError):
        pass

    def __init__(self, workspace_id, lakehouse_id):
        self.workspace_id = workspace_id
        self.lakehouse_id = lakehouse_id
        self.objects = {}
        self.version = 0
        self.uncertain_replace = False

    def read(self, path):
        if path not in self.objects:
            raise FileNotFoundError(path)
        return self.objects[path]

    def create(self, path, body):
        if path in self.objects:
            raise self.Conflict("already exists")
        self.version += 1
        etag = str(self.version)
        self.objects[path] = (body, etag)
        return etag

    def replace(self, path, body, etag):
        if self.read(path)[1] != etag:
            raise self.Conflict("stale ETag")
        self.version += 1
        new_etag = str(self.version)
        self.objects[path] = (body, new_etag)
        if self.uncertain_replace:
            self.uncertain_replace = False
            raise RuntimeError("replacement outcome unknown")
        return new_etag

    def delete(self, path, etag):
        if self.read(path)[1] != etag:
            raise self.Conflict("stale ETag")
        del self.objects[path]


class AuditTests(unittest.TestCase):
    def setUp(self):
        self.workspace, self.lakehouse = str(uuid.uuid4()), str(uuid.uuid4())
        self.job = str(uuid.uuid4())
        self.store = ConditionalStore(self.workspace, self.lakehouse)
        self.audit = audit.BatchAudit(self.store, self.workspace, self.lakehouse)
        self.evidence = {
            "origin": "event", "event_id": "event-1",
            "batch_id": "run_0001", "source_subject": "/Files/incoming/sales/sales_run_0001.csv",
            "sha256": "a" * 64, "row_count": 2, "sales_amount_minor_units": 42,
            "first_sales_line_id": 1, "last_sales_line_id": 2,
            "amount_by_date": {"2024-01-01": 42},
            "amount_by_product_category": {"Beauty": 42},
            "amount_by_customer_region": {"Europe": 42},
            "delivery_status": "submitting",
        }
        self.copy_result = {
            "status": "Succeeded", "pipeline_run_id": self.job, "activity_run_id": str(uuid.uuid4()),
            "files_read": 1, "rows_read": 2, "rows_copied": 2,
        }
        self.proof = {
            "row_count": 2, "distinct_sales_line_ids": 2, "first_sales_line_id": 1,
            "last_sales_line_id": 2, "sales_amount_minor_units": 42,
            "orphan_rows": 0, "row_difference_count": 0,
            **{name: self.evidence[name] for name in audit.GROUPS},
        }

    def test_completed_batch_records_duplicate_without_another_copy(self):
        token = self.audit.acquire(self.job, self.evidence["batch_id"])
        with self.assertRaises(ConditionalStore.Conflict):
            self.audit.acquire(str(uuid.uuid4()), "run_0002")
        self.assertEqual(self.audit.begin(token, self.evidence), "copy-required")
        attempt = self.audit.copy_intent(token, self.evidence["batch_id"])
        self.audit.copy_succeeded(token, self.evidence["batch_id"], attempt, self.copy_result)
        with self.assertRaises(audit.AuditError):
            self.audit.complete(token, self.evidence["batch_id"], {**self.proof, "row_difference_count": 1})

        self.audit.complete(token, self.evidence["batch_id"], self.proof)
        self.audit.release(token, self.evidence["batch_id"])

        duplicate = self.audit.acquire(str(uuid.uuid4()), self.evidence["batch_id"])
        self.assertEqual(self.audit.begin(duplicate, self.evidence), "already-completed")
        with self.assertRaises(audit.AuditError):
            self.audit.copy_intent(duplicate, self.evidence["batch_id"])
        self.audit.release(duplicate, self.evidence["batch_id"])
        document = json.loads(self.store.read(audit.RECORD_PREFIX + "run_0001.json")[0])
        self.assertEqual((document["state"], len(document["observations"])), ("completed", 2))
        self.assertEqual(document["copy_attempt"]["attempt_id"], attempt)
        next_evidence = {**self.evidence, "batch_id": "run_0002",
                         "source_subject": "/Files/incoming/sales/sales_run_0002.csv"}
        next_token = self.audit.acquire(str(uuid.uuid4()), "run_0002")
        self.audit.begin(next_token, next_evidence)
        self.audit.copy_intent(next_token, "run_0002")
        with self.assertRaises(audit.AuditError):
            self.audit.release(next_token, "run_0001")

    def test_batch_keys_must_start_at_one(self):
        token = self.audit.acquire(self.job, self.evidence["batch_id"])
        with self.assertRaises(audit.AuditError):
            self.audit.begin(token, {**self.evidence, "first_sales_line_id": 11, "last_sales_line_id": 12})

    def test_uncertain_or_inflight_copy_keeps_lock_and_blocks_replay(self):
        token = self.audit.acquire(self.job, self.evidence["batch_id"])
        self.audit.begin(token, self.evidence)
        self.store.uncertain_replace = True
        with self.assertRaisesRegex(RuntimeError, "outcome unknown"):
            self.audit.copy_intent(token, self.evidence["batch_id"])
        with self.assertRaises(audit.AuditError):
            self.audit.release(token, self.evidence["batch_id"])
        with self.assertRaises(ConditionalStore.Conflict):
            self.audit.acquire(str(uuid.uuid4()), "run_0002")
        with self.assertRaises(audit.AuditError):
            self.audit.release(token, "run_0002")
        with self.assertRaises(audit.AuditError):
            self.audit.begin(token, self.evidence)

    def test_pre_copy_release_and_conflicting_replay(self):
        token = self.audit.acquire(self.job, self.evidence["batch_id"])
        self.audit.begin(token, self.evidence)
        self.audit.release(token, self.evidence["batch_id"])
        recovery = self.audit.acquire(str(uuid.uuid4()), self.evidence["batch_id"])
        with self.assertRaises(audit.AuditError):
            self.audit.begin(recovery, {**self.evidence, "sha256": "b" * 64})
        self.assertEqual(self.audit.begin(
            recovery, {**self.evidence, "origin": "recovery", "event_id": None}), "copy-required")
        self.audit.release(recovery, self.evidence["batch_id"])

    def test_target_identity_and_copy_readback_are_fail_closed(self):
        with self.assertRaises(audit.AuditError):
            audit.BatchAudit(self.store, str(uuid.uuid4()), self.lakehouse)
        token = self.audit.acquire(self.job, self.evidence["batch_id"])
        self.audit.begin(token, self.evidence)
        attempt = self.audit.copy_intent(token, self.evidence["batch_id"])
        with self.assertRaises(audit.AuditError):
            self.audit.copy_succeeded(
                token, self.evidence["batch_id"], attempt, {**self.copy_result, "files_read": 2})
        with self.assertRaises(audit.AuditError):
            self.audit.release(token, self.evidence["batch_id"])

    def test_tampered_audit_record_cannot_start_copy(self):
        token = self.audit.acquire(self.job, self.evidence["batch_id"])
        self.audit.begin(token, self.evidence)
        path = audit.RECORD_PREFIX + "run_0001.json"
        body, etag = self.store.read(path)
        self.store.replace(path, b'{"contract_version":1,' + body[1:], etag)
        with self.assertRaises(audit.AuditError):
            self.audit.copy_intent(token, self.evidence["batch_id"])


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
        observed = {"duplicate_batch_sales_line_ids": 0, "orphan_rows": 0,
                    "batches": {"run_0001": dict(entry)}}
        self.assertEqual(reconcile.reconcile(manifest, observed), [])
        observed["batches"]["run_0001"]["amount_by_date"] = {"2024-01-01": 41}
        observed["duplicate_batch_sales_line_ids"] = 1
        self.assertEqual(len(reconcile.reconcile(manifest, observed)), 2)
        observed["batches"]["unexpected"] = {}
        self.assertTrue(any("unexpected" in error for error in reconcile.reconcile(manifest, observed)))

    def test_missing_measured_batch_and_bad_integer_fail(self):
        entry = {"batch_id": "run_0001", "batch_class": "measured", "row_count": 1,
                 "sales_amount_minor_units": 42, **{name: {"x": 42} for name in reconcile.GROUPS}}
        manifest = {"contract_version": 1, "batches": {"1": entry}}
        observed = {"duplicate_batch_sales_line_ids": 0, "orphan_rows": 0, "batches": {}}
        self.assertTrue(any("missing" in error for error in reconcile.reconcile(manifest, observed)))
        observed["batches"]["run_0001"] = {**entry, "row_count": True}
        self.assertTrue(any("integer" in error for error in reconcile.reconcile(manifest, observed)))

    @unittest.skipUnless(
        os.environ.get("DEMO3_PRODUCER_PATH") or
        (ROOT.parent / "demo-data-generator" / "generator.py").is_file(),
        "producer fixture not supplied",
    )
    def test_actual_producer_manifest(self):
        import csv
        import io
        from collections import defaultdict
        producer_path = Path(os.environ.get("DEMO3_PRODUCER_PATH") or
                             ROOT.parent / "demo-data-generator" / "generator.py").resolve()
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
            manifest.pop("sales_line_id_reservation", None)
            source_bytes = (Path(directory) / "integration" / entry["filename"]).read_bytes()
            payload = source_bytes.decode("utf-8")
            manifest["batches"]["1"]["deliveries"]["demo-3"]["status"] = "submitting"
            subject = f"/Files/incoming/sales/{entry['filename']}"

            def validate(document=manifest, source=source_bytes, path=subject, **kwargs):
                return batch_gate.validate_batch(
                    producer, config, json.dumps(document).encode("utf-8"), path, source, **kwargs)

            with self.assertRaisesRegex(ValueError, "fingerprint"):
                validate(document={**manifest, "generator_sha256": "0" * 64}, event_id="event-1")

            for status in ("submitting", "submitted", "unknown", "verified"):
                manifest["batches"]["1"]["deliveries"]["demo-3"]["status"] = status
                result = validate(event_id="event-1")
                self.assertEqual((result["batch_id"], result["delivery_status"]),
                                 (entry["batch_id"], status))
            workspace, lakehouse, job = str(uuid.uuid4()), str(uuid.uuid4()), str(uuid.uuid4())
            batch_audit = audit.BatchAudit(ConditionalStore(workspace, lakehouse), workspace, lakehouse)
            writer = batch_audit.acquire(job, result["batch_id"])
            self.assertEqual(batch_audit.begin(writer, result), "copy-required")
            batch_audit.release(writer, result["batch_id"])
            storage = ConditionalStore(workspace, lakehouse)
            authority = ConditionalStore(str(uuid.uuid4()), str(uuid.uuid4()))
            batch_audit = audit.BatchAudit(storage, workspace, lakehouse)
            guarded = delivery_runtime.GuardedDelivery(producer, config, authority, batch_audit)
            authority.create(delivery_runtime.manifest_path(config), json.dumps(manifest).encode("utf-8"))
            storage.create(subject[1:], source_bytes)
            copies = []

            def run_guarded_pipeline(path, event_id, origin, remaining):
                prepared = guarded.prepare(job, path, event_id or None, origin)
                self.assertGreater(remaining, 0)
                self.assertEqual(prepared["filename"], entry["filename"])
                copies.append(prepared["filename"])
                guarded.finish(job, prepared, {
                    "status": "Succeeded", "pipeline_run_id": job, "activity_run_id": None,
                    "files_read": 1, "rows_read": entry["row_count"], "rows_copied": entry["row_count"],
                }, lambda _: {
                    "row_count": result["row_count"], "distinct_sales_line_ids": result["row_count"],
                    "orphan_rows": 0, "row_difference_count": 0,
                    **{key: result[key] for key in (*audit.GROUPS, "first_sales_line_id",
                                                    "last_sales_line_id", "sales_amount_minor_units")},
                })

            guarded.reconcile(run_guarded_pipeline, timeout_seconds=2, poll_seconds=1,
                              event_grace_seconds=0)
            duplicate = guarded.prepare(str(uuid.uuid4()), subject, "late-event", "event")
            self.assertEqual(duplicate["decision"], "already-completed")
            guarded.reconcile(run_guarded_pipeline, timeout_seconds=2, poll_seconds=1,
                              event_grace_seconds=0)
            self.assertEqual(copies, [entry["filename"]])
            self.assertEqual(batch_audit.batch_state(result), "completed")

            uncertain_store = ConditionalStore(workspace, lakehouse)
            uncertain_store.create(subject[1:], source_bytes)
            uncertain = delivery_runtime.GuardedDelivery(
                producer, config, authority, audit.BatchAudit(uncertain_store, workspace, lakehouse))
            submissions = []

            def lost_response(path, event_id, origin, remaining):
                submissions.append(path)
                raise TimeoutError("no response from job submission")

            with self.assertRaisesRegex(TimeoutError, "no response"):
                uncertain.reconcile(lost_response, timeout_seconds=2, poll_seconds=1,
                                    event_grace_seconds=0)
            with self.assertRaises(ConditionalStore.Conflict):
                uncertain.reconcile(lost_response, timeout_seconds=2, poll_seconds=1,
                                    event_grace_seconds=0)
            self.assertEqual(submissions, [subject])

            empty = ConditionalStore(workspace, lakehouse)
            waiting = delivery_runtime.GuardedDelivery(
                producer, config, authority, audit.BatchAudit(empty, workspace, lakehouse))
            times = iter((0, 0, 0, 0, 2))
            with self.assertRaises(TimeoutError):
                waiting.reconcile(run_guarded_pipeline, timeout_seconds=2, poll_seconds=1,
                                  event_grace_seconds=0, clock=lambda: next(times), sleep=lambda _: None)
            self.assertEqual(copies, [entry["filename"]])

            notebook_candidate = guarded_notebook.build_notebook(
                producer_path.parent, dict(manifest["config"]), workspace, lakehouse, "DemoLakehouse",
                authority.workspace_id, authority.lakehouse_id)
            for cell in notebook_candidate["cells"]:
                compile("".join(cell["source"]), "guarded-notebook", "exec")
            self.assertEqual(validate(origin="recovery")["event_id"], None)
            for path in (subject + ".tmp", subject.replace("/sales/", "/sales/../"),
                         subject.replace("_0001.csv", "_1.csv"),
                         subject.replace("_0001.csv", "_0002.csv"),
                         "/Files/incoming/sales/other.csv"):
                with self.subTest(path=path), self.assertRaises(ValueError):
                    validate(path=path, event_id="event-1")
            with self.assertRaises(ValueError):
                validate(source=source_bytes + b"x", event_id="event-1")
            with self.assertRaises(ValueError):
                validate(event_id="")
            with self.assertRaises(ValueError):
                validate(event_id="event\nforged")
            with self.assertRaises(ValueError):
                validate()
            for path, value in (
                (("config", "seed"), 999),
                (("batches", "1", "row_count"), True),
                (("batches", "1", "amount_by_date"), {"2024-01-01": 0}),
                (("batches", "1", "deliveries", "demo-3", "status"), "pending"),
                (("batches", "1", "deliveries", "demo-3", "status"), []),
            ):
                bad = copy.deepcopy(manifest)
                target = bad
                for key in path[:-1]:
                    target = target[key]
                target[path[-1]] = value
                with self.subTest(field=path), self.assertRaises(ValueError):
                    validate(document=bad, event_id="event-1")
            encoded = json.dumps(manifest).encode("utf-8")
            with self.assertRaises(ValueError):
                batch_gate.validate_batch(
                    producer, config, b'{"contract_version":1,' + encoded[1:],
                    subject, source_bytes, event_id="event-1")
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
                key = (row["BatchID"], row["SalesLineID"])
                duplicates += key in seen
                seen.add(key)
                orphans += row["Date"] not in dates or row["ProductID"] not in products or row["CustomerID"] not in customers
                groups["amount_by_date"][row["Date"]] += amount
                groups["amount_by_product_category"][products[row["ProductID"]]] += amount
                groups["amount_by_customer_region"][customers[row["CustomerID"]]] += amount
            result = {"row_count": count, "sales_amount_minor_units": total,
                      **{name: dict(values) for name, values in groups.items()}}
            observed = {"duplicate_batch_sales_line_ids": duplicates, "orphan_rows": orphans,
                        "batches": {entry["batch_id"]: result}}
            self.assertEqual(reconcile.reconcile(manifest, observed), [])


@unittest.skipUnless(
    os.environ.get("DEMO3_PRODUCER_PATH") or
    (ROOT.parent / "demo-data-generator" / "generator.py").is_file(),
    "producer fixture not supplied",
)
class NativeGuardTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import native_guard

        cls.native = native_guard
        cls.producer_path = Path(os.environ.get("DEMO3_PRODUCER_PATH") or
                                 ROOT.parent / "demo-data-generator" / "generator.py").resolve()
        module = importlib.util.spec_from_file_location("native_producer_fixture", cls.producer_path)
        cls.generator = importlib.util.module_from_spec(module)
        sys.modules[module.name] = cls.generator
        cls.addClassCleanup(sys.modules.pop, module.name, None)
        module.loader.exec_module(cls.generator)

    def setUp(self):
        from dataclasses import asdict

        self.config = self.generator.RunConfig(
            "native_alpha_20261124", start_date="2026-01-01", file_count=10, rows_per_file=1000)
        self.policy = {"version": 1, "max_file_count": 10, "max_rows_per_file": 1000,
                       "max_admission_seconds": 3600, "run_template": {
                           key: value for key, value in asdict(self.config).items()
                           if key not in ("run_id", "file_count", "rows_per_file")}}
        self.settings = {"workspace_id": str(uuid.uuid4()), "item_id": str(uuid.uuid4()),
                         "native_policy": self.policy}
        self.now = datetime.now(timezone.utc)
        self.manifest = self.generator.new_manifest(self.config, ["demo-3"])
        self.manifest["demo3_admission"] = {
            "version": 1, "state": "open", "workspace_id": self.settings["workspace_id"],
            "lakehouse_id": self.settings["item_id"], "stop_config_sha256": "c" * 64,
            "config_sha256": self.native.config_sha256(asdict(self.config)),
            "expires_at_utc": (self.now + timedelta(seconds=1800)).isoformat(),
        }
        self.subject = f"/Files/incoming/sales/sales_{self.config.run_id}_0001.csv"
        self.authority = ConditionalStore(str(uuid.uuid4()), str(uuid.uuid4()))
        self.target = ConditionalStore(self.settings["workspace_id"], self.settings["item_id"])

    def publish(self):
        path = delivery_runtime.manifest_path(self.config)
        self.authority.objects[path] = (json.dumps(self.manifest).encode(), "manifest-etag")

    def resolve(self, *, finishing=False):
        return self.native.resolve_config(
            self.generator, self.settings, self.subject, self.authority,
            finishing=finishing, now=self.now)

    def proof(self, entry):
        return {"row_count": entry["row_count"], "distinct_sales_line_ids": entry["row_count"],
                "orphan_rows": 0, "row_difference_count": 0,
                "first_sales_line_id": 1, "last_sales_line_id": entry["row_count"],
                **{key: entry[key] for key in (*audit.GROUPS, "sales_amount_minor_units")}}

    def test_reuses_one_policy_for_two_fresh_runs_ten_files_each_and_idempotent_replays(self):
        from dataclasses import replace

        copies = []
        for run_id in ("native_alpha_20261124", "native_fresh_beta"):
            self.config = replace(self.config, run_id=run_id)
            self.manifest["run_id"] = run_id
            from dataclasses import asdict
            self.manifest["config"] = asdict(self.config)
            self.manifest["demo3_admission"]["config_sha256"] = self.native.config_sha256(asdict(self.config))
            self.manifest["batches"] = {}
            for number in range(1, 11):
                source, entry = self.generator.batch_bytes(self.config, number)
                self.manifest["batches"][str(number)] = {
                    **entry, "deliveries": {"demo-3": {"status": "submitted"}}}
                self.target.create("Files/incoming/sales/" + entry["filename"], source)
            self.publish()
            for number in range(1, 11):
                self.subject = f"/Files/incoming/sales/sales_{run_id}_{number:04d}.csv"
                config = self.resolve()
                batch_audit = self.native.AdmissionAudit(
                    self.target, self.settings["workspace_id"], self.settings["item_id"], self.resolve)
                guard = delivery_runtime.GuardedDelivery(self.generator, config, self.authority, batch_audit)
                job = str(uuid.uuid4())
                prepared = guard.prepare(job, self.subject, "event-" + job, "event")
                entry = self.manifest["batches"][str(number)]
                copies.append(prepared["batch_id"])
                guard.finish(job, prepared, {
                    "status": "Succeeded", "pipeline_run_id": job, "activity_run_id": None,
                    "files_read": 1, "rows_read": 1000, "rows_copied": 1000,
                }, lambda _: self.proof(entry))
                replay = guard.prepare(str(uuid.uuid4()), self.subject, "replay-" + job, "event")
                self.assertEqual(replay["decision"], "already-completed")
        self.assertEqual(len(set(copies)), 20)
        self.assertNotIn(audit.LOCK_PATH, self.target.objects)
        self.assertEqual(sum(json.loads(body)["batch"]["row_count"]
                             for path, (body, _) in self.target.objects.items()
                             if path.startswith(audit.RECORD_PREFIX)), 20000)

    def test_absent_stopped_expired_foreign_or_changed_admissions_fail_closed(self):
        original = copy.deepcopy(self.manifest)
        mutations = [
            lambda m: m.pop("demo3_admission"),
            lambda m: m["demo3_admission"].update(state="stopped"),
            lambda m: m["demo3_admission"].update(expires_at_utc=self.now.isoformat()),
            lambda m: m["demo3_admission"].update(expires_at_utc="invalid"),
            lambda m: m["demo3_admission"].update(workspace_id=str(uuid.uuid4())),
            lambda m: m["demo3_admission"].update(config_sha256="b" * 64),
            lambda m: m["demo3_admission"].update(stop_config_sha256="invalid"),
            lambda m: m["config"].update(file_count=11),
            lambda m: m["config"].update(rows_per_file=1001),
            lambda m: m["config"].update(seed=43),
        ]
        for mutate in mutations:
            self.manifest = copy.deepcopy(original)
            mutate(self.manifest)
            self.publish()
            with self.subTest(manifest=self.manifest), self.assertRaises((ValueError, KeyError)):
                self.resolve()
        self.assertEqual(self.target.objects, {})

    def test_valid_stop_and_foreign_stop_both_prevent_new_admission(self):
        self.publish()
        path = f"Files/producer/runs/{self.config.run_id}/stop.json"
        marker = {"version": 1, "run_id": self.config.run_id,
                  "config_sha256": "c" * 64, "nonce": "native-" + "c" * 32}
        self.authority.create(path, json.dumps(marker).encode())
        with self.assertRaisesRegex(ValueError, "valid stop request"):
            self.resolve()
        marker["config_sha256"] = "d" * 64
        self.authority.objects[path] = (json.dumps(marker).encode(), "changed")
        with self.assertRaisesRegex(ValueError, "stop marker identity differs"):
            self.resolve()
        self.assertEqual(self.resolve(finishing=True), self.config)

    def test_revocation_before_intent_releases_validated_lock_without_copy(self):
        payload, entry = self.generator.batch_bytes(self.config, 1)
        self.manifest["batches"]["1"] = {**entry, "deliveries": {"demo-3": {"status": "submitted"}}}
        self.target.create(self.subject[1:], payload)
        self.publish()
        batch_audit = self.native.AdmissionAudit(
            self.target, self.settings["workspace_id"], self.settings["item_id"],
            mock.Mock(side_effect=[None, ValueError("revoked before Copy intent")]))
        guard = delivery_runtime.GuardedDelivery(self.generator, self.resolve(), self.authority, batch_audit)
        with self.assertRaisesRegex(ValueError, "revoked"):
            guard.prepare(str(uuid.uuid4()), self.subject, "event-revoked", "event")
        self.assertNotIn(audit.LOCK_PATH, self.target.objects)
        record = json.loads(self.target.read(audit.RECORD_PREFIX + entry["batch_id"] + ".json")[0])
        self.assertEqual(record["state"], "validated")
        self.assertIsNone(record["copy_attempt"])

    def test_existing_owned_intent_can_finish_after_stop_but_new_prepare_cannot_start(self):
        payload, entry = self.generator.batch_bytes(self.config, 1)
        self.manifest["batches"]["1"] = {**entry, "deliveries": {"demo-3": {"status": "submitted"}}}
        self.target.create(self.subject[1:], payload)
        self.publish()
        batch_audit = self.native.AdmissionAudit(
            self.target, self.settings["workspace_id"], self.settings["item_id"], self.resolve)
        guard = delivery_runtime.GuardedDelivery(self.generator, self.resolve(), self.authority, batch_audit)
        job = str(uuid.uuid4())
        prepared = guard.prepare(job, self.subject, "event-in-flight", "event")
        self.manifest["demo3_admission"].update(state="stopped", expires_at_utc=self.now.isoformat())
        self.publish()
        with self.assertRaisesRegex(ValueError, "stopped, expired"):
            self.resolve()
        self.assertEqual(self.resolve(finishing=True), self.config)
        with self.assertRaises(audit.AuditError):
            guard.finish(str(uuid.uuid4()), prepared, {}, lambda _: self.proof(entry))
        guard.finish(job, prepared, {
            "status": "Succeeded", "pipeline_run_id": job, "activity_run_id": None,
            "files_read": 1, "rows_read": 1000, "rows_copied": 1000,
        }, lambda _: self.proof(entry))
        self.assertNotIn(audit.LOCK_PATH, self.target.objects)

    def test_canonical_subject_and_native_packaging_do_not_embed_a_run(self):
        for subject in ("../sales_run_0001.csv", "/Files/incoming/sales/sales_run_0000.csv",
                        "/Files/incoming/sales/sales_run_00001.csv",
                        "/Files/incoming/sales/sales_run_0001.csv.tmp"):
            with self.subTest(subject=subject), self.assertRaises(ValueError):
                self.native.source_run(subject)
        notebook = guarded_notebook.build_native_notebook(
            self.producer_path.parent, self.policy, self.settings["workspace_id"],
            self.settings["item_id"], "NativeDemo", str(uuid.uuid4()), str(uuid.uuid4()))
        for cell in notebook["cells"]:
            compile("".join(cell["source"]), "native-notebook", "exec")
        source = "".join(notebook["cells"][1]["source"])
        self.assertIn("from native_guard import execute", source)
        self.assertNotIn(self.config.run_id, source)

    def test_native_notebook_policy_order_does_not_change_packaged_source(self):
        policy = copy.deepcopy(self.policy)
        reordered = dict(reversed(list(policy.items())))
        reordered["run_template"] = dict(reversed(list(policy["run_template"].items())))
        args = (self.settings["workspace_id"], self.settings["item_id"], "NativeDemo",
                str(uuid.uuid4()), str(uuid.uuid4()))
        first = guarded_notebook.build_native_notebook(self.producer_path.parent, policy, *args)
        second = guarded_notebook.build_native_notebook(self.producer_path.parent, reordered, *args)
        self.assertEqual(first, second)

    def test_stop_barrier_waits_for_existing_writer_and_checks_immutable_marker(self):
        import hashlib
        import native_stop

        document = {"data": {"run_id": self.config.run_id}, "shared": {"target_bindings": {
            "demo-3": {"workspace_id": self.target.workspace_id, "item_id": self.target.lakehouse_id,
                       "fact": "dbo.factsales"}}}}
        digest = hashlib.sha256(self.generator.canonical_bytes(document)).hexdigest()
        binding = {"run_id": self.config.run_id, "config_sha256": digest, "nonce": "native-" + digest[:32],
                   "producer": {"workspace_id": self.authority.workspace_id,
                                "staging_lakehouse_id": self.authority.lakehouse_id}}
        marker = {"version": 1, "run_id": self.config.run_id, "config_sha256": digest,
                  "nonce": binding["nonce"]}
        path = f"Files/producer/runs/{self.config.run_id}/stop.json"
        self.authority.create(path, json.dumps(marker).encode())
        audit_writer = audit.BatchAudit(self.target, self.target.workspace_id, self.target.lakehouse_id)
        held = audit_writer.acquire(str(uuid.uuid4()), "older_run_0001")
        original_lock = self.target.read(audit.LOCK_PATH)
        times = [self.now]

        def sleep(seconds):
            self.assertEqual(self.target.read(audit.LOCK_PATH), original_lock)
            times[0] += timedelta(seconds=seconds)
            self.target.delete(audit.LOCK_PATH, held.lock_etag)

        context = {"document": document, "binding": binding, "store": self.authority,
                   "store_factory": lambda ws, lh: self.target,
                   "cleanup_deadline_utc": (self.now + timedelta(seconds=20)).isoformat(),
                   "clock": lambda: times[0], "sleep": sleep}
        modules = {"generator": self.generator,
                   "fabric_storage": types.SimpleNamespace(StorageConflict=ConditionalStore.Conflict)}
        with mock.patch.dict(sys.modules, modules):
            self.assertEqual(native_stop.stop(context, None), {
                "status": "stopped", "scope": "cooperating guarded Copy intents"})
        self.assertNotIn(audit.LOCK_PATH, self.target.objects)
        self.assertEqual(times[0], self.now + timedelta(seconds=5))

        with mock.patch.dict(sys.modules, modules), mock.patch.object(
            self.authority, "read", side_effect=[self.authority.read(path), (b"{}", "changed")]
        ), self.assertRaisesRegex(RuntimeError, "marker or deadline changed"):
            native_stop.stop(context, None)
        self.assertIn(audit.LOCK_PATH, self.target.objects)

    def test_stop_barrier_rejects_bad_marker_and_never_retries_uncertain_create(self):
        import hashlib
        import native_stop

        document = {"data": {"run_id": self.config.run_id}, "shared": {"target_bindings": {
            "demo-3": {"workspace_id": self.target.workspace_id, "item_id": self.target.lakehouse_id,
                       "fact": "dbo.factsales"}}}}
        digest = hashlib.sha256(self.generator.canonical_bytes(document)).hexdigest()
        binding = {"run_id": self.config.run_id, "config_sha256": digest, "nonce": "native-" + digest[:32],
                   "producer": {"workspace_id": self.authority.workspace_id,
                                "staging_lakehouse_id": self.authority.lakehouse_id}}
        path = f"Files/producer/runs/{self.config.run_id}/stop.json"
        self.authority.create(path, b'{"version":true}')
        context = {"document": document, "binding": binding, "store": self.authority,
                   "store_factory": lambda ws, lh: self.target,
                   "cleanup_deadline_utc": (self.now + timedelta(seconds=20)).isoformat(),
                   "clock": lambda: self.now, "sleep": mock.Mock()}
        modules = {"generator": self.generator,
                   "fabric_storage": types.SimpleNamespace(StorageConflict=ConditionalStore.Conflict)}
        with mock.patch.dict(sys.modules, modules), self.assertRaisesRegex(ValueError, "stop marker"):
            native_stop.stop(context, None)
        self.assertNotIn(audit.LOCK_PATH, self.target.objects)
        marker = {"version": 1, "run_id": self.config.run_id, "config_sha256": digest,
                  "nonce": binding["nonce"]}
        self.authority.objects[path] = (json.dumps(marker).encode(), "marker")
        with mock.patch.dict(sys.modules, modules), mock.patch.object(
            self.target, "create", side_effect=RuntimeError("outcome unknown")
        ) as create, self.assertRaisesRegex(RuntimeError, "outcome unknown"):
            native_stop.stop(context, None)
        self.assertEqual(create.call_count, 1)
        context["sleep"].assert_not_called()
        with mock.patch.dict(sys.modules, modules), mock.patch.object(
            self.target, "read", side_effect=ConditionalStore.Conflict("read conflict")
        ) as read, self.assertRaisesRegex(ConditionalStore.Conflict, "read conflict"):
            native_stop.stop(context, None)
        self.assertEqual(read.call_count, 1)
        self.assertIn(audit.LOCK_PATH, self.target.objects)
        self.target.objects.pop(audit.LOCK_PATH)
        with mock.patch.dict(sys.modules, modules), mock.patch.object(
            self.target, "delete", side_effect=RuntimeError("delete outcome unknown")
        ) as delete, self.assertRaisesRegex(RuntimeError, "delete outcome unknown"):
            native_stop.stop(context, None)
        self.assertEqual(delete.call_count, 1)
        self.assertIn(audit.LOCK_PATH, self.target.objects)

        times = [self.now]
        context["clock"] = lambda: times[0]
        context["sleep"] = lambda seconds: times.__setitem__(0, times[0] + timedelta(seconds=seconds))
        existing = self.target.read(audit.LOCK_PATH)
        with mock.patch.dict(sys.modules, modules), self.assertRaisesRegex(TimeoutError, "deadline"):
            native_stop.stop(context, None)
        self.assertEqual(self.target.read(audit.LOCK_PATH), existing)

    def test_native_lifecycle_requires_enabled_graph_and_verifies_each_event_batch(self):
        from dataclasses import asdict, replace

        config = replace(self.config, file_count=2, rows_per_file=2)
        self.config = config
        self.manifest = self.generator.new_manifest(config, ["demo-3"])
        self.manifest["demo3_admission"] = {
            "version": 1, "state": "open", "workspace_id": self.target.workspace_id,
            "lakehouse_id": self.target.lakehouse_id, "stop_config_sha256": "c" * 64,
            "config_sha256": self.native.config_sha256(asdict(config)),
            "expires_at_utc": (self.now + timedelta(seconds=1800)).isoformat(),
        }
        entries = {}
        for number in (1, 2):
            source, entry = self.generator.batch_bytes(config, number)
            entries[entry["batch_id"]] = entry
            self.manifest["batches"][str(number)] = {**entry, "deliveries": {"demo-3": {"status": "submitted"}}}
            self.target.create("Files/incoming/sales/" + entry["filename"], source)
        self.publish()
        batch_audit = self.native.AdmissionAudit(
            self.target, self.target.workspace_id, self.target.lakehouse_id, self.resolve)
        guarded = delivery_runtime.GuardedDelivery(self.generator, config, self.authority, batch_audit)
        for entry in entries.values():
            self.subject = "/Files/incoming/sales/" + entry["filename"]
            job = str(uuid.uuid4())
            attempt = guarded.prepare(job, self.subject, "actual-event", "event")
            guarded.finish(job, attempt, {
                "status": "Succeeded", "pipeline_run_id": job, "activity_run_id": None,
                "files_read": 1, "rows_read": 2, "rows_copied": 2,
            }, lambda _: self.proof(entry))

        ids = [str(uuid.uuid4()) for _ in range(5)]
        tenant, notebook_id, pipeline_id, reflex_id, connection_id = ids
        sample = {"type": activator.EVENT_TYPE, "id": "sample",
                  "subject": self.subject,
                  "source": f"/tenants/{tenant}/workspaces/{self.target.workspace_id}/items/{self.target.lakehouse_id}"}
        graph = activator.build_native_definition(
            tenant, self.target.workspace_id, self.target.lakehouse_id, pipeline_id, sample)
        settings = {"producer_workspace_id": self.authority.workspace_id,
                    "producer_item_id": self.authority.lakehouse_id}
        notebook = {"cells": [{"source": ["verified"]},
                              {"source": [f"execute(spark, notebookutils, {settings!r}, inputs)\n"]}],
                    "metadata": {"trident": {"lakehouse": {"default_lakehouse": self.target.lakehouse_id}}}}
        pipeline = {"properties": {"guarded": True}}
        marker = "owned-marker-123"
        data = {"notebook-content.ipynb": notebook, "pipeline-content.json": pipeline,
                "ReflexEntities.json": graph}

        class Client:
            def list_all(client, path):
                if "lakehouses" in path:
                    identity = self.target.lakehouse_id
                elif "type=Notebook" in path:
                    identity = notebook_id
                elif "type=DataPipeline" in path:
                    identity = pipeline_id
                elif "reflexes" in path:
                    identity = reflex_id
                else:
                    return []
                return [{"id": identity, "description": marker}]

            def get(client, path, deadline=None):
                return {}, {"id": connection_id, "connectionDetails": {"type": "Lakehouse"}}

            def request(client, method, path, payload=None, timeout=30):
                if method == "GET":
                    return 200, {}, {"status": "Completed"}
                part = ("ReflexEntities.json" if "reflexes" in path else
                        "notebook-content.ipynb" if "format=ipynb" in path else "pipeline-content.json")
                encoded = base64.b64encode(json.dumps(data[part]).encode()).decode()
                return 200, {}, {"definition": {"parts": [
                    {"path": part, "payloadType": "InlineBase64", "payload": encoded}]}}

        modules = {"generator": self.generator, "fabric_runtime": types.SimpleNamespace(
            run_config=lambda document: (config, 1800)),
            "guard_spark": types.SimpleNamespace(query_target=lambda *args: self.proof(entries[args[-1]]))}
        with mock.patch.dict(sys.modules, modules):
            import native_demo3

            generated = guarded_notebook.build_native_notebook(
                self.producer_path.parent, self.policy, self.target.workspace_id, self.target.lakehouse_id,
                "TestLakehouse", self.authority.workspace_id, self.authority.lakehouse_id)
            self.assertEqual(native_demo3._guard_staging(generated),
                             (self.authority.workspace_id, self.authority.lakehouse_id))
            proof = {"version": 1, "workspace_id": self.target.workspace_id,
                     "lakehouse_id": self.target.lakehouse_id, "guard_notebook_id": notebook_id,
                     "pipeline_id": pipeline_id, "reflex_id": reflex_id, "connection_id": connection_id,
                     "marker": marker, "native_policy": self.policy,
                     "generator_sha256": self.generator.generator_source_sha256(),
                     "guard_sha256": native_demo3._digest(native_demo3._guard(notebook)),
                     "pipeline_sha256": native_demo3._digest(pipeline["properties"]),
                     "graph_disabled_sha256": native_demo3._digest(native_demo3._graph(graph, enabled=False))}
            document = {"data": {"run_id": config.run_id}, "demo-3": {"native_proof": proof},
                        "shared": {"workspace_id": self.authority.workspace_id,
                                   "staging_lakehouse_id": self.authority.lakehouse_id, "target_bindings": {
                "demo-3": {"workspace_id": self.target.workspace_id, "item_id": self.target.lakehouse_id,
                           "fact": "dbo.factsales"}}}}
            digest = native_demo3._digest(document)
            binding = {"run_id": config.run_id, "config_sha256": digest, "nonce": "native-" + digest[:32],
                       "producer": {"workspace_id": self.authority.workspace_id,
                                    "staging_lakehouse_id": self.authority.lakehouse_id}}
            client = Client()
            context = {"document": document, "binding": binding, "store": self.authority,
                       "fabric_factory": lambda skill: client,
                       "store_factory": lambda ws, lh: self.target, "read_manifest": lambda: self.manifest,
                       "clock": lambda: self.now, "sleep": lambda seconds: None, "stop_requested": lambda: False,
                       "preparation_deadline_utc": (self.now + timedelta(minutes=5)).isoformat(),
                       "verification_deadline_utc": (self.now + timedelta(minutes=10)).isoformat(),
                       "spark": object(), "notebookutils": object()}
            with mock.patch.object(native_demo3, "_dimensions"), self.assertRaisesRegex(ValueError, "rule state"):
                native_demo3.prepare(context)
            graph[3]["payload"]["definition"]["settings"]["shouldRun"] = True
            with mock.patch.object(native_demo3, "_dimensions"):
                prepared = native_demo3.prepare(context)
            self.assertEqual(native_demo3.verify(context, prepared)["batch_count"], 2)
            other_stage = str(uuid.uuid4())
            document["shared"]["staging_lakehouse_id"] = other_stage
            context["store"] = types.SimpleNamespace(workspace_id=self.authority.workspace_id,
                                                      lakehouse_id=other_stage)
            binding["producer"]["staging_lakehouse_id"] = other_stage
            binding["config_sha256"] = native_demo3._digest(document)
            binding["nonce"] = "native-" + binding["config_sha256"][:32]
            with mock.patch.object(native_demo3, "_dimensions"), \
                    self.assertRaisesRegex(RuntimeError, "staging bindings differ"):
                native_demo3.prepare(context)
            document["shared"]["staging_lakehouse_id"] = self.authority.lakehouse_id
            context["store"] = self.authority
            binding["producer"]["staging_lakehouse_id"] = self.authority.lakehouse_id
            binding["config_sha256"] = native_demo3._digest(document)
            binding["nonce"] = "native-" + binding["config_sha256"][:32]
            path = audit.RECORD_PREFIX + entry["batch_id"] + ".json"
            original = self.target.read(path)
            changed = json.loads(original[0])
            changed["observations"][0].update(origin="recovery", event_id=None)
            self.target.objects[path] = (json.dumps(changed).encode(), original[1])
            with self.assertRaisesRegex(RuntimeError, "no matching event"):
                native_demo3.verify(context, prepared)
            self.target.objects[path] = original
            with mock.patch.object(native_demo3.guard_spark, "query_target",
                                   return_value={**self.proof(entry), "row_difference_count": 1}):
                with self.assertRaisesRegex(RuntimeError, "fact or dimension"):
                    native_demo3.verify(context, prepared)
            self.manifest["demo3_admission"]["workspace_id"] = str(uuid.uuid4())
            with self.assertRaisesRegex(ValueError, "destination-bound"):
                native_demo3.verify(context, prepared)
            self.manifest["demo3_admission"]["workspace_id"] = self.target.workspace_id
            graph[3]["payload"]["definition"]["instance"] = "{}"
            with mock.patch.object(native_demo3, "_dimensions"), self.assertRaisesRegex(RuntimeError, "differs"):
                native_demo3.prepare(context)

    def test_native_definition_read_polls_only_the_recorded_operation(self):
        operation = str(uuid.uuid4())
        payload = base64.b64encode(b'{"properties":{"ok":true}}').decode()
        client = types.SimpleNamespace(request=mock.Mock(side_effect=[
            (202, {"x-ms-operation-id": operation}, None),
            (200, {}, {"status": "Running"}),
            (200, {}, {"status": "Succeeded"}),
            (200, {}, {"definition": {"parts": [{
                "path": "pipeline-content.json", "payloadType": "InlineBase64", "payload": payload}]}}),
        ]))
        clock = [self.now]
        context = {"clock": lambda: clock[0],
                   "sleep": lambda seconds: clock.__setitem__(0, clock[0] + timedelta(seconds=seconds)),
                   "stop_requested": lambda: False}
        modules = {"generator": self.generator, "fabric_runtime": types.SimpleNamespace(run_config=None),
                   "guard_spark": types.SimpleNamespace()}
        with mock.patch.dict(sys.modules, modules):
            import native_demo3

            result = native_demo3._definition(
                context, client, "/v1/workspaces/a/items/b/getDefinition", "pipeline-content.json",
                self.now + timedelta(seconds=20))
        self.assertEqual(result, {"properties": {"ok": True}})
        self.assertEqual([call.args[:2] for call in client.request.call_args_list], [
            ("POST", "/v1/workspaces/a/items/b/getDefinition"),
            ("GET", "/v1/operations/" + operation),
            ("GET", "/v1/operations/" + operation),
            ("GET", "/v1/operations/" + operation + "/result")])

    def test_native_lock_readback_conflict_never_retries_successful_create(self):
        guarded = self.native.AdmissionAudit(
            self.target, self.settings["workspace_id"], self.settings["item_id"], lambda: None,
            lock_conflict=ConditionalStore.Conflict, sleep=mock.Mock())
        with mock.patch.object(self.target, "read", side_effect=ConditionalStore.Conflict("read conflict")):
            with self.assertRaisesRegex(ConditionalStore.Conflict, "read conflict"):
                guarded.acquire(str(uuid.uuid4()), self.config.run_id + "_0001")
        self.assertIn(audit.LOCK_PATH, self.target.objects)
        guarded.sleep.assert_not_called()

    def test_contended_event_waits_past_sixty_seconds_without_stealing_writer(self):
        first = audit.BatchAudit(self.target, self.settings["workspace_id"], self.settings["item_id"])
        held = first.acquire(str(uuid.uuid4()), "older_run_0001")
        original = self.target.read(audit.LOCK_PATH)
        elapsed = [0]

        def sleep(seconds):
            self.assertEqual(self.target.read(audit.LOCK_PATH), original)
            elapsed[0] += seconds
            if elapsed[0] >= 65:
                self.target.delete(audit.LOCK_PATH, held.lock_etag)

        job = str(uuid.uuid4())
        waiting = self.native.AdmissionAudit(
            self.target, self.settings["workspace_id"], self.settings["item_id"], lambda: None,
            lock_conflict=self.target.Conflict,
            observation={"event_id": "preserved-event", "origin": "event"},
            wait_seconds=120, clock=lambda: elapsed[0], sleep=sleep)
        with mock.patch("builtins.print"):
            token = waiting.acquire(job, self.config.run_id + "_0001")
        self.assertEqual(elapsed[0], 65)
        self.assertEqual(token.pipeline_run_id, job)
        ticket = json.loads(self.target.read("Files/demo3-audit/waits/" + job + ".json")[0])
        self.assertEqual(ticket["status"], "acquired")
        self.assertEqual(ticket["observation"]["event_id"], "preserved-event")

    def test_contention_timeout_preserves_event_and_never_steals_or_retries_uncertain_write(self):
        first = audit.BatchAudit(self.target, self.settings["workspace_id"], self.settings["item_id"])
        first.acquire(str(uuid.uuid4()), "older_run_0001")
        original = self.target.read(audit.LOCK_PATH)
        elapsed = [0]

        def sleep(seconds):
            elapsed[0] += seconds

        job = str(uuid.uuid4())
        waiting = self.native.AdmissionAudit(
            self.target, self.settings["workspace_id"], self.settings["item_id"], lambda: None,
            lock_conflict=self.target.Conflict,
            observation={"event_id": "blocked-event", "origin": "event"},
            wait_seconds=60, clock=lambda: elapsed[0], sleep=sleep)
        with mock.patch("builtins.print"), self.assertRaisesRegex(TimeoutError, "durably blocked"):
            waiting.acquire(job, self.config.run_id + "_0001")
        self.assertEqual(self.target.read(audit.LOCK_PATH), original)
        ticket = json.loads(self.target.read("Files/demo3-audit/waits/" + job + ".json")[0])
        self.assertEqual((ticket["status"], ticket["reason"]), ("blocked", "writer-lock-timeout"))
        with mock.patch.object(self.target, "create", side_effect=RuntimeError("outcome unknown")) as create:
            with self.assertRaisesRegex(RuntimeError, "outcome unknown"):
                waiting.acquire(str(uuid.uuid4()), self.config.run_id + "_0002")
            create.assert_called_once()


if __name__ == "__main__":
    unittest.main()
