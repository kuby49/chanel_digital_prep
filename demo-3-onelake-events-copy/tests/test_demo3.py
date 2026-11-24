import copy
import importlib.util
import json
import os
import subprocess
import sys
import tempfile
import unittest
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

    def test_run_filter_is_bound_to_the_approved_run(self):
        ids = [str(uuid.uuid4()) for _ in range(4)]
        sample = {**self.event, "source": f"/tenants/{ids[0]}/workspaces/{ids[1]}/items/{ids[2]}"}
        entities = activator.build_definition(*ids, sample, run_id="new_run-2026")
        source = next(entity for entity in entities if entity["type"] == "realTimeHubSource-v1")
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
        self.assertIn("setup_allow_dimension_expansion = False", parameters)
        for cell in notebook["cells"]:
            compile("".join(cell["source"]), cell["id"], "exec")
        namespace = {}
        exec("".join(notebook["cells"][1]["source"]), namespace)
        self.addCleanup(namespace["_setup_generator_dir"].cleanup)
        self.addCleanup(sys.modules.pop, namespace["_setup_generator_name"], None)
        self.assertEqual(namespace["_setup_generator_path"].read_bytes(), self.generator.encode("utf-8"))
        self.assertEqual(namespace["dimensions"](namespace["RunConfig"]()), {})

    def test_dimension_expansion_is_explicit_and_boolean(self):
        notebook = setup.build_notebook(
            self.generator, self.config, self.workspace_id, self.lakehouse_id, "DemoLakehouse",
            allow_dimension_expansion=True)
        self.assertIn("setup_allow_dimension_expansion = True", "".join(notebook["cells"][0]["source"]))
        for invalid in ("yes", 1, None):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                setup.build_notebook(
                    self.generator, self.config, self.workspace_id, self.lakehouse_id, "DemoLakehouse",
                    allow_dimension_expansion=invalid)

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
        supported_features = {
            "minReaderVersion": 3, "minWriterVersion": 7,
            "tableFeatures": ["deletionVectors", "invariants"],
            "properties": {"delta.enableDeletionVectors": "false"},
        }
        view_validation.validate_copy_protocol(supported_features)
        for detail in (
            {"minReaderVersion": 3, "minWriterVersion": 7},
            {**supported_features, "tableFeatures": [["deletionVectors"], "invariants"]},
            {**supported_features, "tableFeatures": ["deletionVectors", "invariants", "appendOnly"]},
            {**supported_features, "tableFeatures": ["deletionVectors", "invariants", "columnMapping"]},
            {**supported_features, "properties": {"delta.enableDeletionVectors": "true"}},
            {**supported_features, "properties": {}},
            {},
            {"minReaderVersion": True, "minWriterVersion": 2},
        ):
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

    def test_batch_ids_must_start_at_one(self):
        token = self.audit.acquire(self.job, self.evidence["batch_id"])
        with self.assertRaises(audit.AuditError):
            self.audit.begin(token, {**self.evidence, "first_sales_line_id": 11,
                                     "last_sales_line_id": 12})

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
        with mock.patch.dict(sys.modules, {"generator": producer}):
            import guard_spark
            self.assertEqual(guard_spark.onelake_endpoint(
                "https://onelake.dfs.fabric.microsoft.com/"), "onelake.dfs.fabric.microsoft.com")
            with self.assertRaises(RuntimeError):
                guard_spark.onelake_endpoint("https://other.invalid/")
        config = producer.RunConfig("integration", date_count=4, products=3, customers=5,
                                    file_count=1, rows_per_file=12)
        shared = {"contract_version": 1, "data": {
            "run_id": "integration", "start_date": config.start_date, "seed": config.seed,
            "date_count": 4, "product_count": 3, "customer_count": 5, "file_count": 1,
            "rows_per_file": 12, "interval_seconds": config.interval_seconds,
        }}
        from dataclasses import asdict
        self.assertEqual(guarded_notebook.run_config_from_shared(producer, shared), asdict(config))
        for invalid in (
            {"contract_version": 1, "data": {}},
            {**shared, "data": {**shared["data"], "run_id": "../unsafe"}},
            {**shared, "data": {**shared["data"], "file_count": True}},
        ):
            with self.subTest(config=invalid), self.assertRaises(ValueError):
                guarded_notebook.run_config_from_shared(producer, invalid)
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

            import hashlib
            from types import SimpleNamespace
            lines = source_bytes.decode("utf-8").splitlines()
            shifted = ("\n".join([lines[0]] + [
                f"{int(line.split(',', 1)[0]) + 10},{line.split(',', 1)[1]}"
                for line in lines[1:]
            ]) + "\n").encode("utf-8")
            generated, generated_metadata = producer.batch_bytes(config, 1)
            self.assertEqual(generated, source_bytes)
            shifted_metadata = {**generated_metadata, "sha256": hashlib.sha256(shifted).hexdigest()}
            shifted_manifest = copy.deepcopy(manifest)
            shifted_manifest["batches"]["1"].update(shifted_metadata)
            shifted_producer = SimpleNamespace(
                RunConfig=producer.RunConfig, HEADER=producer.HEADER,
                strict_json_loads=producer.strict_json_loads,
                generator_source_sha256=producer.generator_source_sha256,
                batch_bytes=lambda _config, _number: (shifted, shifted_metadata))
            with self.assertRaisesRegex(ValueError, "contiguous from 1"):
                batch_gate.validate_batch(shifted_producer, config, json.dumps(shifted_manifest).encode(),
                                          subject, shifted, event_id="event-1")

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
                (("generator_sha256",), "0" * 64),
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
    def test_two_batches_reuse_ids_and_late_event_does_not_copy_again(self):
        import csv
        import io
        from collections import Counter, defaultdict
        producer_path = Path(os.environ.get("DEMO3_PRODUCER_PATH") or
                             ROOT.parent / "demo-data-generator" / "generator.py").resolve()
        sys.path.insert(0, str(producer_path.parent))
        self.addCleanup(sys.path.remove, str(producer_path.parent))
        producer_spec = importlib.util.spec_from_file_location("producer_fixture_two_batches", producer_path)
        producer = importlib.util.module_from_spec(producer_spec)
        sys.modules[producer_spec.name] = producer
        self.addCleanup(sys.modules.pop, producer_spec.name, None)
        producer_spec.loader.exec_module(producer)
        config = producer.RunConfig("two_batches", date_count=4, products=3, customers=5,
                                    file_count=2, rows_per_file=3)
        manifest = producer.new_manifest(config, ["demo-3"])
        workspace, lakehouse = str(uuid.uuid4()), str(uuid.uuid4())
        storage = ConditionalStore(workspace, lakehouse)
        authority = ConditionalStore(str(uuid.uuid4()), str(uuid.uuid4()))
        audit_store = audit.BatchAudit(storage, workspace, lakehouse)
        guarded = delivery_runtime.GuardedDelivery(producer, config, authority, audit_store)
        for number in (1, 2):
            payload, entry = producer.batch_bytes(config, number)
            manifest["batches"][str(number)] = {
                **entry, "deliveries": {"demo-3": {"status": "submitted"}}}
            storage.create(f"Files/incoming/sales/{entry['filename']}", payload)
        authority.create(delivery_runtime.manifest_path(config), json.dumps(manifest).encode())
        target, submissions = {}, []
        dimensions = producer.dimensions(config)
        products = {str(item["ProductID"]): item["ProductCategory"] for item in dimensions["DimProduct"]}
        customers = {str(item["CustomerID"]): item["CustomerRegion"] for item in dimensions["DimCustomer"]}
        dates = {item["Date"] for item in dimensions["DimDate"]}

        def copy_batch(subject, event_id, origin, remaining):
            job = str(uuid.uuid4())
            prepared = guarded.prepare(job, subject, event_id or None, origin)
            self.assertEqual(prepared["decision"], "copy-required")
            submissions.append(subject)
            payload, _ = storage.read(subject[1:])
            rows = list(csv.DictReader(io.StringIO(payload.decode())))
            for row in rows:
                target[(row["BatchID"], int(row["SalesLineID"]))] = row

            def read_target(batch_id):
                actual = [row for (batch, _), row in target.items() if batch == batch_id]
                ids = [int(row["SalesLineID"]) for row in actual]
                groups = {name: defaultdict(int) for name in audit.GROUPS}
                orphan_rows = 0
                for row in actual:
                    amount = int(row["SalesAmountMinorUnits"])
                    if row["Date"] not in dates or row["ProductID"] not in products or row["CustomerID"] not in customers:
                        orphan_rows += 1
                        continue
                    groups["amount_by_date"][row["Date"]] += amount
                    groups["amount_by_product_category"][products[row["ProductID"]]] += amount
                    groups["amount_by_customer_region"][customers[row["CustomerID"]]] += amount
                expected_rows = Counter(tuple(row[name] for name in producer.HEADER) for row in rows)
                actual_rows = Counter(tuple(row[name] for name in producer.HEADER) for row in actual)
                return {
                    "row_count": len(actual), "distinct_sales_line_ids": len(set(ids)),
                    "first_sales_line_id": min(ids), "last_sales_line_id": max(ids),
                    "sales_amount_minor_units": sum(int(row["SalesAmountMinorUnits"]) for row in actual),
                    "orphan_rows": orphan_rows,
                    "row_difference_count": sum((expected_rows - actual_rows).values())
                                            + sum((actual_rows - expected_rows).values()),
                    **{name: dict(value) for name, value in groups.items()},
                }

            guarded.finish(job, prepared, {
                "status": "Succeeded", "pipeline_run_id": job, "activity_run_id": None,
                "files_read": 1, "rows_read": len(rows), "rows_copied": len(rows),
            }, read_target)

        guarded.reconcile(copy_batch, timeout_seconds=5, poll_seconds=1, event_grace_seconds=0)
        batch_ids = [manifest["batches"][str(n)]["batch_id"] for n in (1, 2)]
        self.assertEqual(set(target), {(batch, key) for batch in batch_ids for key in (1, 2, 3)})
        first = manifest["batches"]["1"]["filename"]
        replay = guarded.prepare(str(uuid.uuid4()), f"/Files/incoming/sales/{first}", "late", "event")
        self.assertEqual(replay["decision"], "already-completed")
        guarded.reconcile(copy_batch, timeout_seconds=5, poll_seconds=1, event_grace_seconds=0)
        self.assertEqual(len(submissions), 2)
        self.assertEqual(len(target), 6)


if __name__ == "__main__":
    unittest.main()
