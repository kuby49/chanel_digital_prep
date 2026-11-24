import copy
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from definition_plan import (
    bootstrap_csv_bytes,
    build_refresh_schedule_request,
    effective_verification_timeout_seconds,
    render_bronze_sql,
    render_silver_sql,
)
from manifest_validation import measured_batches, validate_manifest
from ownership import _config_fingerprint


SCRIPT = Path(__file__).with_name("deploy.py")
MANIFEST_FIXTURE = Path(__file__).with_name("testdata") / "producer-manifest-v1.json"
TENANT = "4526cee1-4399-4e4b-9134-46fabd583527"
WORKSPACE = "2aaaaaaa-1111-4111-8111-111111111111"
LAKEHOUSE = "32222222-2222-4222-8222-222222222222"
OWNER = "74c0c844-c9ee-45ba-a099-aec374c28a09"
INTENT = "3b28eeb1-792d-493d-80e9-86a6b75d4831"


def candidate_config():
    return {
        "contract_version": 1, "prefix": "chanel", "tenant_id": TENANT,
        "capacity": {"id": "de12195c-40d2-4bee-af60-f0d6dedfbb0f"},
        "demo-1": {
            "workspace_name": "{prefix}-demo1-mlv", "lakehouse_name": "ChanelDemo1Lakehouse",
            "silver_schema": "silver", "silver_view": "sales_joined",
        },
    }


def candidate_journal(config):
    marker = f"chanel-demo1-{OWNER}"
    def resource(kind, resource_id, name):
        return {
            "status": "confirmed", "type": kind, "id": resource_id,
            "created_by_owner": OWNER, "create_intent_id": INTENT,
            "create_response": {"id": resource_id},
            "readback": {"id": resource_id, "type": kind, "name": name,
                         "owner_marker": marker},
        }
    workspace = resource("Workspace", WORKSPACE, "chanel-demo1-mlv")
    lakehouse = resource("Lakehouse", LAKEHOUSE, "ChanelDemo1Lakehouse")
    lakehouse["workspace_id"] = lakehouse["readback"]["workspace_id"] = WORKSPACE
    return {
        "contract_version": 1, "track": "demo-1", "tenant_id": TENANT,
        "prefix": "chanel", "config_sha256": _config_fingerprint(config, "chanel"),
        "pending_operations": [], "uncertain_operations": [],
        "owner": {"id": OWNER, "marker": marker}, "workspace": workspace,
        "lakehouse": lakehouse,
        "fact": {"status": "confirmed", "workspace_id": WORKSPACE, "item_id": LAKEHOUSE,
                 "name": "silver.sales_joined", "type": "MATERIALIZED_LAKE_VIEW"},
    }


class ContractTest(unittest.TestCase):
    def invoke(self, mode, config, *extra):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            config_path = root / "config.json"
            config_path.write_text(json.dumps(config), encoding="utf-8")
            result_path = root / "result.json"
            process = subprocess.run(
                [sys.executable, str(SCRIPT), "--mode", mode, "--config", str(config_path),
                 "--prefix", "chanel", "--state-dir", str(root / "state"),
                 "--result", str(result_path), *extra],
                capture_output=True, text=True, check=False,
            )
            return process.returncode, json.loads(result_path.read_text(encoding="utf-8"))

    def test_dry_run_does_not_claim_readiness(self):
        code, result = self.invoke("dry-run", {"contract_version": 1, "prefix": "chanel"})
        self.assertEqual(code, 0)
        self.assertEqual(result["status"], "pass")
        self.assertFalse(result["ready"])

    def test_dry_run_reports_effective_verification_timeout(self):
        config = {
            "contract_version": 1,
            "execution": {"verification_timeout_seconds": 600},
            "demo-1": {"verification_timeout_seconds": 120},
        }
        code, result = self.invoke("dry-run", config)
        self.assertEqual(code, 0)
        self.assertIn("Per-batch verification timeout: 120 seconds.", result["diagnostics"])

    def test_invalid_verification_timeout_fails_closed(self):
        config = {
            "contract_version": 1,
            "demo-1": {"verification_timeout_seconds": 0},
        }
        code, result = self.invoke("dry-run", config)
        self.assertNotEqual(code, 0)
        self.assertEqual(result["status"], "fail")
        self.assertIn(
            "demo-1.verification_timeout_seconds must be a positive integer",
            result["diagnostics"],
        )

    def test_mutations_and_validation_remain_blocked(self):
        for mode, extra in (("deploy", ()), ("validate-only", ()), ("cleanup", ("--yes",))):
            with self.subTest(mode=mode):
                code, result = self.invoke(mode,                 {"contract_version": 1, "prefix": "chanel"}, *extra)
                self.assertNotEqual(code, 0)
                self.assertEqual(result["status"], "blocked")
                self.assertFalse(result["ready"])
                self.assertTrue(any("header-only bootstrap" in item for item in result["diagnostics"]))
                self.assertFalse(any("empty-folder Bronze MLV creation failed" in item
                                     for item in result["diagnostics"]))

    def test_target_candidate_fails_closed_without_ownership_journal(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            config = root / "config.json"
            config.write_text(json.dumps(candidate_config()), encoding="utf-8")
            result = root / "result.json"
            process = subprocess.run(
                [sys.executable, str(SCRIPT), "--mode", "resolve-target-candidate",
                 "--config", str(config), "--prefix", "chanel",
                 "--state-dir", str(root / "state"), "--result", str(result)],
                capture_output=True, text=True, check=False,
            )
            envelope = json.loads(result.read_text(encoding="utf-8"))
            self.assertNotEqual(process.returncode, 0)
            self.assertEqual(envelope["status"], "blocked")
            self.assertFalse(envelope["ready"])
            self.assertTrue(any("ownership journal" in item for item in envelope["diagnostics"]))
            self.assertFalse((root / "demo-1-target-candidate.json").exists())

    def test_target_candidate_preserves_unresolved_journal(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            config = root / "config.json"
            config.write_text(json.dumps(candidate_config()), encoding="utf-8")
            state = root / "state"
            state.mkdir()
            journal = state / "demo-1-state.json"
            journal.write_text('{"status":"pending"}', encoding="utf-8")
            result = root / "result.json"
            process = subprocess.run(
                [sys.executable, str(SCRIPT), "--mode", "resolve-target-candidate",
                 "--config", str(config), "--prefix", "chanel",
                 "--state-dir", str(state), "--result", str(result)],
                capture_output=True, text=True, check=False,
            )
            envelope = json.loads(result.read_text(encoding="utf-8"))
            self.assertNotEqual(process.returncode, 0)
            self.assertEqual(envelope["status"], "blocked")
            self.assertEqual(journal.read_text(encoding="utf-8"), '{"status":"pending"}')
            self.assertFalse((root / "demo-1-target-candidate.json").exists())

    def test_candidate_from_confirmed_journal_is_local_only_and_non_overwriting(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            config = candidate_config()
            config_path = root / "config.json"
            config_path.write_text(json.dumps(config), encoding="utf-8")
            state = root / "state"
            state.mkdir()
            journal_path = state / "demo-1-state.json"
            journal = json.dumps(candidate_journal(config))
            journal_path.write_text(journal, encoding="utf-8")
            result_path = root / "result.json"
            command = [sys.executable, str(SCRIPT), "--mode", "resolve-target-candidate",
                       "--config", str(config_path), "--prefix", "chanel", "--state-dir",
                       str(state), "--result", str(result_path)]
            process = subprocess.run(command, capture_output=True, text=True, check=False)
            result = json.loads(result_path.read_text(encoding="utf-8"))
            self.assertEqual(process.returncode, 0, process.stdout + process.stderr)
            self.assertEqual(result["status"], "pass")
            self.assertFalse(result["ready"])
            path = root / "demo-1-target-candidate.json"
            self.assertEqual(json.loads(path.read_text(encoding="utf-8")), {
                "contract_version": 1, "track": "demo-1", "tenant_id": TENANT,
                "prefix": "chanel", "target": {
                    "workspace_id": WORKSPACE, "item_id": LAKEHOUSE, "fact": "silver.sales_joined",
                },
            })
            original = path.read_bytes()
            repeat = subprocess.run(command, capture_output=True, text=True, check=False)
            self.assertNotEqual(repeat.returncode, 0)
            self.assertEqual(json.loads(result_path.read_text())["status"], "blocked")
            self.assertEqual(path.read_bytes(), original)
            self.assertEqual(journal_path.read_text(encoding="utf-8"), journal)
            self.assertFalse((state / "demo-1-state.lock").exists())

    def test_candidate_rejects_mismatched_and_uncertain_journals(self):
        config = candidate_config()
        def mutate(journal, case):
            if case == "pending":
                journal["pending_operations"] = ["create"]
            elif case == "uncertain":
                journal["uncertain_operations"] = ["create"]
            elif case == "tenant":
                journal["tenant_id"] = WORKSPACE
            elif case == "config":
                journal["config_sha256"] = "0" * 64
            elif case == "owner":
                journal["workspace"]["created_by_owner"] = WORKSPACE
            elif case == "parent":
                journal["lakehouse"]["readback"]["workspace_id"] = LAKEHOUSE
            elif case == "response":
                journal["workspace"]["create_response"]["id"] = LAKEHOUSE
            elif case == "fact":
                journal["fact"]["item_id"] = WORKSPACE
            elif case == "uuid":
                journal["workspace"]["id"] = WORKSPACE.upper()
        for case in ("pending", "uncertain", "tenant", "config", "owner", "parent",
                     "response", "fact", "uuid"):
            with self.subTest(case=case), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                config_path = root / "config.json"
                config_path.write_text(json.dumps(config), encoding="utf-8")
                state = root / "state"
                state.mkdir()
                journal = candidate_journal(config)
                mutate(journal, case)
                journal_path = state / "demo-1-state.json"
                raw = json.dumps(journal)
                journal_path.write_text(raw, encoding="utf-8")
                result = root / "result.json"
                process = subprocess.run(
                    [sys.executable, str(SCRIPT), "--mode", "resolve-target-candidate",
                     "--config", str(config_path), "--prefix", "chanel", "--state-dir",
                     str(state), "--result", str(result)], capture_output=True, text=True)
                self.assertNotEqual(process.returncode, 0)
                self.assertEqual(json.loads(result.read_text())["status"], "blocked")
                self.assertEqual(journal_path.read_text(encoding="utf-8"), raw)
                self.assertFalse((root / "demo-1-target-candidate.json").exists())

    def test_candidate_rejects_lock_and_protects_journal_result_path(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            config = root / "config.json"
            config.write_text(json.dumps(candidate_config()), encoding="utf-8")
            state = root / "state"
            state.mkdir()
            journal = state / "demo-1-state.json"
            journal.write_text("unchanged", encoding="utf-8")
            (state / "demo-1-state.lock").write_text("stale", encoding="utf-8")
            command = [sys.executable, str(SCRIPT), "--mode", "resolve-target-candidate",
                       "--config", str(config), "--prefix", "chanel", "--state-dir", str(state)]
            result = root / "result.json"
            locked = subprocess.run(command + ["--result", str(result)], capture_output=True, text=True)
            self.assertNotEqual(locked.returncode, 0)
            self.assertEqual(json.loads(result.read_text())["status"], "blocked")
            collision = subprocess.run(command + ["--result", str(journal)], capture_output=True, text=True)
            self.assertNotEqual(collision.returncode, 0)
            self.assertEqual(journal.read_text(encoding="utf-8"), "unchanged")
            self.assertEqual((state / "demo-1-state.lock").read_text(), "stale")

    def test_candidate_rejects_duplicate_journal_keys_and_conflicting_binding(self):
        for case in ("duplicate", "binding"):
            with self.subTest(case=case), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                config = candidate_config()
                if case == "binding":
                    config["shared"] = {"target_bindings": {"demo-1": {
                        "workspace_id": WORKSPACE, "item_id": WORKSPACE,
                        "fact": "silver.sales_joined",
                    }}}
                config_path = root / "config.json"
                config_path.write_text(json.dumps(config), encoding="utf-8")
                state = root / "state"
                state.mkdir()
                raw = json.dumps(candidate_journal(config))
                if case == "duplicate":
                    raw = raw[:-1] + ', "track": "demo-1"}'
                journal = state / "demo-1-state.json"
                journal.write_text(raw, encoding="utf-8")
                result = root / "result.json"
                process = subprocess.run(
                    [sys.executable, str(SCRIPT), "--mode", "resolve-target-candidate",
                     "--config", str(config_path), "--prefix", "chanel", "--state-dir",
                     str(state), "--result", str(result)], capture_output=True, text=True)
                self.assertNotEqual(process.returncode, 0)
                self.assertEqual(json.loads(result.read_text())["status"], "blocked")
                self.assertEqual(journal.read_text(encoding="utf-8"), raw)
                self.assertFalse((root / "demo-1-target-candidate.json").exists())

    def test_cleanup_requires_confirmation_and_contract_must_match(self):
        for mode, config in (("cleanup", {"contract_version": 1, "prefix": "chanel"}),
                             ("dry-run", {"contract_version": 2, "prefix": "chanel"})):
            with self.subTest(mode=mode):
                code, result = self.invoke(mode, config)
                self.assertNotEqual(code, 0)
                self.assertEqual(result["status"], "fail")

    def test_verification_timeout_override_precedence_and_validation(self):
        self.assertEqual(effective_verification_timeout_seconds({}), 900)
        self.assertEqual(
            effective_verification_timeout_seconds(
                {"execution": {"verification_timeout_seconds": 600}}
            ),
            600,
        )
        self.assertEqual(
            effective_verification_timeout_seconds(
                {
                    "execution": {"verification_timeout_seconds": 600},
                    "demo-1": {"verification_timeout_seconds": 120},
                }
            ),
            120,
        )
        for config in (
            {"execution": {"verification_timeout_seconds": 0}},
            {"execution": {"verification_timeout_seconds": True}},
            {"demo-1": {"verification_timeout_seconds": -1}},
            {"demo-1": {"verification_timeout_seconds": "120"}},
        ):
            with self.subTest(config=config):
                with self.assertRaisesRegex(ValueError, "positive integer"):
                    effective_verification_timeout_seconds(config)

    def test_bootstrap_csv_is_exactly_the_header_and_no_data(self):
        self.assertEqual(
            bootstrap_csv_bytes(),
            b"SalesLineID,BatchID,SaleTimestamp,CustomerID,ProductID,Date,"
            b"Quantity,UnitPriceMinorUnits,SalesAmountMinorUnits\n",
        )

    def test_manifest_contract_and_batch_class_filtering(self):
        manifest = json.loads(MANIFEST_FIXTURE.read_text(encoding="utf-8"))
        self.assertEqual(validate_manifest(manifest), [])
        self.assertEqual([b["batch_class"] for b in measured_batches(manifest)], ["measured"])
        batch = manifest["batches"]["1"]
        self.assertEqual(
            {
                "amount_by_date", "amount_by_product_category",
                "amount_by_customer_region",
            }.intersection(batch),
            {
                "amount_by_date", "amount_by_product_category",
                "amount_by_customer_region",
            },
        )

        mixed = copy.deepcopy(manifest)
        bootstrap = copy.deepcopy(mixed["batches"]["1"])
        bootstrap.update(batch_number=2, batch_class="bootstrap")
        negative = copy.deepcopy(mixed["batches"]["1"])
        negative.update(batch_number=3, batch_class="negative-test")
        mixed["batches"].update({"2": bootstrap, "3": negative})
        self.assertEqual([b["batch_class"] for b in measured_batches(mixed)], ["measured"])

    def test_manifest_requires_supported_version_and_class(self):
        manifest = json.loads(MANIFEST_FIXTURE.read_text(encoding="utf-8"))
        manifest["contract_version"] = 2
        manifest["batches"]["1"]["batch_class"] = "unclassified"
        diagnostics = validate_manifest(manifest)
        self.assertTrue(any("contract_version" in item for item in diagnostics))
        self.assertTrue(any("batch_class" in item for item in diagnostics))

    def test_renders_bronze_and_qualified_silver_definitions(self):
        config = {
            "contract_version": 1,
            "demo-1": {
                "workspace_name": "{prefix}-demo1-mlv",
                "lakehouse_name": "demo1",
                "landing_uri": "abfss://workspace@onelake.dfs.fabric.microsoft.com/lakehouse/Files/incoming/sales/",
                "bronze_schema": "bronze",
                "silver_schema": "silver",
                "bronze_view": "sales_files",
                "silver_view": "sales_joined",
                "csv_options": {},
            },
        }
        bronze = render_bronze_sql(config, "chanel")
        silver = render_silver_sql(config, "chanel")
        self.assertIn("USING OneLake_Files", bronze)
        self.assertIn("'schema_mode' = 'FIXED'", bronze)
        self.assertIn("'refresh_mode' = 'APPEND_ONLY'", bronze)
        self.assertIn("__filepath__", bronze)
        self.assertIn("'inferSchema' = 'false'", bronze)
        self.assertIn("'columnNameOfCorruptRecord' = '_corrupt_record'", bronze)
        self.assertIn(r"'escape' = '\\'", bronze)
        self.assertIn(r"'nullValue' = '\\N'", bronze)
        config["demo-1"]["csv_options"] = {"quote": "'"}
        self.assertIn(r"'quote' = '\''", render_bronze_sql(config, "chanel"))
        self.assertIn("`chanel-demo1-mlv`.`demo1`.`silver`.`sales_joined`", silver)
        self.assertIn("`chanel-demo1-mlv`.`demo1`.`silver`.`DimCustomer`", silver)
        self.assertIn("corrupt_record IS NULL", silver)
        self.assertNotIn("{{", silver)

    def test_bounded_refresh_schedule_requires_last_batch_window(self):
        request = build_refresh_schedule_request(
            "2026-01-01T00:00:00Z",
            "2026-01-01T00:20:00Z",
            file_count=10,
            interval_seconds=60,
            processing_margin_minutes=5,
        )
        self.assertEqual(request["configuration"]["interval"], 5)
        self.assertEqual(request["configuration"]["type"], "Cron")
        self.assertEqual(request["configuration"]["startDateTime"], "2026-01-01T00:00:00Z")
        self.assertEqual(request["configuration"]["endDateTime"], "2026-01-01T00:20:00Z")
        with self.assertRaisesRegex(ValueError, "last delivery"):
            build_refresh_schedule_request(
                "2026-01-01T00:00:00Z",
                "2026-01-01T00:18:00Z",
                file_count=10,
                interval_seconds=60,
                processing_margin_minutes=5,
            )
        with self.assertRaisesRegex(ValueError, "at least five"):
            build_refresh_schedule_request(
                "2026-01-01T00:00:00Z",
                "2026-01-01T00:20:00Z",
                file_count=1,
                interval_seconds=60,
                processing_margin_minutes=0,
                refresh_interval_minutes=3,
            )


if __name__ == "__main__":
    unittest.main()
