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


SCRIPT = Path(__file__).with_name("deploy.py")
MANIFEST_FIXTURE = Path(__file__).with_name("testdata") / "producer-manifest-v1.json"


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
                code, result = self.invoke(mode, {"contract_version": 1, "prefix": "chanel"}, *extra)
                self.assertNotEqual(code, 0)
                self.assertEqual(result["status"], "blocked")
                self.assertFalse(result["ready"])
                self.assertTrue(any("header-only bootstrap" in item for item in result["diagnostics"]))
                self.assertFalse(any("empty-folder Bronze MLV creation failed" in item
                                     for item in result["diagnostics"]))

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
