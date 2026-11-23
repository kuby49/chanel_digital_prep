import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from manifest_validation import measured_batches, validate_manifest


SCRIPT = Path(__file__).with_name("deploy.py")


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

    def test_mutations_and_validation_remain_blocked(self):
        for mode, extra in (("deploy", ()), ("validate-only", ()), ("cleanup", ("--yes",))):
            with self.subTest(mode=mode):
                code, result = self.invoke(mode, {"contract_version": 1, "prefix": "chanel"}, *extra)
                self.assertNotEqual(code, 0)
                self.assertEqual(result["status"], "blocked")
                self.assertFalse(result["ready"])

    def test_cleanup_requires_confirmation_and_contract_must_match(self):
        for mode, config in (("cleanup", {"contract_version": 1, "prefix": "chanel"}),
                             ("dry-run", {"contract_version": 2, "prefix": "chanel"})):
            with self.subTest(mode=mode):
                code, result = self.invoke(mode, config)
                self.assertNotEqual(code, 0)
                self.assertEqual(result["status"], "fail")

    def test_manifest_contract_and_batch_class_filtering(self):
        base_batch = {
            "batch_id": "run-1-001", "batch_number": 1, "filename": "sales.csv",
            "sha256": "abc", "row_count": 1, "sales_amount_minor_units": 123,
            "grouped_totals": {}, "generated_at_utc": "2026-01-01T00:00:00Z",
            "deliveries": {},
        }
        manifest = {
            "contract_version": 1, "run_id": "run-1", "config": {},
            "destinations": [],
            "batches": [
                {**base_batch, "batch_class": "measured"},
                {**base_batch, "batch_id": "boot", "batch_class": "bootstrap"},
                {**base_batch, "batch_id": "bad", "batch_class": "negative-test"},
            ],
        }
        self.assertEqual(validate_manifest(manifest), [])
        self.assertEqual([b["batch_class"] for b in measured_batches(manifest)], ["measured"])

    def test_manifest_requires_supported_version_and_class(self):
        manifest = {
            "contract_version": 2, "run_id": "run-1", "config": {},
            "destinations": [], "batches": [{"batch_class": "unclassified"}],
        }
        diagnostics = validate_manifest(manifest)
        self.assertTrue(any("contract_version" in item for item in diagnostics))
        self.assertTrue(any("batch_class" in item for item in diagnostics))


if __name__ == "__main__":
    unittest.main()
