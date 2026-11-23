import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


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


if __name__ == "__main__":
    unittest.main()
