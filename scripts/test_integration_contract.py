"""Verify that Demo 1 consumes the actual producer manifest, not a fixture."""

import json
from pathlib import Path
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "demo-data-generator"))
sys.path.insert(0, str(ROOT / "demo-1-materialized-lake-views"))
from generator import RunConfig, stage_batch
from manifest_validation import measured_batches, validate_manifest


class SharedManifestIntegrationTest(unittest.TestCase):
    def test_demo1_accepts_staged_manifest_and_selects_measured_batch(self):
        with tempfile.TemporaryDirectory() as root:
            config = RunConfig(run_id="integration", file_count=1, rows_per_file=2)
            entry = stage_batch(root, config, 1, ["demo-1"])
            manifest = json.loads(
                (Path(root) / config.run_id / "manifest.json").read_text(encoding="utf-8")
            )
            self.assertEqual(validate_manifest(manifest), [])
            self.assertEqual(measured_batches(manifest), [entry])


if __name__ == "__main__":
    unittest.main()
