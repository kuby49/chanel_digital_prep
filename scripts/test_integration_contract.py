"""Exercise integration between the actual shared and track entry points."""

import importlib.machinery
import importlib.util
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest import mock


ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "demo-data-generator"))
sys.path.insert(0, str(ROOT / "demo-1-materialized-lake-views"))
from generator import RunConfig, stage_batch
from manifest_validation import measured_batches, validate_manifest


def load_script(name, path):
    loader = importlib.machinery.SourceFileLoader(name, str(path))
    spec = importlib.util.spec_from_loader(name, loader)
    module = importlib.util.module_from_spec(spec)
    loader.exec_module(module)
    return module


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

    def test_orchestrator_resolves_real_producer_journal_without_cloud_calls(self):
        producer = load_script("producer_deploy_integration", ROOT / "demo-data-generator" / "deploy.py")
        orchestration = load_script("orchestration_integration", ROOT / "scripts" / "deploy-demos")
        config = {
            "contract_version": 1, "prefix": "sample",
            "tenant_id": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
            "capacity": {"id": "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"},
            "shared": {"workspace_name": "{prefix}-producer", "workspace_id": None,
                       "staging_lakehouse_id": None,
                       "identity_id": "cccccccc-cccc-4ccc-8ccc-cccccccccccc"},
        }
        workspace = "11111111-1111-4111-8111-111111111111"
        lakehouse = "22222222-2222-4222-8222-222222222222"
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            state_dir, invocation_dir = root / "state", root / "invocation"
            invocation_dir.mkdir()
            plan = producer.parse_config(config, "sample")
            state = producer.new_state(plan)
            state["deployment_id"] = "66666666-6666-4666-8666-666666666666"
            ids = {"workspace": workspace, "staging_lakehouse": lakehouse,
                   "environment_item": "77777777-7777-4777-8777-777777777777",
                   "notebook": "33333333-3333-4333-8333-333333333333",
                   "pipeline": "44444444-4444-4444-8444-444444444444"}
            for kind, resource_id in ids.items():
                state["resources"][kind] = producer._record(state, plan, kind, resource_id)
            producer.StateStore(state_dir).write(state)
            (invocation_dir / "input-config.json").write_text(json.dumps(config), encoding="utf-8")
            def resolve_target(tracks, mode, *args):
                self.assertEqual((tracks, mode), (["demo-1"], "resolve-target-candidate"))
                candidate = {"contract_version": 1, "track": "demo-1",
                             "tenant_id": config["tenant_id"], "prefix": "sample",
                             "target": {"workspace_id": "dddddddd-dddd-4ddd-8ddd-dddddddddddd",
                                        "item_id": "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee",
                                        "fact": "silver.sales_joined"}}
                (invocation_dir / "demo-1-target-candidate.json").write_text(
                    json.dumps(candidate), encoding="utf-8")
                return {"demo-1": orchestration.result("demo-1", mode, "pass", "candidate only")}
            with mock.patch.object(orchestration, "run_phase", side_effect=resolve_target):
                phases, resolved, path = orchestration.effective_run_config(
                    config, "sample", state_dir, invocation_dir, 30, ["demo-1"], 1)
            self.assertEqual(phases["resolve-candidate-ids"]["shared"]["status"], "pass",
                             (phases, (invocation_dir / "shared-resolve-candidate-ids.log").read_text()))
            self.assertFalse(phases["resolve-candidate-ids"]["shared"]["ready"])
            self.assertEqual(resolved["shared"]["workspace_id"], workspace)
            self.assertEqual(resolved["shared"]["staging_lakehouse_id"], lakehouse)
            self.assertEqual(json.loads(path.read_text(encoding="utf-8")), resolved)
            self.assertIsNone(config["shared"]["workspace_id"])


if __name__ == "__main__":
    unittest.main()
