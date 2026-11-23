import contextlib
import importlib.machinery
import importlib.util
import io
import json
from pathlib import Path
import tempfile
import unittest


SCRIPT = Path(__file__).with_name("deploy-demos")
loader = importlib.machinery.SourceFileLoader("deploy_demos", str(SCRIPT))
spec = importlib.util.spec_from_loader(loader.name, loader)
deploy = importlib.util.module_from_spec(spec)
loader.exec_module(deploy)

FAKE_TRACK = """
import argparse, json, pathlib, sys
p = argparse.ArgumentParser()
for option in ('mode', 'config', 'prefix', 'state-dir', 'result'):
    p.add_argument('--' + option, required=True)
p.add_argument('--yes', action='store_true')
p.add_argument('--selected-demos')
a = p.parse_args()
config = json.loads(pathlib.Path(a.config).read_text())
track = TRACK
behavior = config.get('behavior', {}).get(track + ':' + a.mode, {})
with pathlib.Path(config['audit']).open('a') as f:
    f.write(json.dumps({'track': track, 'mode': a.mode, 'yes': a.yes, 'selected': a.selected_demos}) + '\\n')
if behavior.get('omit_result'):
    sys.exit(0)
status = behavior.get('status', 'pass')
result = {'contract_version': 1, 'track': track, 'mode': a.mode, 'status': status,
          'ready': behavior.get('ready', a.mode in ('deploy', 'validate-only')),
          'diagnostics': [] if status == 'pass' else ['not ready']}
pathlib.Path(a.result).write_text(json.dumps(result))
sys.exit(0 if status == 'pass' else 1)
"""


class OrchestrationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.previous_tracks = deploy.TRACKS
        self.addCleanup(setattr, deploy, "TRACKS", self.previous_tracks)
        deploy.TRACKS = {}
        for track in ("demo-1", "demo-2", "demo-3", "shared"):
            script = self.root / (track + ".py")
            script.write_text("TRACK = " + repr(track) + "\n" + FAKE_TRACK, encoding="utf-8")
            deploy.TRACKS[track] = script
        self.audit = self.root / "audit.jsonl"
        self.config = {
            "contract_version": 1, "prefix": "chanel", "audit": str(self.audit),
            "execution": {"max_concurrency": 4, "track_timeout_seconds": 10},
            "data": {"run_id": "run-1", "file_count": 2, "rows_per_file": 2,
                     "interval_seconds": 1, "max_duration_seconds": 10, "currency": "USD"},
        }

    def run_cli(self, *args):
        config_path = self.root / "config.json"
        config_path.write_text(json.dumps(self.config), encoding="utf-8")
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            code = deploy.main([*args, "--config", str(config_path),
                                "--state-dir", str(self.root / "state")])
        states = list((self.root / "state").glob("run-*")) if (self.root / "state").exists() else []
        summary = json.loads((states[-1] / "summary.json").read_text()) if states else None
        events = [json.loads(line) for line in self.audit.read_text().splitlines()] if self.audit.exists() else []
        return code, summary, events

    def test_subset_does_not_run_other_tracks_and_does_not_start_data(self):
        code, summary, events = self.run_cli("demo-1,demo-3", "--deploy")
        self.assertEqual(code, 0)
        self.assertEqual(set(summary["phases"]["deploy"]), {"demo-1", "demo-3", "shared"})
        self.assertEqual({event["mode"] for event in events}, {"deploy"})

    def test_failure_does_not_mask_independent_track(self):
        self.config["behavior"] = {"demo-1:deploy": {"status": "blocked", "ready": False}}
        code, summary, events = self.run_cli("all", "--deploy", "--run-data")
        self.assertEqual(code, 1)
        self.assertEqual(summary["phases"]["deploy"]["demo-1"]["status"], "blocked")
        self.assertEqual(len(events), 4)
        self.assertNotIn("run-data", summary["phases"])

    def test_fresh_unready_validation_prevents_producer(self):
        self.config["behavior"] = {"demo-2:validate-only": {"ready": False}}
        code, summary, events = self.run_cli("all", "--deploy", "--run-data")
        self.assertEqual(code, 1)
        self.assertEqual(summary["phases"]["validate-only"]["demo-2"]["status"], "blocked")
        self.assertFalse(any(event["mode"] == "run-data" for event in events))

    def test_producer_starts_only_after_all_checks_with_selected_destinations(self):
        code, summary, events = self.run_cli("demo-1,demo-3", "--deploy", "--run-data")
        self.assertEqual(code, 0)
        self.assertEqual([event["mode"] for event in events][-1], "run-data")
        self.assertEqual(events[-1]["selected"], "demo-1,demo-3")
        self.assertEqual(summary["phases"]["run-data"]["shared"]["status"], "pass")

    def test_partial_cleanup_excludes_shared(self):
        code, _, events = self.run_cli("demo-2", "--cleanup", "--yes")
        self.assertEqual(code, 0)
        self.assertEqual([(event["track"], event["yes"]) for event in events], [("demo-2", True)])

    def test_cleanup_requires_confirmation(self):
        with self.assertRaises(SystemExit) as error:
            self.run_cli("all", "--cleanup")
        self.assertEqual(error.exception.code, 2)
        self.assertFalse(self.audit.exists())

    def test_invalid_or_missing_result_is_failure(self):
        self.config["behavior"] = {"demo-1:deploy": {"omit_result": True}}
        code, summary, _ = self.run_cli("demo-1", "--deploy")
        self.assertEqual(code, 1)
        self.assertEqual(summary["phases"]["deploy"]["demo-1"]["status"], "fail")

    def test_invalid_run_bound_prevents_any_invocation(self):
        self.config["data"]["max_duration_seconds"] = 1
        code, _, events = self.run_cli("demo-1", "--deploy", "--run-data")
        self.assertEqual(code, 2)
        self.assertEqual(events, [])

    def test_unsupported_currency_prevents_any_invocation(self):
        self.config["data"]["currency"] = "EUR"
        code, _, events = self.run_cli("demo-1", "--deploy")
        self.assertEqual(code, 2)
        self.assertEqual(events, [])

    def test_invalid_per_demo_timeout_prevents_any_invocation(self):
        self.config["demo-1"] = {"verification_timeout_seconds": 0}
        code, _, events = self.run_cli("demo-1", "--deploy")
        self.assertEqual(code, 2)
        self.assertEqual(events, [])

    def test_missing_entrypoint_is_blocked(self):
        deploy.TRACKS["demo-1"] = self.root / "missing.py"
        code, summary, events = self.run_cli("demo-1", "--dry-run")
        self.assertEqual(code, 1)
        self.assertEqual(summary["phases"]["dry-run"]["demo-1"]["status"], "blocked")
        self.assertEqual([event["track"] for event in events], ["shared"])


if __name__ == "__main__":
    unittest.main()
