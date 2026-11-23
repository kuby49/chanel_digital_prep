import contextlib
import importlib.machinery
import importlib.util
import io
import json
from pathlib import Path
import tempfile
import threading
import unittest
from unittest import mock


SCRIPT = Path(__file__).with_name("deploy-demos")
loader = importlib.machinery.SourceFileLoader("deploy_demos", str(SCRIPT))
spec = importlib.util.spec_from_loader(loader.name, loader)
deploy = importlib.util.module_from_spec(spec)
loader.exec_module(deploy)

FAKE_TRACK = """
import argparse, datetime, json, pathlib, sys, time
p = argparse.ArgumentParser()
for option in ('mode', 'config', 'prefix', 'state-dir', 'result'):
    p.add_argument('--' + option, required=True)
p.add_argument('--yes', action='store_true')
p.add_argument('--selected-demos')
a = p.parse_args()
config = json.loads(pathlib.Path(a.config).read_text())
track = TRACK
behavior = config.get('behavior', {}).get(track + ':' + a.mode, {})
start_path = pathlib.Path(a.state_dir) / 'demo-2-monitor-start.json'
stop_job = json.loads(start_path.read_text())['job_id'] if a.mode == 'stop-monitor' and start_path.exists() else None
event_path = pathlib.Path(a.state_dir) / ('event-' + str(time.time_ns()) + '-' + track + '-' + a.mode + '.json')
event_path.write_text(json.dumps({'track': track, 'mode': a.mode, 'yes': a.yes, 'selected': a.selected_demos,
                                  'stop_job': stop_job}))
if behavior.get('delay'):
    time.sleep(behavior['delay'])
if behavior.get('omit_result'):
    sys.exit(0)
status = behavior.get('status', 'pass')
monitor_path = pathlib.Path(a.state_dir) / 'demo-2-monitor.json'
if track == 'demo-2' and a.mode in ('start-monitor', 'monitor-status') and status == 'pass':
    if a.mode == 'start-monitor':
        shared = config['shared']
        root = ('https://onelake.blob.fabric.microsoft.com/' + shared['workspace_id'] + '/' +
                shared['staging_lakehouse_id'] + '/Files/producer/runs/' + config['data']['run_id'])
        evidence = {'contract_version': 1, 'run_id': config['data']['run_id'], 'job_id': 'job-1',
                    'manifest_uri': root + '/manifest.json', 'state': 'submitted',
                    'deadline_utc': (datetime.datetime.now(datetime.timezone.utc) +
                                     datetime.timedelta(seconds=10)).isoformat()}
    else:
        evidence = json.loads(monitor_path.read_text())
        evidence['state'] = 'running'
    evidence.update(behavior.get('evidence', {}))
    if not behavior.get('omit_evidence'):
        monitor_path.write_text(json.dumps(evidence))
    if a.mode == 'start-monitor' and not behavior.get('omit_start_record'):
        start_path.write_text(json.dumps(evidence))
    if a.mode == 'monitor-status' and behavior.get('start_record_evidence'):
        original = json.loads(start_path.read_text())
        original.update(behavior['start_record_evidence'])
        start_path.write_text(json.dumps(original))
result = {'contract_version': 1, 'track': track, 'mode': a.mode, 'status': status,
          'ready': behavior.get('ready', a.mode in ('deploy', 'validate-only', 'monitor-status')),
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
        self.config = {
            "contract_version": 1, "prefix": "chanel",
            "execution": {"max_concurrency": 4, "track_timeout_seconds": 10,
                          "verification_timeout_seconds": 15},
            "data": {"run_id": "run-1", "file_count": 2, "rows_per_file": 2,
                     "interval_seconds": 1, "max_duration_seconds": 10, "currency": "USD"},
            "shared": {"workspace_id": "11111111-1111-4111-8111-111111111111",
                       "staging_lakehouse_id": "22222222-2222-4222-8222-222222222222",
                       "run_state_location": None},
        }

    def run_cli(self, *args):
        config_path = self.root / "config.json"
        config_path.write_text(json.dumps(self.config), encoding="utf-8")
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            code = deploy.main([*args, "--config", str(config_path),
                                "--state-dir", str(self.root / "state")])
        states = list((self.root / "state").glob("run-*")) if (self.root / "state").exists() else []
        latest = max(states, key=lambda state: (state / "summary.json").stat().st_mtime_ns) if states else None
        summary = json.loads((latest / "summary.json").read_text()) if latest else None
        events = [json.loads(path.read_text()) for path in sorted(latest.glob("event-*.json"))] if latest else []
        return code, summary, events

    def test_subset_does_not_run_other_tracks_and_does_not_start_data(self):
        code, summary, events = self.run_cli("demo-1,demo-3", "--deploy")
        self.assertEqual(code, 0)
        self.assertEqual(set(summary["phases"]["deploy"]), {"demo-1", "demo-3", "shared"})
        self.assertEqual({event["mode"] for event in events}, {"deploy"})

    def test_parallel_tracks_obey_concurrency_bound(self):
        guard = threading.Lock()
        barrier = threading.Barrier(2)
        active = 0
        peak = 0

        def fake_invoke(track, mode, *args):
            nonlocal active, peak
            with guard:
                active += 1
                peak = max(peak, active)
            barrier.wait(timeout=5)
            with guard:
                active -= 1
            return deploy.result(track, mode, "blocked", "test")

        with mock.patch.object(deploy, "invoke", side_effect=fake_invoke):
            outcomes = deploy.run_phase(list(deploy.TRACKS), "dry-run", self.root / "config",
                                        "chanel", self.root, 10, ["demo-1"], 2, False)
        self.assertEqual(len(outcomes), 4)
        self.assertEqual(peak, 2)

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

    def test_demo2_monitor_is_ready_before_selected_producer_starts(self):
        code, summary, events = self.run_cli("demo-2,demo-3", "--deploy", "--run-data")
        self.assertEqual(code, 0)
        modes = [event["mode"] for event in events]
        self.assertEqual(modes[-3:], ["start-monitor", "monitor-status", "run-data"])
        self.assertTrue(all(mode == "validate-only" for mode in modes[3:-3]))
        self.assertEqual(events[-1]["selected"], "demo-2,demo-3")
        self.assertEqual(events[-2]["selected"], "demo-2,demo-3")
        self.assertTrue(summary["phases"]["monitor-status"]["demo-2"]["ready"])

    def test_failed_monitor_start_attempts_stop_and_blocks_producer(self):
        self.config["behavior"] = {"demo-2:start-monitor": {"status": "blocked", "ready": False}}
        code, summary, events = self.run_cli("demo-2", "--deploy", "--run-data")
        self.assertEqual(code, 1)
        self.assertEqual([event["mode"] for event in events][-2:], ["start-monitor", "stop-monitor"])
        self.assertEqual(summary["phases"]["start-monitor"]["demo-2"]["status"], "blocked")
        self.assertNotIn("run-data", summary["phases"])

    def test_failed_monitor_stop_is_not_hidden(self):
        self.config["behavior"] = {
            "demo-2:start-monitor": {"status": "blocked", "ready": False},
            "demo-2:stop-monitor": {"status": "fail", "ready": False},
        }
        code, summary, _ = self.run_cli("demo-2", "--deploy", "--run-data")
        self.assertEqual(code, 1)
        self.assertEqual(summary["phases"]["stop-monitor"]["demo-2"]["status"], "fail")

    def test_wrong_monitor_evidence_blocks_producer_and_attempts_stop(self):
        for evidence in ({"run_id": "another-run"}, {"job_id": "different-job"},
                         {"manifest_uri": "https://example.invalid/manifest.json"},
                         {"deadline_utc": "2100-01-01T00:00:00Z"},
                         {"state": "not-running"}):
            with self.subTest(evidence=evidence):
                self.config["behavior"] = {"demo-2:monitor-status": {"evidence": evidence}}
                code, summary, events = self.run_cli("demo-2", "--deploy", "--run-data")
                self.assertEqual(code, 1)
                self.assertEqual(summary["phases"]["monitor-status"]["demo-2"]["status"], "fail")
                self.assertNotIn("run-data", summary["phases"])
                self.assertEqual(events[-1]["mode"], "stop-monitor")
                self.assertEqual(events[-1]["stop_job"], "job-1")

    def test_monitor_must_report_fresh_readiness(self):
        self.config["behavior"] = {"demo-2:monitor-status": {"ready": False}}
        code, summary, events = self.run_cli("demo-2", "--deploy", "--run-data")
        self.assertEqual(code, 1)
        self.assertEqual(summary["phases"]["monitor-status"]["demo-2"]["status"], "blocked")
        self.assertEqual(events[-1]["mode"], "stop-monitor")

    def test_missing_start_evidence_attempts_stop(self):
        self.config["behavior"] = {"demo-2:start-monitor": {"omit_evidence": True}}
        code, summary, events = self.run_cli("demo-2", "--deploy", "--run-data")
        self.assertEqual(code, 1)
        self.assertEqual(summary["phases"]["start-monitor"]["demo-2"]["status"], "fail")
        self.assertEqual(events[-1]["mode"], "stop-monitor")
        self.assertNotIn("run-data", summary["phases"])

    def test_missing_original_job_record_blocks_producer(self):
        self.config["behavior"] = {"demo-2:start-monitor": {"omit_start_record": True}}
        code, summary, events = self.run_cli("demo-2", "--deploy", "--run-data")
        self.assertEqual(code, 1)
        self.assertEqual(summary["phases"]["start-monitor"]["demo-2"]["status"], "fail")
        self.assertEqual(events[-1]["mode"], "stop-monitor")
        self.assertIsNone(events[-1]["stop_job"])
        self.assertNotIn("run-data", summary["phases"])

    def test_changed_original_job_record_blocks_producer(self):
        self.config["behavior"] = {"demo-2:monitor-status": {
            "start_record_evidence": {"job_id": "different-job"}}}
        code, summary, events = self.run_cli("demo-2", "--deploy", "--run-data")
        self.assertEqual(code, 1)
        self.assertEqual(summary["phases"]["monitor-status"]["demo-2"]["status"], "fail")
        self.assertEqual(summary["phases"]["stop-monitor"]["demo-2"]["status"], "fail")
        self.assertEqual(events[-1]["mode"], "monitor-status")
        self.assertNotIn("run-data", summary["phases"])

    def test_monitor_start_timeout_attempts_stop_before_producer(self):
        self.config["execution"]["track_timeout_seconds"] = 1
        self.config["behavior"] = {"demo-2:start-monitor": {"delay": 3}}
        code, summary, events = self.run_cli("demo-2", "--deploy", "--run-data")
        self.assertEqual(code, 1)
        self.assertEqual(summary["phases"]["start-monitor"]["demo-2"]["status"], "fail")
        self.assertEqual(events[-1]["mode"], "stop-monitor")
        self.assertNotIn("run-data", summary["phases"])

    def test_missing_status_result_attempts_stop(self):
        self.config["behavior"] = {"demo-2:monitor-status": {"omit_result": True}}
        code, summary, events = self.run_cli("demo-2", "--deploy", "--run-data")
        self.assertEqual(code, 1)
        self.assertEqual(summary["phases"]["monitor-status"]["demo-2"]["status"], "fail")
        self.assertEqual(events[-1]["mode"], "stop-monitor")
        self.assertNotIn("run-data", summary["phases"])

    def test_expired_or_naive_monitor_deadlines_fail_closed(self):
        for deadline in ("2000-01-01T00:00:00Z", "2100-01-01T00:00:00"):
            with self.subTest(deadline=deadline):
                self.config["behavior"] = {"demo-2:start-monitor": {"evidence": {"deadline_utc": deadline}}}
                code, summary, events = self.run_cli("demo-2", "--deploy", "--run-data")
                self.assertEqual(code, 1)
                self.assertEqual(summary["phases"]["start-monitor"]["demo-2"]["status"], "fail")
                self.assertEqual(events[-1]["mode"], "stop-monitor")
                self.assertNotIn("run-data", summary["phases"])

    def test_uncertain_producer_failure_does_not_claim_monitor_or_producer_stopped(self):
        self.config["behavior"] = {"shared:run-data": {"status": "fail", "ready": False}}
        code, summary, events = self.run_cli("demo-2", "--deploy", "--run-data")
        self.assertEqual(code, 1)
        self.assertEqual(events[-1]["mode"], "run-data")
        self.assertNotIn("stop-monitor", summary["phases"])

    def test_demo2_manifest_config_mismatch_blocks_before_deploy(self):
        self.config["shared"]["run_state_location"] = "https://example.invalid/other-root"
        code, _, events = self.run_cli("demo-2", "--deploy", "--run-data")
        self.assertEqual(code, 2)
        self.assertEqual(events, [])

    def test_monitor_uses_default_timeout_when_demo_override_is_null(self):
        self.config["demo-2"] = {"verification_timeout_seconds": None}
        _, duration = deploy.monitor_config(self.config)
        self.assertEqual(duration, 45)
        self.config["demo-2"]["verification_timeout_seconds"] = 30
        _, duration = deploy.monitor_config(self.config)
        self.assertEqual(duration, 60)

    def test_partial_cleanup_excludes_shared(self):
        code, _, events = self.run_cli("demo-2", "--cleanup", "--yes")
        self.assertEqual(code, 0)
        self.assertEqual([(event["track"], event["yes"]) for event in events], [("demo-2", True)])

    def test_cleanup_requires_confirmation(self):
        with self.assertRaises(SystemExit) as error:
            self.run_cli("all", "--cleanup")
        self.assertEqual(error.exception.code, 2)
        self.assertFalse((self.root / "state").exists())

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
