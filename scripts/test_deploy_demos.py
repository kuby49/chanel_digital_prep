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

TARGETS = {
    "demo-1": {"workspace_id": "44444444-4444-4444-8444-444444444444",
               "item_id": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", "fact": "silver.sales_joined"},
    "demo-2": {"workspace_id": "55555555-5555-4555-8555-555555555555",
               "item_id": "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb", "fact": "FactSales"},
    "demo-3": {"workspace_id": "66666666-6666-4666-8666-666666666666",
               "item_id": "cccccccc-cccc-4ccc-8ccc-cccccccccccc", "fact": "dbo.factsales"},
}

FAKE_TRACK = """
import argparse, datetime, hashlib, json, pathlib, sys, time
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
event_path = pathlib.Path(a.result).parent / ('event-' + str(time.time_ns()) + '-' + track + '-' + a.mode + '.json')
event_path.write_text(json.dumps({'track': track, 'mode': a.mode, 'yes': a.yes, 'selected': a.selected_demos,
                                  'stop_job': stop_job, 'state_dir': a.state_dir, 'config_path': a.config,
                                  'shared': config.get('shared'), 'prefix': config.get('prefix'),
                                  'common_prefix': config.get('common', {}).get('prefix'),
                                  'argument_prefix': a.prefix}))
if behavior.get('delay'):
    time.sleep(behavior['delay'])
if behavior.get('omit_result'):
    sys.exit(0)
status = behavior.get('status', 'pass')
if track == 'shared' and a.mode == 'resolve-candidate-ids' and status == 'pass' and not behavior.get('omit_candidate'):
    candidate = behavior.get('candidate', {'version': 1,
        'workspace_id': '11111111-1111-4111-8111-111111111111',
        'staging_lakehouse_id': '22222222-2222-4222-8222-222222222222'})
    (pathlib.Path(a.result).parent / 'shared-producer-candidate-ids.json').write_text(json.dumps(candidate))
if track != 'shared' and a.mode == 'resolve-target-candidate' and status == 'pass' and not behavior.get('omit_candidate'):
    targets = {
        'demo-1': {'workspace_id': '44444444-4444-4444-8444-444444444444',
                   'item_id': 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa', 'fact': 'silver.sales_joined'},
        'demo-2': {'workspace_id': '55555555-5555-4555-8555-555555555555',
                   'item_id': 'bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb', 'fact': 'FactSales'},
        'demo-3': {'workspace_id': '66666666-6666-4666-8666-666666666666',
                   'item_id': 'cccccccc-cccc-4ccc-8ccc-cccccccccccc', 'fact': 'dbo.factsales'}}
    candidate = behavior.get('candidate', {'contract_version': 1, 'track': track,
        'tenant_id': config['tenant_id'], 'prefix': a.prefix, 'target': targets[track]})
    sidecar = pathlib.Path(a.result).parent / (track + '-target-candidate.json')
    sidecar.write_text(behavior.get('raw_candidate', json.dumps(candidate)))
monitor_path = pathlib.Path(a.state_dir) / 'demo-2-monitor.json'
if track == 'demo-2' and a.mode in ('start-monitor', 'monitor-status', 'wait-monitor') and status == 'pass':
    if a.mode == 'start-monitor':
        shared = config['shared']
        root = ('https://onelake.blob.fabric.microsoft.com/' + shared['workspace_id'] + '/' +
                shared['staging_lakehouse_id'] + '/Files/producer/runs/' + config['data']['run_id'])
        data = config['data']
        producer_config = {'run_id': data.get('run_id'), 'batch_class': 'measured', 'start_date': data.get('start_date'),
                           'seed': data.get('seed'), 'date_count': data.get('date_count'),
                           'products': data.get('product_count'), 'customers': data.get('customer_count'),
                           'file_count': data.get('file_count'), 'rows_per_file': data.get('rows_per_file'),
                           'interval_seconds': data.get('interval_seconds')}
        source = (pathlib.Path.cwd() / 'demo-data-generator' / 'generator.py').read_bytes().decode('utf-8')
        generator_digest = hashlib.sha256(source.replace('\\r\\n', '\\n').replace('\\r', '\\n').encode('utf-8')).hexdigest()
        binding = {'targets': shared['target_bindings'], 'generator_sha256': generator_digest,
                   'config': producer_config}
        binding_digest = hashlib.sha256(json.dumps(binding, sort_keys=True, separators=(',', ':'),
                                                   ensure_ascii=False, allow_nan=False).encode('utf-8')).hexdigest()
        evidence = {'contract_version': 1, 'run_id': config['data']['run_id'], 'job_id': 'job-1',
                    'manifest_uri': root + '/manifest.json', 'state': 'submitted',
                    'target_bindings': shared['target_bindings'], 'generator_sha256': generator_digest,
                    'producer_config': producer_config, 'monitor_binding_sha256': binding_digest,
                    'deadline_utc': (datetime.datetime.now(datetime.timezone.utc) +
                                     datetime.timedelta(seconds=10)).isoformat()}
    else:
        evidence = json.loads(monitor_path.read_text())
        evidence['state'] = 'completed' if a.mode == 'wait-monitor' else 'running'
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
          'ready': behavior.get('ready', a.mode in ('deploy', 'validate-only', 'monitor-status', 'wait-monitor')),
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
            "tenant_id": "33333333-3333-4333-8333-333333333333",
            "execution": {"max_concurrency": 4, "track_timeout_seconds": 10,
                          "verification_timeout_seconds": 15},
            "data": {"run_id": "run-1", "seed": 42, "start_date": "2026-01-01",
                     "date_count": 365, "product_count": 20, "customer_count": 100,
                     "file_count": 2, "rows_per_file": 2,
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
        state_root = self.root / "state"
        states = ([run for scoped in state_root.iterdir() if scoped.is_dir()
                   for run in (scoped / "runs").glob("run-*") if (run / "summary.json").exists()]
                  if state_root.exists() else [])
        latest = max(states, key=lambda state: (state / "summary.json").stat().st_mtime_ns) if states else None
        summary = json.loads((latest / "summary.json").read_text()) if latest else None
        events = [json.loads(path.read_text()) for path in sorted(latest.glob("event-*.json"))] if latest else []
        return code, summary, events

    def test_subset_does_not_run_other_tracks_and_does_not_start_data(self):
        code, summary, events = self.run_cli("demo-1,demo-3", "--deploy")
        self.assertEqual(code, 0)
        self.assertEqual(set(summary["phases"]["deploy"]), {"demo-1", "demo-3", "shared"})
        self.assertEqual({event["mode"] for event in events}, {"deploy"})

    def test_demo2_owned_scope_is_used_for_candidates_and_monitor(self):
        owned = self.root / "demo2-owned"
        owned.mkdir()
        self.config["demo-2"] = {
            "deployment_prefix": "demo2synthetic-123456abcdef",
            "deployment_state_dir": str(owned),
        }
        code, summary, events = self.run_cli("demo-2,demo-3", "--deploy", "--run-data")
        self.assertEqual(code, 0, summary)
        demo2_events = [event for event in events if event["track"] == "demo-2"]
        self.assertTrue(any(event["mode"] == "resolve-target-candidate" for event in demo2_events))
        self.assertTrue(any(event["mode"] == "wait-monitor" for event in demo2_events))
        self.assertTrue(all(event["state_dir"] == str(owned) and
                            event["argument_prefix"] == "demo2synthetic-123456abcdef"
                            for event in demo2_events))
        self.assertTrue(all(event["state_dir"] != str(owned) and event["argument_prefix"] == "chanel"
                            for event in events if event["track"] != "demo-2"))

    def test_demo2_monitor_failure_stops_job_in_owned_scope(self):
        owned = self.root / "demo2-owned"
        owned.mkdir()
        self.config["demo-2"] = {
            "deployment_prefix": "demo2synthetic-123456abcdef",
            "deployment_state_dir": str(owned),
        }
        self.config["behavior"] = {"demo-2:monitor-status": {"status": "blocked", "ready": False}}
        code, summary, events = self.run_cli("demo-2", "--deploy", "--run-data")
        self.assertEqual(code, 1)
        self.assertNotIn("run-data", summary["phases"])
        self.assertEqual(summary["phases"]["stop-monitor"]["demo-2"]["status"], "pass")
        stop = next(event for event in events if event["mode"] == "stop-monitor")
        self.assertEqual((stop["state_dir"], stop["argument_prefix"], stop["stop_job"]),
                         (str(owned), "demo2synthetic-123456abcdef", "job-1"))

    def test_demo2_scope_requires_both_valid_fields_and_existing_absolute_directory(self):
        for settings in ({"deployment_prefix": "owned"},
                         {"deployment_state_dir": str(self.root)},
                         {"deployment_prefix": "owned", "deployment_state_dir": "relative"},
                         {"deployment_prefix": "../unsafe", "deployment_state_dir": str(self.root)},
                         {"deployment_prefix": "owned", "deployment_state_dir": str(self.root / "missing")}):
            with self.subTest(settings=settings):
                self.config["demo-2"] = settings
                code, summary, events = self.run_cli("demo-2", "--deploy")
                self.assertEqual(code, 2)
                self.assertIsNone(summary)
                self.assertEqual(events, [])

    def test_reruns_reuse_durable_state_but_not_results(self):
        code, _, first = self.run_cli("demo-1", "--deploy")
        self.assertEqual(code, 0)
        state_dir = Path(first[0]["state_dir"])
        marker = state_dir / "shared-producer-v1.json"
        marker.write_text('{"owned":"original"}', encoding="utf-8")
        code, _, second = self.run_cli("demo-1", "--validate-only")
        self.assertEqual(code, 0)
        self.assertEqual({event["state_dir"] for event in first + second}, {str(state_dir)})
        self.assertEqual(json.loads(marker.read_text()), {"owned": "original"})
        self.assertEqual(len(list((state_dir / "runs").glob("run-*"))), 2)

    def test_different_tenants_use_separate_state(self):
        code, _, first = self.run_cli("demo-1", "--deploy")
        self.assertEqual(code, 0)
        self.config["tenant_id"] = "44444444-4444-4444-8444-444444444444"
        code, _, second = self.run_cli("demo-1", "--deploy")
        self.assertEqual(code, 0)
        self.assertNotEqual(first[0]["state_dir"], second[0]["state_dir"])

    def test_prefix_override_is_applied_to_snapshot_for_all_phases(self):
        self.config["common"] = {"prefix": "chanel"}
        self.config["shared"]["workspace_id"] = None
        code, _, events = self.run_cli("demo-2", "--deploy", "--run-data", "--prefix", "alternate")
        self.assertEqual(code, 0)
        self.assertTrue(all(event["prefix"] == event["common_prefix"] == event["argument_prefix"] == "alternate"
                            for event in events))
        original = json.loads((self.root / "config.json").read_text())
        self.assertEqual(original["prefix"], "chanel")
        self.assertEqual(original["common"]["prefix"], "chanel")

    def test_external_config_change_does_not_change_this_invocation(self):
        run_phase = deploy.run_phase

        def mutate_original(*args, **kwargs):
            if args[1] == "deploy":
                changed = dict(self.config)
                changed["data"] = dict(self.config["data"], run_id="different-run")
                (self.root / "config.json").write_text(json.dumps(changed), encoding="utf-8")
            return run_phase(*args, **kwargs)

        with mock.patch.object(deploy, "run_phase", side_effect=mutate_original):
            code, summary, events = self.run_cli("demo-2", "--deploy", "--run-data")
        self.assertEqual(code, 0)
        self.assertEqual(summary["phases"]["run-data"]["shared"]["status"], "pass")
        run_event = next(event for event in events if event["mode"] == "run-data")
        self.assertEqual(json.loads(Path(run_event["config_path"]).read_text())["data"]["run_id"], "run-1")

    def test_changed_effective_config_after_validation_blocks_producer(self):
        run_phase = deploy.run_phase

        def mutate_snapshot(*args, **kwargs):
            outcomes = run_phase(*args, **kwargs)
            if args[1] == "validate-only":
                path = args[2]
                changed = json.loads(path.read_text(encoding="utf-8"))
                changed["data"]["run_id"] = "different-run"
                path.write_text(json.dumps(changed), encoding="utf-8")
            return outcomes

        with mock.patch.object(deploy, "run_phase", side_effect=mutate_snapshot):
            code, summary, events = self.run_cli("demo-1", "--deploy", "--run-data")
        self.assertEqual(code, 1)
        self.assertNotIn("run-data", summary["phases"])
        self.assertNotIn("run-data", [event["mode"] for event in events])

    def test_changed_effective_config_after_monitor_status_stops_exact_monitor(self):
        invoke = deploy.invoke

        def mutate_snapshot(*args, **kwargs):
            outcome = invoke(*args, **kwargs)
            if args[:2] == ("demo-2", "monitor-status"):
                path = args[2]
                changed = json.loads(path.read_text(encoding="utf-8"))
                changed["data"]["run_id"] = "different-run"
                path.write_text(json.dumps(changed), encoding="utf-8")
            return outcome

        with mock.patch.object(deploy, "invoke", side_effect=mutate_snapshot):
            code, summary, events = self.run_cli("demo-2", "--deploy", "--run-data")
        self.assertEqual(code, 1)
        self.assertNotIn("run-data", summary["phases"])
        self.assertEqual(events[-1]["mode"], "stop-monitor")
        self.assertEqual(events[-1]["stop_job"], "job-1")
        stop_config = json.loads(Path(events[-1]["config_path"]).read_text(encoding="utf-8"))
        self.assertEqual(stop_config["data"]["run_id"], "run-1")

    def test_changed_candidate_config_blocks_resolution(self):
        run_phase = deploy.run_phase

        def mutate_snapshot(*args, **kwargs):
            outcomes = run_phase(*args, **kwargs)
            if args[1] == "resolve-target-candidate":
                path = args[2]
                changed = json.loads(path.read_text(encoding="utf-8"))
                changed["data"]["run_id"] = "different-run"
                path.write_text(json.dumps(changed), encoding="utf-8")
            return outcomes

        with mock.patch.object(deploy, "run_phase", side_effect=mutate_snapshot):
            code, summary, events = self.run_cli("demo-1", "--deploy", "--run-data")
        self.assertEqual(code, 1)
        self.assertEqual(summary["phases"]["resolve-shared-config"]["shared"]["status"], "blocked")
        self.assertNotIn("validate-only", summary["phases"])
        self.assertNotIn("run-data", [event["mode"] for event in events])

    def test_changed_config_during_producer_marks_outcome_unconfirmed(self):
        invoke = deploy.invoke

        def mutate_snapshot(*args, **kwargs):
            outcome = invoke(*args, **kwargs)
            if args[:2] == ("shared", "run-data"):
                path = args[2]
                changed = json.loads(path.read_text(encoding="utf-8"))
                changed["data"]["run_id"] = "different-run"
                path.write_text(json.dumps(changed), encoding="utf-8")
            return outcome

        with mock.patch.object(deploy, "invoke", side_effect=mutate_snapshot):
            code, summary, _ = self.run_cli("demo-1", "--deploy", "--run-data")
        self.assertEqual(code, 1)
        outcome = summary["phases"]["run-data"]["shared"]
        self.assertEqual(outcome["status"], "fail")
        self.assertIn("unconfirmed", outcome["diagnostics"][0])

    def test_existing_orchestrator_lock_blocks_without_clearing_it(self):
        state = self.root / "state" / ("chanel-" + self.config["tenant_id"])
        state.mkdir(parents=True)
        lock = state / ".orchestrator-v1.lock"
        lock.write_text("other-owner", encoding="utf-8")
        code, _, events = self.run_cli("demo-1", "--deploy")
        self.assertEqual(code, 1)
        self.assertEqual(events, [])
        self.assertEqual(lock.read_text(), "other-owner")

    def test_lock_excludes_other_invocations_and_releases(self):
        state = self.root / "lock-test"
        state.mkdir()
        lock = state / ".orchestrator-v1.lock"
        with deploy.deployment_lock(state):
            self.assertTrue(lock.exists())
            with self.assertRaisesRegex(ValueError, "locked"):
                with deploy.deployment_lock(state):
                    self.fail("overlapping invocation acquired the lock")
        self.assertFalse(lock.exists())

    def test_replaced_lock_is_not_removed(self):
        state = self.root / "lock-test"
        state.mkdir()
        lock = state / ".orchestrator-v1.lock"
        with self.assertRaisesRegex(ValueError, "lock changed"):
            with deploy.deployment_lock(state):
                lock.write_text("other-owner", encoding="utf-8")
        self.assertEqual(lock.read_text(), "other-owner")

    def test_legacy_journal_blocks_without_adoption(self):
        legacy = self.root / "state" / "run-legacy"
        legacy.mkdir(parents=True)
        journal = legacy / "shared-producer-v1.json"
        journal.write_text('{"unverified":true}', encoding="utf-8")
        code, _, events = self.run_cli("demo-1", "--deploy")
        self.assertEqual(code, 2)
        self.assertEqual(events, [])
        self.assertEqual(json.loads(journal.read_text()), {"unverified": True})

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
                                        "chanel", self.root, self.root, 10, ["demo-1"], 2, False)
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
        self.assertEqual(modes[-4:], ["start-monitor", "monitor-status", "run-data", "wait-monitor"])
        self.assertLess(max(index for index, mode in enumerate(modes) if mode == "resolve-target-candidate"),
                        min(index for index, mode in enumerate(modes) if mode == "validate-only"))
        self.assertEqual(events[-2]["selected"], "demo-2,demo-3")
        self.assertEqual(events[-3]["selected"], "demo-2,demo-3")
        self.assertTrue(summary["phases"]["monitor-status"]["demo-2"]["ready"])
        self.assertTrue(summary["phases"]["wait-monitor"]["demo-2"]["ready"])

    def test_missing_ids_resolved_once_before_fresh_validation_and_monitor(self):
        self.config["shared"]["workspace_id"] = None
        self.config["shared"]["staging_lakehouse_id"] = None
        self.config["shared"]["identity_id"] = "55555555-5555-4555-8555-555555555555"
        self.config["data"]["seed"] = 42
        self.config["demo-2"] = {"verification_batch": "bootstrap", "query_endpoint": "example"}
        code, summary, events = self.run_cli("demo-2,demo-3", "--deploy", "--run-data")
        self.assertEqual(code, 0)
        self.assertEqual(summary["phases"]["resolve-candidate-ids"]["shared"]["status"], "pass")
        modes = [event["mode"] for event in events]
        self.assertLess(modes.index("resolve-candidate-ids"), modes.index("validate-only"))
        after = [event for event in events if event["mode"] in
                 ("validate-only", "start-monitor", "monitor-status", "run-data", "wait-monitor")]
        self.assertEqual(len({event["config_path"] for event in after}), 1)
        self.assertTrue(all(event["shared"]["workspace_id"] == "11111111-1111-4111-8111-111111111111"
                            and event["shared"]["staging_lakehouse_id"] == "22222222-2222-4222-8222-222222222222"
                            for event in after))
        effective = json.loads(Path(after[0]["config_path"]).read_text())
        expected = json.loads(json.dumps(self.config))
        expected["shared"]["workspace_id"] = "11111111-1111-4111-8111-111111111111"
        expected["shared"]["staging_lakehouse_id"] = "22222222-2222-4222-8222-222222222222"
        expected["shared"]["target_bindings"] = {track: TARGETS[track] for track in ("demo-2", "demo-3")}
        self.assertEqual(effective, expected)
        self.assertIsNone(self.config["shared"]["workspace_id"])
        self.assertIsNone(json.loads((self.root / "config.json").read_text())["shared"]["workspace_id"])

    def test_explicit_ids_do_not_require_producer_journal(self):
        code, _, events = self.run_cli("demo-1", "--deploy", "--run-data")
        self.assertEqual(code, 0)
        self.assertNotIn("resolve-candidate-ids", [event["mode"] for event in events])
        self.assertEqual(len({event["config_path"] for event in events
                              if event["mode"] not in ("deploy", "resolve-target-candidate")}), 1)

    def test_candidate_conflict_and_missing_sidecar_block_before_validation(self):
        for behavior in (
            {"candidate": {"version": 1, "workspace_id": "44444444-4444-4444-8444-444444444444",
                           "staging_lakehouse_id": "22222222-2222-4222-8222-222222222222"}},
            {"omit_candidate": True},
            {"candidate": {"version": True, "workspace_id": "11111111-1111-4111-8111-111111111111",
                           "staging_lakehouse_id": "22222222-2222-4222-8222-222222222222"}},
            {"status": "blocked"},
        ):
            with self.subTest(behavior=behavior):
                self.config["shared"]["staging_lakehouse_id"] = None
                self.config["behavior"] = {"shared:resolve-candidate-ids": behavior}
                code, summary, events = self.run_cli("demo-2", "--deploy", "--run-data")
                self.assertEqual(code, 1)
                self.assertNotIn("validate-only", summary["phases"])
                self.assertNotIn("run-data", summary["phases"])
                self.assertNotIn("start-monitor", [event["mode"] for event in events])

    def test_resolved_manifest_root_mismatch_blocks_before_validation(self):
        self.config["shared"]["workspace_id"] = None
        self.config["shared"]["run_state_location"] = "https://example.invalid/other-root"
        code, summary, events = self.run_cli("demo-2", "--deploy", "--run-data")
        self.assertEqual(code, 1)
        self.assertEqual(summary["phases"]["resolve-shared-config"]["shared"]["status"], "blocked")
        self.assertNotIn("validate-only", [event["mode"] for event in events])

    def test_prior_invocation_candidate_cannot_be_reused(self):
        self.config["shared"]["workspace_id"] = None
        code, _, _ = self.run_cli("demo-1", "--deploy", "--run-data")
        self.assertEqual(code, 0)
        self.config["behavior"] = {"shared:resolve-candidate-ids": {"omit_candidate": True}}
        code, summary, events = self.run_cli("demo-1", "--deploy", "--run-data")
        self.assertEqual(code, 1)
        self.assertEqual(summary["phases"]["resolve-shared-config"]["shared"]["status"], "blocked")
        self.assertNotIn("validate-only", [event["mode"] for event in events])

    def test_resolved_config_still_requires_fresh_readiness(self):
        self.config["shared"]["workspace_id"] = None
        self.config["behavior"] = {"shared:validate-only": {"status": "blocked", "ready": False}}
        code, summary, events = self.run_cli("demo-2", "--deploy", "--run-data")
        self.assertEqual(code, 1)
        self.assertEqual(summary["phases"]["validate-only"]["shared"]["status"], "blocked")
        self.assertNotIn("start-monitor", [event["mode"] for event in events])
        self.assertNotIn("run-data", summary["phases"])

    def test_selected_targets_bind_to_every_validation_and_monitor_call(self):
        self.config["shared"]["target_bindings"] = dict(TARGETS)
        code, summary, events = self.run_cli("demo-1,demo-3", "--deploy", "--run-data")
        self.assertEqual(code, 0)
        self.assertEqual(set(summary["phases"]["resolve-target-candidate"]), {"demo-1", "demo-3"})
        self.assertFalse(any(event["track"] == "demo-2" for event in events))
        after = [event for event in events if event["mode"] in ("validate-only", "run-data")]
        self.assertEqual(len({event["config_path"] for event in after}), 1)
        self.assertTrue(all(event["shared"]["target_bindings"] ==
                            {name: TARGETS[name] for name in ("demo-1", "demo-3")} for event in after))
        self.assertEqual(json.loads((self.root / "config.json").read_text())["shared"]["target_bindings"],
                         TARGETS)

    def test_unavailable_or_untrusted_target_candidates_block_the_run(self):
        for behavior in ({"status": "blocked", "ready": False}, {"omit_candidate": True},
                         {"ready": True}, {"raw_candidate": '{"contract_version":1,"contract_version":1}'}):
            with self.subTest(behavior=behavior):
                self.config["behavior"] = {"demo-2:resolve-target-candidate": behavior}
                code, summary, events = self.run_cli("demo-2,demo-3", "--deploy", "--run-data")
                self.assertEqual(code, 1)
                self.assertNotEqual(summary["phases"]["resolve-target-candidate"]["demo-2"]["status"], "pass")
                self.assertEqual(summary["phases"]["resolve-target-candidate"]["demo-3"]["status"], "pass")
                self.assertNotIn("validate-only", summary["phases"])
                self.assertNotIn("start-monitor", [event["mode"] for event in events])
                self.assertNotIn("run-data", summary["phases"])

    def test_previous_target_candidate_cannot_be_reused(self):
        code, _, _ = self.run_cli("demo-1", "--deploy", "--run-data")
        self.assertEqual(code, 0)
        self.config["behavior"] = {"demo-1:resolve-target-candidate": {"omit_candidate": True}}
        code, summary, events = self.run_cli("demo-1", "--deploy", "--run-data")
        self.assertEqual(code, 1)
        self.assertEqual(summary["phases"]["resolve-target-candidate"]["demo-1"]["status"], "fail")
        self.assertNotIn("validate-only", [event["mode"] for event in events])

    def test_explicit_target_and_per_track_conflicts_block(self):
        cases = (
            {"shared": {"target_bindings": {"demo-2": TARGETS["demo-3"]}}},
            {"demo-2": {"workspace_id": TARGETS["demo-3"]["workspace_id"]}},
            {"demo-2": {"kql_database_id": TARGETS["demo-3"]["item_id"]}},
            {"demo-2": {"sales_table": "DifferentTable"}},
        )
        for case in cases:
            with self.subTest(case=case):
                self.config.pop("demo-2", None)
                self.config["shared"].pop("target_bindings", None)
                for key, value in case.items():
                    if key == "shared":
                        self.config["shared"].update(value)
                    else:
                        self.config[key] = value
                code, summary, events = self.run_cli("demo-2", "--deploy", "--run-data")
                self.assertEqual(code, 1)
                self.assertEqual(summary["phases"]["resolve-target-candidate"]["demo-2"]["status"], "fail")
                self.assertNotIn("validate-only", [event["mode"] for event in events])

    def test_demo3_fact_binding_preserves_physical_identifier_case(self):
        self.config["demo-3"] = {"destination_table": "dbo.FactSales"}
        code, summary, events = self.run_cli("demo-3", "--deploy", "--run-data")
        self.assertEqual(code, 1)
        self.assertEqual(summary["phases"]["resolve-target-candidate"]["demo-3"]["status"], "fail")
        self.assertNotIn("validate-only", [event["mode"] for event in events])
        self.config["demo-3"]["destination_table"] = "dbo.factsales"
        code, summary, _ = self.run_cli("demo-3", "--deploy", "--run-data")
        self.assertEqual(code, 0)
        self.assertEqual(summary["phases"]["resolve-target-candidate"]["demo-3"]["status"], "pass")

    def test_shared_or_duplicate_demo_workspace_blocks_execution(self):
        for workspace in (self.config["shared"]["workspace_id"], TARGETS["demo-1"]["workspace_id"]):
            with self.subTest(workspace=workspace):
                candidate = {"contract_version": 1, "track": "demo-3",
                             "tenant_id": self.config["tenant_id"], "prefix": "chanel",
                             "target": dict(TARGETS["demo-3"], workspace_id=workspace)}
                self.config["behavior"] = {"demo-3:resolve-target-candidate": {"candidate": candidate}}
                code, summary, events = self.run_cli("demo-1,demo-3", "--deploy", "--run-data")
                self.assertEqual(code, 1)
                self.assertEqual(summary["phases"]["resolve-target-candidate"]["demo-3"]["status"], "fail")
                self.assertNotIn("validate-only", [event["mode"] for event in events])

    def test_target_sidecar_strictly_checks_identity_and_canonical_ids(self):
        candidate = {"contract_version": 1, "track": "demo-2", "tenant_id": self.config["tenant_id"],
                     "prefix": "chanel", "target": TARGETS["demo-2"]}
        invalid = (
            dict(candidate, contract_version=True),
            dict(candidate, track="demo-1"),
            dict(candidate, tenant_id="44444444-4444-4444-8444-444444444444"),
            dict(candidate, prefix="other"),
            dict(candidate, target=dict(TARGETS["demo-2"], workspace_id="00000000-0000-0000-0000-000000000000")),
            dict(candidate, target=dict(TARGETS["demo-2"], item_id=TARGETS["demo-2"]["item_id"].upper())),
            dict(candidate, target=dict(TARGETS["demo-2"], fact="bad name")),
            dict(candidate, target=dict(TARGETS["demo-2"], extra=True)),
        )
        path = self.root / "candidate.json"
        for value in invalid:
            with self.subTest(value=value):
                path.write_text(json.dumps(value), encoding="utf-8")
                with self.assertRaises(ValueError):
                    deploy.target_candidate(path, "demo-2", self.config["tenant_id"], "chanel")

    def test_validate_only_resolves_missing_ids_without_requiring_run_id(self):
        self.config["shared"]["workspace_id"] = None
        self.config["data"]["run_id"] = None
        code, summary, events = self.run_cli("demo-2", "--validate-only")
        self.assertEqual(code, 0)
        self.assertEqual(summary["phases"]["resolve-candidate-ids"]["shared"]["status"], "pass")
        validations = [event for event in events if event["mode"] == "validate-only"]
        self.assertEqual(len(validations), 2)
        self.assertEqual(len({event["config_path"] for event in validations}), 1)
        self.assertTrue(all(event["shared"]["workspace_id"] == "11111111-1111-4111-8111-111111111111"
                            for event in validations))
        self.assertNotIn("start-monitor", [event["mode"] for event in events])

    def test_selected_demos_reach_shared_validation_only(self):
        for mode in (("--validate-only",), ("--deploy", "--run-data")):
            with self.subTest(mode=mode):
                code, _, events = self.run_cli("demo-3,demo-1", *mode)
                self.assertEqual(code, 0)
                validations = [event for event in events if event["mode"] == "validate-only"]
                shared = next(event for event in validations if event["track"] == "shared")
                self.assertEqual(shared["selected"], "demo-3,demo-1")
                self.assertEqual(shared["shared"]["target_bindings"],
                                 {name: TARGETS[name] for name in ("demo-3", "demo-1")})
                self.assertTrue(all(event["selected"] is None for event in validations
                                    if event["track"] != "shared"))

    def test_validate_only_resolution_failure_reports_other_track_results_but_fails(self):
        self.config["shared"]["workspace_id"] = None
        self.config["behavior"] = {"shared:resolve-candidate-ids": {"status": "blocked", "ready": False}}
        code, summary, events = self.run_cli("demo-1", "--validate-only")
        self.assertEqual(code, 1)
        self.assertEqual(summary["phases"]["resolve-candidate-ids"]["shared"]["status"], "blocked")
        self.assertEqual(set(summary["phases"]["validate-only"]), {"demo-1", "shared"})
        self.assertEqual({event["mode"] for event in events}, {"resolve-candidate-ids", "validate-only"})
        self.assertEqual(next(event["selected"] for event in events
                              if event["track"] == "shared" and event["mode"] == "validate-only"), "demo-1")

    def test_failed_monitor_start_attempts_stop_and_blocks_producer(self):
        self.config["behavior"] = {"demo-2:start-monitor": {"status": "blocked", "ready": False}}
        code, summary, events = self.run_cli("demo-2", "--deploy", "--run-data")
        self.assertEqual(code, 1)
        self.assertEqual([event["mode"] for event in events][-2:], ["start-monitor", "stop-monitor"])
        self.assertEqual(summary["phases"]["start-monitor"]["demo-2"]["status"], "blocked")
        self.assertNotIn("run-data", summary["phases"])

    def test_unavailable_binding_blocks_before_monitor_submission(self):
        with mock.patch.object(deploy, "monitor_binding", side_effect=OSError("generator unavailable")):
            code, summary, events = self.run_cli("demo-2", "--deploy", "--run-data")
        self.assertEqual(code, 1)
        self.assertEqual(summary["phases"]["start-monitor"]["demo-2"]["status"], "blocked")
        self.assertNotIn("start-monitor", [event["mode"] for event in events])
        self.assertNotIn("stop-monitor", summary["phases"])
        self.assertNotIn("run-data", summary["phases"])

    def test_generator_change_after_monitor_start_blocks_producer_and_attempts_stop(self):
        binding = deploy.monitor_binding
        calls = 0

        def changed_after_start(config):
            nonlocal calls
            calls += 1
            current = binding(config)
            if calls == 2:
                current["generator_sha256"] = "0" * 64
            return current

        with mock.patch.object(deploy, "monitor_binding", side_effect=changed_after_start):
            code, summary, events = self.run_cli("demo-2", "--deploy", "--run-data")
        self.assertEqual(code, 1)
        self.assertEqual(summary["phases"]["monitor-status"]["demo-2"]["status"], "fail")
        self.assertEqual(events[-1]["mode"], "stop-monitor")
        self.assertEqual(events[-1]["stop_job"], "job-1")
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
                         {"state": "not-running"},
                         {"target_bindings": {"demo-2": TARGETS["demo-3"]}},
                         {"generator_sha256": "0" * 64},
                         {"monitor_binding_sha256": "0" * 64},
                         {"producer_config": {"run_id": "different-run"}}):
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
        for change in ({"job_id": "different-job"}, {"target_bindings": {}},
                       {"monitor_binding_sha256": "0" * 64}):
            with self.subTest(change=change):
                self.config["behavior"] = {"demo-2:monitor-status": {"start_record_evidence": change}}
                code, summary, events = self.run_cli("demo-2", "--deploy", "--run-data")
                self.assertEqual(code, 1)
                self.assertEqual(summary["phases"]["monitor-status"]["demo-2"]["status"], "fail")
                self.assertEqual(summary["phases"]["stop-monitor"]["demo-2"]["status"], "fail")
                self.assertEqual(events[-1]["mode"], "monitor-status")
                self.assertNotIn("run-data", summary["phases"])

    def test_duplicate_monitor_start_record_blocks_stop(self):
        state = self.root / "monitor-state"
        state.mkdir()
        initial = {
            "contract_version": 1, "run_id": "run-1", "job_id": "job-1",
            "manifest_uri": "https://example.invalid/manifest.json",
            "deadline_utc": "2100-01-01T00:00:00Z", "target_bindings": {},
            "generator_sha256": "a" * 64, "producer_config": {},
            "monitor_binding_sha256": "b" * 64,
        }
        (state / "demo-2-monitor-start.json").write_text(
            json.dumps(initial)[:-1] + ', "job_id": "job-1"}', encoding="utf-8")
        with mock.patch.object(deploy, "invoke") as invoke:
            outcome = deploy.stop_monitor(initial, self.root / "config.json", "chanel", state,
                                          self.root, 10, ["demo-2"])
        self.assertEqual(outcome["status"], "fail")
        self.assertIn("termination unconfirmed", outcome["diagnostics"][0])
        invoke.assert_not_called()

    def test_monitor_start_timeout_attempts_stop_before_producer(self):
        self.config["execution"]["track_timeout_seconds"] = 5
        self.config["behavior"] = {"demo-2:start-monitor": {"delay": 8}}
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

    def test_producer_recovery_inspection_cannot_claim_termination(self):
        state = self.root / "recovery-state"
        state.mkdir()
        config_path = self.root / "config.json"
        for mode in ("producer-status", "stop-producer"):
            for ready in (False, True):
                with self.subTest(mode=mode, ready=ready):
                    self.config["behavior"] = {f"shared:{mode}": {"ready": ready}}
                    config_path.write_text(json.dumps(self.config), encoding="utf-8")
                    outcome = deploy.invoke("shared", mode, config_path, "chanel", state, self.root,
                                            10, ["demo-1"])
                    self.assertEqual((outcome["status"], outcome["ready"]), ("blocked", False))
                    self.assertIn("termination unconfirmed", outcome["diagnostics"][0].lower())

    def test_incomplete_or_unready_monitor_never_reports_success(self):
        for behavior in ({"status": "blocked", "ready": False},
                         {"ready": False}, {"evidence": {"state": "running"}},
                         {"evidence": {"job_id": "different-job"}},
                         {"evidence": {"target_bindings": {"demo-2": TARGETS["demo-3"]}}},
                         {"evidence": {"generator_sha256": "0" * 64}},
                         {"evidence": {"producer_config": {"run_id": "different-run"}}},
                         {"evidence": {"monitor_binding_sha256": "0" * 64}}):
            with self.subTest(behavior=behavior):
                self.config["behavior"] = {"demo-2:wait-monitor": behavior}
                code, summary, events = self.run_cli("demo-2", "--deploy", "--run-data")
                self.assertEqual(code, 1)
                self.assertNotEqual(summary["phases"]["wait-monitor"]["demo-2"]["status"], "pass")
                self.assertEqual(events[-1]["mode"], "stop-monitor")
                self.assertEqual(events[-1]["stop_job"], "job-1")

    def test_unavailable_terminal_binding_fails_and_attempts_exact_stop(self):
        binding = deploy.monitor_binding
        calls = 0

        def unavailable_after_start(config):
            nonlocal calls
            calls += 1
            if calls <= 2:
                return binding(config)
            raise OSError("generator unavailable")

        with mock.patch.object(deploy, "monitor_binding", side_effect=unavailable_after_start):
            code, summary, events = self.run_cli("demo-2", "--deploy", "--run-data")
        self.assertEqual(code, 1)
        self.assertEqual(summary["phases"]["wait-monitor"]["demo-2"]["status"], "fail")
        self.assertEqual(events[-1]["mode"], "stop-monitor")
        self.assertEqual(events[-1]["stop_job"], "job-1")

    def test_demo2_manifest_config_mismatch_blocks_before_deploy(self):
        self.config["shared"]["run_state_location"] = "https://example.invalid/other-root"
        code, _, events = self.run_cli("demo-2", "--deploy", "--run-data")
        self.assertEqual(code, 2)
        self.assertEqual(events, [])

    def test_monitor_uses_default_timeout_when_demo_override_is_null(self):
        self.config["demo-2"] = {"verification_timeout_seconds": None}
        _, duration = deploy.monitor_config(self.config)
        self.assertEqual(duration, 1115)
        self.config["demo-2"]["verification_timeout_seconds"] = 30
        _, duration = deploy.monitor_config(self.config)
        self.assertEqual(duration, 1130)

    def test_producer_timeout_includes_validators_and_wrapper_headroom(self):
        self.config["demo-2"] = {"verification_timeout_seconds": 30}
        self.assertEqual(deploy.producer_timeout(self.config, ["demo-1"], 10), 1070)
        self.assertEqual(deploy.producer_timeout(self.config, ["demo-1", "demo-2"], 10), 1080)

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
