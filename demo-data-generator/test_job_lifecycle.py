import copy
import hashlib
import json
import subprocess
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from job_lifecycle import LifecycleBlocked, PipelineJob, activity_timeout
from generator import canonical_bytes


WORKSPACE = "11111111-1111-4111-8111-111111111111"
PIPELINE = "22222222-2222-4222-8222-222222222222"
NOTEBOOK = "33333333-3333-4333-8333-333333333333"
TENANT = "44444444-4444-4444-8444-444444444444"
JOB = "55555555-5555-4555-8555-555555555555"
LOCATION = f"https://api.fabric.microsoft.com/v1/workspaces/{WORKSPACE}/items/{PIPELINE}/jobs/instances/{JOB}"
CONFIG = {"data": {"run_id": "test-run"}}
NONCE = "nonce-0123456789abcdef"
PREFLIGHT = {"executionData": {"parameters": {
    "operation": "preflight", "probe_id": NONCE, "runtime_evidence_json": "{}",
    "run_config_json": json.dumps(CONFIG), "selected_demos_json": '["demo-1","demo-2"]',
}}}


def binding():
    return {"run_id": "test-run", "nonce": NONCE, "tenant_id": TENANT, "prefix": "test",
            "config_sha256": hashlib.sha256(canonical_bytes(CONFIG)).hexdigest(),
            "selected_demos": ["demo-1", "demo-2"],
            "workspace_id": WORKSPACE, "pipeline_id": PIPELINE, "notebook_id": NOTEBOOK,
            "manifest_uri": "https://example.invalid/manifest",
            "deadline_utc": (datetime.now(timezone.utc) + timedelta(minutes=5)).isoformat()}


class Client:
    def __init__(self):
        self.posts = []
        self.reads = []
        self.post_response = (202, {"Location": LOCATION, "Retry-After": "1"}, None)
        self.statuses = ["InProgress"]
        self.on_post = lambda: None

    def request(self, method, path, payload=None, timeout=None):
        self.on_post()
        self.posts.append((method, path, payload, timeout))
        if isinstance(self.post_response, Exception):
            raise self.post_response
        return self.post_response

    def get(self, path, deadline):
        self.reads.append(path)
        status = self.statuses.pop(0) if len(self.statuses) > 1 else self.statuses[0]
        return {}, {"id": JOB, "itemId": PIPELINE, "status": status}


class LifecycleTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.client = Client()
        self.binding = binding()
        self.job = PipelineJob(self.temp.name, self.binding, self.client)

    def state(self):
        return json.loads(self.job.path.read_text(encoding="utf-8"))

    def allow_poll(self):
        state = self.state()
        state["poll_not_before_utc"] = "2000-01-01T00:00:00Z"
        self.job.path.write_text(json.dumps(state), encoding="utf-8")

    def test_submit_persists_intent_and_exact_identity_and_never_resubmits(self):
        self.client.on_post = lambda: self.assertEqual(self.state()["phase"], "submitting")
        self.assertEqual(self.job.submit(PREFLIGHT), JOB)
        self.assertEqual(self.state()["job_id"], JOB)
        with self.assertRaisesRegex(LifecycleBlocked, "already submitted"):
            self.job.submit(PREFLIGHT)
        self.assertEqual(len(self.client.posts), 1)
        self.allow_poll()
        self.assertEqual(self.job.status(), "InProgress")
        self.assertTrue(self.client.reads[0].endswith("/" + JOB))

    def test_unknown_submission_and_mismatched_location_never_retry(self):
        self.client.post_response = OSError("lost response")
        with self.assertRaises(OSError):
            self.job.submit(PREFLIGHT)
        with self.assertRaisesRegex(LifecycleBlocked, "outcome unknown"):
            self.job.submit(PREFLIGHT)
        with self.assertRaisesRegex(LifecycleBlocked, "termination unconfirmed"):
            self.job.cancel()
        self.assertEqual(len(self.client.posts), 1)

        other = PipelineJob(Path(self.temp.name) / "other", self.binding, self.client)
        self.client.post_response = (202, {"Location": LOCATION.replace(PIPELINE, NOTEBOOK)}, None)
        with self.assertRaisesRegex(LifecycleBlocked, "ambiguous"):
            other.submit(PREFLIGHT)
        self.assertEqual(json.loads(other.path.read_text())["phase"], "submitting")

    def test_invalid_retry_after_preserves_exact_job_identity(self):
        self.client.post_response = (202, {"Location": LOCATION, "Retry-After": "bad"}, None)
        with self.assertRaisesRegex(LifecycleBlocked, "Retry-After"):
            self.job.submit(PREFLIGHT)
        self.assertEqual(self.state()["job_id"], JOB)
        with self.assertRaisesRegex(LifecycleBlocked, "poll time unknown"):
            self.job.status()
        self.assertEqual(self.client.reads, [])

    def test_failed_identity_persistence_leaves_unrepeatable_intent(self):
        from job_lifecycle import _write
        writes = []

        def fail_second(path, value):
            writes.append(value["phase"])
            if len(writes) == 2:
                raise OSError("disk failed")
            return _write(path, value)

        with patch("job_lifecycle._write", side_effect=fail_second):
            with self.assertRaises(OSError):
                self.job.submit(PREFLIGHT)
        self.assertEqual(self.state()["phase"], "submitting")
        with self.assertRaisesRegex(LifecycleBlocked, "outcome unknown"):
            self.job.submit(PREFLIGHT)
        self.assertEqual(len(self.client.posts), 1)

    def test_binding_changes_and_wrong_readback_are_rejected(self):
        self.job.submit(PREFLIGHT)
        changed = PipelineJob(self.temp.name, {**self.binding, "config_sha256": "b" * 64}, self.client)
        with self.assertRaisesRegex(LifecycleBlocked, "binding"):
            changed.status()
        self.allow_poll()
        with patch.object(self.client, "get", return_value=({}, {"id": NOTEBOOK, "itemId": PIPELINE,
                                                                "status": "Completed"})):
            with self.assertRaisesRegex(LifecycleBlocked, "unproven"):
                self.job.status()

    def test_cancel_persists_intent_and_reads_terminal(self):
        self.job.submit(PREFLIGHT)
        self.allow_poll()
        self.client.statuses = ["InProgress", "Cancelled"]
        self.client.on_post = lambda: self.assertEqual(self.state()["phase"], "cancelling")
        self.client.post_response = (202, {"Retry-After": "1"}, None)
        self.assertEqual(self.job.cancel(timeout=5), "Cancelled")
        self.assertTrue(self.client.posts[-1][1].endswith("/" + JOB + "/cancel"))
        self.assertEqual(self.state()["phase"], "cancel_requested")
        self.assertEqual(len(self.client.reads), 2)

    def test_uncertain_cancel_is_not_repeated(self):
        self.job.submit(PREFLIGHT)
        self.allow_poll()
        self.client.post_response = OSError("lost cancellation")
        with self.assertRaises(OSError):
            self.job.cancel()
        with self.assertRaisesRegex(LifecycleBlocked, "outcome uncertain"):
            self.job.cancel()
        self.assertEqual(len(self.client.posts), 2)

    def test_lock_is_released_and_rejects_concurrent_controllers(self):
        from job_lifecycle import _lock
        with _lock(self.job.lock_path):
            with self.assertRaisesRegex(LifecycleBlocked, "Another lifecycle operation"):
                self.job.submit(PREFLIGHT)
        self.assertEqual(self.job.submit(PREFLIGHT), JOB)

    def test_lock_is_released_when_launcher_exits(self):
        source = ("import sys,time; from pathlib import Path; from job_lifecycle import _lock\n"
                  "with _lock(Path(sys.argv[1])):\n"
                  " print('locked', flush=True); time.sleep(60)\n")
        child = subprocess.Popen([sys.executable, "-c", source, str(self.job.lock_path)],
                                 cwd=str(Path(__file__).parent), stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE, text=True)
        try:
            self.assertEqual(child.stdout.readline().strip(), "locked")
            with self.assertRaisesRegex(LifecycleBlocked, "Another lifecycle operation"):
                self.job.submit(PREFLIGHT)
        finally:
            child.terminate()
            child.communicate(timeout=10)
        self.assertEqual(self.job.submit(PREFLIGHT), JOB)

    def test_expired_deadline_never_submits(self):
        expired = {**self.binding, "deadline_utc": "2000-01-01T00:00:00Z"}
        job = PipelineJob(self.temp.name, expired, self.client)
        with self.assertRaisesRegex(LifecycleBlocked, "deadline"):
            job.submit(PREFLIGHT)
        self.assertFalse(job.path.exists())
        self.assertEqual(self.client.posts, [])

    def test_measured_operation_is_refused_before_journaling(self):
        measured = {"executionData": {"parameters": {"operation": "run-data"}}}
        with self.assertRaisesRegex(LifecycleBlocked, "no-sales preflight"):
            self.job.submit(measured)
        self.assertFalse(self.job.path.exists())
        self.assertEqual(self.client.posts, [])

    def test_measured_submission_requires_fresh_admission_before_one_post(self):
        lakehouse = "77777777-7777-4777-8777-777777777777"
        identity = "88888888-8888-4888-8888-888888888888"
        document = {"data": {"run_id": "test-run", "max_duration_seconds": 1800},
                    "shared": {"workspace_id": WORKSPACE, "staging_lakehouse_id": lakehouse,
                               "identity_id": identity}}
        measured_binding = dict(self.binding, config_sha256=hashlib.sha256(canonical_bytes(document)).hexdigest(),
                                manifest_uri=f"https://onelake.blob.fabric.microsoft.com/{WORKSPACE}/{lakehouse}/"
                                             "Files/producer/runs/test-run/manifest.json")
        runtime = {"nonce": NONCE, "run_id": "test-run", "config_sha256": measured_binding["config_sha256"],
                   "tenant_id": TENANT, "identity_id": identity,
                   "producer": {"workspace_id": WORKSPACE, "staging_lakehouse_id": lakehouse,
                                "notebook_id": NOTEBOOK}}
        payload = {"executionData": {"parameters": {
            "operation": "run-data", "probe_id": NONCE, "runtime_evidence_json": json.dumps(runtime),
            "run_config_json": json.dumps(document), "selected_demos_json": '["demo-1","demo-2"]',
            "activity_timeout": activity_timeout(1800),
        }}}
        job = PipelineJob(self.temp.name, measured_binding, self.client)
        with self.assertRaisesRegex(LifecycleBlocked, "fresh producer and destination admission"):
            job.submit(payload)
        with self.assertRaisesRegex(RuntimeError, "not ready"):
            job.submit(payload, admission=lambda: (_ for _ in ()).throw(RuntimeError("not ready")))
        self.assertFalse(job.path.exists())
        self.assertEqual(self.client.posts, [])
        checked = []
        self.client.on_post = lambda: self.assertEqual(checked, ["fresh"])
        self.assertEqual(job.submit(payload, admission=lambda: checked.append("fresh")), JOB)
        self.assertTrue(job.matches_payload(payload))
        with self.assertRaisesRegex(LifecycleBlocked, "already submitted"):
            job.submit(payload, admission=lambda: checked.append("again"))
        self.assertEqual((checked, len(self.client.posts)), (["fresh"], 1))

    def test_activity_timeout_includes_startup_headroom_and_is_bounded(self):
        self.assertEqual(activity_timeout(1800), "0.00:45:00")
        self.assertEqual(activity_timeout(24 * 3600), "1.00:15:00")
        for duration in (0, True, 7 * 86400 - 899):
            with self.subTest(duration=duration):
                with self.assertRaises(LifecycleBlocked):
                    activity_timeout(duration)

    def test_preflight_parameters_must_match_journal_binding_before_post(self):
        mutations = (
            ("probe_id", "another-probe-0123456789"),
            ("run_config_json", '{"data":{"run_id":"another-run"}}'),
            ("run_config_json", '{"data":{"run_id":"test-run"},"seed":1}'),
            ("run_config_json", '{"data":{"run_id":"test-run","run_id":"test-run"}}'),
            ("run_config_json", '{"data":{"run_id":"test-run"},"seed":NaN}'),
            ("selected_demos_json", '["demo-1"]'),
            ("selected_demos_json", '["demo-1","demo-1","demo-2"]'),
            ("selected_demos_json", "{bad"),
            ("runtime_evidence_json", '{"nonce":"wrong"}'),
        )
        for field, value in mutations:
            with self.subTest(field=field):
                payload = copy.deepcopy(PREFLIGHT)
                payload["executionData"]["parameters"][field] = value
                with self.assertRaises(LifecycleBlocked):
                    self.job.submit(payload)
        self.assertFalse(self.job.path.exists())
        self.assertEqual(self.client.posts, [])

    def test_status_rechecks_payload_binding_under_its_lock(self):
        self.job.submit(PREFLIGHT)
        state = self.state()
        state.pop("payload_binding_version")
        self.job.path.write_text(json.dumps(state), encoding="utf-8")
        with self.assertRaisesRegex(LifecycleBlocked, "not bound"):
            self.job.status()
        self.assertEqual(self.client.reads, [])

    def test_terminal_parent_waits_for_same_child_without_cancelling(self):
        self.job.submit(PREFLIGHT)
        ticks = [0]
        self.job.monotonic = lambda: ticks[0]
        self.job.sleep = lambda seconds: ticks.__setitem__(0, ticks[0] + seconds)
        parent = {"id": JOB, "status": "Completed"}
        child = "66666666-6666-4666-8666-666666666666"
        with (patch.object(self.job, "_parent", return_value=parent) as read_parent,
              patch.object(self.job, "_notebook_detail", side_effect=[
                  {"job_id": child, "status": "InProgress"},
                  {"job_id": child, "status": "Completed"},
              ]) as read_child):
            snapshot = self.job.status_detail(timeout=10, include_notebook=True,
                                              wait_for_terminal_child=True)
        self.assertEqual(snapshot["notebook"], {"job_id": child, "status": "Completed"})
        self.assertEqual((read_parent.call_count, read_child.call_count), (2, 2))
        self.assertEqual(len(self.client.posts), 1)

    def test_terminal_child_wait_timeout_and_identity_change_fail_closed(self):
        self.job.submit(PREFLIGHT)
        ticks = [0]
        self.job.monotonic = lambda: ticks[0]
        self.job.sleep = lambda seconds: ticks.__setitem__(0, ticks[0] + seconds)
        parent = {"id": JOB, "status": "Completed"}
        child = "66666666-6666-4666-8666-666666666666"
        with (patch.object(self.job, "_parent", return_value=parent),
              patch.object(self.job, "_notebook_detail", return_value={"job_id": child, "status": "InProgress"})):
            snapshot = self.job.status_detail(timeout=5, include_notebook=True,
                                              wait_for_terminal_child=True)
        self.assertEqual(snapshot["notebook"]["status"], "InProgress")
        self.assertIn("wait expired", snapshot["notebook_diagnostic"])
        ticks[0] = 0
        with (patch.object(self.job, "_parent", return_value=parent),
              patch.object(self.job, "_notebook_detail", side_effect=[
                  {"job_id": child, "status": "InProgress"},
                  {"job_id": NOTEBOOK, "status": "Completed"},
              ])):
            with self.assertRaisesRegex(LifecycleBlocked, "identity changed"):
                self.job.status_detail(timeout=10, include_notebook=True, wait_for_terminal_child=True)
        ticks[0] = 0
        with (patch.object(self.job, "_parent", side_effect=[parent, {"id": JOB, "status": "Failed"}]),
              patch.object(self.job, "_notebook_detail", return_value={"job_id": child, "status": "InProgress"})):
            with self.assertRaisesRegex(LifecycleBlocked, "Parent terminal state changed"):
                self.job.status_detail(timeout=10, include_notebook=True, wait_for_terminal_child=True)
        self.assertEqual(len(self.client.posts), 1)


if __name__ == "__main__":
    unittest.main()
