import copy
import hashlib
import io
import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest import mock

import deploy
from job_lifecycle import PipelineJob
from sales_ids import canonical_bytes
from test_deploy import (LAKEHOUSE, NOTEBOOK, PIPELINE, WORKSPACE, ReadbackClient,
                         config, owned_state, run_data)


JOB = "99999999-9999-4999-8999-999999999999"


class SubmitClient:
    def request(self, method, path, payload, timeout):
        location = f"https://api.fabric.microsoft.com/v1/workspaces/{WORKSPACE}/items/{PIPELINE}/jobs/instances/{JOB}"
        return 202, {"Location": location, "Retry-After": "1"}, None


class RecoveryClient(ReadbackClient):
    def __init__(self, plan, state):
        super().__init__(plan, state)
        self.pipeline_status = "InProgress"
        self.returned_job_id = JOB
        self.posts = []

    def get(self, path, deadline=None):
        if "/jobs/instances/" in path:
            self.calls.append(("GET", path))
            return {}, {"id": self.returned_job_id, "itemId": PIPELINE, "status": self.pipeline_status}
        if path == f"/v1/workspaces/{WORKSPACE}":
            self.calls.append(("GET", path))
            return {}, copy.deepcopy(self.workspace)
        return super().get(path, deadline)

    def request(self, *args, **kwargs):
        self.posts.append((args, kwargs))
        raise AssertionError("Recovery must not issue a POST")


class RecoveryCLITests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.state_dir = self.root / "state"
        self.document = config()
        self.document["data"] = run_data()
        self.plan = deploy.parse_config(self.document, "sample")
        self.state = owned_state(self.plan)
        deploy.StateStore(self.state_dir).write(self.state)
        self.binding = {
            "run_id": "run-1", "nonce": "independent-probe", "tenant_id": self.plan["tenant_id"],
            "prefix": "sample", "config_sha256": hashlib.sha256(canonical_bytes(self.document)).hexdigest(),
            "selected_demos": ["demo-1", "demo-2"], "workspace_id": WORKSPACE, "pipeline_id": PIPELINE,
            "notebook_id": NOTEBOOK,
            "manifest_uri": f"https://onelake.blob.fabric.microsoft.com/{WORKSPACE}/{LAKEHOUSE}/"
                            "Files/producer/runs/run-1/manifest.json",
            "deadline_utc": (datetime.now(timezone.utc) + timedelta(minutes=5)).isoformat(),
        }
        self.job = PipelineJob(self.state_dir, self.binding, SubmitClient())
        self.job.submit({"executionData": {"parameters": {
            "operation": "preflight", "probe_id": self.binding["nonce"], "runtime_evidence_json": "{}",
            "run_config_json": json.dumps(self.document), "selected_demos_json": '["demo-1","demo-2"]',
        }}})
        self.allow_poll()
        self.service = RecoveryClient(self.plan, self.state)

    def allow_poll(self):
        value = json.loads(self.job.path.read_text(encoding="utf-8"))
        value["poll_not_before_utc"] = "2000-01-01T00:00:00Z"
        self.job.path.write_text(json.dumps(value), encoding="utf-8")

    def invoke(self, mode, document=None, selected="demo-1,demo-2"):
        with (mock.patch.object(deploy, "azure_cli_token", return_value="token") as auth,
              mock.patch.object(deploy, "FabricClient", return_value=self.service)):
            result = deploy.perform(mode, document or self.document, "sample", str(self.state_dir), selected)
        return result, auth

    def test_status_reports_exact_parent_but_not_termination(self):
        for status in ("InProgress", "Completed", "Failed", "Cancelled"):
            with self.subTest(status=status):
                self.service.pipeline_status = status
                diagnostics, auth = self.invoke("producer-status")
                self.assertIn(f"{JOB} has live status {status}", diagnostics[0])
                self.assertTrue(any("termination unconfirmed" in entry for entry in diagnostics))
                auth.assert_called_once()
        self.assertEqual(self.service.posts, [])

    def test_stop_never_cancels_without_authenticated_child_ownership(self):
        diagnostics, _ = self.invoke("stop-producer")
        self.assertTrue(any("Cancellation was not requested" in entry for entry in diagnostics))
        self.assertTrue(any("termination unconfirmed" in entry for entry in diagnostics))
        self.assertEqual(self.service.posts, [])

    def test_config_selection_and_manifest_mismatch_block_before_auth(self):
        changed = copy.deepcopy(self.document)
        changed["data"]["seed"] += 1
        for doc, selected in ((changed, "demo-1,demo-2"), (self.document, "demo-1")):
            with self.subTest(selected=selected, doc=doc):
                with mock.patch.object(deploy, "azure_cli_token") as auth:
                    with self.assertRaisesRegex(deploy.Blocked, "configuration"):
                        deploy.perform("producer-status", doc, "sample", str(self.state_dir), selected)
                auth.assert_not_called()
        state = json.loads(self.job.path.read_text(encoding="utf-8"))
        state["binding"]["manifest_uri"] += "-other"
        self.job.path.write_text(json.dumps(state), encoding="utf-8")
        with self.assertRaisesRegex(deploy.Blocked, "configuration"):
            self.invoke("stop-producer")

    def test_wrong_job_or_workspace_readback_cannot_authorize_stop(self):
        self.service.returned_job_id = NOTEBOOK
        with self.assertRaisesRegex(deploy.Blocked, "(?i)termination unconfirmed"):
            self.invoke("stop-producer")
        self.assertEqual(self.service.posts, [])
        self.service.returned_job_id = JOB
        self.service.workspace["id"] = NOTEBOOK
        with self.assertRaisesRegex(deploy.Blocked, "(?i)termination unconfirmed"):
            self.invoke("stop-producer")
        self.assertEqual(self.service.posts, [])

    def test_uncertain_submission_and_location_mismatch_never_reach_fabric(self):
        state = json.loads(self.job.path.read_text(encoding="utf-8"))
        state["phase"] = "submitting"
        submitting = {key: state[key] for key in
                      ("version", "payload_binding_version", "binding", "phase", "payload_sha256", "intent_at_utc")}
        self.job.path.write_text(json.dumps(submitting), encoding="utf-8")
        with mock.patch.object(deploy, "azure_cli_token") as auth:
            with self.assertRaisesRegex(deploy.Blocked, "Submission outcome unknown"):
                deploy.perform("stop-producer", self.document, "sample", str(self.state_dir), "demo-1,demo-2")
        auth.assert_not_called()
        state["phase"] = "submitted"
        state["submission_location"] = state["submission_location"].replace(JOB, NOTEBOOK)
        self.job.path.write_text(json.dumps(state), encoding="utf-8")
        with mock.patch.object(deploy, "azure_cli_token") as auth:
            with self.assertRaisesRegex(deploy.Blocked, "differs from submission"):
                deploy.perform("stop-producer", self.document, "sample", str(self.state_dir), "demo-1,demo-2")
        auth.assert_not_called()

    def test_invalid_or_duplicate_journal_is_blocked(self):
        for content in ('{"version":1,"version":1}', '{"version":NaN}', "not json"):
            with self.subTest(content=content):
                self.job.path.write_text(content, encoding="utf-8")
                with self.assertRaisesRegex(deploy.Blocked, "journal"):
                    self.invoke("producer-status")
        self.assertEqual(self.service.calls, [])

    def test_incomplete_journal_does_not_authorize_status_or_stop(self):
        state = json.loads(self.job.path.read_text(encoding="utf-8"))
        state.pop("payload_sha256")
        self.job.path.write_text(json.dumps(state), encoding="utf-8")
        with mock.patch.object(deploy, "azure_cli_token") as auth:
            with self.assertRaisesRegex(deploy.Blocked, "incomplete"):
                deploy.perform("stop-producer", self.document, "sample", str(self.state_dir), "demo-1,demo-2")
        auth.assert_not_called()
        self.assertEqual(self.service.posts, [])

    def test_legacy_journal_cannot_claim_bound_submission(self):
        state = json.loads(self.job.path.read_text(encoding="utf-8"))
        state.pop("payload_binding_version")
        self.job.path.write_text(json.dumps(state), encoding="utf-8")
        with mock.patch.object(deploy, "azure_cli_token") as auth:
            with self.assertRaisesRegex(deploy.Blocked, "not bound"):
                deploy.perform("stop-producer", self.document, "sample", str(self.state_dir), "demo-1,demo-2")
        auth.assert_not_called()

    def test_cli_envelope_is_blocked_even_for_terminal_parent(self):
        self.service.pipeline_status = "Completed"
        config_path = self.root / "config.json"
        result_path = self.root / "result.json"
        config_path.write_text(json.dumps(self.document), encoding="utf-8")
        args = ["--mode", "producer-status", "--config", str(config_path), "--prefix", "sample",
                "--state-dir", str(self.state_dir), "--result", str(result_path),
                "--selected-demos", "demo-1,demo-2"]
        with (mock.patch.object(deploy, "azure_cli_token", return_value="token"),
              mock.patch.object(deploy, "FabricClient", return_value=self.service)):
            self.assertEqual(deploy.main(args), 1)
        result = json.loads(result_path.read_text(encoding="utf-8"))
        self.assertEqual(set(result), {"contract_version", "track", "mode", "status", "ready", "diagnostics"})
        self.assertEqual((result["status"], result["ready"]), ("blocked", False))
        self.assertTrue(any("termination unconfirmed" in item for item in result["diagnostics"]))

    def test_result_and_config_cannot_overwrite_recovery_journal(self):
        config_path = self.root / "config.json"
        config_path.write_text(json.dumps(self.document), encoding="utf-8")
        args = ["--mode", "producer-status", "--config", str(config_path), "--prefix", "sample",
                "--state-dir", str(self.state_dir), "--result", str(self.job.path),
                "--selected-demos", "demo-1,demo-2"]
        before = self.job.path.read_bytes()
        with mock.patch("sys.stderr"):
            self.assertEqual(deploy.main(args), 1)
        self.assertEqual(self.job.path.read_bytes(), before)
        args[3] = str(self.job.path)
        args[9] = str(self.root / "result.json")
        with mock.patch("sys.stderr"):
            self.assertEqual(deploy.main(args), 1)
        self.assertEqual(self.job.path.read_bytes(), before)

    def test_other_state_or_missing_arguments_cannot_overwrite_recovery_journal(self):
        config_path = self.root / "config.json"
        config_path.write_text(json.dumps(self.document), encoding="utf-8")
        args = ["--mode", "stop-producer", "--config", str(config_path), "--prefix", "sample",
                "--state-dir", str(self.root / "other-state"), "--result", str(self.job.path),
                "--selected-demos", "demo-1,demo-2"]
        before = self.job.path.read_bytes()
        for invalid_args in (args, args[:6] + args[8:]):
            with self.subTest(args=invalid_args), mock.patch("sys.stderr", new_callable=io.StringIO) as stderr:
                self.assertEqual(deploy.main(invalid_args), 1)
            self.assertTrue(any("termination unconfirmed" in entry.lower()
                                for entry in json.loads(stderr.getvalue())["diagnostics"]))
            self.assertEqual(self.job.path.read_bytes(), before)

    def test_recovery_failures_and_result_write_errors_report_unconfirmed_termination(self):
        config_path = self.root / "config.json"
        result_path = self.root / "result.json"
        config_path.write_text(json.dumps(self.document), encoding="utf-8")
        for mode in ("producer-status", "stop-producer"):
            args = ["--mode", mode, "--config", str(config_path), "--prefix", "sample",
                    "--state-dir", str(self.state_dir), "--result", str(result_path),
                    "--selected-demos", "demo-1,demo-2"]
            for failure in (OSError("private"), RuntimeError("private"), deploy.FabricTransportError("read failed")):
                with self.subTest(mode=mode, failure=type(failure).__name__):
                    with mock.patch.object(deploy, "_recover_pipeline", side_effect=failure):
                        self.assertEqual(deploy.main(args), 1)
                    result = json.loads(result_path.read_text(encoding="utf-8"))
                    self.assertEqual((result["status"], result["ready"]), ("fail", False))
                    self.assertTrue(any("termination unconfirmed" in item.lower() for item in result["diagnostics"]))
                    self.assertNotIn("private", json.dumps(result))
            with (mock.patch.object(deploy, "_atomic_json", side_effect=OSError("private")),
                  mock.patch("sys.stderr", new_callable=io.StringIO) as stderr):
                self.assertEqual(deploy.main(args), 1)
            self.assertTrue(any("termination unconfirmed" in item.lower()
                                for item in json.loads(stderr.getvalue())["diagnostics"]))
            self.assertNotIn("private", stderr.getvalue())


if __name__ == "__main__":
    unittest.main()
