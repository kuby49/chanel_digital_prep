import copy
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import publish_monitor as publisher
import test_native_demo2 as native_tests
from test_native_demo2 import uid


class PublicationTests(unittest.TestCase):
    def setUp(self):
        context = tempfile.TemporaryDirectory()
        self.addCleanup(context.cleanup)
        self.directory = Path(context.name)
        self.key = "measured_monitor_notebook"
        self.target = {"workspace_id": uid(5), "item_id": uid(6), "fact": "SyntheticFactSales"}
        settings = {"reuse_synthetic_target": True, f"{self.key}_name": "Demo2MeasuredMonitor",
                    "lakehouse_name": "SalesLakehouse"}
        notebook = publisher.deploy._load_monitor_builder().build_monitor_notebook(
            workspace_id=uid(5), lakehouse_id=uid(7), lakehouse_name="SalesLakehouse",
            reused_synthetic_target=self.target,
        )
        self.desired = publisher.fabric_definition(notebook)
        notebook["cells"][1]["source"] = "# Historical executable\n"
        self.old = {"definition": publisher.fabric_definition(notebook)}
        self.live = self.old
        self.approval = {"monitor_notebook_id": uid(9),
                         "old_definition_sha256": publisher.native.definition_sha256(self.old),
                         "new_definition_sha256": publisher.native.definition_sha256(self.desired)}
        self.journal = self.directory / publisher.JOURNAL
        self.archive = self.directory / publisher.ARCHIVE
        self.resources = {role: {"id": uid(number), "confirmed": True} for role, number in
                          zip(("workspace", "eventhouse", "database", "table", "lakehouse", self.key),
                              (5, 10, 6, 6, 7, 9))}
        self.deployment = SimpleNamespace(
            resources=self.resources, s=settings, ledger=SimpleNamespace(directory=self.directory),
            verify_target_ownership=Mock(), reused_synthetic_target=lambda: self.target,
            item=Mock(side_effect=lambda key, *args: self.resources[key]),
            client=SimpleNamespace(fabric=Mock(side_effect=self.fabric)),
            _poll_operation=Mock(),
        )
        mocked = patch.object(publisher, "terminal_evidence", return_value={"state_etag": "stable"})
        self.terminal = mocked.start()
        self.addCleanup(mocked.stop)
        self.submit = Mock(return_value=(202, {"x-ms-operation-id": uid(20), "retry-after": "5"}, None))

    def fabric(self, method, path, body=None):
        self.assertEqual(self.deployment.client.fabric_skill, "spark-cli")
        self.assertEqual(method, "POST")
        if path.endswith("/getDefinition?format=ipynb"):
            return 200, {}, self.live
        self.assertTrue(path.endswith("/updateDefinition?updateMetadata=false"))
        self.assertEqual(body, {"definition": self.desired})
        self.assertEqual(json.loads(self.journal.read_text())["phase"], "pending")
        self.assertEqual(json.loads(self.archive.read_text())["definition"], self.old)
        return self.submit()

    def publish(self, resume=False):
        return publisher.publish(self.deployment, self.approval, resume_only=resume)

    def test_journal_before_post_and_reconcile_accepted_lro_without_resubmission(self):
        self.deployment._poll_operation.side_effect = publisher.deploy.Blocked("deadline")
        with self.assertRaisesRegex(publisher.deploy.Blocked, "deadline"):
            self.publish()
        self.assertEqual(json.loads(self.journal.read_text())["operation_id"], uid(20))
        before = self.archive.read_bytes()
        self.live = self.desired
        self.deployment._poll_operation.side_effect = None
        self.assertEqual(self.publish(resume=True)["monitor_definition_sha256"],
                         self.approval["new_definition_sha256"])
        self.deployment._poll_operation.assert_called_with(uid(20), "5", fetch_result=False)
        self.submit.assert_called_once()
        self.assertEqual(self.archive.read_bytes(), before)
        self.assertEqual(json.loads(self.journal.read_text())["verification_source"], "lro_and_readback")

    def test_uncertain_update_is_never_reposted_even_if_baseline_is_still_live(self):
        self.submit.side_effect = publisher.deploy.Blocked("connection lost")
        with self.assertRaisesRegex(publisher.deploy.Blocked, "without repeating"):
            self.publish()
        with self.assertRaisesRegex(publisher.deploy.Blocked, "without repeating"):
            self.publish()
        self.submit.assert_called_once()
        self.live = self.desired
        self.publish(resume=True)
        self.submit.assert_called_once()
        self.deployment._poll_operation.assert_not_called()
        self.assertEqual(json.loads(self.journal.read_text())["verification_source"], "readback_only")

    def test_missing_lro_identity_leaves_unconfirmed_intent(self):
        self.submit.return_value = (202, {}, None)
        with self.assertRaisesRegex(publisher.deploy.Blocked, "operation ID unconfirmed"):
            self.publish()
        self.assertEqual(json.loads(self.journal.read_text())["phase"], "pending")
        with self.assertRaisesRegex(publisher.deploy.Blocked, "without repeating"):
            self.publish(resume=True)
        self.submit.assert_called_once()

    def test_expired_final_preflight_never_updates_definition(self):
        with patch.object(publisher, "monotonic", side_effect=[0, 120]):
            with self.assertRaisesRegex(publisher.deploy.Blocked, "freshness window"):
                self.publish()
        self.assertTrue(self.archive.exists())
        self.assertFalse(self.journal.exists())
        self.submit.assert_not_called()

    def test_altered_archive_blocks_reconciliation_even_if_executable_is_unchanged(self):
        self.submit.return_value = (202, {}, None)
        with self.assertRaises(publisher.deploy.Blocked):
            self.publish()
        saved = json.loads(self.archive.read_text())
        saved["definition"]["extra"] = "changed"
        self.archive.write_text(json.dumps(saved))
        with self.assertRaisesRegex(publisher.deploy.Blocked, "Immutable old definition archive differs"):
            self.publish(resume=True)
        self.submit.assert_called_once()

    def test_verified_journal_cannot_hide_readback_drift(self):
        def finish():
            self.live = self.desired
            return 200, {}, None

        self.submit.side_effect = finish
        self.publish()
        self.live = self.old
        with self.assertRaisesRegex(publisher.deploy.Blocked, "New definition is not confirmed"):
            self.publish(resume=True)
        self.submit.assert_called_once()

    def test_unconfirmed_ownership_or_mismatched_hash_blocks_publication(self):
        self.resources[self.key]["confirmed"] = False
        with self.assertRaisesRegex(publisher.deploy.Blocked, "confirmed ownership"):
            self.publish()
        self.deployment.verify_target_ownership.assert_not_called()
        self.resources[self.key]["confirmed"] = True
        self.approval["new_definition_sha256"] = "f" * 64
        with self.assertRaisesRegex(publisher.deploy.Blocked, "approval does not match"):
            self.publish()
        self.submit.assert_not_called()

    def test_active_job_or_drift_blocks_before_update(self):
        self.terminal.side_effect = publisher.deploy.Blocked("old job not terminal")
        with self.assertRaisesRegex(publisher.deploy.Blocked, "not terminal"):
            self.publish()
        self.assertFalse(self.archive.exists())
        self.assertFalse(self.journal.exists())
        self.terminal.side_effect = None
        self.live = self.desired
        with self.assertRaisesRegex(publisher.deploy.Blocked, "baseline SHA-256 differs"):
            self.publish()
        self.submit.assert_not_called()


class TerminalEvidenceTests(unittest.TestCase):
    def test_original_lifecycle_and_exact_jobs_must_all_be_terminal_without_uncertainty(self):
        fixture = native_tests.NativeDemo2Tests()
        fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        ctx = fixture.context
        doc = ctx["document"]
        doc["shared"]["producer_notebook_id"] = uid(12)
        doc["demo-2"]["monitor_definition_sha256"] = "b" * 64
        run = fixture.run_with_journal()
        run.record["jobs"] = {"bootstrap": {"job_id": uid(15)}, "monitor": {"job_id": uid(16)}}
        run.save()
        lifecycle = {"document": doc, "run_id": doc["data"]["run_id"], "config_sha256": publisher.native._digest(doc),
                     "verification_deadline_utc": publisher.deploy._stamp(ctx["verification_deadline_utc"]),
                     "expires_at_utc": publisher.deploy._stamp(ctx["expires_at_utc"]),
                     "tracks": {"demo-2": {"prepare": {"monitor_job_id": uid(16)}}}}
        base = f"Files/producer/runs/{lifecycle['run_id']}"
        run.store.create(f"{base}/native-lifecycle.json", publisher.native.monitor._canonical_json(lifecycle))
        state = {k: run.cfg[k] for k in ("run_id", "controller_id", "tenant_id", "identity_id",
                                        "generator_sha256", "monitor_binding_sha256")}
        state.update(contract_version=1, manifest_uri=run.cfg["manifest_url"], deadline_utc=run.params["DEADLINE_UTC"],
                     state="failed", sentinel_verified=True, job_access_verified=True,
                     batches={"x": {"status": "failed", "job_id": uid(17)}})
        fixture.target.create(run.state, publisher.native.monitor._canonical_json(state))
        fixture.target.create(run.lock, publisher.native.monitor._canonical_json(
            {"contract_version": 1, "run_id": run.cfg["run_id"], "controller_id": run.cfg["controller_id"]}))
        approval = {"producer_workspace_id": uid(3), "producer_lakehouse_id": uid(4), "producer_notebook_id": uid(12),
                    "parent_pipeline_id": uid(11), "parent_job_id": uid(13), "producer_job_id": uid(14),
                    "run_id": lifecycle["run_id"], "old_config_sha256": lifecycle["config_sha256"],
                    "old_definition_sha256": "b" * 64}
        jobs = {uid(job): {"id": uid(job), "itemId": uid(item), "status": "Failed",
                          "endTimeUtc": "2026-11-25T13:33:35.2605463"}
                for item, job in ((11, 13), (12, 14), (8, 15), (9, 16), (8, 17))}
        jobs[uid(13)].update(jobType="Pipeline", startTimeUtc="2026-11-25T13:05:00Z")
        jobs[uid(14)].update(jobType="PipelineRunNotebook")
        activity = {
            "pipelineRunId": uid(13), "activityRunId": uid(14), "activityName": "RunProducer",
            "activityType": "TridentNotebook", "input": {"workspaceId": uid(3), "notebookId": uid(12)},
            "output": {"result": {"runId": uid(14)}},
        }

        def fabric(method, path, body=None):
            if path.endswith(f"/pipelineruns/{uid(13)}/queryactivityruns"):
                self.assertEqual(method, "POST")
                self.assertEqual(body["filters"], [])
                return 200, {}, {"value": [activity]}
            self.assertEqual(method, "GET")
            return 200, {}, jobs[path.rsplit("/", 1)[-1]]

        deployment = SimpleNamespace(client=SimpleNamespace(fabric=Mock(side_effect=fabric)),
                                     s={"tenant_id": uid(1)}, ledger=None,
                                     resources={"workspace": {"id": uid(5)}, "lakehouse": {"id": uid(7)}})
        with patch.object(publisher, "ReadOnlyStore", side_effect=lambda _, ws, lh:
                          fixture.store if ws == uid(3) else fixture.target), \
                patch.object(publisher.deploy, "_journal_target", return_value=doc["shared"]["target_bindings"]["demo-2"]):
            before = copy.deepcopy(fixture.target.files)
            self.assertEqual(publisher.terminal_evidence(deployment, approval, uid(9))["child_job_ids"], [uid(17)])
            self.assertEqual(fixture.target.files, before)
            jobs[uid(13)]["status"] = "InProgress"
            with self.assertRaisesRegex(publisher.deploy.Blocked, "not terminal"):
                publisher.terminal_evidence(deployment, approval, uid(9))
            jobs[uid(13)]["status"] = "Failed"
            activity["output"]["result"]["runId"] = uid(18)
            with self.assertRaisesRegex(publisher.deploy.Blocked, "Parent activity does not identify"):
                publisher.terminal_evidence(deployment, approval, uid(9))
            activity["output"]["result"]["runId"] = uid(14)
            jobs[uid(17)]["status"] = "InProgress"
            with self.assertRaisesRegex(publisher.deploy.Blocked, "not terminal"):
                publisher.terminal_evidence(deployment, approval, uid(9))
            state["batches"]["x"]["submission_uncertain"] = True
            fixture.target.replace(run.state, publisher.native.monitor._canonical_json(state), before[run.state][1])
            with self.assertRaisesRegex(publisher.native.monitor.MonitorError, "submission is uncertain"):
                publisher.terminal_evidence(deployment, approval, uid(9))
