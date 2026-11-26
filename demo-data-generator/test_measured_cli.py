import base64
import io
import json
import tempfile
import unittest
from contextlib import redirect_stderr
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import deploy
import fabric_assets
import fabric_runtime
from fabric_runner import run_fabric_batches
from generator import RunConfig, canonical_bytes
from job_lifecycle import LifecycleBlocked
from test_deploy import (IDENTITY, LAKEHOUSE, NOTEBOOK, PIPELINE, TENANT, WORKSPACE,
                         config, owned_state, run_data)
from test_fabric_ledger import MemoryStore, TEST_TARGETS
from test_fabric_runner import Adapter, EventhouseAdapter, FakeClock
from test_recovery_cli import CHILD, JOB


class FreshAdmissionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.state = self.root / "shared"
        self.demo1_state = self.root / "demo1"
        self.demo2_state = self.root / "demo2"
        self.demo3_state = self.root / "demo3"
        self.state.mkdir()
        self.demo1_state.mkdir()
        self.demo2_state.mkdir()
        self.demo3_state.mkdir()
        self.document = {
            "data": {"run_id": "run-1", "max_duration_seconds": 60},
            "execution": {"track_timeout_seconds": 5, "verification_timeout_seconds": 20},
            "demo-1": {"deployment_prefix": "original-one", "deployment_state_dir": str(self.demo1_state)},
            "demo-2": {"deployment_prefix": "original", "deployment_state_dir": str(self.demo2_state),
                       "reuse_synthetic_target": True},
            "demo-3": {"deployment_prefix": "original-three", "deployment_state_dir": str(self.demo3_state)},
        }
        self.config_path = self.root / "config.json"
        self.config_path.write_bytes(canonical_bytes(self.document))
        deadline = (datetime.now(timezone.utc) + timedelta(hours=3)).isoformat()
        self.helper = SimpleNamespace(
            deployment_scope=mock.Mock(side_effect=lambda config, track, prefix, state: {
                "demo-1": ("original-one", self.demo1_state),
                "demo-2": ("original", self.demo2_state),
                "demo-3": ("original-three", self.demo3_state),
            }[track]),
            monitor_config=mock.Mock(return_value=("manifest", 20000)),
            monitor_binding=mock.Mock(return_value={"bound": "run"}),
            monitor_evidence=mock.Mock(side_effect=lambda path, run, uri, duration, state, binding:
                                       {"state": state, "deadline_utc": deadline}),
            matching_monitor_identity=mock.Mock(),
        )
        self.calls = []

    def invoke(self, validator=None):
        def accepted(track, mode, path, prefix, state, directory, timeout, selected):
            self.calls.append((track, mode, prefix, Path(state)))
            if validator:
                validator(track, mode)

        with (mock.patch.object(deploy, "_monitor_contract", return_value=self.helper),
              mock.patch.object(deploy, "_invoke_fresh", side_effect=accepted)):
            deploy._fresh_admission(self.document, self.config_path, "sample", self.state, self.root,
                                    ["demo-1", "demo-2", "demo-3"], lambda: self.calls.append(("token",)))

    def test_selected_validators_and_running_monitor_use_original_demo2_scope(self):
        self.invoke()
        checks = set(self.calls[:4])
        self.assertEqual(checks, {("demo-1", "validate-only", "original-one", self.demo1_state),
                                  ("demo-2", "validate-only", "original", self.demo2_state),
                                  ("demo-3", "validate-only", "original-three", self.demo3_state),
                                  ("shared", "validate-only", "sample", self.state)})
        self.assertEqual(self.calls[4:], [("token",),
                                         ("demo-2", "monitor-status", "original", self.demo2_state)])
        self.assertEqual([call.args[4] for call in self.helper.monitor_evidence.call_args_list],
                         ["start", "running"])
        self.helper.matching_monitor_identity.assert_called_once()
        self.assertTrue(self.helper.deployment_scope.call_args.args[0]["demo-2"]["reuse_synthetic_target"])

    def test_failed_validator_or_changed_config_prevents_monitor_and_token(self):
        def fail(track, mode):
            if track == "demo-1":
                raise deploy.Blocked("not ready")

        with self.assertRaisesRegex(deploy.Blocked, "not ready"):
            self.invoke(fail)
        self.assertNotIn(("token",), self.calls)
        self.assertFalse(any(len(call) > 1 and call[1] == "monitor-status" for call in self.calls))
        self.calls.clear()

        def mutate(track, mode):
            if track == "demo-1":
                self.config_path.write_text('{"changed":true}', encoding="utf-8")

        with self.assertRaisesRegex(deploy.Blocked, "configuration changed"):
            self.invoke(mutate)
        self.assertNotIn(("token",), self.calls)

    def test_admission_respects_configured_concurrency(self):
        self.document["execution"]["max_concurrency"] = 1
        self.config_path.write_bytes(canonical_bytes(self.document))
        executor = deploy.concurrent.futures.ThreadPoolExecutor
        with mock.patch.object(deploy.concurrent.futures, "ThreadPoolExecutor", wraps=executor) as pool:
            self.invoke()
        pool.assert_called_once_with(max_workers=1)
        self.assertEqual([call[0] for call in self.calls[:4]], ["demo-1", "demo-2", "demo-3", "shared"])

        for value in (0, 5, True, "1"):
            self.document["execution"]["max_concurrency"] = value
            with self.subTest(value=value), self.assertRaisesRegex(deploy.ContractError, "max_concurrency"):
                deploy._admission_concurrency(self.document)

    def test_monitor_must_be_running_and_cover_execution_and_verification(self):
        self.helper.monitor_evidence.side_effect = ValueError("monitor is not running")
        with self.assertRaisesRegex(deploy.Blocked, "not running"):
            self.invoke()
        short_deadline = (datetime.now(timezone.utc) + timedelta(seconds=1000)).isoformat()
        self.helper.monitor_evidence.side_effect = lambda *args: {"deadline_utc": short_deadline}
        with self.assertRaisesRegex(deploy.Blocked, "does not cover"):
            self.invoke()

    def test_owner_scope_rejects_missing_or_unpaired_journal(self):
        helper = deploy._monitor_contract()
        self.assertEqual(helper.deployment_scope({}, "demo-3", "sample", self.state),
                         ("sample", self.state))
        for settings in ({"deployment_prefix": "chanel"},
                         {"deployment_state_dir": str(self.demo1_state)},
                         {"deployment_prefix": "chanel",
                          "deployment_state_dir": str(self.root / "missing")}):
            with self.subTest(settings=settings), self.assertRaises(ValueError):
                helper.deployment_scope({"demo-3": settings}, "demo-3", "sample", self.state)

    def test_validator_result_is_strict_and_requires_ready(self):
        script = self.root / "track.py"
        script.touch()
        result = {"contract_version": 1, "track": "demo-1", "mode": "validate-only",
                  "status": "pass", "ready": True, "diagnostics": []}

        def completed(args, **kwargs):
            Path(args[args.index("--result") + 1]).write_text(json.dumps(result), encoding="utf-8")
            return SimpleNamespace(returncode=0 if result["status"] == "pass" else 1)

        with (mock.patch.dict(deploy.TRACK_SCRIPTS, {"demo-1": script}),
              mock.patch.object(deploy.subprocess, "run", side_effect=completed)):
            self.assertEqual(deploy._invoke_fresh("demo-1", "validate-only", self.config_path, "sample",
                                                  self.state, self.root, 5, ["demo-1"]), result)
            result["ready"] = False
            with self.assertRaisesRegex(deploy.Blocked, "not ready"):
                deploy._invoke_fresh("demo-1", "validate-only", self.config_path, "sample",
                                     self.state, self.root, 5, ["demo-1"])
            result.update(status="blocked", diagnostics=["Verification timeout: 900 seconds.",
                                                         "Azure CLI could not acquire a Fabric token."])
            with self.assertRaisesRegex(deploy.Blocked, "Azure CLI could not acquire a Fabric token"):
                deploy._invoke_fresh("demo-1", "validate-only", self.config_path, "sample",
                                     self.state, self.root, 5, ["demo-1"])
            result["extra"] = "unexpected"
            with self.assertRaisesRegex(deploy.Blocked, "invalid result"):
                deploy._invoke_fresh("demo-1", "validate-only", self.config_path, "sample",
                                     self.state, self.root, 5, ["demo-1"])

    def test_demo2_admission_requires_existing_proof(self):
        script = self.root / "track.py"
        script.touch()
        result = {"contract_version": 1, "track": "demo-2", "mode": "validate-only",
                  "status": "pass", "ready": True, "diagnostics": []}

        def completed(args, **kwargs):
            self.assertIn("--require-existing-proof", args)
            Path(args[args.index("--result") + 1]).write_text(json.dumps(result), encoding="utf-8")
            return SimpleNamespace(returncode=0)

        with (mock.patch.dict(deploy.TRACK_SCRIPTS, {"demo-2": script}),
              mock.patch.object(deploy.subprocess, "run", side_effect=completed)):
            deploy._invoke_fresh("demo-2", "validate-only", self.config_path, "sample",
                                 self.state, self.root, 5, ["demo-1", "demo-2"])


class MeasuredSubmissionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.state_dir = Path(self.temp.name) / "state"
        self.document = config()
        self.document["data"] = run_data()
        self.document["shared"].update(workspace_id=WORKSPACE, staging_lakehouse_id=LAKEHOUSE,
                                       target_bindings={"demo-1": TEST_TARGETS["demo-1"]})
        self.plan = deploy.parse_config(self.document, "sample")
        self.state = owned_state(self.plan)
        deploy.StateStore(self.state_dir).write(self.state)

    def invoke(self):
        return deploy._run_measured(self.document, self.plan, str(self.state_dir), "demo-1")

    def test_failed_admission_does_not_create_intent_or_submit(self):
        with (mock.patch.object(deploy, "_fresh_admission", side_effect=deploy.Blocked("not ready")),
              mock.patch.object(deploy, "FabricClient") as client):
            with self.assertRaisesRegex(deploy.Blocked, "not ready"):
                self.invoke()
        client.assert_not_called()
        self.assertEqual(list((self.state_dir / "producer-jobs-v1").glob("*.json")), [])

    def test_serial_admission_budget_covers_each_validator(self):
        self.document["execution"] = {"max_concurrency": 1, "track_timeout_seconds": 600}
        start = datetime.now(timezone.utc)
        with (mock.patch.object(deploy, "PipelineJob", wraps=deploy.PipelineJob) as job,
              mock.patch.object(deploy, "_fresh_admission", side_effect=deploy.Blocked("not ready"))):
            with self.assertRaisesRegex(deploy.Blocked, "not ready"):
                self.invoke()
        binding = job.call_args.args[1]
        deadline = datetime.fromisoformat(binding["deadline_utc"])
        expected = 2 * 600 + self.document["data"]["max_duration_seconds"] + \
            deploy.STARTUP_HEADROOM_SECONDS + deploy.STOP_CHILD_WAIT_SECONDS + deploy.AZURE_CLI_TOKEN_TIMEOUT_SECONDS
        self.assertGreaterEqual((deadline - start).total_seconds(), expected)
        self.assertLess((deadline - start).total_seconds(), expected + 5)

    def test_one_bound_submission_is_journaled_and_never_resubmitted(self):
        client = mock.Mock()
        location = (f"https://api.fabric.microsoft.com/v1/workspaces/{WORKSPACE}/items/{PIPELINE}/"
                    f"jobs/instances/{JOB}")
        client.request.return_value = (202, {"Location": location, "Retry-After": "1"}, None)

        def admission(config, path, prefix, state, directory, selected, token_ready):
            token_ready()

        with (mock.patch.object(deploy, "_fresh_admission", side_effect=admission),
              mock.patch.object(deploy, "azure_cli_token", return_value="token"),
              mock.patch.object(deploy, "FabricClient", return_value=client),
              mock.patch.object(deploy, "_wait_for_measured", return_value=["verified"]) as wait,
              redirect_stderr(io.StringIO())):
            self.assertEqual(self.invoke(), ["verified"])
            with self.assertRaises(LifecycleBlocked):
                self.invoke()
        self.assertEqual(client.request.call_count, 1)
        wait.assert_called_once()
        journal = next((self.state_dir / "producer-jobs-v1").glob("*.json"))
        record = json.loads(journal.read_text(encoding="utf-8"))
        self.assertEqual((record["phase"], record["job_id"]), ("submitted", JOB))
        self.assertEqual(record["binding"]["selected_demos"], ["demo-1"])

    def test_oversized_activity_timeout_is_rejected_before_admission(self):
        self.document["data"]["max_duration_seconds"] = 7 * 86400 - 899
        with mock.patch.object(deploy, "_fresh_admission") as admission:
            with self.assertRaisesRegex(deploy.ContractError, "activity timeout"):
                self.invoke()
        admission.assert_not_called()
        with self.assertRaisesRegex(ValueError, "pipeline timeout"):
            fabric_runtime.run_config(self.document)

    def test_measured_wrapper_pins_timeout_to_the_validated_duration(self):
        self.document["data"]["max_duration_seconds"] = 1800
        rendered = deploy._parts(deploy._definition(self.state, "pipeline", self.document))["pipeline-content.json"]
        properties = rendered["properties"]
        self.assertEqual(properties["activities"][0]["policy"]["timeout"], "0.00:45:00")
        self.assertEqual(properties["parameters"]["activity_timeout"]["defaultValue"], "0.00:45:00")
        self.document["data"]["max_duration_seconds"] = 3600
        changed = deploy._parts(deploy._definition(self.state, "pipeline", self.document))["pipeline-content.json"]
        self.assertEqual(changed["properties"]["activities"][0]["policy"]["timeout"], "0.01:15:00")
        self.assertNotEqual(rendered, changed)
        with mock.patch.object(deploy, "_read_definition", return_value=(None, {"pipeline-content.json": rendered})):
            with self.assertRaisesRegex(deploy.Blocked, "Live item definition differs"):
                deploy._verify_definition(None, self.state, "pipeline", self.document)
        for invalid in (None, 0, True, 7 * 86400):
            with self.subTest(invalid=invalid):
                self.document["data"]["max_duration_seconds"] = invalid
                with self.assertRaises(deploy.Blocked):
                    deploy._definition(self.state, "pipeline", self.document)

    def test_native_wrapper_defaults_pin_one_run_without_publishing_desktop_config(self):
        target = TEST_TARGETS["demo-1"]
        self.document["demo-1"] = {
            "landing_uri": (f"abfss://{target['workspace_id']}@onelake.dfs.fabric.microsoft.com/"
                            f"{target['item_id']}/Files/raw"), "deployment_state_dir": "do-not-publish"}
        expiry = datetime(2030, 1, 1, tzinfo=timezone.utc)
        self.document["shared"].update(native_launch=True, native_selected_demos="demo-1",
                                       producer_notebook_id=NOTEBOOK, native_launch_expires_at_utc=expiry.isoformat())
        self.document["execution"] = {"desktop_only": "do-not-publish"}
        rendered = deploy._parts(deploy._definition(self.state, "pipeline", self.document))["pipeline-content.json"]
        properties = rendered["properties"]
        self.assertEqual(properties["parameters"]["operation"]["defaultValue"], "run-native")
        self.assertEqual(properties["parameters"]["run_config_json"]["defaultValue"], "{}")
        activity = properties["activities"][0]["typeProperties"]
        self.assertNotIn("do-not-publish", json.dumps(rendered))
        profile = json.loads(activity["parameters"]["native_profile_json"]["value"])
        self.assertEqual(properties["activities"][0]["policy"]["timeout"], profile["activity_timeout"])
        document, selected, binding, _, _ = fabric_runtime.native_parameters(
            profile, now=expiry - timedelta(hours=1))
        self.assertEqual(selected, ["demo-1"])
        self.assertEqual(binding["producer"]["notebook_id"], NOTEBOOK)
        run, _ = fabric_runtime.run_config(document)
        self.assertEqual((run.run_id, run.file_count, run.rows_per_file, run.interval_seconds), ("run-1", 10, 1000, 60))
        with self.assertRaisesRegex(deploy.Blocked, "reserved"):
            self.invoke()
        with self.assertRaisesRegex(ValueError, "expired"):
            fabric_runtime.native_parameters(profile, now=expiry - timedelta(minutes=1))
        profile["document"]["data"]["run_id"] = "unbound"
        with self.assertRaisesRegex(ValueError, "run binding"):
            fabric_runtime.native_parameters(profile, now=expiry - timedelta(hours=1))
        blocked = deploy._parts(deploy._definition(self.state, "pipeline"))["pipeline-content.json"]
        self.assertEqual(blocked["properties"]["parameters"]["operation"]["defaultValue"], "blocked")
        with self.assertRaisesRegex(ValueError, "No prepared"):
            fabric_runtime.native_parameters({}, now=expiry)

    def test_reusable_profile_is_stable_across_clicks_and_has_no_static_expiry(self):
        target = TEST_TARGETS["demo-1"]
        self.document["demo-1"] = {"workspace_id": target["workspace_id"], "lakehouse_id": target["item_id"],
                                   "lakehouse_name": "demo_lakehouse",
                                   "landing_uri": (f"abfss://{target['workspace_id']}@onelake.dfs.fabric.microsoft.com/"
                                                   f"{target['item_id']}/Files/raw")}
        self.document["shared"].update(native_launch=True, native_reusable=True, native_selected_demos="demo-1",
                                       producer_notebook_id=NOTEBOOK,
                                       notebook_default_lakehouse={"workspace_id": target["workspace_id"],
                                                                   "lakehouse_id": target["item_id"],
                                                                   "lakehouse_name": "demo_lakehouse"})
        self.document["data"]["run_id"] = None
        first = deploy._parts(deploy._definition(self.state, "pipeline", self.document))["pipeline-content.json"]
        self.assertIsNone(self.document["data"]["run_id"])
        self.document["data"]["run_id"] = "previous-template-id-does-not-control-click"
        second = deploy._parts(deploy._definition(self.state, "pipeline", self.document))["pipeline-content.json"]
        self.assertEqual(first, second)
        del self.document["data"]["run_id"]
        omitted = deploy._parts(deploy._definition(self.state, "pipeline", self.document))["pipeline-content.json"]
        self.assertEqual(first, omitted)
        self.assertNotIn("run_id", self.document["data"])
        profile = json.loads(first["properties"]["activities"][0]["typeProperties"]["parameters"][
            "native_profile_json"]["value"])
        self.assertEqual(profile["version"], 2)
        self.assertEqual(first["properties"]["activities"][0]["policy"]["timeout"], "0.02:15:00")
        self.assertEqual(profile["activity_timeout"], "0.02:15:00")
        wrong_timeout = dict(profile, activity_timeout="0.00:15:00")
        with self.assertRaisesRegex(ValueError, "authorized window"):
            fabric_assets.pipeline_definition(WORKSPACE, NOTEBOOK, native_profile=wrong_timeout)
        self.assertNotIn("binding", profile)
        self.assertNotIn("expires_at_utc", profile)
        self.assertEqual(profile["document"]["data"]["run_id"], "native_template")
        self.assertEqual(profile["document"]["demo-1"]["lakehouse_name"], "demo_lakehouse")
        self.assertEqual(profile["document"]["shared"]["notebook_default_lakehouse"],
                         self.document["shared"]["notebook_default_lakehouse"])
        now = datetime(2030, 1, 1, tzinfo=timezone.utc)
        first_run = fabric_runtime.native_parameters(profile, now=now)
        next_run = fabric_runtime.native_parameters(profile, now=now + timedelta(days=1))
        self.assertNotEqual(first_run[0]["data"]["run_id"], next_run[0]["data"]["run_id"])
        self.assertEqual(datetime.fromisoformat(next_run[4]) - datetime.fromisoformat(first_run[4]), timedelta(days=1))

        self.document["data"]["run_id"] = None
        self.document["data"]["seed"] = None
        with self.assertRaisesRegex(deploy.ContractError, "all data parameters"):
            deploy._native_profile(self.document, self.state["resources"])
        self.document["data"]["seed"] = 42
        with self.assertRaisesRegex(deploy.ContractError, "all data parameters"):
            deploy._validate_run(self.document, "demo-1")
        self.document["shared"]["native_reusable"] = False
        with self.assertRaisesRegex(deploy.ContractError, "all data parameters"):
            deploy._native_profile(self.document, self.state["resources"])


class ManifestReconciliationTests(unittest.TestCase):
    def completed(self, destination):
        store, clock = MemoryStore(), FakeClock()
        run = RunConfig("reconcile", date_count=4, products=3, customers=3,
                        file_count=1, rows_per_file=1, interval_seconds=0)
        adapter = EventhouseAdapter(clock, "succeeded") if destination == "demo-2" else Adapter(clock)
        target = {destination: TEST_TARGETS[destination]}
        result = run_fabric_batches(store, run, {destination: adapter}, 10, 1, selected_targets=target,
                                    clock=clock.clock, sleep=clock.sleep)
        self.assertTrue(result["verified"])
        document = {"data": {"run_id": run.run_id, "seed": run.seed, "start_date": run.start_date,
                             "date_count": run.date_count, "product_count": run.products,
                             "customer_count": run.customers, "file_count": run.file_count,
                             "rows_per_file": run.rows_per_file, "interval_seconds": run.interval_seconds,
                             "max_duration_seconds": 10, "currency": "USD"},
                    "shared": {"target_bindings": target}}
        return store, document

    def change_manifest(self, store, change):
        path = "Files/producer/runs/reconcile/manifest.json"
        raw, etag = store.read(path)
        manifest = json.loads(raw)
        change(manifest)
        store.files[path] = (json.dumps(manifest).encode("utf-8"), etag)

    def test_verified_manifest_and_partial_unknown_delivery(self):
        store, document = self.completed("demo-1")
        self.assertTrue(deploy._manifest_summary(store, document, ["demo-1"])[0])
        self.change_manifest(store, lambda manifest: manifest["batches"]["1"]["deliveries"]["demo-1"]
                             .update(status="unknown"))
        verified, diagnostics = deploy._manifest_summary(store, document, ["demo-1"])
        self.assertFalse(verified)
        self.assertTrue(any("unknown" in item for item in diagnostics))

    def test_invalid_verification_or_staged_bytes_never_pass(self):
        for field, value in (("verified_row_count", True), ("verified_at_utc", "invalid")):
            with self.subTest(field=field):
                store, document = self.completed("demo-1")
                self.change_manifest(store, lambda manifest: manifest["batches"]["1"]["deliveries"]["demo-1"]
                                     .update({field: value}))
                with self.assertRaisesRegex(deploy.Blocked, "invalid.*verification"):
                    deploy._manifest_summary(store, document, ["demo-1"])
        store, document = self.completed("demo-1")
        staged = next(path for path in store.files if path.endswith(".csv"))
        store.files[staged] = (b"tampered", store.files[staged][1])
        with self.assertRaisesRegex(deploy.Blocked, "checksum"):
            deploy._manifest_summary(store, document, ["demo-1"])

    def test_eventhouse_source_identity_must_match_submission(self):
        store, document = self.completed("demo-2")
        self.assertTrue(deploy._manifest_summary(store, document, ["demo-2"])[0])
        self.change_manifest(store, lambda manifest: manifest["batches"]["1"]["deliveries"]["demo-2"]
                             ["ingestion_outcome"].update(source_id="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"))
        with self.assertRaisesRegex(deploy.Blocked, "invalid Eventhouse"):
            deploy._manifest_summary(store, document, ["demo-2"])


class MeasuredRecoveryTests(unittest.TestCase):
    def job(self, snapshots):
        job = mock.Mock()
        job.binding = {"deadline_utc": (datetime.now(timezone.utc) + timedelta(minutes=5)).isoformat(),
                       "workspace_id": WORKSPACE}
        job.status_detail.side_effect = snapshots
        return job

    def snapshot(self, parent, child_id=CHILD, child_status="InProgress"):
        child = {"job_id": child_id, "status": child_status} if child_id else None
        return {"job_id": JOB, "status": parent, "notebook": child, "notebook_diagnostic": "not observed"}

    def test_parent_completion_waits_for_same_terminal_child(self):
        job = self.job([self.snapshot("Completed"), self.snapshot("Completed", child_status="Completed")])
        plan = {"tenant_id": TENANT, "identity_id": IDENTITY}
        with (mock.patch.object(deploy.time, "sleep"),
              mock.patch.object(deploy, "azure_cli_token", return_value="token"),
              mock.patch.object(deploy, "OneLakeStore"),
              mock.patch.object(deploy, "_manifest_summary", return_value=(True, ["landing verified"]))):
            result = deploy._wait_for_measured(job, JOB, {}, ["demo-1"], plan, LAKEHOUSE)
        self.assertEqual(job.status_detail.call_count, 2)
        self.assertTrue(any("notebook job" in item and CHILD in item for item in result))

    def test_changing_child_or_terminal_parent_fails_closed(self):
        for second in (self.snapshot("Completed", "77777777-7777-4777-8777-777777777777", "Completed"),
                       self.snapshot("Failed", child_status="Completed")):
            with self.subTest(second=second):
                job = self.job([self.snapshot("Completed"), second])
                with mock.patch.object(deploy.time, "sleep"):
                    with self.assertRaisesRegex(deploy.Blocked, "changed"):
                        deploy._wait_for_measured(job, JOB, {}, ["demo-1"],
                                                  {"tenant_id": TENANT, "identity_id": IDENTITY}, LAKEHOUSE)

    def test_stop_marker_is_bound_and_idempotent(self):
        store = MemoryStore()
        binding = {"run_id": "run-1", "nonce": "run-abc", "config_sha256": "a" * 64}
        deploy._write_stop_request(store, binding)
        deploy._write_stop_request(store, binding)
        self.assertTrue(fabric_runtime.stop_requested(store, "run-1", "run-abc", "a" * 64))
        with self.assertRaisesRegex(deploy.Blocked, "different"):
            deploy._write_stop_request(store, {**binding, "nonce": "run-other"})
        with self.assertRaisesRegex(RuntimeError, "differs"):
            fabric_runtime.stop_requested(store, "run-1", "run-other", "a" * 64)


class MeasuredNotebookTests(unittest.TestCase):
    def test_unprepared_native_operation_does_not_acquire_token_or_stage(self):
        notebook = json.loads((Path(__file__).parent / "notebook.ipynb").read_text(encoding="utf-8"))
        source = "".join(notebook["cells"][1]["source"])
        token = mock.Mock()
        namespace = {"operation": "run-native", "native_profile_json": "{}", "run_config_json": "not JSON",
                     "notebookutils": SimpleNamespace(credentials=SimpleNamespace(getToken=token)),
                     "spark": SimpleNamespace(conf=SimpleNamespace(set=lambda *args: None))}
        with mock.patch("fabric_runner.run_fabric_batches") as run:
            with self.assertRaisesRegex(ValueError, "No prepared"):
                exec(compile(source, "notebook.ipynb", "exec"), namespace)
        token.assert_not_called()
        run.assert_not_called()

    def test_pipeline_run_without_context_run_id_reports_verified_delivery(self):
        notebook = json.loads((Path(__file__).parent / "notebook.ipynb").read_text(encoding="utf-8"))
        source = "".join(notebook["cells"][1]["source"])
        claims = base64.urlsafe_b64encode(json.dumps({"tid": TENANT, "oid": IDENTITY}).encode()).decode().rstrip("=")
        exits = []
        notebookutils = SimpleNamespace(
            credentials=SimpleNamespace(getToken=lambda audience: "x." + claims + ".x"),
            runtime=SimpleNamespace(context={}),
            notebook=SimpleNamespace(exit=exits.append),
        )
        namespace = {"operation": "run-data", "probe_id": "run-" + "a" * 32,
                     "runtime_evidence_json": json.dumps({"tenant_id": TENANT, "identity_id": IDENTITY,
                                                          "config_sha256": "a" * 64}),
                     "run_config_json": "{}", "selected_demos_json": '["demo-1"]',
                     "spark": SimpleNamespace(conf=SimpleNamespace(set=lambda *args: None)),
                     "notebookutils": notebookutils}
        producer = {"workspace_id": WORKSPACE, "staging_lakehouse_id": LAKEHOUSE}
        with (mock.patch.object(fabric_runtime, "prepare_measured",
                                return_value=(SimpleNamespace(run_id="run-1"), 60, {}, producer)),
              mock.patch.object(fabric_runtime, "build_adapters", return_value={"demo-1": SimpleNamespace()}),
              mock.patch("fabric_storage.OneLakeStore"),
              mock.patch("fabric_runner.run_fabric_batches", return_value={"verified": True})):
            exec(compile(source, "notebook.ipynb", "exec"), namespace)
        result = json.loads(exits[0])
        self.assertEqual((result["run_id"], result["status"], result["producer_job_id"]),
                         ("run-1", "verified", None))

    def test_runtime_identity_failure_prevents_staging(self):
        notebook = json.loads((Path(__file__).parent / "notebook.ipynb").read_text(encoding="utf-8"))
        source = "".join(notebook["cells"][1]["source"])
        claims = base64.urlsafe_b64encode(json.dumps({"tid": TENANT, "oid": IDENTITY}).encode()).decode().rstrip("=")
        notebookutils = SimpleNamespace(
            credentials=SimpleNamespace(getToken=lambda audience: "x." + claims + ".x"),
            runtime=SimpleNamespace(context={}),
        )
        namespace = {"operation": "run-data", "probe_id": "run-" + "a" * 32,
                     "runtime_evidence_json": json.dumps({"tenant_id": TENANT, "identity_id": IDENTITY}),
                     "run_config_json": "{}", "selected_demos_json": '["demo-1"]',
                     "spark": SimpleNamespace(conf=SimpleNamespace(set=lambda *args: None)),
                     "notebookutils": notebookutils}
        with (mock.patch.object(fabric_runtime, "prepare_measured", side_effect=ValueError("runtime identity mismatch")),
              mock.patch("fabric_runner.run_fabric_batches") as run):
            with self.assertRaisesRegex(ValueError, "identity mismatch"):
                exec(compile(source, "notebook.ipynb", "exec"), namespace)
        run.assert_not_called()


if __name__ == "__main__":
    unittest.main()
