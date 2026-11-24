import json
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch
from uuid import UUID

import native_demo2 as native
from monitor_notebook_builder import build_monitor_notebook
from notebook_builder import build_notebook, fabric_definition
from test_monitor_controller import batch, verified_delivery


def uid(number):
    return str(UUID(int=number))


class Store:
    def __init__(self):
        self.files = {}
        self.version = 0

    def read(self, path):
        if path not in self.files:
            raise FileNotFoundError(path)
        return self.files[path]

    def create(self, path, content):
        assert path not in self.files
        return self.replace(path, content, None)

    def replace(self, path, content, etag):
        assert self.files.get(path, (None, None))[1] == etag
        self.version += 1
        self.files[path] = (content, str(self.version))
        return str(self.version)

    def delete(self, path, etag):
        assert self.files[path][1] == etag
        del self.files[path]


class NativeDemo2Tests(unittest.TestCase):
    def setUp(self):
        self.now = datetime(2026, 11, 24, tzinfo=timezone.utc)
        self.store, self.target = Store(), Store()
        self.request = Mock()
        self.addCleanup(patch.stopall)
        patch.dict(sys.modules, generator=SimpleNamespace(generator_source_sha256=lambda: "a" * 64)).start()
        self.context = {
            "document": {"tenant_id": uid(1), "shared": {
                "identity_id": uid(2), "workspace_id": uid(3), "staging_lakehouse_id": uid(4),
                "target_bindings": {"demo-2": {"workspace_id": uid(5), "item_id": uid(6),
                                               "fact": "SyntheticFactSales"}},
            }, "demo-2": {"workspace_id": uid(5), "lakehouse_id": uid(7), "verifier_notebook_id": uid(8),
                          "monitor_notebook_id": uid(9), "sales_table": "SyntheticFactSales",
                          "verification_timeout_seconds": 900, "poll_interval_seconds": 10,
                          "max_concurrent_batches": 2},
                "data": {"run_id": "native_demo_1", "seed": 42, "start_date": "2026-01-01",
                         "date_count": 365, "product_count": 20, "customer_count": 100, "rows_per_file": 1000,
                         "file_count": 10, "interval_seconds": 60, "max_duration_seconds": 1800}},
            "store": self.store, "store_factory": lambda ws, lh: self.target,
            "manifest_path": "Files/producer/runs/native_demo_1/manifest.json",
            "clock": lambda: self.now, "sleep": lambda _: None, "stop_requested": lambda: False,
            "expires_at_utc": self.now + timedelta(hours=3),
            "verification_deadline_utc": self.now + timedelta(hours=2),
            "preparation_deadline_utc": self.now + timedelta(minutes=15),
            "cleanup_deadline_utc": self.now + timedelta(hours=3),
            "fabric": SimpleNamespace(request=self.request),
        }

    def run_with_journal(self):
        run = native._Run(self.context, "preparation")
        run.load()
        run.record = {"contract_version": 1, "binding": run.binding, "controller_id": run.cfg["controller_id"],
                      "jobs": {}, "cancel_intents": {}}
        run.save()
        return run

    def test_lost_submission_response_is_journaled_and_blocks_stop(self):
        run = self.run_with_journal()

        def uncertain(*args, **kwargs):
            record = json.loads(self.store.read(run.journal)[0])
            self.assertIsNone(record["jobs"]["monitor"]["job_id"])
            raise TimeoutError

        self.request.side_effect = uncertain
        with self.assertRaises(TimeoutError):
            run.start("monitor", run.params)
        with self.assertRaisesRegex(native.monitor.MonitorError, "uncertain submission"):
            native.stop(self.context, None)
        self.assertEqual(self.request.call_count, 1)

    def test_stop_uses_only_journaled_jobs_and_conditional_lock_release(self):
        run = self.run_with_journal()
        run.record["jobs"]["monitor"] = {"job_id": uid(10)}
        run.save()
        state = {k: run.cfg[k] for k in ("run_id", "controller_id", "tenant_id", "identity_id",
                                        "generator_sha256", "monitor_binding_sha256")}
        state.update(contract_version=1, manifest_uri=run.cfg["manifest_url"], deadline_utc=run.params["DEADLINE_UTC"],
                     state="running", sentinel_verified=True, job_access_verified=True,
                     batches={"x": {"status": "running", "job_id": uid(11)}})
        self.target.create(run.state, native.monitor._canonical_json(state))
        lock = {"contract_version": 1, "run_id": run.cfg["run_id"], "controller_id": run.cfg["controller_id"]}
        self.target.create(run.lock, native.monitor._canonical_json(lock))
        def completed(item, job):
            return 200, {}, {"id": uid(job), "itemId": uid(item), "status": "Cancelled"}
        self.request.side_effect = [completed(9, 10), completed(8, 11)]
        self.context["stop_requested"] = lambda: True
        self.assertEqual(native.stop(self.context, None)["status"], "stopped")
        self.assertNotIn(run.lock, self.target.files)
        self.assertTrue(all(call.args[0] == "GET" for call in self.request.call_args_list))

    def test_cleanup_does_not_infer_ownership_from_missing_journal(self):
        run = native._Run(self.context, "cleanup")
        self.target.create(run.lock, b"{}")
        with self.assertRaisesRegex(native.monitor.MonitorError, "without a job journal"):
            native.stop(self.context, None)
        self.assertIn(run.lock, self.target.files)
        self.request.assert_not_called()

    def test_binding_changes_with_auto_allocated_run_id(self):
        first = native._Run(self.context, "preparation")
        self.context["document"]["data"]["run_id"] = "native_demo_2"
        self.context["manifest_path"] = "Files/producer/runs/native_demo_2/manifest.json"
        second = native._Run(self.context, "preparation")
        self.assertEqual(first.items, second.items)
        self.assertNotEqual(first.binding["binding"], second.binding["binding"])
        self.assertTrue(second.cfg["manifest_url"].endswith("/native_demo_2/manifest.json"))

    def test_prepare_requires_exact_definitions_and_a_confirmed_running_monitor(self):
        doc = self.context["document"]
        data = {key: doc["data"][key] for key in ("seed", "start_date", "date_count", "product_count",
                                                  "customer_count")}
        data["run_id"] = None
        source = "def generator_source_sha256(): pass\n"
        with tempfile.TemporaryDirectory() as directory:
            generator = Path(directory) / "generator.py"
            generator.write_text(source, encoding="utf-8")
            verifier = build_notebook(generator, workspace_id=uid(5), lakehouse_id=uid(7),
                                      lakehouse_name="SalesLakehouse", shortcut_name="EventhouseFactSales",
                                      data=data, dimension_maintenance=True)
        digest = native.hashlib.sha256(source.encode()).hexdigest()
        sys.modules["generator"].generator_source_sha256 = lambda: digest
        target = doc["shared"]["target_bindings"]["demo-2"]
        controller = build_monitor_notebook(workspace_id=uid(5), lakehouse_id=uid(7),
                                            lakehouse_name="SalesLakehouse", reused_synthetic_target=target)
        definitions = [fabric_definition(x) for x in (verifier, controller)]
        doc["demo-2"].update(verifier_definition_sha256=native.definition_sha256(definitions[0]),
                             monitor_definition_sha256=native.definition_sha256(definitions[1]),
                             verification_batch={"batch_id": "bootstrap_0001", "sha256": "b" * 64,
                                                 "row_count": 5, "sales_amount_minor_units": 42,
                                                 "amount_by_date": {"2026-01-01": 42},
                                                 "amount_by_product_category": {"Fashion": 42},
                                                 "amount_by_customer_region": {"Europe": 42}})
        self.context["producer_access_verified"] = True
        def read_job(run, item, job_id):
            if item == uid(8):
                return {}, {"status": "Completed", "properties": {"exitValue": {
                    "mode": "verify_batch", "status": "verified",
                    "batch": {"batch_id": "bootstrap_0001", "row_count": 5, "sales_amount_minor_units": 42},
                }}}
            return {}, {"status": "InProgress"}

        def state(run):
            return {"state": "running", "heartbeat_at_utc": native.monitor._stamp(self.now)}, "1"

        def submit(method, path, **kwargs):
            job = uid(10) if uid(8) in path else uid(11)
            item = uid(8) if job == uid(10) else uid(9)
            location = f"{native.monitor.FABRIC}/v1/workspaces/{uid(5)}/items/{item}/jobs/instances/{job}"
            return 202, {"Location": location}, None

        self.request.side_effect = submit
        with patch.object(native._Run, "shortcut"), patch.object(native._Run, "kql"), \
                patch.object(native._Run, "read_definition", side_effect=definitions), \
                patch.object(native._Run, "job", read_job), patch.object(native._Run, "state_read", state), \
                patch.object(native._Run, "lock_read", return_value="2"):
            self.assertEqual(native.prepare(self.context), {"status": "ready", "run_id": "native_demo_1",
                                                            "monitor_job_id": uid(11)})
        self.assertEqual(self.request.call_count, 2)
        self.assertIn(uid(8), self.request.call_args_list[0].args[1])
        self.assertIn(uid(9), self.request.call_args_list[1].args[1])

    def test_verify_reconciles_manifest_with_batch_and_run_jobs(self):
        self.context["document"]["data"].update(file_count=1, rows_per_file=3)
        run = self.run_with_journal()
        run.record["jobs"]["monitor"] = {"job_id": uid(10)}
        run.save()
        run_id = run.cfg["run_id"]
        entry = batch(1, accepted=self.now)
        entry.update(batch_id=f"{run_id}_0001", row_count=3)
        delivery = verified_delivery(entry, timestamp=self.now)
        at = native.monitor._stamp(self.now)
        manifest = {"contract_version": 1, "run_id": run_id, "generator_sha256": run.cfg["generator_sha256"],
                    "config": run.cfg["producer_config"], "target_bindings": run.cfg["target_bindings"],
                    "destinations": ["demo-2"], "batches": {"1": entry},
                    "scheduler": {"status": "completed", "started_at_utc": at, "finished_at_utc": at,
                                  "max_duration_seconds": 1800, "result": {"verified": True, "errors": [],
                                  "deadline_utc": native.monitor._stamp(self.now + timedelta(seconds=1800))}}}
        self.store.create(run.manifest, native.monitor._canonical_json(manifest))
        state = {k: run.cfg[k] for k in ("run_id", "controller_id", "tenant_id", "identity_id",
                                        "generator_sha256", "monitor_binding_sha256")}
        state.update(contract_version=1, manifest_uri=run.cfg["manifest_url"], deadline_utc=run.params["DEADLINE_UTC"],
                     state="completed", sentinel_verified=True, job_access_verified=True, manifest_observed=True,
                     batches={entry["batch_id"]: {"status": "verified", "job_id": uid(11),
                              "operation_id": delivery["operation_id"], "fingerprint": native.monitor.batch_fingerprint(entry),
                              "deadline_utc": native.monitor._stamp(self.now + timedelta(seconds=900)),
                              "verified_at_utc": at}}, run_verification={"status": "verified", "job_id": uid(12)})
        self.target.create(run.state, native.monitor._canonical_json(state))
        summary = {"batch_ids": [entry["batch_id"]], "row_count": 3, "sales_amount_minor_units": 300}
        def completed(item, job, mode, **evidence):
            return 200, {}, {"id": uid(job), "itemId": uid(item), "status": "Completed",
                             "properties": {"exitValue": {"mode": mode, "status": "verified", **evidence}}}
        responses = [
            completed(9, 10, "monitor", run_id=run_id),
            completed(8, 11, "verify_batch", batch={key: entry[key] for key in
                      ("batch_id", "row_count", "sales_amount_minor_units")}),
            completed(8, 12, "verify_run_keys", **summary),
        ]
        self.request.side_effect = responses
        prepared = {"status": "ready", "run_id": run_id, "monitor_job_id": uid(10)}
        with patch.object(native._Run, "shortcut"), patch.object(native._Run, "kql") as kql:
            self.assertEqual(native.verify(self.context, prepared), {"status": "verified", "run_id": run_id, **summary})
            kql.assert_called_once_with(entry)
            self.request.side_effect = responses[:2] + [completed(8, 12, "verify_run_keys", **{**summary, "row_count": 2})]
            with self.assertRaisesRegex(native.monitor.MonitorError, "run evidence does not match"):
                native.verify(self.context, prepared)
