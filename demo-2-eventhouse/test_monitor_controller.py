import copy
import json
import unittest
import uuid
from datetime import datetime, timedelta, timezone

from monitor_controller import (
    Controller, FabricClient, MonitorError, RetryLater, settings, verify_delivery_evidence,
)
from monitor_notebook_builder import build_monitor_notebook


WORKSPACE = "aab2ed7b-fd2f-4696-84b3-b34ef931681f"
LAKEHOUSE = "04113b0b-8698-4a61-a69d-ad862e96b6e6"
PRODUCER_WORKSPACE = "48593f2f-d3ac-474b-ba96-08282d6f69df"
PRODUCER_LAKEHOUSE = "fca649c9-0297-4997-8e2d-8ac5338aaa1a"
NOTEBOOK = "1ce63551-cd56-4cf6-91ea-bc854aec7198"
IDENTITY = "c2934d7c-4724-4cef-a50e-2a5d89afb4a4"
TENANT = "4526cee1-4399-4e4b-9134-46fabd583527"
BASE = "https://onelake.blob.fabric.microsoft.com"
RUN = "test_run"
NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)


def stamp(value):
    return value.isoformat().replace("+00:00", "Z")


def parameters(count=1, concurrency=2):
    producer = f"{BASE}/{PRODUCER_WORKSPACE}/{PRODUCER_LAKEHOUSE}"
    return {
        "RUN_ID": RUN,
        "MANIFEST_URL": f"{producer}/Files/producer/runs/{RUN}/manifest.json",
        "SENTINEL_URL": f"{producer}/Files/preflight/producer-monitor-heartbeat-v1.txt",
        "CONTROLLER_ID": str(uuid.UUID(int=1)),
        "STATE_URL": f"{BASE}/{WORKSPACE}/{LAKEHOUSE}/Files/demo2-verification/runs/{RUN}/state.json",
        "DEADLINE_UTC": stamp(NOW + timedelta(hours=1)),
        "VERIFIER_WORKSPACE_ID": WORKSPACE,
        "VERIFIER_NOTEBOOK_ID": NOTEBOOK,
        "EXPECTED_IDENTITY_ID": IDENTITY,
        "EXPECTED_TENANT_ID": TENANT,
        "EXPECTED_BATCH_COUNT": count,
        "VERIFICATION_TIMEOUT_SECONDS": 30,
        "POLL_INTERVAL_SECONDS": 1,
        "MAX_CONCURRENT_BATCHES": concurrency,
    }


def batch(number, accepted=NOW):
    return {
        "batch_id": f"{RUN}_{number:04d}",
        "batch_number": number,
        "batch_class": "measured",
        "sha256": "a" * 64,
        "row_count": 2,
        "sales_amount_minor_units": 300,
        "amount_by_date": {"2026-01-01": 300},
        "amount_by_product_category": {"Beauty": 300},
        "amount_by_customer_region": {"Europe": 300},
        "deliveries": {
            "demo-2": {
                "status": "submitted",
                "submission_status": "submitted",
                "submission_started_at_utc": stamp(accepted),
                "submission_result_at_utc": stamp(accepted),
                "operation_id": str(uuid.UUID(int=number + 10)),
            }
        },
    }


def manifest(*batches, count=None):
    return {
        "contract_version": 1,
        "run_id": RUN,
        "config": {"run_id": RUN, "file_count": count or len(batches)},
        "destinations": ["demo-2"],
        "batches": {str(item["batch_number"]): item for item in batches},
        "scheduler": {"status": "running"},
    }


def verified_delivery(entry, timestamp=NOW):
    delivery = entry["deliveries"]["demo-2"]
    delivery.update(
        status="verified",
        verified_at_utc=stamp(timestamp),
        verified_row_count=entry["row_count"],
        verified_total_minor_units=entry["sales_amount_minor_units"],
        verified_distinct_sales_line_ids=entry["row_count"],
        ingestion_outcome={
            "status": "succeeded",
            "timestamp_utc": stamp(timestamp),
            "source_id": delivery["operation_id"],
            "service_operation_id": None,
            "error_code": None,
            "diagnostic": None,
        },
    )
    return delivery


class Clock:
    def __init__(self):
        self.now = NOW

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += timedelta(seconds=seconds)


class FakeClient:
    def __init__(self, cfg):
        self.cfg = cfg
        self.objects = {}
        self.manifest = None
        self.serial = 0
        self.started = []
        self.results = {}
        self.cancelled = []
        self.sentinel_checks = 0
        self.snapshots = []

    def preflight(self, cfg):
        self.verify_sentinel(cfg["sentinel_url"])

    def verify_sentinel(self, url):
        self.sentinel_checks += 1

    def prepare_directory(self, run_id):
        pass

    def create_lock(self, url, body):
        if url in self.objects:
            raise MonitorError("The controller lock is already owned")
        self.objects[url] = (body, "lock-etag")
        return "lock-etag"

    def read(self, url, *, missing=False):
        if url == self.cfg["manifest_url"]:
            if self.manifest is None:
                return None, None
            return json.dumps(self.manifest).encode("utf-8"), "manifest-etag"
        return self.objects.get(url, (None, None))

    def write(self, url, body, etag):
        previous, current = self.objects.get(url, (None, None))
        if current != etag:
            raise MonitorError("Conditional state write failed")
        self.serial += 1
        new_etag = f"etag-{self.serial}"
        self.objects[url] = (body, new_etag)
        self.snapshots.append(json.loads(body))
        return new_etag

    def delete_lock(self, url, etag):
        if self.objects.get(url, (None, None))[1] != etag:
            raise MonitorError("Wrong lock owner")
        del self.objects[url]

    def start_job(self, mode, value, timeout, poll):
        job_id = str(uuid.UUID(int=100 + len(self.started)))
        self.started.append((mode, copy.deepcopy(value), timeout, job_id))
        return job_id, 1

    def job(self, job_id):
        return self.results.get(job_id, {"status": "InProgress"}), 1

    def cancel_job(self, job_id):
        self.cancelled.append(job_id)


class MonitorControllerTests(unittest.TestCase):
    def setUp(self):
        self.params = parameters()
        self.cfg = settings(self.params, lakehouse_id=LAKEHOUSE)
        self.clock = Clock()
        self.client = FakeClient(self.cfg)
        self.controller = Controller(self.cfg, self.client, clock=self.clock, sleep=self.clock.advance)

    def test_notebook_packages_sources_and_binds_expected_lakehouse(self):
        notebook = build_monitor_notebook(
            workspace_id=WORKSPACE, lakehouse_id=LAKEHOUSE, lakehouse_name="SalesLakehouse"
        )
        self.assertEqual(notebook["metadata"]["trident"]["lakehouse"]["default_lakehouse"], LAKEHOUSE)
        self.assertEqual(notebook["cells"][0]["metadata"]["tags"], ["parameters"])
        for cell in notebook["cells"]:
            compile("".join(cell["source"]), "notebook", "exec")
        self.assertIn("Controller(cfg, FabricClient", "".join(notebook["cells"][1]["source"]))

    def test_rejects_untrusted_urls_and_invalid_timeouts(self):
        for field, value in (
            ("MANIFEST_URL", self.params["MANIFEST_URL"].replace(".com/", ".com.evil/")),
            ("SENTINEL_URL", self.params["SENTINEL_URL"] + "?token=bad"),
            ("STATE_URL", self.params["STATE_URL"].replace(LAKEHOUSE, PRODUCER_LAKEHOUSE)),
            ("VERIFICATION_TIMEOUT_SECONDS", True),
            ("MAX_CONCURRENT_BATCHES", 0),
        ):
            with self.subTest(field=field), self.assertRaises(MonitorError):
                settings({**self.params, field: value}, lakehouse_id=LAKEHOUSE)

    def test_missing_manifest_is_waiting_not_storage_proof(self):
        self.controller._begin()
        self.controller._tick()
        state = self.client.snapshots[-1]
        self.assertEqual(state["state"], "running")
        self.assertFalse(state["manifest_observed"])
        self.assertTrue(state["sentinel_verified"])
        self.assertGreaterEqual(self.client.sentinel_checks, 2)
        self.assertEqual(self.client.started, [])

    def test_submitted_batch_and_run_level_verification_reconcile_exactly(self):
        entry = batch(1)
        doc = manifest(entry)
        doc["batches"]["bootstrap"] = {"batch_id": "bootstrap_0001", "batch_class": "bootstrap"}
        self.client.manifest = doc
        self.controller._begin()
        self.controller._tick()
        mode, value, timeout, job_id = self.client.started[0]
        self.assertEqual(mode, "verify_batch")
        self.assertEqual(value["batch_id"], f"{RUN}_0001")
        self.assertEqual(timeout, 30)
        self.assertTrue(any(snapshot["batches"][f"{RUN}_0001"]["status"] == "submitting"
                            for snapshot in self.client.snapshots if snapshot["batches"]))

        self.clock.advance(1)
        self.client.results[job_id] = {
            "status": "Completed",
            "properties": {"exitValue": json.dumps({
                "mode": "verify_batch", "status": "verified",
                "batch": {"batch_id": f"{RUN}_0001", "row_count": 2,
                          "sales_amount_minor_units": 300},
            })},
        }
        verified_delivery(doc["batches"]["1"], self.clock())
        doc["scheduler"] = {"status": "completed", "finished_at_utc": stamp(self.clock()),
                            "result": {"verified": True, "errors": [],
                                       "deadline_utc": stamp(NOW + timedelta(minutes=5))}}
        self.controller._tick()
        self.assertEqual(self.client.started[1][0], "verify_run_keys")
        run_job = self.client.started[1][3]
        self.clock.advance(1)
        self.client.results[run_job] = {
            "status": "Completed",
            "properties": {"exitValue": json.dumps({
                "mode": "verify_run_keys", "status": "verified",
                "batch_ids": [f"{RUN}_0001"], "row_count": 2,
                "sales_amount_minor_units": 300,
            })},
        }
        self.controller._tick()
        self.assertEqual(self.controller.state["state"], "completed")
        self.assertEqual(self.controller.state["batches"][f"{RUN}_0001"]["status"], "verified")
        self.assertNotIn("bootstrap_0001", self.controller.state["batches"])

    def test_ambiguous_submission_uses_original_deadline_after_resolution(self):
        entry = batch(1)
        delivery = entry["deliveries"]["demo-2"]
        delivery["submission_status"] = "unknown"
        delivery["status"] = "unknown"
        delivery["operation_id"] = None
        self.client.manifest = manifest(entry)
        self.controller._begin()
        self.controller._tick()
        record = self.controller.state["batches"][entry["batch_id"]]
        self.assertEqual(record["status"], "awaiting_submission")
        self.assertEqual(record["deadline_utc"], stamp(NOW + timedelta(seconds=30)))
        self.assertEqual(self.client.started, [])
        self.clock.advance(10)
        delivery.update(submission_status="submitted", status="submitted",
                        operation_id=str(uuid.UUID(int=11)), submission_result_at_utc=stamp(self.clock()))
        self.controller._tick()
        self.assertEqual(record["deadline_utc"], stamp(NOW + timedelta(seconds=30)))
        self.assertEqual(self.client.started[0][2], 20)

    def test_concurrency_queue_does_not_extend_delivery_deadlines(self):
        self.params = parameters(count=2, concurrency=1)
        self.cfg = settings(self.params, lakehouse_id=LAKEHOUSE)
        self.client = FakeClient(self.cfg)
        self.controller = Controller(self.cfg, self.client, clock=self.clock, sleep=self.clock.advance)
        self.client.manifest = manifest(batch(1), batch(2), count=2)
        self.controller._begin()
        self.controller._tick()
        self.assertEqual(len(self.client.started), 1)
        self.clock.advance(30)
        with self.assertRaisesRegex(MonitorError, "exceeded its verification deadline"):
            self.controller._tick()
        self.assertEqual(len(self.client.started), 1)

    def test_disappearing_manifest_is_a_failure_not_an_unbounded_wait(self):
        self.client.manifest = manifest(batch(1))
        self.controller._begin()
        self.controller._tick()
        self.client.manifest = None
        with self.assertRaisesRegex(MonitorError, "manifest disappeared"):
            self.controller._tick()
        self.clock.advance(30)
        with self.assertRaisesRegex(MonitorError, "exceeded its verification deadline"):
            self.controller._tick()

    def test_spark_success_without_terminal_kusto_evidence_is_not_success(self):
        self.client.manifest = manifest(batch(1))
        self.controller._begin()
        self.controller._tick()
        job_id = self.client.started[0][3]
        self.client.results[job_id] = {
            "status": "Completed",
            "properties": {"exitValue": json.dumps({
                "mode": "verify_batch", "status": "verified",
                "batch": {"batch_id": f"{RUN}_0001", "row_count": 2,
                          "sales_amount_minor_units": 300},
            })},
        }
        self.clock.advance(1)
        self.controller._tick()
        self.assertEqual(self.controller.state["batches"][f"{RUN}_0001"]["status"], "spark_verified")
        self.assertEqual(self.controller.state["state"], "running")

    def test_completed_scheduler_requires_error_free_result_and_valid_deadline(self):
        for result in (
            {"verified": True, "errors": ["delivery failed"], "deadline_utc": stamp(NOW)},
            {"verified": True, "errors": [], "deadline_utc": "invalid"},
        ):
            with self.subTest(result=result):
                client = FakeClient(self.cfg)
                client.manifest = manifest(batch(1))
                client.manifest["scheduler"] = {
                    "status": "completed", "finished_at_utc": stamp(NOW), "result": result,
                }
                controller = Controller(self.cfg, client, clock=self.clock, sleep=self.clock.advance)
                controller._begin()
                with self.assertRaises(MonitorError):
                    controller._tick()
                self.assertEqual(client.started, [])

    def test_uncertain_job_submission_is_not_replayed_on_restart(self):
        self.client.manifest = manifest(batch(1))
        self.controller._begin()
        self.controller._tick()
        prior = json.loads(self.client.objects[self.cfg["state_url"]][0])
        prior["batches"][f"{RUN}_0001"]["status"] = "submitting"
        prior["batches"][f"{RUN}_0001"].pop("job_id")
        self.client.objects[self.cfg["state_url"]] = (json.dumps(prior).encode("utf-8"), "previous-etag")
        del self.client.objects[self.cfg["lock_url"]]
        restarted = Controller(self.cfg, self.client, clock=self.clock, sleep=self.clock.advance)
        with self.assertRaisesRegex(MonitorError, "uncertain"):
            restarted._begin()
        self.assertEqual(len(self.client.started), 1)
        self.assertEqual(json.loads(self.client.objects[self.cfg["state_url"]][0]), prior)

    def test_known_inflight_job_is_resumed_without_duplicate_submission(self):
        self.client.manifest = manifest(batch(1))
        self.controller._begin()
        self.controller._tick()
        original_job = self.client.started[0][3]
        old_deadline = self.controller.state["batches"][f"{RUN}_0001"]["deadline_utc"]
        del self.client.objects[self.cfg["lock_url"]]
        resumed_cfg = {**self.cfg, "controller_id": str(uuid.UUID(int=2))}
        resumed = Controller(resumed_cfg, self.client, clock=self.clock, sleep=self.clock.advance)
        resumed._begin()
        self.clock.advance(1)
        resumed._tick()
        record = resumed.state["batches"][f"{RUN}_0001"]
        self.assertEqual(record["job_id"], original_job)
        self.assertEqual(record["deadline_utc"], old_deadline)
        self.assertEqual(len(self.client.started), 1)

    def test_completed_run_is_not_restarted_and_temporary_lock_is_released(self):
        previous = {
            "contract_version": 1, "run_id": RUN, "manifest_uri": self.cfg["manifest_url"],
            "state": "completed", "batches": {},
        }
        self.client.objects[self.cfg["state_url"]] = (json.dumps(previous).encode("utf-8"), "old-etag")
        with self.assertRaisesRegex(MonitorError, "explicit reconciliation"):
            self.controller.run()
        self.assertNotIn(self.cfg["lock_url"], self.client.objects)
        self.assertEqual(json.loads(self.client.objects[self.cfg["state_url"]][0]), previous)
        self.assertEqual(self.client.started, [])

    def test_failure_retains_lock_and_requests_cancellation_of_owned_jobs(self):
        self.client.manifest = manifest(batch(1))

        def fail_producer_after_sleep(seconds):
            self.clock.advance(seconds)
            self.client.manifest["scheduler"] = {"status": "failed"}

        self.controller.sleep = fail_producer_after_sleep
        with self.assertRaisesRegex(MonitorError, "scheduler reported failure"):
            self.controller.run()
        record = self.controller.state["batches"][f"{RUN}_0001"]
        self.assertEqual(self.controller.state["state"], "failed")
        self.assertEqual(record["status"], "failed")
        self.assertEqual(self.client.cancelled, [record["job_id"]])
        self.assertTrue(record["cancel_requested"])
        self.assertIn(self.cfg["lock_url"], self.client.objects)

    def test_uncertain_job_submission_is_persisted_without_a_guessed_id(self):
        self.client.manifest = manifest(batch(1))

        def uncertain(*_args):
            raise MonitorError("Job submission could not be confirmed")

        self.client.start_job = uncertain
        with self.assertRaisesRegex(MonitorError, "submission could not be confirmed"):
            self.controller.run()
        state = self.client.snapshots[-1]
        record = state["batches"][f"{RUN}_0001"]
        self.assertEqual(record["status"], "failed")
        self.assertTrue(record["submission_uncertain"])
        self.assertNotIn("job_id", record)
        self.assertIn(self.cfg["lock_url"], self.client.objects)

    def test_permanent_delivery_failure_has_a_durable_batch_outcome(self):
        entry = batch(1)
        entry["deliveries"]["demo-2"]["submission_status"] = "permanent_failure"
        self.client.manifest = manifest(entry)
        with self.assertRaisesRegex(MonitorError, "permanently rejected"):
            self.controller.run()
        record = self.client.snapshots[-1]["batches"][entry["batch_id"]]
        self.assertEqual(record["status"], "failed")
        self.assertIn("permanently rejected", record["diagnostic"])

    def test_throttling_cannot_extend_a_pending_batch_deadline(self):
        self.client.manifest = manifest(batch(1))
        self.controller._begin()
        self.controller._tick()

        def throttled(_url):
            raise RetryLater(31)

        self.client.verify_sentinel = throttled
        self.controller._begin = lambda: None
        with self.assertRaisesRegex(MonitorError, "retry would exceed"):
            self.controller.run()
        self.assertEqual(self.client.cancelled, [self.client.started[0][3]])

    def test_notebook_job_reads_nested_exit_value_from_beta_endpoint(self):
        client = FabricClient(None, self.cfg)
        job_id = str(uuid.UUID(int=200))
        observed = []

        def request(method, url, **kwargs):
            observed.append(url)
            return 200, {}, json.dumps({
                "id": job_id, "itemId": NOTEBOOK, "status": "Completed",
                "properties": {"exitValue": '{"mode":"verify_batch","status":"verified"}'},
            }).encode("utf-8")

        client._request = request
        result, _ = client.job(job_id)
        self.assertIn(f"/notebooks/{NOTEBOOK}/jobs/execute/instances/{job_id}?beta=true", observed[0])
        self.assertIn("exitValue", result["properties"])

    def test_changed_storage_sentinel_is_not_readiness_evidence(self):
        client = FabricClient(None, self.cfg)
        client._request = lambda *_args, **_kwargs: (200, {}, b"unexpected sentinel contents")
        with self.assertRaisesRegex(MonitorError, "sentinel changed"):
            client.verify_sentinel(self.cfg["sentinel_url"])

    def test_mismatching_or_late_exit_cannot_verify_a_batch(self):
        self.client.manifest = manifest(batch(1))
        self.controller._begin()
        self.controller._tick()
        job_id = self.client.started[0][3]
        self.client.results[job_id] = {
            "status": "Completed",
            "properties": {"exitValue": json.dumps({
                "mode": "verify_batch", "status": "verified",
                "batch": {"batch_id": "another", "row_count": 2,
                          "sales_amount_minor_units": 300},
            })},
        }
        self.clock.advance(1)
        with self.assertRaisesRegex(MonitorError, "does not match"):
            self.controller._tick()

    def test_terminal_success_must_match_the_submitted_source_id(self):
        entry = batch(1)
        self.client.manifest = manifest(entry)
        self.controller._begin()
        self.controller._tick()
        job_id = self.client.started[0][3]
        self.client.results[job_id] = {
            "status": "Completed",
            "properties": {"exitValue": json.dumps({
                "mode": "verify_batch", "status": "verified",
                "batch": {"batch_id": f"{RUN}_0001", "row_count": 2,
                          "sales_amount_minor_units": 300},
            })},
        }
        delivery = verified_delivery(entry, self.clock())
        delivery["ingestion_outcome"]["source_id"] = str(uuid.UUID(int=99))
        self.clock.advance(1)
        with self.assertRaisesRegex(MonitorError, "terminal success evidence"):
            self.controller._tick()

    def test_terminal_success_requires_exact_kql_reconciliation_evidence(self):
        entry = batch(1)
        delivery = verified_delivery(entry)
        record = {"operation_id": delivery["operation_id"]}
        verify_delivery_evidence(entry, record)
        changes = (
            ("verified_row_count", 1),
            ("verified_total_minor_units", 299),
            ("verified_distinct_sales_line_ids", 1),
            ("verified_row_count", True),
            ("verified_at_utc", "invalid"),
        )
        for field, value in changes:
            with self.subTest(field=field, value=value):
                modified = copy.deepcopy(entry)
                modified["deliveries"]["demo-2"][field] = value
                with self.assertRaises(MonitorError):
                    verify_delivery_evidence(modified, record)
        for field, value in (("error_code", "Forbidden"), ("timestamp_utc", "invalid")):
            with self.subTest(field=field, value=value):
                modified = copy.deepcopy(entry)
                modified["deliveries"]["demo-2"]["ingestion_outcome"][field] = value
                with self.assertRaises(MonitorError):
                    verify_delivery_evidence(modified, record)

    def test_duplicate_tag_skip_without_serialized_replay_proof_is_not_verified(self):
        entry = batch(1)
        self.client.manifest = manifest(entry)
        self.controller._begin()
        self.controller._tick()
        job_id = self.client.started[0][3]
        self.client.results[job_id] = {
            "status": "Completed",
            "properties": {"exitValue": json.dumps({
                "mode": "verify_batch", "status": "verified",
                "batch": {"batch_id": f"{RUN}_0001", "row_count": 2,
                          "sales_amount_minor_units": 300},
            })},
        }
        delivery = verified_delivery(entry, self.clock())
        delivery["ingestion_outcome"].update(
            status="failed", error_code="Skipped_IngestByTagAlreadyExists",
        )
        self.clock.advance(1)
        with self.assertRaisesRegex(MonitorError, "terminal success evidence"):
            self.controller._tick()

    def test_notebook_result_received_after_delivery_deadline_is_not_verified(self):
        entry = batch(1)
        self.client.manifest = manifest(entry)
        self.controller._begin()
        self.controller._tick()
        job_id = self.client.started[0][3]
        self.client.results[job_id] = {
            "status": "Completed",
            "properties": {"exitValue": json.dumps({
                "mode": "verify_batch", "status": "verified",
                "batch": {"batch_id": f"{RUN}_0001", "row_count": 2,
                          "sales_amount_minor_units": 300},
            })},
        }
        verified_delivery(entry, self.clock())
        original_job = self.client.job

        def delayed_job(value):
            self.clock.advance(30)
            return original_job(value)

        self.client.job = delayed_job
        self.clock.advance(1)
        with self.assertRaisesRegex(MonitorError, "exceeded its verification deadline"):
            self.controller._tick()


if __name__ == "__main__":
    unittest.main()
