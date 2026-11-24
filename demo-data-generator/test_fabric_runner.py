import json
import unittest
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from fabric_runner import run_fabric_batches
from fabric_storage import StorageOutcomeUnknown
from generator import RunConfig
from runner import Submission
from test_fabric_ledger import MemoryStore, TEST_SCOPE, TEST_TARGETS, initialized_store


def run_with_scope(store, config, adapters, max_duration_seconds, max_attempts, **kwargs):
    return run_fabric_batches(store, config, adapters, max_duration_seconds, max_attempts,
                              allocator_scope=TEST_SCOPE,
                              selected_targets={name: TEST_TARGETS[name] for name in adapters}, **kwargs)


class FakeClock:
    def __init__(self):
        self.now = datetime(2024, 1, 1, tzinfo=timezone.utc)

    def clock(self):
        return self.now

    def sleep(self, seconds):
        self.now += timedelta(seconds=seconds)


class Adapter:
    def __init__(self, clock, uncertain=False):
        self.clock = clock
        self.uncertain = uncertain
        self.calls = []
        self.verifications = 0

    def submit(self, path, batch_id, sha256, row_count, total):
        self.calls.append((batch_id, self.clock.now))
        if self.uncertain:
            raise TimeoutError("possibly queued")
        return Submission("submitted", "operation")

    def verify(self, batch_id, sha256, row_count, total, operation_id):
        self.verifications += 1
        if self.uncertain:
            return SimpleNamespace(status="pending", diagnostic="not visible yet")
        return SimpleNamespace(status="verified", row_count=row_count, total_minor_units=total,
                               distinct_sales_line_ids=row_count, diagnostic=None)

    def ingestion_status(self, operation_id):
        raise AssertionError("an uncertain submission has no source ID")


class EventhouseAdapter(Adapter):
    source_id = "9eebc93c-a700-4f87-91b7-a1471c389aa1"

    def __init__(self, clock, status, error_code=None, *, source_id=None, matched=True):
        super().__init__(clock)
        self.status = status
        self.error_code = error_code
        self.reported_source = source_id or self.source_id
        self.matched = matched

    def submit(self, path, batch_id, sha256, row_count, total):
        self.calls.append((batch_id, self.clock.now))
        return Submission("submitted", self.source_id)

    def ingestion_status(self, operation_id):
        return SimpleNamespace(status=self.status, timestamp_utc=self.clock.now.isoformat(),
                               source_id=self.reported_source, service_operation_id=None,
                               error_code=self.error_code, diagnostic=None)

    def verify(self, batch_id, sha256, row_count, total, operation_id):
        self.verifications += 1
        if not self.matched:
            return SimpleNamespace(status="failed", diagnostic="KQL totals differ")
        return SimpleNamespace(status="verified", row_count=row_count, total_minor_units=total,
                               distinct_sales_line_ids=row_count, diagnostic=None)


class FabricRunnerTests(unittest.TestCase):
    def test_deadline_cadence_and_uncertain_submission_are_persisted(self):
        store, clock = initialized_store(), FakeClock()
        good, uncertain = Adapter(clock), Adapter(clock, uncertain=True)
        config = RunConfig("timed", file_count=3, rows_per_file=2, interval_seconds=60)
        result = run_with_scope(store, config, {"demo-1": good, "demo-2": uncertain}, 125, 2,
                                clock=clock.clock, sleep=clock.sleep)
        self.assertFalse(result["verified"])
        self.assertEqual(len(good.calls), 3)
        self.assertEqual(len(uncertain.calls), 3)
        self.assertEqual([int((t - good.calls[0][1]).total_seconds()) for _, t in good.calls], [0, 60, 120])
        self.assertEqual(clock.now, datetime(2024, 1, 1, tzinfo=timezone.utc) + timedelta(seconds=125))
        resumed = run_with_scope(store, config, {"demo-1": good, "demo-2": uncertain}, 125, 2,
                                 clock=clock.clock, sleep=clock.sleep)
        self.assertEqual(resumed, result)
        self.assertEqual((len(good.calls), len(uncertain.calls)), (3, 3))
        raw, _ = store.read("Files/producer/runs/timed/manifest.json")
        scheduler = json.loads(raw)["scheduler"]
        self.assertEqual(scheduler["status"], "failed")
        self.assertEqual(scheduler["result"], result)
        self.assertEqual(scheduler["finished_at_utc"], clock.now.isoformat())

    def test_fully_verified_file_run_succeeds(self):
        store, clock = initialized_store(), FakeClock()
        good = Adapter(clock)
        config = RunConfig("ok", file_count=2, rows_per_file=2, interval_seconds=1)
        result = run_with_scope(store, config, {"demo-1": good}, 10, 1,
                                clock=clock.clock, sleep=clock.sleep)
        self.assertTrue(result["verified"])
        self.assertEqual(result["errors"], [])
        raw, _ = store.read("Files/producer/runs/ok/manifest.json")
        manifest = json.loads(raw)
        self.assertEqual(manifest["scheduler"]["status"], "completed")
        self.assertEqual(manifest["scheduler"]["result"], result)
        delivery = manifest["batches"]["1"]["deliveries"]["demo-1"]
        self.assertEqual(delivery["submission_status"], "submitted")
        self.assertTrue(delivery["submission_result_at_utc"])

    def test_unknown_manifest_write_does_not_repeat_the_state_transition(self):
        class LostResponseStore(MemoryStore):
            def replace(self, path, data, etag):
                result = super().replace(path, data, etag)
                if b'"status": "submitted"' in data:
                    raise StorageOutcomeUnknown("response lost after commit")
                return result

        store, clock = initialized_store(LostResponseStore()), FakeClock()
        good = Adapter(clock)
        config = RunConfig("lost", file_count=1, rows_per_file=1)
        with self.assertRaises(StorageOutcomeUnknown):
            run_with_scope(store, config, {"demo-1": good}, 10, 1,
                           clock=clock.clock, sleep=clock.sleep)
        self.assertEqual(len(good.calls), 1)
        self.assertIn("Files/producer/runs/lost/writer.lock", store.files)

    def test_eventhouse_requires_terminal_status_and_matching_kql(self):
        cases = [
            ("pending", None, True, False, "submitted", 0),
            ("unknown", None, True, False, "submitted", 0),
            ("succeeded", None, True, True, "verified", 1),
            ("failed", "Skipped_IngestByTagAlreadyExists", True, False, "verification_failed", 0),
            ("failed", "Skipped_IngestByTagAlreadyExists", False, False, "verification_failed", 0),
            ("failed", "OtherIngestionFailure", True, False, "verification_failed", 0),
        ]
        for status, error_code, matched, verified, final_status, queries in cases:
            with self.subTest(status=status, error_code=error_code, matched=matched):
                store, clock = initialized_store(), FakeClock()
                adapter = EventhouseAdapter(clock, status, error_code, matched=matched)
                config = RunConfig("eventhouse", file_count=1, rows_per_file=2)
                result = run_with_scope(store, config, {"demo-2": adapter}, 2, 1,
                                        clock=clock.clock, sleep=clock.sleep)
                self.assertEqual(result["verified"], verified)
                self.assertEqual(adapter.verifications, queries)
                self.assertEqual(len(adapter.calls), 1)
                raw, _ = store.read("Files/producer/runs/eventhouse/manifest.json")
                delivery = json.loads(raw)["batches"]["1"]["deliveries"]["demo-2"]
                self.assertEqual(delivery["status"], final_status)
                self.assertEqual(delivery["ingestion_outcome"]["status"], status)
                self.assertEqual(delivery["ingestion_outcome"]["source_id"], adapter.source_id)

    def test_eventhouse_rejects_mismatched_status_source(self):
        store, clock = initialized_store(), FakeClock()
        adapter = EventhouseAdapter(clock, "succeeded", source_id="9eebc93c-a700-4f87-91b7-a1471c389aa2")
        config = RunConfig("wrong-source", file_count=1, rows_per_file=1)
        result = run_with_scope(store, config, {"demo-2": adapter}, 2, 1,
                                clock=clock.clock, sleep=clock.sleep)
        self.assertFalse(result["verified"])
        self.assertEqual(adapter.verifications, 0)
        self.assertEqual(len(adapter.calls), 1)

    def test_terminal_status_survives_queue_message_expiry_while_kql_catches_up(self):
        class ExpiringStatus(EventhouseAdapter):
            def __init__(self, clock):
                super().__init__(clock, "succeeded")
                self.status_reads = 0

            def ingestion_status(self, operation_id):
                self.status_reads += 1
                self.status = "succeeded" if self.status_reads == 1 else "pending"
                return super().ingestion_status(operation_id)

            def verify(self, batch_id, sha256, row_count, total, operation_id):
                self.verifications += 1
                if self.verifications == 1:
                    return SimpleNamespace(status="pending", diagnostic="not visible yet")
                return SimpleNamespace(status="verified", row_count=row_count, total_minor_units=total,
                                       distinct_sales_line_ids=row_count, diagnostic=None)

        store, clock = initialized_store(), FakeClock()
        adapter = ExpiringStatus(clock)
        config = RunConfig("expiring", file_count=1, rows_per_file=1)
        result = run_with_scope(store, config, {"demo-2": adapter}, 10, 1,
                                clock=clock.clock, sleep=clock.sleep)
        self.assertTrue(result["verified"])
        self.assertEqual((len(adapter.calls), adapter.status_reads, adapter.verifications), (1, 2, 2))
        raw, _ = store.read("Files/producer/runs/expiring/manifest.json")
        delivery = json.loads(raw)["batches"]["1"]["deliveries"]["demo-2"]
        self.assertEqual(delivery["ingestion_outcome"]["status"], "succeeded")

    def test_eventhouse_adapter_without_status_readback_is_rejected(self):
        class NoStatus:
            def submit(self, *args):
                pass

            def verify(self, *args):
                pass

        with self.assertRaisesRegex(ValueError, "terminal ingestion status"):
            run_with_scope(MemoryStore(), RunConfig("no-status", file_count=1),
                           {"demo-2": NoStatus()}, 2, 1)


if __name__ == "__main__":
    unittest.main()
