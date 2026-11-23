import json
import unittest
from urllib.error import HTTPError

from fabric_ledger import FabricLedger
from fabric_storage import OneLakeStore, StorageConflict, StorageError, StorageOutcomeUnknown
from generator import RunConfig


class StorageResponse:
    def __init__(self, status, headers=None):
        self.status = status
        self.headers = headers or {}

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def read(self):
        return b""


class OneLakeStoreTests(unittest.TestCase):
    def test_conditional_delete_accepts_onelake_success_statuses(self):
        for status in (200, 202):
            with self.subTest(status=status):
                requests = []

                def opener(request, timeout):
                    requests.append((request, timeout))
                    return StorageResponse(status)

                store = OneLakeStore("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
                                     "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
                                     lambda: "token", opener=opener)
                store.delete("Files/preflight/sentinel.txt", '"current"')
                self.assertEqual(len(requests), 1)
                request, timeout = requests[0]
                self.assertEqual(request.method, "DELETE")
                self.assertEqual(request.get_header("If-match"), '"current"')
                self.assertEqual(timeout, 30)

    def test_conditional_delete_fails_on_stale_or_rejected_headers(self):
        for outcome in ("stale", "rejected", "unexpected"):
            with self.subTest(outcome=outcome):
                def opener(request, timeout):
                    if outcome == "stale":
                        raise HTTPError(request.full_url, 412, "condition failed", {}, None)
                    if outcome == "rejected":
                        return StorageResponse(200, {"x-ms-rejected-headers": "If-Match"})
                    return StorageResponse(204)

                store = OneLakeStore("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
                                     "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
                                     lambda: "token", opener=opener)
                error = StorageConflict if outcome == "stale" else StorageError
                with self.assertRaises(error):
                    store.delete("Files/preflight/sentinel.txt", '"stale"')


class MemoryStore:
    def __init__(self):
        self.files = {}
        self.version = 0
        self.unknown_create_suffix = None

    def _etag(self):
        self.version += 1
        return f'"{self.version}"'

    def read(self, path):
        if path not in self.files:
            raise FileNotFoundError(path)
        return self.files[path]

    def create(self, path, data):
        if path in self.files:
            raise StorageConflict("exists")
        etag = self._etag()
        self.files[path] = (data, etag)
        if self.unknown_create_suffix and path.endswith(self.unknown_create_suffix):
            self.unknown_create_suffix = None
            raise StorageOutcomeUnknown("response lost")
        return etag

    def replace(self, path, data, etag):
        if path not in self.files or self.files[path][1] != etag:
            raise StorageConflict("stale")
        next_etag = self._etag()
        self.files[path] = (data, next_etag)
        return next_etag

    def delete(self, path, etag):
        if path not in self.files or self.files[path][1] != etag:
            raise StorageConflict("stale")
        del self.files[path]


class FabricLedgerTests(unittest.TestCase):
    def setUp(self):
        self.store = MemoryStore()
        self.config = RunConfig("run-1", file_count=1, rows_per_file=3)

    def test_resume_reconciles_without_resubmitting_and_checks_evidence(self):
        with FabricLedger(self.store, self.config, ["demo-2"]) as ledger:
            batch = ledger.stage_batch(1)
            request = ledger.begin_submission(1, "demo-2", 2)
            self.assertEqual(request["sha256"], batch["sha256"])
            ledger.record_submission(1, "demo-2", "unknown", diagnostic="response lost")
        with FabricLedger(self.store, self.config, ["demo-2"]) as resumed:
            self.assertEqual(resumed.stage_batch(1)["sha256"], batch["sha256"])
            with self.assertRaises(RuntimeError):
                resumed.begin_submission(1, "demo-2", 2)
            resumed.record_observation(1, "demo-2", "pending")
            with self.assertRaises(ValueError):
                resumed.record_verification(1, "demo-2", 3, batch["sales_amount_minor_units"], 3)

    def test_eventhouse_verification_requires_matching_terminal_source(self):
        source_id = "9eebc93c-a700-4f87-91b7-a1471c389aa1"
        with FabricLedger(self.store, self.config, ["demo-2"]) as ledger:
            batch = ledger.stage_batch(1)
            ledger.begin_submission(1, "demo-2", 1)
            ledger.record_submission(1, "demo-2", "submitted", source_id)
            outcome = {"status": "pending", "timestamp_utc": "2024-01-01T00:00:00+00:00",
                       "source_id": source_id, "service_operation_id": None, "error_code": None,
                       "diagnostic": None}
            ledger.record_ingestion_outcome(1, "demo-2", outcome)
            with self.assertRaises(RuntimeError):
                ledger.record_verification(1, "demo-2", 3, batch["sales_amount_minor_units"], 3)
            outcome["status"] = "succeeded"
            outcome["source_id"] = "9eebc93c-a700-4f87-91b7-a1471c389aa2"
            with self.assertRaises(ValueError):
                ledger.record_ingestion_outcome(1, "demo-2", outcome)
            outcome["source_id"] = source_id
            ledger.record_ingestion_outcome(1, "demo-2", outcome)
            outcome["status"] = "failed"
            outcome["error_code"] = "Skipped_IngestByTagAlreadyExists"
            ledger.record_ingestion_outcome(1, "demo-2", outcome)
            with self.assertRaises(RuntimeError):
                ledger.record_verification(1, "demo-2", 3, batch["sales_amount_minor_units"], 3)
            outcome["status"] = "succeeded"
            outcome["error_code"] = None
            ledger.record_ingestion_outcome(1, "demo-2", outcome)
            with self.assertRaises(ValueError):
                ledger.record_verification(1, "demo-2", 3, batch["sales_amount_minor_units"], 2)
            ledger.record_verification(1, "demo-2", 3, batch["sales_amount_minor_units"], 3)
            self.assertEqual(ledger.manifest["batches"]["1"]["deliveries"]["demo-2"]["status"], "verified")

    def test_lost_staging_response_retains_lock_and_recovers_exact_bytes(self):
        self.store.unknown_create_suffix = ".csv"
        with self.assertRaises(StorageOutcomeUnknown):
            with FabricLedger(self.store, self.config, ["demo-1"]) as ledger:
                ledger.stage_batch(1)
        lock = "Files/producer/runs/run-1/writer.lock"
        self.assertIn(lock, self.store.files)
        with self.assertRaisesRegex(RuntimeError, "locked"):
            with FabricLedger(self.store, self.config, ["demo-1"]):
                pass
        # An operator may release the exact lock only after confirming the old writer stopped.
        self.store.delete(lock, self.store.files[lock][1])
        with FabricLedger(self.store, self.config, ["demo-1"]) as recovered:
            entry = recovered.stage_batch(1)
            self.assertEqual(entry["row_count"], 3)
            self.assertEqual(recovered.staged_bytes(1).count(b"\n"), 4)

    def test_stale_manifest_and_config_mismatch_fail_closed(self):
        lock = "Files/producer/runs/run-1/writer.lock"
        with self.assertRaises(StorageConflict):
            with FabricLedger(self.store, self.config, ["demo-3"]) as ledger:
                path = ledger.manifest_path
                original, etag = self.store.read(path)
                self.store.replace(path, original, etag)
                ledger.stage_batch(1)
        self.assertIn(lock, self.store.files)
        self.store.delete(lock, self.store.files[lock][1])
        with self.assertRaises(ValueError):
            with FabricLedger(self.store, RunConfig("run-1", rows_per_file=4), ["demo-3"]):
                pass
        self.assertNotIn(lock, self.store.files)

    def test_failed_verification_is_persisted_and_cannot_be_retried(self):
        with FabricLedger(self.store, self.config, ["demo-2"]) as ledger:
            ledger.stage_batch(1)
            ledger.begin_submission(1, "demo-2", 2)
            ledger.record_submission(1, "demo-2", "submitted", "source-id")
            ledger.record_observation(1, "demo-2", "failed", "duplicate rows")
        manifest_path = "Files/producer/runs/run-1/manifest.json"
        manifest = json.loads(self.store.read(manifest_path)[0])
        delivery = manifest["batches"]["1"]["deliveries"]["demo-2"]
        self.assertEqual(delivery["status"], "verification_failed")
        with FabricLedger(self.store, self.config, ["demo-2"]) as ledger:
            with self.assertRaises(RuntimeError):
                ledger.begin_submission(1, "demo-2", 2)

    def test_retry_clears_previous_submission_timestamp_and_status(self):
        with FabricLedger(self.store, self.config, ["demo-2"]) as ledger:
            ledger.stage_batch(1)
            ledger.begin_submission(1, "demo-2", 2)
            ledger.record_submission(1, "demo-2", "retryable_failure")
            delivery = ledger.manifest["batches"]["1"]["deliveries"]["demo-2"]
            self.assertEqual(delivery["submission_status"], "retryable_failure")
            self.assertTrue(delivery["submission_result_at_utc"])
            ledger.begin_submission(1, "demo-2", 2)
            delivery = ledger.manifest["batches"]["1"]["deliveries"]["demo-2"]
            self.assertEqual(delivery["status"], "submitting")
            self.assertIsNone(delivery["submission_status"])
            self.assertIsNone(delivery["submission_result_at_utc"])


if __name__ == "__main__":
    unittest.main()
