import tempfile
import unittest
from pathlib import Path

from fabric_delivery import OneLakeFileAdapter
from generator import RunConfig, batch_bytes
from test_fabric_ledger import MemoryStore


class FileDeliveryTests(unittest.TestCase):
    def setUp(self):
        self.store = MemoryStore()
        self.uri = ("abfss://aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa@onelake.dfs.fabric.microsoft.com/"
                    "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb/Files/incoming/sales/")
        self.adapter = OneLakeFileAdapter(self.uri, lambda: "token", store_factory=lambda *_: self.store)
        self.payload, self.batch = batch_bytes(RunConfig("run-1", file_count=1, rows_per_file=3), 1)

    def submit(self, path):
        b = self.batch
        return self.adapter.submit(str(path), b["batch_id"], b["sha256"], b["row_count"],
                                   b["sales_amount_minor_units"])

    def verify(self):
        b = self.batch
        return self.adapter.verify(b["batch_id"], b["sha256"], b["row_count"], b["sales_amount_minor_units"])

    def test_create_and_exact_readback_are_idempotent(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "batch.csv"
            path.write_bytes(self.payload)
            self.assertEqual(self.submit(path).status, "submitted")
            result = self.verify()
            self.assertEqual((result.status, result.row_count, result.distinct_sales_line_ids), ("verified", 3, 3))
            self.assertEqual(result.total_minor_units, self.batch["sales_amount_minor_units"])
            self.assertEqual(self.submit(path).status, "submitted")
            destination = "Files/incoming/sales/sales_run-1_0001.csv"
            self.store.files[destination] = (b"different", '"corrupt"')
            self.assertEqual(self.submit(path).status, "permanent_failure")
            self.assertEqual(self.verify().status, "failed")

    def test_ambiguous_write_is_reconciled_without_reupload(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "batch.csv"
            path.write_bytes(self.payload)
            self.store.unknown_create_suffix = ".csv"
            self.assertEqual(self.submit(path).status, "unknown")
            self.assertEqual(self.verify().status, "verified")

    def test_invalid_source_and_unsafe_landing_fail_without_delivery(self):
        for uri in (self.uri.replace("onelake.dfs.fabric.microsoft.com", "evil.example"),
                    self.uri.replace("Files/incoming", "Files/../incoming"),
                    self.uri + "?token=secret"):
            with self.assertRaises(ValueError):
                OneLakeFileAdapter(uri, lambda: "token", store_factory=lambda *_: self.store)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "batch.csv"
            path.write_bytes(self.payload + b"bad")
            self.assertEqual(self.submit(path).status, "permanent_failure")
        self.assertEqual(self.store.files, {})


if __name__ == "__main__":
    unittest.main()
