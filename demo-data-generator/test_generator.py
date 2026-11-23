import csv
import hashlib
import io
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from urllib.parse import urlsplit
from urllib.request import url2pathname

from generator import HEADER, RunConfig, batch_bytes, dimensions, stage_batch
from delivery import begin_submission, record_submission, record_verification
from fabric_files import materialize_staged


class GeneratorTests(unittest.TestCase):
    def test_default_run_has_expected_coverage_and_count(self):
        config = RunConfig("default")
        customers, products, dates, ids = set(), set(), set(), set()
        for number in range(1, config.file_count + 1):
            payload, meta = batch_bytes(config, number)
            rows = list(csv.DictReader(io.StringIO(payload.decode())))
            self.assertEqual(len(rows), 1000)
            self.assertEqual(meta["row_count"], 1000)
            for row in rows:
                ids.add(row["SalesLineID"])
                customers.add(row["CustomerID"])
                products.add(row["ProductID"])
                dates.add(row["Date"])
        self.assertEqual((len(ids), len(customers), len(products), len(dates)), (10000, 100, 20, 365))

    def test_batches_are_deterministic_and_reconcile(self):
        config = RunConfig("run-1", date_count=4, products=3, customers=5, file_count=2, rows_per_file=12)
        all_ids, customers, products, dates = set(), set(), set(), set()
        dims = dimensions(config)
        for number in (1, 2):
            payload, meta = batch_bytes(config, number)
            self.assertEqual((payload, meta), batch_bytes(config, number))
            self.assertEqual(meta["sha256"], hashlib.sha256(payload).hexdigest())
            rows = list(csv.DictReader(io.StringIO(payload.decode())))
            self.assertEqual(tuple(rows[0]), HEADER)
            self.assertEqual(len(rows), 12)
            self.assertEqual(meta["batch_class"], "measured")
            self.assertEqual(sum(int(r["SalesAmountMinorUnits"]) for r in rows), meta["sales_amount_minor_units"])
            self.assertEqual(sum(meta["amount_by_date"].values()), meta["sales_amount_minor_units"])
            self.assertEqual(sum(meta["amount_by_product_category"].values()), meta["sales_amount_minor_units"])
            self.assertEqual(sum(meta["amount_by_customer_region"].values()), meta["sales_amount_minor_units"])
            for row in rows:
                self.assertEqual(int(row["SalesAmountMinorUnits"]), int(row["Quantity"]) * int(row["UnitPriceMinorUnits"]))
                self.assertEqual(row["BatchID"], meta["batch_id"])
                self.assertTrue(row["SaleTimestamp"].endswith("Z"))
                self.assertNotIn(row["SalesLineID"], all_ids)
                all_ids.add(row["SalesLineID"])
                customers.add(int(row["CustomerID"]))
                products.add(int(row["ProductID"]))
                dates.add(row["Date"])
        self.assertEqual(customers, {r["CustomerID"] for r in dims["DimCustomer"]})
        self.assertEqual(products, {r["ProductID"] for r in dims["DimProduct"]})
        self.assertEqual(dates, {r["Date"] for r in dims["DimDate"]})

    def test_resume_reuses_bytes_and_detects_changes(self):
        with tempfile.TemporaryDirectory() as root:
            config = RunConfig("abc", rows_per_file=2)
            entry = stage_batch(root, config, 1, ["demo-1", "demo-3"])
            manifest = json.loads((Path(root) / "abc" / "manifest.json").read_text(encoding="utf-8"))
            self.assertEqual(manifest["contract_version"], 1)
            self.assertEqual(manifest["batches"]["1"]["batch_class"], "measured")
            self.assertEqual(entry, stage_batch(root, config, 1, ["demo-3", "demo-1"]))
            with self.assertRaises(ValueError):
                stage_batch(root, RunConfig("abc", rows_per_file=3), 1, ["demo-1", "demo-3"])
            path = Path(root) / "abc" / entry["filename"]
            path.write_bytes(b"corrupt")
            with self.assertRaises(RuntimeError):
                stage_batch(root, config, 1, ["demo-1", "demo-3"])

    def test_invalid_inputs_are_rejected(self):
        for config in (RunConfig("../bad"), RunConfig("x", rows_per_file=0), RunConfig("x", seed=True),
                       RunConfig("x", batch_class="invalid")):
            with self.assertRaises(ValueError):
                batch_bytes(config, 1)
        with self.assertRaises(ValueError):
            batch_bytes(RunConfig("ok"), 0)
        self.assertEqual(batch_bytes(RunConfig("test", batch_class="bootstrap"), 1)[1]["batch_class"], "bootstrap")
        self.assertEqual(batch_bytes(RunConfig("test", batch_class="negative-test"), 1)[1]["batch_class"], "negative-test")

    def test_delivery_recovers_safely_and_requires_verification(self):
        with tempfile.TemporaryDirectory() as root:
            config = RunConfig("delivery", rows_per_file=2)
            entry = stage_batch(root, config, 1, ["demo-2", "demo-3"])
            request = begin_submission(root, config.run_id, 1, "demo-2", 2)
            self.assertEqual(Path(request["staged_path"]).read_bytes(), batch_bytes(config, 1)[0])
            self.assertEqual(request["sha256"], entry["sha256"])
            with self.assertRaises(RuntimeError):
                begin_submission(root, config.run_id, 1, "demo-2", 2)
            record_submission(root, config.run_id, 1, "demo-2", "retryable_failure")
            begin_submission(root, config.run_id, 1, "demo-2", 2)
            record_submission(root, config.run_id, 1, "demo-2", "submitted", "operation")
            with self.assertRaises(ValueError):
                record_verification(root, config.run_id, 1, "demo-2", 2, entry["sales_amount_minor_units"], 1)
            record_verification(root, config.run_id, 1, "demo-2", 2, entry["sales_amount_minor_units"], 2)
            manifest = json.loads((Path(root) / config.run_id / "manifest.json").read_text(encoding="utf-8"))
            deliveries = manifest["batches"]["1"]["deliveries"]
            self.assertEqual(deliveries["demo-2"]["status"], "verified")
            self.assertEqual(deliveries["demo-2"]["attempts"], 2)
            self.assertEqual(deliveries["demo-3"]["status"], "pending")
            with self.assertRaises(RuntimeError):
                begin_submission(root, config.run_id, 1, "demo-2", 2)

    def test_ambiguous_submission_cannot_be_retried(self):
        with tempfile.TemporaryDirectory() as root:
            stage_batch(root, RunConfig("uncertain", rows_per_file=1), 1, ["demo-2"])
            begin_submission(root, "uncertain", 1, "demo-2", 3)
            record_submission(root, "uncertain", 1, "demo-2", "unknown")
            with self.assertRaises(RuntimeError):
                begin_submission(root, "uncertain", 1, "demo-2", 3)

    def test_materialize_verifies_and_removes_driver_file(self):
        class FakeFs:
            def exists(self, uri):
                return uri == "Files/stage/batch.csv"

            def cp(self, source, destination, recurse):
                Path(url2pathname(urlsplit(destination).path)).write_bytes(b"batch")
                return True

        digest = hashlib.sha256(b"batch").hexdigest()
        with materialize_staged(FakeFs(), "Files/stage/batch.csv", digest) as path:
            self.assertEqual(Path(path).read_bytes(), b"batch")
        self.assertFalse(Path(path).exists())
        with self.assertRaises(RuntimeError):
            with materialize_staged(FakeFs(), "Files/stage/batch.csv", "0" * 64):
                self.fail("mismatched bytes must not be delivered")

    def test_notebook_and_pipeline_drafts_have_matching_parameters(self):
        directory = Path(__file__).parent
        notebook = json.loads((directory / "notebook.ipynb").read_text(encoding="utf-8"))
        pipeline = json.loads((directory / "pipeline-content.json").read_text(encoding="utf-8"))
        for cell in notebook["cells"]:
            compile("".join(cell["source"]), "notebook", "exec")
        self.assertEqual(set(pipeline["properties"]["parameters"]), {"run_config_json", "selected_demos_json"})
        self.assertEqual(set(pipeline["properties"]["activities"][0]["typeProperties"]["parameters"]),
                         {"run_config_json", "selected_demos_json"})

    def test_deploy_contract_never_claims_unimplemented_readiness(self):
        script = Path(__file__).with_name("deploy.py")
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config, result = root / "config.json", root / "result.json"
            config.write_text('{"contract_version": 1}', encoding="utf-8")
            for mode, status, code in (("dry-run", "pass", 0), ("deploy", "blocked", 1), ("validate-only", "blocked", 1)):
                completed = subprocess.run([sys.executable, str(script), "--mode", mode, "--config", str(config),
                                            "--prefix", "chanel", "--state-dir", str(root), "--result", str(result)])
                self.assertEqual(completed.returncode, code)
                data = json.loads(result.read_text(encoding="utf-8"))
                self.assertEqual(set(data), {"contract_version", "track", "mode", "status", "ready", "diagnostics"})
                self.assertEqual(data["status"], status)
                self.assertFalse(data["ready"])

    def test_run_data_requires_a_safe_bounded_configuration(self):
        script = Path(__file__).with_name("deploy.py")
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config, result = root / "config.json", root / "result.json"
            data = {"run_id": "run-1", "seed": 42, "start_date": "2024-01-01",
                    "date_count": 365, "product_count": 20, "customer_count": 100,
                    "rows_per_file": 1000, "file_count": 10, "interval_seconds": 60,
                    "max_duration_seconds": 540, "currency": "USD"}
            args = [sys.executable, str(script), "--mode", "run-data", "--config", str(config),
                    "--prefix", "chanel", "--state-dir", str(root), "--result", str(result),
                    "--selected-demos", "demo-1,demo-2"]
            for duration, status in ((540, "fail"), (541, "blocked")):
                data["max_duration_seconds"] = duration
                config.write_text(json.dumps({"contract_version": 1, "data": data}), encoding="utf-8")
                self.assertEqual(subprocess.run(args).returncode, 1)
                self.assertEqual(json.loads(result.read_text(encoding="utf-8"))["status"], status)


if __name__ == "__main__":
    unittest.main()
