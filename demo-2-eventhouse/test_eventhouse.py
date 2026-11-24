import csv
import hashlib
import importlib.util
import io
import os
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import patch

from eventhouse_core import (
    CSV_HEADER,
    ConfigurationError,
    discover_mirror_location,
    schema_commands,
    shortcut_payload,
)
from ingestion import EventhouseAdapter
from lakehouse_validation import (
    NotVisible, VerificationError, measured_batches, measured_reservation, reservation_bounds,
    verify_batch_with_timeout, verify_run_keys,
)


WORKSPACE_ID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
ITEM_ID = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"


def csv_bytes(rows):
    output = io.StringIO(newline="")
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow(CSV_HEADER)
    writer.writerows(rows)
    return output.getvalue().encode("utf-8")


def sample_row(sales_id=1, batch_id="run_0001"):
    return [
        sales_id, batch_id, "2026-01-01T00:00:00Z", 1, 1, "2026-01-01",
        2, 123, 246,
    ]


class FakeIngestClient:
    def __init__(self, error=None):
        self.calls = []
        self.error = error

    def ingest_from_file(self, descriptor, properties):
        from azure.kusto.ingest import IngestionStatus

        self.calls.append((Path(descriptor.path).read_bytes(), properties))
        if self.error:
            raise self.error
        return types.SimpleNamespace(status=IngestionStatus.QUEUED, source_id=descriptor.source_id)


class FakeQueryClient:
    def __init__(self, rows):
        self.rows = rows
        self.queries = []

    def execute_query(self, database, query):
        self.queries.append((database, query))
        return types.SimpleNamespace(primary_results=[self.rows])


class IngestionTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name) / "batch.csv"
        self.client = FakeIngestClient()
        self.query = FakeQueryClient([])
        self.adapter = EventhouseAdapter(
            database="SalesDB",
            table="FactSales",
            mapping="SalesCsvV1",
            ingest_client=self.client,
            query_client=self.query,
        )

    def submit(self, payload, rows=1, total=246):
        self.path.write_bytes(payload)
        return self.adapter.submit(
            str(self.path), "run_0001", hashlib.sha256(payload).hexdigest(), rows, total
        )

    def test_submit_uses_exact_csv_and_stable_replay_tag(self):
        from azure.kusto.data import DataFormat
        from azure.kusto.ingest import ReportLevel, ValidationImplications, ValidationOptions

        payload = csv_bytes([sample_row()])
        first = self.submit(payload)
        second = self.submit(payload)
        self.assertEqual((first.status, second.status), ("submitted", "submitted"))
        self.assertNotEqual(first.operation_id, second.operation_id)
        sent, properties = self.client.calls[0]
        self.assertEqual(sent, payload)
        self.assertEqual(properties.format, DataFormat.CSV)
        self.assertEqual(properties.ingestion_mapping_reference, "SalesCsvV1")
        self.assertTrue(properties.ignore_first_record)
        self.assertEqual(properties.report_level, ReportLevel.FailuresAndSuccesses)
        self.assertEqual(properties.validation_policy.ValidationOptions,
                         ValidationOptions.ValidateCsvInputConstantColumns)
        self.assertEqual(properties.validation_policy.ValidationImplications,
                         ValidationImplications.Fail)
        self.assertEqual(properties.ingest_if_not_exists, properties.ingest_by_tags)
        self.assertEqual(properties.ingest_by_tags, self.client.calls[1][1].ingest_by_tags)

    def test_invalid_csv_is_rejected_before_submission(self):
        invalid_rows = [
            [sample_row(batch_id="other")],
            [sample_row(), sample_row()],
            [sample_row()[:8] + [245]],
            [sample_row()[:2] + ["2026-01-01T01:00:00+01:00"] + sample_row()[3:]],
            [sample_row()[:3] + [2**31] + sample_row()[4:]],
        ]
        for rows in invalid_rows:
            with self.subTest(rows=rows):
                result = self.submit(csv_bytes(rows), rows=len(rows), total=246 * len(rows))
                self.assertEqual(result.status, "permanent_failure")
        self.assertEqual(self.client.calls, [])

    def test_checksum_mismatch_is_not_submitted(self):
        self.path.write_bytes(csv_bytes([sample_row()]))
        result = self.adapter.submit(str(self.path), "run_0001", "0" * 64, 1, 246)
        self.assertEqual(result.status, "permanent_failure")
        self.assertEqual(self.client.calls, [])

    def test_ambiguous_network_failure_requires_reconciliation(self):
        self.client.error = TimeoutError("do not expose this")
        result = self.submit(csv_bytes([sample_row()]))
        self.assertEqual(result.status, "unknown")
        self.assertTrue(result.operation_id)
        self.assertNotIn("do not expose this", result.diagnostic)

    def test_ingestion_status_peeks_for_the_matching_terminal_source(self):
        source_id = "11111111-1111-4111-8111-111111111111"
        other_id = "22222222-2222-4222-8222-222222222222"
        success = types.SimpleNamespace(
            IngestionSourceId=source_id, OperationId="service-operation",
        )
        failure = types.SimpleNamespace(
            IngestionSourceId=other_id, OperationId="other", ErrorCode="Forbidden",
        )

        class FakeQueue:
            def __init__(self, messages):
                self.messages = messages
                self.requests = []

            def peek(self, count):
                self.requests.append(count)
                return self.messages

        successes, failures = FakeQueue([success]), FakeQueue([failure])
        queues = types.SimpleNamespace(success=successes, failure=failures)
        with patch("azure.kusto.ingest.status.KustoIngestStatusQueues", return_value=queues):
            outcome = self.adapter.ingestion_status(source_id)
            self.assertEqual(outcome.status, "succeeded")
            self.assertEqual(outcome.service_operation_id, "service-operation")
            self.assertEqual(self.adapter.ingestion_status(other_id).error_code, "Forbidden")
            self.assertEqual(
                self.adapter.ingestion_status("33333333-3333-4333-8333-333333333333").status,
                "pending",
            )
            failures.messages = [types.SimpleNamespace(
                IngestionSourceId=source_id, OperationId="other", ErrorCode="private secret",
            )]
            self.assertEqual(self.adapter.ingestion_status(source_id).status, "unknown")
        self.assertEqual(successes.requests, [32, 32, 32, 32])
        self.assertEqual(failures.requests, [32, 32, 32, 32])
        self.assertEqual(self.adapter.ingestion_status("invalid").status, "unknown")

    def test_reconciliation_requires_exact_count_total_and_unique_keys(self):
        digest = hashlib.sha256(csv_bytes([sample_row()])).hexdigest()
        cases = [
            ({"RowCount": 0, "DistinctSalesLineIDs": 0, "AmountMinorUnits": 0,
              "InvalidRows": 0}, "pending"),
            ({"RowCount": 1, "DistinctSalesLineIDs": 1, "AmountMinorUnits": 246,
              "InvalidRows": 0}, "verified"),
            ({"RowCount": 1, "DistinctSalesLineIDs": 1, "AmountMinorUnits": 245,
              "InvalidRows": 0}, "failed"),
            ({"RowCount": 2, "DistinctSalesLineIDs": 1, "AmountMinorUnits": 492,
              "InvalidRows": 0}, "failed"),
            ({"RowCount": 1, "DistinctSalesLineIDs": 1, "AmountMinorUnits": 246,
              "InvalidRows": 1}, "failed"),
            ({"RowCount": 1}, "unknown"),
        ]
        for row, expected in cases:
            with self.subTest(row=row):
                self.query.rows = [row]
                result = self.adapter.verify("run_0001", digest, 1, 246, "source-id")
                self.assertEqual(result.status, expected)
                self.assertEqual(result.operation_id, "source-id")
        self.assertIn("BatchID == 'run_0001'", self.query.queries[-1][1])


class MirrorTests(unittest.TestCase):
    def policy(self, *locations):
        return {
            "IsEnabled": True,
            "Format": "parquet",
            "TargetLatencyInMinutes": 5,
            "ConnectionStrings": list(locations),
        }

    def test_shortcut_uses_the_returned_location_without_credentials(self):
        location = (
            f"abfss://{WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/"
            f"{ITEM_ID}/Tables/FactSales?sig=secret"
        )
        result = discover_mirror_location(self.policy(location), "FactSales")
        self.assertNotIn("secret", result.abfss_path)
        payload = shortcut_payload(result, "EventhouseFactSales")
        self.assertEqual(payload["target"]["oneLake"], {
            "workspaceId": WORKSPACE_ID,
            "itemId": ITEM_ID,
            "path": "Tables/FactSales",
        })

    def test_regional_api_mirror_path_uses_documented_regional_dfs_endpoint(self):
        returned = (
            f"https://westus3-api.onelake.fabric.microsoft.com/{WORKSPACE_ID}/"
            f"{ITEM_ID}/Tables/FactSales;secret"
        )
        result = discover_mirror_location(self.policy(returned), "FactSales")
        self.assertEqual(result.policy_path, returned.split(";", 1)[0])
        self.assertEqual(
            result.abfss_path,
            f"abfss://{WORKSPACE_ID}@westus3-onelake.dfs.fabric.microsoft.com/"
            f"{ITEM_ID}/Tables/FactSales",
        )

    def test_rejects_unverified_or_ambiguous_mirror_locations(self):
        valid = f"https://onelake.dfs.fabric.microsoft.com/{WORKSPACE_ID}/{ITEM_ID}/Tables/FactSales"
        invalid = [
            self.policy(f"https://example.com/{WORKSPACE_ID}/{ITEM_ID}/Tables/FactSales"),
            self.policy(f"{valid}/../FactSales"),
            self.policy(valid, valid.replace(ITEM_ID, WORKSPACE_ID)),
            {**self.policy(valid), "TargetLatencyInMinutes": 3},
            {**self.policy(valid), "IsEnabled": False},
            {**self.policy(valid), "ConnectionStrings": []},
        ]
        for policy in invalid:
            with self.subTest(policy=policy):
                with self.assertRaises(ConfigurationError):
                    discover_mirror_location(policy, "FactSales")

    def test_schema_keeps_money_as_long(self):
        table, mapping, policy = schema_commands("FactSales", "SalesCsvV1")
        self.assertIn("UnitPriceMinorUnits: long", table)
        self.assertIn("SalesAmountMinorUnits: long", table)
        self.assertIn('"Ordinal":"8"', mapping)
        self.assertIn("TargetLatencyInMinutes=5", policy)
        with self.assertRaises(ConfigurationError):
            schema_commands("FactSales; .drop table Other", "SalesCsvV1")


class ManifestTests(unittest.TestCase):
    def batch(self, batch_id="run_0001", batch_class="measured"):
        return {
            "batch_id": batch_id,
            "batch_class": batch_class,
            "sha256": "a" * 64,
            "row_count": 1,
            "sales_amount_minor_units": 246,
            "amount_by_date": {"2026-01-01": 246},
            "amount_by_product_category": {"Beauty": 246},
            "amount_by_customer_region": {"Americas": 246},
        }

    def test_excludes_bootstrap_and_negative_batches(self):
        measured = self.batch()
        manifest = {"contract_version": 1, "batches": {
            "1": measured,
            "2": self.batch("bootstrap_0001", "bootstrap"),
            "3": self.batch("negative_0001", "negative-test"),
        }}
        self.assertEqual(measured_batches(manifest), [measured])

    def test_rejects_inconsistent_groups_or_duplicate_batch_ids(self):
        invalid = self.batch()
        invalid["amount_by_date"] = {"2026-01-01": 245}
        for batches in (
            {"1": invalid},
            {"1": self.batch(), "2": self.batch()},
            {"1": self.batch(), "2": self.batch(batch_class="bootstrap")},
        ):
            with self.subTest(batches=batches):
                with self.assertRaises(VerificationError):
                    measured_batches({"contract_version": 1, "batches": batches})
        with self.assertRaises(VerificationError):
            measured_batches({"contract_version": True, "batches": {"1": self.batch()}})

    def test_run_key_verification_rejects_non_measured_batches(self):
        with self.assertRaises(VerificationError):
            verify_run_keys(
                None, [self.batch(batch_class="bootstrap")], "Lakehouse", "Facts", reservation={}
            )

    def test_reservation_requires_canonical_inclusive_bigint_range(self):
        self.assertEqual(reservation_bounds({"start": "-1", "end": "1", "count": "3"}), (-1, 1, 3))
        invalid = (
            {"start": "01", "end": "1", "count": "1"},
            {"start": "-0", "end": "1", "count": "2"},
            {"start": 1, "end": "1", "count": "1"},
            {"start": "1", "end": "1", "count": "01"},
            {"start": "1", "end": "2", "count": "1"},
            {"start": "2", "end": "1", "count": "1"},
            {"start": "1", "end": str(2**63), "count": str(2**63)},
            {"start": "9" * 5000, "end": "9" * 5000, "count": "1"},
        )
        for reservation in invalid:
            with self.subTest(reservation=reservation), self.assertRaises(VerificationError):
                reservation_bounds(reservation)

    def test_measured_reservation_binds_run_generator_and_count(self):
        batch = {**self.batch(), "batch_number": 1}
        reservation = {
            "version": 1, "allocator_id": "a" * 64, "run_id": "run",
            "batch_class": "measured", "start": "10", "end": "10", "count": "1",
            "generator_sha256": "b" * 64, "fingerprint": "c" * 64,
        }
        manifest = {"run_id": "run", "sales_line_id_reservation": reservation}
        self.assertEqual(measured_reservation(manifest, [batch], "b" * 64), reservation)
        for changes in (
            {"run_id": "another_run"}, {"batch_class": "bootstrap"},
            {"generator_sha256": "d" * 64}, {"fingerprint": "invalid"},
            {"start": "10", "end": "11", "count": "2"},
        ):
            with self.subTest(changes=changes), self.assertRaises(VerificationError):
                measured_reservation(
                    {**manifest, "sales_line_id_reservation": {**reservation, **changes}},
                    [batch], "b" * 64,
                )

    def test_shared_generator_csv_and_manifest_contract(self):
        default = Path(__file__).resolve().parent.parent / "demo-data-generator" / "generator.py"
        source = Path(os.environ.get("CHANEL_PRODUCER_GENERATOR", str(default)))
        if not source.is_file():
            self.skipTest("The shared producer has not been merged into this worktree")
        spec = importlib.util.spec_from_file_location("chanel_shared_generator", source)
        generator = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = generator
        self.addCleanup(sys.modules.pop, spec.name, None)
        spec.loader.exec_module(generator)
        config = generator.RunConfig(
            run_id="contract", start_date="2026-01-01", file_count=1, rows_per_file=20
        )
        payload, batch = generator.batch_bytes(config, 1)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / batch["filename"]
            path.write_bytes(payload)
            client = FakeIngestClient()
            adapter = EventhouseAdapter(database="SalesDB", table="FactSales",
                                        mapping="SalesCsvV1", ingest_client=client,
                                        query_client=FakeQueryClient([]))
            submitted = adapter.submit(str(path), batch["batch_id"], batch["sha256"],
                                       batch["row_count"], batch["sales_amount_minor_units"])
        self.assertEqual(submitted.status, "submitted")
        self.assertEqual(client.calls[0][0], payload)
        self.assertEqual(measured_batches({"contract_version": 1, "batches": {"1": batch}}),
                         [batch])


class VerificationTimeoutTests(unittest.TestCase):
    def test_incomplete_batch_stops_at_exact_deadline(self):
        now = [0]
        sleeps = []

        def sleep(seconds):
            sleeps.append(seconds)
            now[0] += seconds

        spark = types.SimpleNamespace(
            catalog=types.SimpleNamespace(tableExists=lambda _: True)
        )
        with patch("lakehouse_validation.verify_lakehouse_batch", side_effect=NotVisible("pending")):
            with self.assertRaises(TimeoutError):
                verify_batch_with_timeout(
                    spark, {}, "Lakehouse", "Facts", None, 10, 4,
                    clock=lambda: now[0], sleep=sleep,
                )
        self.assertEqual(sleeps, [4, 4, 2])
        self.assertEqual(now[0], 10)

    def test_completion_after_deadline_is_not_a_pass(self):
        now = [0]
        spark = types.SimpleNamespace(
            catalog=types.SimpleNamespace(tableExists=lambda _: True)
        )

        def late_verification(*_):
            now[0] = 11
            return {"row_count": 1}

        with patch("lakehouse_validation.verify_lakehouse_batch", side_effect=late_verification):
            with self.assertRaises(TimeoutError):
                verify_batch_with_timeout(
                    spark, {}, "Lakehouse", "Facts", None, 10, 4,
                    clock=lambda: now[0], sleep=lambda _: None,
                )
        now[0] = 0

        def at_deadline(*_):
            now[0] = 10
            return {"row_count": 1}

        with patch("lakehouse_validation.verify_lakehouse_batch", side_effect=at_deadline):
            result = verify_batch_with_timeout(
                spark, {}, "Lakehouse", "Facts", None, 10, 4,
                clock=lambda: now[0], sleep=lambda _: None,
            )
        self.assertEqual(result["row_count"], 1)
        for value in (0, True, 1.5):
            with self.subTest(value=value), self.assertRaises(VerificationError):
                verify_batch_with_timeout(spark, {}, "Lakehouse", "Facts", None, value, 4)


if __name__ == "__main__":
    unittest.main()
