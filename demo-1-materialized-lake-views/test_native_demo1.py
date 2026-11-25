import copy
from datetime import datetime, timedelta, timezone
import json
import re
import unittest
from unittest.mock import patch

import native_demo1
from test_deploy import INTENT, LAKEHOUSE, OWNER, TENANT, WORKSPACE


class Store:
    def __init__(self):
        self.data, self.version = {}, 0

    def read(self, path):
        if path not in self.data:
            raise FileNotFoundError(path)
        return self.data[path]

    def create(self, path, raw):
        if path in self.data:
            raise RuntimeError("conditional create conflict")
        self.version += 1
        self.data[path] = raw, str(self.version)
        return str(self.version)

    def replace(self, path, raw, etag):
        if self.read(path)[1] != etag:
            raise RuntimeError("conditional replace conflict")
        self.version += 1
        self.data[path] = raw, str(self.version)
        return str(self.version)


class Fabric:
    """Deliberately has no request/mutation method."""

    def __init__(self, receipt):
        self.schedule = copy.deepcopy(receipt["create_response"])
        self.extra_schedules = []
        self.jobs = [{"id": LAKEHOUSE, "itemId": LAKEHOUSE, "status": "Completed", "invokeType": "Manual",
                      "startTimeUtc": "2025-12-31T23:00:00", "endTimeUtc": "2025-12-31T23:01:00"}]
        self.reads = []

    def list_all(self, path):
        self.reads.append(path)
        if path.endswith("/instances"):
            return copy.deepcopy(self.jobs)
        if path.endswith("/schedules"):
            return copy.deepcopy([self.schedule, *self.extra_schedules])
        raise AssertionError(path)

    def get(self, path, deadline=None):
        self.reads.append(path)
        if not path.endswith("/schedules/" + self.schedule["id"]):
            raise AssertionError(path)
        return {}, copy.deepcopy(self.schedule)


class Spark:
    def __init__(self):
        self.bronze, self.silver, self.queries = {}, {}, []
        self.bad_dimension = False

    def sql(self, query):
        self.queries.append(query)
        if not query.startswith("SELECT "):
            raise AssertionError("Only read-only SQL is permitted")
        if "LIMIT 0" in query:
            rows = []
        elif "`DimDate`" in query:
            rows = [{"n": 365, "distinct_n": 365, "lo": "2026-01-01", "hi": "2026-12-31", "bad": 0}]
        elif "`DimProduct`" in query:
            rows = [{"n": 20, "distinct_n": 20, "lo": 1, "hi": 20, "bad": int(self.bad_dimension)}]
        elif "`DimCustomer`" in query:
            rows = [{"n": 100, "distinct_n": 100, "lo": 1, "hi": 100, "bad": 0}]
        else:
            keys = {value.strip().strip("'") for value in re.search(
                r"WHERE BatchID IN \(([^)]+)\)", query).group(1).split(",")}
            source = self.bronze if "`bronze`" in query else self.silver
            batches = {key: value for key, value in source.items() if key in keys}
            if "COUNT(*) AS n FROM" in query:
                rows = [{"n": sum(batch["row_count"] for batch in batches.values())}]
            elif "COUNT(DISTINCT TRY_CAST" in query:
                rows = [{"BatchID": key, "n": batch["row_count"], "distinct_n": batch["row_count"],
                         "lo": 1, "hi": batch["row_count"], "amount": batch["sales_amount_minor_units"],
                         "bad": batch.get("bad", 0)} for key, batch in batches.items()]
            elif " AS grouping" in query:
                field = ("amount_by_date" if "CAST(`Date` AS STRING)" in query else
                         "amount_by_product_category" if "ProductCategory AS grouping" in query else
                         "amount_by_customer_region")
                rows = [{"BatchID": key, "grouping": group, "amount": amount}
                        for key, batch in batches.items() for group, amount in batch[field].items()]
            else:
                raise AssertionError(query)
        result = [type("Row", (), {"asDict": lambda self, recursive=True, value=row: value})() for row in rows]
        return type("Frame", (), {"collect": lambda self: result})()


class NativeDemo1Test(unittest.TestCase):
    def setUp(self):
        self.now = datetime(2026, 1, 1, tzinfo=timezone.utc)
        self.run_id = "native_demo_001"
        request = {"enabled": True, "configuration": {
            "startDateTime": (self.now - timedelta(minutes=1)).isoformat(),
            "endDateTime": (self.now + timedelta(minutes=59)).isoformat(),
            "localTimeZoneId": "UTC", "type": "Cron", "interval": 5}}
        self.receipt = {"contract_version": 1, "track": "demo-1", "status": "confirmed",
                        "setup_run_id": "setup_001", "tenant_id": TENANT, "principal_id": OWNER,
                        "workspace_id": WORKSPACE, "lakehouse_id": LAKEHOUSE, "schedule_id": INTENT,
                        "request": request, "create_response": {
                            "id": INTENT, "owner": {"id": OWNER, "type": "User"}, **copy.deepcopy(request)}}
        data = {"run_id": self.run_id, "file_count": 10, "rows_per_file": 1000, "interval_seconds": 60,
                "seed": 42, "currency": "USD", "start_date": "2026-01-01",
                "date_count": 365, "product_count": 20, "customer_count": 100}
        self.document = {"tenant_id": TENANT, "data": data, "shared": {
            "identity_id": OWNER, "target_bindings": {
                "demo-1": {"workspace_id": WORKSPACE, "item_id": LAKEHOUSE, "fact": "silver.sales_joined"}}},
            "demo-1": {"workspace_id": WORKSPACE, "workspace_name": "demo-workspace", "lakehouse_id": LAKEHOUSE,
                       "lakehouse_name": "Lakehouse", "native_schedule": self.receipt,
                       "landing_uri": f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{LAKEHOUSE}/Files/incoming/sales/",
                       "bronze_schema": "bronze", "bronze_view": "sales_files",
                       "silver_schema": "silver", "silver_view": "sales_joined"}}
        self.fabric, self.store, self.spark = Fabric(self.receipt), Store(), Spark()
        self.manifest = {"contract_version": 1, "run_id": self.run_id, "batches": {}}
        self.stopped = False
        self.on_sleep = None
        self.context = {
            "document": self.document, "fabric": self.fabric, "store": self.store, "spark": self.spark,
            "clock": lambda: self.now, "sleep": self.sleep, "stop_requested": lambda: self.stopped,
            "read_manifest": lambda: self.manifest,
            "expires_at_utc": (self.now + timedelta(minutes=30)).isoformat(),
            "verification_deadline_utc": (self.now + timedelta(minutes=25)).isoformat(),
            "preparation_deadline_utc": (self.now + timedelta(minutes=5)).isoformat(),
            "cleanup_deadline_utc": (self.now + timedelta(minutes=30)).isoformat()}

    def sleep(self, seconds):
        self.now += timedelta(seconds=seconds)
        if self.on_sleep:
            self.on_sleep()

    def complete_manifest(self):
        start = self.now
        self.now += timedelta(minutes=10)
        self.manifest.update(config={
            "run_id": self.document["data"]["run_id"], "file_count": 10, "rows_per_file": 1000,
            "interval_seconds": 60, "seed": 42, "start_date": "2026-01-01", "date_count": 365,
            "products": 20, "customers": 100, "batch_class": "measured"}, destinations=["demo-1"])
        batches = {}
        for number in range(1, 11):
            batch_id, total = f"{self.document['data']['run_id']}_{number:04d}", number * 1000
            generated = start + timedelta(minutes=number - 1)
            batch = {"batch_id": batch_id, "batch_number": number, "batch_class": "measured", "row_count": 1000,
                     "sales_amount_minor_units": total, "sha256": "a" * 64, "generated_at_utc": generated.isoformat(),
                     "amount_by_date": {"2026-01-01": total}, "amount_by_product_category": {"Beauty": total},
                     "amount_by_customer_region": {"Americas": total},
                     "deliveries": {"demo-1": {"status": "verified", "submission_status": "submitted",
                         "submission_result_at_utc": (generated + timedelta(seconds=1)).isoformat(),
                         "timestamp_utc": (generated + timedelta(seconds=2)).isoformat(),
                         "verified_at_utc": (generated + timedelta(seconds=2)).isoformat()}}}
            batches[str(number)] = batch
        self.manifest["batches"] = batches
        self.spark.bronze.update({batch["batch_id"]: copy.deepcopy(batch) for batch in batches.values()})
        self.spark.silver.update({batch["batch_id"]: copy.deepcopy(batch) for batch in batches.values()})
        self.fabric.jobs.append({"id": OWNER, "itemId": LAKEHOUSE, "status": "Completed", "invokeType": "Scheduled",
                                 "startTimeUtc": (self.now - timedelta(minutes=2)).isoformat(),
                                 "endTimeUtc": self.now.isoformat()})

    def test_full_lifecycle_is_read_only_and_shared_setup_is_reusable(self):
        prepared = native_demo1.prepare(self.context)
        self.assertEqual(prepared["status"], "ready")
        self.assertEqual(native_demo1.check_ready(self.context, prepared)["status"], "ready")
        self.complete_manifest()
        baseline = copy.deepcopy(next(iter(self.spark.silver.values())))
        baseline["sales_amount_minor_units"] = 3000
        self.spark.silver["synthetic-baseline"] = baseline
        result = native_demo1.verify(self.context, prepared)
        self.assertEqual((result["status"], result["row_count"], result["batch_count"]), ("verified", 10000, 10))
        self.assertEqual(result["sales_amount_minor_units"], 55000)
        self.assertEqual(len(result["observations"]), 10)
        self.assertEqual(result["observations"][-1]["delivery_to_observation_seconds"], 59)
        self.stopped = True
        stopped = native_demo1.stop(self.context, prepared)
        self.assertEqual((stopped["status"], stopped["scope"]), ("stopped", native_demo1.STOP_SCOPE))
        self.assertTrue(self.fabric.schedule["enabled"])
        self.stopped = False
        self.document["data"]["run_id"] = "native_demo_002"
        self.manifest = {"contract_version": 1, "run_id": "native_demo_002", "batches": {}}
        self.context["preparation_deadline_utc"] = (self.now + timedelta(minutes=5)).isoformat()
        self.context["verification_deadline_utc"] = (self.now + timedelta(minutes=25)).isoformat()
        self.context["expires_at_utc"] = (self.now + timedelta(minutes=30)).isoformat()
        self.context["cleanup_deadline_utc"] = self.context["expires_at_utc"]
        self.assertEqual(native_demo1.prepare(self.context)["status"], "ready")
        self.assertEqual(len(self.store.data), 2)

    def test_receipt_identity_scope_and_binding_fail_before_reads(self):
        changes = [
            lambda: self.receipt.update(principal_id=WORKSPACE),
            lambda: self.receipt["request"]["configuration"].update(interval=1),
            lambda: self.receipt["create_response"].update(executionData={"mlvExecutionDefinitionId": OWNER}),
            lambda: self.document["demo-1"].update(landing_uri="abfss://wrong"),
            lambda: self.document["data"].update(rows_per_file=999),
        ]
        for change in changes:
            with self.subTest(change=change):
                self.setUp()
                change()
                with self.assertRaises((ValueError, RuntimeError)):
                    native_demo1.prepare(self.context)
                self.assertEqual(self.fabric.reads, [])

    def test_default_two_hour_deadline_cannot_fit_one_hour_setup(self):
        self.context["verification_deadline_utc"] = (self.now + timedelta(seconds=6900)).isoformat()
        self.context["expires_at_utc"] = (self.now + timedelta(hours=2)).isoformat()
        self.context["cleanup_deadline_utc"] = self.context["expires_at_utc"]
        with self.assertRaisesRegex(RuntimeError, "coverage insufficient"):
            native_demo1.prepare(self.context)
        self.assertEqual(self.store.data, {})

    def test_long_setup_respects_exact_bound_and_runtime_deadline(self):
        settings = self.receipt["request"]["configuration"]
        settings["startDateTime"] = (self.now - timedelta(minutes=1)).isoformat()
        settings["endDateTime"] = (self.now + timedelta(minutes=179)).isoformat()
        self.receipt["create_response"]["configuration"] = copy.deepcopy(settings)
        self.fabric.schedule = copy.deepcopy(self.receipt["create_response"])
        self.context["verification_deadline_utc"] = (self.now + timedelta(minutes=115)).isoformat()
        self.context["expires_at_utc"] = (self.now + timedelta(minutes=120)).isoformat()
        self.context["cleanup_deadline_utc"] = self.context["expires_at_utc"]
        prepared = native_demo1.prepare(self.context)
        self.assertEqual(native_demo1.check_ready(self.context, prepared)["status"], "ready")

        self.setUp()
        settings = self.receipt["request"]["configuration"]
        settings["endDateTime"] = (self.now + timedelta(minutes=179, microseconds=1)).isoformat()
        self.receipt["create_response"]["configuration"] = copy.deepcopy(settings)
        self.fabric.schedule = copy.deepcopy(self.receipt["create_response"])
        with self.assertRaisesRegex(ValueError, "invalid operating bounds"):
            native_demo1.prepare(self.context)

    def test_default_verification_deadline_does_not_fit_115_minute_setup(self):
        settings = self.receipt["request"]["configuration"]
        settings["endDateTime"] = (self.now + timedelta(minutes=114)).isoformat()
        self.receipt["create_response"]["configuration"] = copy.deepcopy(settings)
        self.fabric.schedule = copy.deepcopy(self.receipt["create_response"])
        self.context["verification_deadline_utc"] = (self.now + timedelta(minutes=115)).isoformat()
        self.context["expires_at_utc"] = (self.now + timedelta(minutes=120)).isoformat()
        self.context["cleanup_deadline_utc"] = self.context["expires_at_utc"]
        with self.assertRaisesRegex(RuntimeError, "coverage insufficient"):
            native_demo1.prepare(self.context)

    def test_exact_minimum_coverage_passes_but_one_microsecond_less_blocks(self):
        self.context["verification_deadline_utc"] = (self.now + timedelta(minutes=19)).isoformat()
        prepared = native_demo1.prepare(self.context)
        self.assertEqual(native_demo1.check_ready(self.context, prepared)["status"], "ready")
        self.now += timedelta(microseconds=1)
        with self.assertRaisesRegex(RuntimeError, "at least 19 minutes"):
            native_demo1.check_ready(self.context, prepared)

    def test_source_gate_rechecks_owner_jobs_deadlines_and_reused_ids(self):
        variants = ("owner", "active", "deadline", "reused", "competing", "disabled")
        for variant in variants:
            with self.subTest(variant=variant):
                self.setUp()
                prepared = native_demo1.prepare(self.context)
                if variant == "owner":
                    self.fabric.schedule["owner"]["id"] = WORKSPACE
                elif variant == "active":
                    self.fabric.jobs.append({"itemId": LAKEHOUSE, "status": "InProgress"})
                elif variant == "deadline":
                    self.now += timedelta(minutes=7)
                elif variant == "reused":
                    self.spark.bronze[f"{self.run_id}_0001"] = {"row_count": 1}
                elif variant == "competing":
                    self.fabric.extra_schedules.append({"id": OWNER, "enabled": True})
                else:
                    self.fabric.schedule["enabled"] = False
                with self.assertRaises(RuntimeError):
                    native_demo1.check_ready(self.context, prepared)

    def test_admission_waits_for_active_lineage_then_rechecks(self):
        self.fabric.jobs.append({"id": OWNER, "itemId": LAKEHOUSE, "status": "InProgress",
                                 "startTimeUtc": self.now.isoformat()})
        self.on_sleep = lambda: self.fabric.jobs[-1].update(status="Completed")
        prepared = native_demo1.prepare(self.context)
        self.assertEqual(prepared["status"], "ready")
        self.assertEqual(self.now, datetime(2026, 1, 1, tzinfo=timezone.utc) + timedelta(seconds=10))
        self.fabric.jobs[-1]["status"] = "InProgress"
        self.assertEqual(native_demo1.check_ready(self.context, prepared)["status"], "ready")

    def test_admission_wait_stops_at_preparation_deadline(self):
        self.context["preparation_deadline_utc"] = (self.now + timedelta(seconds=20)).isoformat()
        self.fabric.jobs.append({"id": OWNER, "itemId": LAKEHOUSE, "status": "InProgress",
                                 "startTimeUtc": self.now.isoformat()})
        with self.assertRaises(RuntimeError):
            native_demo1.prepare(self.context)
        self.assertEqual(self.now, datetime(2026, 1, 1, tzinfo=timezone.utc) + timedelta(seconds=20))
        self.assertEqual(self.store.data, {})

    def test_admission_does_not_wait_past_minimum_coverage(self):
        self.context["verification_deadline_utc"] = (self.now + timedelta(minutes=19)).isoformat()
        self.fabric.jobs.append({"id": OWNER, "itemId": LAKEHOUSE, "status": "InProgress",
                                 "startTimeUtc": self.now.isoformat()})
        with self.assertRaisesRegex(RuntimeError, "fixed preparation and coverage bounds"):
            native_demo1.prepare(self.context)
        self.assertEqual(self.store.data, {})

    def test_source_gate_rechecks_state_after_wait(self):
        for variant in ("reused", "manifest", "disabled", "failed", "stop"):
            with self.subTest(variant=variant):
                self.setUp()
                prepared = native_demo1.prepare(self.context)
                self.fabric.jobs.append({"id": OWNER, "itemId": LAKEHOUSE, "status": "InProgress",
                                         "startTimeUtc": self.now.isoformat()})

                def complete():
                    self.fabric.jobs[-1]["status"] = "Completed"
                    if variant == "reused":
                        self.spark.bronze[f"{self.run_id}_0001"] = {"row_count": 1}
                    elif variant == "manifest":
                        self.manifest["batches"]["1"] = {}
                    elif variant == "disabled":
                        self.fabric.schedule["enabled"] = False
                    elif variant == "failed":
                        self.fabric.jobs[-1]["status"] = "Failed"
                    else:
                        self.stopped = True

                self.on_sleep = complete
                with self.assertRaises(RuntimeError):
                    native_demo1.check_ready(self.context, prepared)

    def test_dimension_corruption_blocks(self):
        self.spark.bad_dimension = True
        with self.assertRaisesRegex(RuntimeError, "DimProduct"):
            native_demo1.prepare(self.context)
        self.assertEqual(self.store.data, {})

    def test_setup_window_must_be_active_not_merely_enabled(self):
        for start_offset, end_offset in ((1, 31), (-30, -1)):
            with self.subTest(start=start_offset):
                self.setUp()
                settings = self.receipt["request"]["configuration"]
                settings.update(startDateTime=(self.now + timedelta(minutes=start_offset)).isoformat(),
                                endDateTime=(self.now + timedelta(minutes=end_offset)).isoformat())
                self.receipt["create_response"]["configuration"] = copy.deepcopy(settings)
                self.fabric.schedule = copy.deepcopy(self.receipt["create_response"])
                with self.assertRaisesRegex(RuntimeError, "active window"):
                    native_demo1.prepare(self.context)

    def test_run_evidence_prevents_reprepare_and_handle_tampering(self):
        prepared = native_demo1.prepare(self.context)
        with self.assertRaisesRegex(RuntimeError, "conditional create"):
            native_demo1.prepare(self.context)
        prepared["receipt_sha256"] = "b" * 64
        with self.assertRaisesRegex(RuntimeError, "handle differs"):
            native_demo1.check_ready(self.context, prepared)

    def test_incomplete_or_wrong_grouped_totals_never_verify(self):
        for variant in ("missing", "total", "group", "quality", "bronze", "manifest"):
            with self.subTest(variant=variant):
                self.setUp()
                prepared = native_demo1.prepare(self.context)
                self.complete_manifest()
                key = f"{self.run_id}_0010"
                if variant == "missing":
                    del self.spark.silver[key]
                elif variant == "total":
                    self.spark.silver[key]["sales_amount_minor_units"] += 1
                elif variant == "group":
                    self.spark.silver[key]["amount_by_customer_region"] = {"Wrong": 10000}
                elif variant == "quality":
                    self.spark.silver[key]["bad"] = 1
                elif variant == "bronze":
                    self.spark.bronze[key]["row_count"] = 999
                else:
                    self.manifest["batches"]["10"]["batch_class"] = "bootstrap"
                with self.assertRaises(RuntimeError):
                    native_demo1.verify(self.context, prepared)

    def test_recent_job_must_follow_final_delivery_not_first(self):
        prepared = native_demo1.prepare(self.context)
        self.complete_manifest()
        self.fabric.jobs[-1]["endTimeUtc"] = (self.now - timedelta(minutes=5)).isoformat()
        with self.assertRaisesRegex(RuntimeError, "fixed verification deadline"):
            native_demo1.verify(self.context, prepared)

    def test_verification_never_substitutes_generic_timestamp_for_delivery_ack(self):
        prepared = native_demo1.prepare(self.context)
        self.complete_manifest()
        del self.manifest["batches"]["10"]["deliveries"]["demo-1"]["submission_result_at_utc"]
        with self.assertRaisesRegex(ValueError, "UTC timestamps"):
            native_demo1.verify(self.context, prepared)

    def test_verification_cannot_succeed_if_conditional_evidence_update_conflicts(self):
        prepared = native_demo1.prepare(self.context)
        self.complete_manifest()
        with patch.object(self.store, "replace", side_effect=RuntimeError("conditional replace conflict")):
            with self.assertRaisesRegex(RuntimeError, "conditional replace conflict"):
                native_demo1.verify(self.context, prepared)
        record = json.loads(next(iter(self.store.data.values()))[0])
        self.assertEqual(record["status"], "prepared")

    def test_active_job_waits_without_cancelling_and_stop_ignores_stop_flag(self):
        prepared = native_demo1.prepare(self.context)
        self.complete_manifest()
        self.fabric.jobs[-1]["status"] = "InProgress"
        self.on_sleep = lambda: self.fabric.jobs[-1].update(status="Completed")
        self.assertEqual(native_demo1.verify(self.context, prepared)["status"], "verified")
        self.stopped = True
        self.fabric.jobs[-1]["status"] = "InProgress"
        self.assertEqual(native_demo1.stop(self.context, prepared)["status"], "stopped")
        self.assertTrue(self.fabric.schedule["enabled"])

    def test_cleanup_timeout_retains_evidence_and_never_disables_setup(self):
        prepared = native_demo1.prepare(self.context)
        self.fabric.jobs[-1]["status"] = "InProgress"
        self.now += timedelta(minutes=29, seconds=59)
        with self.assertRaisesRegex(RuntimeError, "retain native writer lock"):
            native_demo1.stop(self.context, prepared)
        self.assertTrue(self.fabric.schedule["enabled"])
        record = json.loads(next(iter(self.store.data.values()))[0])
        self.assertEqual(record["status"], "prepared")

    def test_missing_evidence_only_safe_without_prepared_handle(self):
        self.assertEqual(native_demo1.stop(self.context, None)["status"], "stopped")
        with self.assertRaisesRegex(RuntimeError, "no retained evidence"):
            native_demo1.stop(self.context, {"run_id": self.run_id})


if __name__ == "__main__":
    unittest.main()
