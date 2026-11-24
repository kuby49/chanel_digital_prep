import base64
import hashlib
import json
import os
import sys
import unittest
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import ModuleType, SimpleNamespace
from unittest import mock

import deploy
import synthetic_admission as admission


def uid(n):
    return str(uuid.UUID(int=n))


def stamp(value):
    return value.isoformat()


@dataclass
class Run:
    run_id: str
    batch_class: str
    start_date: str
    seed: int
    date_count: int
    products: int
    customers: int
    file_count: int
    rows_per_file: int
    interval_seconds: int


def batch_bytes(run, number):
    batch_id = f"{run.run_id}_{number:04d}"
    content = (batch_id + "-fixture").encode()
    return content, {"batch_id": batch_id, "batch_number": number, "batch_class": run.batch_class,
                     "filename": f"sales_{batch_id}.csv", "sha256": hashlib.sha256(content).hexdigest(),
                     "row_count": run.rows_per_file, "sales_amount_minor_units": 7020}


class Fixture:
    def __init__(self):
        self.now = datetime.now(timezone.utc)
        self.start = self.now - timedelta(seconds=50)
        self.end = self.now - timedelta(seconds=5)
        self.prefix = "demo2synthetic-38bb0e3e0d0e"
        self.run_id = "demo2synthetic_38bb0e3e0d0e"
        self.target = {"workspace_id": uid(1), "item_id": uid(2), "fact": "SyntheticFactSales"}
        self.database = {"id": uid(2), "name": "SalesDatabase",
                         "query_uri": "https://query.example", "ingestion_uri": "https://ingest.example"}
        self.proof = dict(zip(
            ("producer_workspace_id", "staging_lakehouse_id", "notebook_id", "environment_id",
             "pipeline_id", "preflight_parent_job_id", "preflight_child_job_id", "auth_parent_job_id",
             "auth_child_job_id", "auth_source_id", "auth_service_operation_id"),
            (uid(n) for n in range(10, 21))))
        self.proof["nonce"] = "synthetic_nonce_1234"
        self.notebook = {"cells": [], "metadata": {"binding": "test"}}
        self.proof["definition_sha256"] = hashlib.sha256(
            json.dumps(self.notebook, sort_keys=True).encode()).hexdigest()
        self.settings = {"prefix": self.prefix, "data": {"run_id": self.run_id, "seed": 42,
                                                          "start_date": "2026-01-01", "date_count": 4,
                                                          "product_count": 3, "customer_count": 3},
                         "workspace_name": self.prefix + "-demo2-eventhouse",
                         "table": "SyntheticFactSales", "mapping": "SalesCsvV1",
                         "database_name": "SalesDatabase", "tenant_id": uid(21),
                         "producer_identity": uid(22), "max_duration": 300,
                         "expected_batch_count": 2, "rows_per_file": 5, "interval_seconds": 30,
                         "shared_workspace_id": self.proof["producer_workspace_id"],
                         "shared_lakehouse_id": self.proof["staging_lakehouse_id"]}
        producer_config = {"run_id": self.run_id, "batch_class": "bootstrap", "start_date": "2026-01-01",
                           "seed": 42, "date_count": 4, "products": 3, "customers": 3,
                           "file_count": 2, "rows_per_file": 5, "interval_seconds": 30}
        root = f"{deploy.ONELAKE}/{self.proof['producer_workspace_id']}/{self.proof['staging_lakehouse_id']}"
        self.config = {"run_id": self.run_id, "producer_config": producer_config,
                       "target_bindings": {"demo-2": self.target}, "generator_sha256": "a" * 64,
                       "manifest_uri": f"{root}/Files/producer/runs/{self.run_id}/manifest.json",
                       "sentinel_uri": f"{root}/Files/preflight/producer-monitor-heartbeat-v1.txt"}
        effective = {"contract_version": 1, "common": {"tenant_id": self.settings["tenant_id"]},
                     "shared": {"workspace_id": self.settings["shared_workspace_id"],
                                "staging_lakehouse_id": self.settings["shared_lakehouse_id"],
                                "identity_id": self.settings["producer_identity"],
                                "target_bindings": self.config["target_bindings"]},
                     "data": {"run_id": self.run_id, "batch_class": "bootstrap", "start_date": "2026-01-01",
                              "seed": 42, "date_count": 4, "product_count": 3, "customer_count": 3,
                              "file_count": 2, "rows_per_file": 5, "interval_seconds": 30,
                              "max_duration_seconds": 300, "currency": "USD"}}
        self.proof["effective_config_sha256"] = hashlib.sha256(json.dumps(
            effective, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
            allow_nan=False).encode()).hexdigest()
        self.preflight = self.make_preflight()
        run = Run(**producer_config)
        auth_run = Run(run.run_id.replace("demo2synthetic_", "demo2synthetic_auth_"),
                       "bootstrap", run.start_date, run.seed, run.date_count, run.products,
                       run.customers, 1, 1, 0)
        auth_bytes, expected = batch_bytes(auth_run, 1)
        self.proof["auth_csv_sha256"] = expected["sha256"]
        when = stamp(self.start + timedelta(seconds=10))
        outcome = {"status": "succeeded", "timestamp_utc": when, "source_id": self.proof["auth_source_id"],
                   "service_operation_id": self.proof["auth_service_operation_id"], "error_code": None,
                   "diagnostic": None}
        delivery = {"status": "verified", "submission_status": "submitted",
                    "submission_started_at_utc": when, "submission_result_at_utc": when,
                    "operation_id": self.proof["auth_source_id"], "ingestion_outcome": outcome,
                    "verified_at_utc": when, "verified_row_count": 1, "verified_distinct_sales_line_ids": 1,
                    "verified_total_minor_units": 7020}
        auth_batch = dict(expected, generated_at_utc=when, deliveries={"demo-2": delivery})
        auth_manifest = {"contract_version": 1, "run_id": auth_run.run_id, "config": vars(auth_run),
                         "generator_sha256": self.config["generator_sha256"], "destinations": ["demo-2"],
                         "target_bindings": self.config["target_bindings"], "batches": {"1": auth_batch},
                         "scheduler": {"status": "completed", "result": {"verified": True, "errors": []}}}
        self.auth_exit = {"kind": "demo2-synthetic-auth-v1", "ready": False, "status": "verified",
                          "run_id": auth_run.run_id, "batch_class": "bootstrap",
                          "manifest_path": f"Files/producer/runs/{auth_run.run_id}/manifest.json",
                          "batch_id": expected["batch_id"], "row_count": 1, "sales_amount_minor_units": 7020,
                          "source_id": self.proof["auth_source_id"], "ingestion_outcome": outcome,
                          "verified_at_utc": when, "producer_job_id": None}
        auth_root = self.config["manifest_uri"].rsplit("/", 2)[0] + "/" + auth_run.run_id
        self.objects = {auth_root + "/manifest.json": json.dumps(auth_manifest).encode(),
                        auth_root + "/" + expected["filename"]: auth_bytes,
                        self.config["sentinel_uri"]: b"sentinel"}
        manifest = {"contract_version": 1, "run_id": self.run_id, "config": producer_config,
                    "generator_sha256": self.config["generator_sha256"], "destinations": ["demo-2"],
                    "target_bindings": self.config["target_bindings"], "batches": {}}
        stage_root = self.config["manifest_uri"].rsplit("/", 1)[0]
        for n in (1, 2):
            payload, metadata = batch_bytes(run, n)
            manifest["batches"][str(n)] = dict(metadata, generated_at_utc=when,
                                               deliveries={"demo-2": {"status": "pending", "timestamp_utc": None}})
            self.objects[stage_root + "/" + metadata["filename"]] = payload
        self.objects[self.config["manifest_uri"]] = json.dumps(manifest).encode()
        self.calls = []
        self.activity_change = None
        self.child_change = None
        self.parent_change = None
        self.kql_rows = [{"RowCount": 1, "DistinctSalesLineIDs": 1, "AmountMinorUnits": 7020, "InvalidRows": 0}]
        self.deployment = SimpleNamespace(s=self.settings, client=self, ledger=object(),
                                          verify_target_ownership=lambda: self.database, _kql=self.kql)

    def make_preflight(self):
        p, s, d = self.proof, self.settings, self.database
        checks = [
            ("runtime-binding", p["notebook_id"], "notebook-runtime-context", "passed"),
            *(("token-identity", a, "runtime-token-claims", "passed") for a in ("pbi", "storage", "kusto")),
            ("runtime-dependencies", p["environment_id"], "runtime-import-and-version", "passed"),
            ("query", d["query_uri"], "kusto-read-only-query", "passed"),
            ("table-schema-read", s["table"], "kusto-read-only-management", "passed"),
            ("mapping-read", s["table"], "kusto-read-only-management", "passed"),
            ("status-queue", d["ingestion_uri"], "sdk-success-and-failure-nondestructive-peek", "passed"),
            ("conditional-storage", p["staging_lakehouse_id"], "read-only-synthetic-probe", "not-run"),
            ("effective-ingestion-authorization", d["id"], "authoritative-role-or-approved-ingestion-probe-required",
             "unverified")]
        target = dict(self.target, database=d["name"], mapping=s["mapping"], query_service_uri=d["query_uri"],
                      ingestion_service_uri=d["ingestion_uri"],
                      checks=[dict(zip(("capability", "resource", "method", "result"), c)) for c in checks])
        return {"version": 1, "kind": "producer-runtime-preflight", "authority": "runtime-observation-only",
                "status": "blocked", "nonce": p["nonce"], "run_id": self.run_id,
                "tenant_id": s["tenant_id"], "identity_id": s["producer_identity"],
                "selected_demos": ["demo-2"], "effective_config_sha256": p["effective_config_sha256"],
                "issued_at_utc": stamp(self.start + timedelta(seconds=20)),
                "expires_at_utc": stamp(self.start + timedelta(seconds=290)),
                "producer": {"workspace_id": p["producer_workspace_id"],
                             "staging_lakehouse_id": p["staging_lakehouse_id"], "notebook_id": p["notebook_id"],
                             "environment_id": p["environment_id"], "definition_sha256": p["definition_sha256"],
                             "job_id": None, "observed_run_id": None,
                             "observed_notebook_id": p["notebook_id"],
                             "runtime_context_checks": {k: True for k in ("pipeline", "workspace", "lakehouse", "notebook")}},
                "targets": {"demo-2": target}, "status_queue_peeks": {"success": "passed", "failure": "passed"},
                "token_identities": {a: {"identity_id": s["producer_identity"], "tenant_id": s["tenant_id"]}
                                     for a in ("pbi", "storage", "kusto")}}

    def fabric(self, method, path, body=None, allow_missing=False):
        self.calls.append((method, path, body))
        p = self.proof
        if "getDefinition" in path:
            data = base64.b64encode(json.dumps(self.notebook).encode()).decode()
            return 200, {}, {"definition": {"parts": [{"path": "notebook-content.ipynb", "payload": data,
                                                        "payloadType": "InlineBase64"}]}}
        if "queryactivityruns" in path:
            parent = path.split("/")[-2]
            which = "preflight" if parent == p["preflight_parent_job_id"] else "auth"
            child = p[which + "_child_job_id"]
            activity = {"activityName": "RunProducer", "activityType": "TridentNotebook",
                        "status": "Succeeded", "pipelineRunId": parent, "activityRunId": child,
                        "input": {"workspaceId": p["producer_workspace_id"], "notebookId": p["notebook_id"]},
                        "output": {"result": {"runId": child,
                                              "exitValue": json.dumps(
                                                  self.preflight if which == "preflight" else self.auth_exit)}}}
            if self.activity_change:
                self.activity_change(activity)
            return 200, {}, {"value": [activity]}
        job_id = path.rsplit("/", 1)[-1]
        if job_id in (p["preflight_parent_job_id"], p["auth_parent_job_id"]):
            job = {"id": job_id, "itemId": p["pipeline_id"], "jobType": "Pipeline", "status": "Completed",
                   "startTimeUtc": self.start.replace(tzinfo=None).isoformat(timespec="microseconds") + "0",
                   "endTimeUtc": self.end.replace(tzinfo=None).isoformat(timespec="microseconds") + "0"}
            if self.parent_change:
                self.parent_change(job)
            return 200, {}, job
        job = {"id": job_id, "itemId": p["notebook_id"], "jobType": "PipelineRunNotebook",
               "status": "Completed"}
        if self.child_change:
            self.child_change(job)
        return 200, {}, job

    def kql(self, database, command, query=False):
        if "WrongIds" in command:
            return [{"WrongIds": 0}]
        if query:
            return self.kql_rows
        return []

    def run(self):
        with mock.patch.object(deploy, "_journal_target", return_value=self.target), \
             mock.patch.object(deploy, "verify_mapping"), \
             mock.patch.object(admission, "_generator", return_value={"RunConfig": Run, "batch_bytes": batch_bytes}), \
             mock.patch.object(admission, "_read_bytes", side_effect=lambda client, uri: self.objects[uri]), \
             mock.patch.object(admission, "_queue") as queue, \
             mock.patch("monitor_controller.SENTINEL_SHA256", hashlib.sha256(b"sentinel").hexdigest()):
            expiry = admission.verify_synthetic_runtime_access(self.deployment, self.config, self.proof)
            self_expiry = datetime.fromisoformat(self.preflight["expires_at_utc"])
            if expiry != self_expiry:
                raise AssertionError("admission did not return the checked expiry")
            queue.assert_called_once()


class AdmissionTests(unittest.TestCase):
    def test_valid_fixture_uses_only_read_calls(self):
        fixture = Fixture()
        fixture.run()
        self.assertTrue(all(method == "GET" or "getDefinition" in path or "queryactivityruns" in path
                            for method, path, _ in fixture.calls))
        queries = [body for _, path, body in fixture.calls if "queryactivityruns" in path]
        self.assertEqual(len(queries), 2)
        self.assertEqual(queries[0]["lastUpdatedAfter"],
                         (fixture.start - timedelta(minutes=3)).isoformat().replace("+00:00", "Z"))
        self.assertEqual(queries[0]["lastUpdatedBefore"],
                         (fixture.end + timedelta(minutes=3)).isoformat().replace("+00:00", "Z"))

    def test_preflight_mismatch_staleness_and_unsupported_claims_block(self):
        mutations = [
            lambda f: f.preflight.update(nonce="other_nonce_123456"),
            lambda f: f.preflight.update(expires_at_utc=stamp(f.now - timedelta(seconds=1))),
            lambda f: f.preflight.update(issued_at_utc=stamp(f.start)),
            lambda f: f.preflight.update(expires_at_utc=stamp(f.start + timedelta(seconds=321))),
            lambda f: f.preflight["producer"]["runtime_context_checks"].update(workspace=False),
            lambda f: f.preflight["producer"].update(observed_run_id=f.proof["preflight_child_job_id"]),
            lambda f: f.preflight["token_identities"]["kusto"].update(identity_id=uid(99)),
            lambda f: f.preflight["targets"]["demo-2"].update(item_id=uid(99)),
            lambda f: f.preflight["targets"]["demo-2"]["checks"][-1].update(result="passed"),
            lambda f: f.preflight["status_queue_peeks"].update(failure="failed"),
            lambda f: f.preflight.update(effective_config_sha256="b" * 64),
            lambda f: f.proof.update(effective_config_sha256="b" * 64),
        ]
        for mutate in mutations:
            with self.subTest(mutate=mutate):
                fixture = Fixture()
                mutate(fixture)
                with self.assertRaises(deploy.Blocked):
                    fixture.run()

    def test_jobs_definition_and_replay_block(self):
        mutations = [
            lambda f: setattr(f, "activity_change", lambda a: a["output"]["result"].update(runId=uid(99))),
            lambda f: setattr(f, "activity_change", lambda a: a["input"].update(workspaceId=uid(99))),
            lambda f: setattr(f, "activity_change", lambda a: a.update(pipelineRunId=uid(99))),
            lambda f: setattr(f, "activity_change", lambda a: a.update(status="Failed")),
            lambda f: setattr(f, "activity_change", lambda a: a.update(retryAttempt=False)),
            lambda f: setattr(f, "activity_change", lambda a: a["output"]["result"].pop("exitValue")),
            lambda f: setattr(f, "activity_change", lambda a: a["output"]["result"].update(exitValue="{}")),
            lambda f: setattr(f, "activity_change", lambda a: a["output"]["result"].update(exitValue="{")),
            lambda f: setattr(f, "child_change", lambda j: j.update(status="Failed")),
            lambda f: setattr(f, "child_change", lambda j: j.update(properties={"exitValue": '{"kind":"other"}'})),
            lambda f: setattr(f, "parent_change", lambda j: j.update(startTimeUtc="2026-11-24 08:21:48")),
            lambda f: f.proof.update(auth_parent_job_id=f.proof["preflight_parent_job_id"]),
            lambda f: f.notebook.update(cells=[{"changed": True}]),
        ]
        for mutate in mutations:
            with self.subTest(mutate=mutate):
                fixture = Fixture()
                mutate(fixture)
                with self.assertRaises(deploy.Blocked):
                    fixture.run()

    def test_auth_activity_requires_matching_exit(self):
        for change in (lambda result: result.pop("exitValue"),
                       lambda result: result.update(exitValue='{"kind":"other"}')):
            fixture = Fixture()
            fixture.activity_change = lambda activity: (
                change(activity["output"]["result"])
                if activity["pipelineRunId"] == fixture.proof["auth_parent_job_id"] else None)
            with self.subTest(change=change), self.assertRaises(deploy.Blocked):
                fixture.run()

    def test_auth_source_and_staged_manifest_mismatches_block(self):
        def mutate_manifest(f, change):
            path = f.config["manifest_uri"]
            manifest = json.loads(f.objects[path])
            change(manifest)
            f.objects[path] = json.dumps(manifest).encode()

        mutations = [
            lambda f: f.proof.update(auth_source_id=uid(99)),
            lambda f: f.auth_exit["ingestion_outcome"].update(service_operation_id=uid(99)),
            lambda f: f.kql_rows[0].update(InvalidRows=1),
            lambda f: mutate_manifest(f, lambda m: m["batches"]["1"]["deliveries"]["demo-2"].update(status="verified")),
            lambda f: mutate_manifest(f, lambda m: m["batches"]["2"].update(batch_class="measured")),
            lambda f: mutate_manifest(f, lambda m: m["config"].update(products=True)),
            lambda f: mutate_manifest(f, lambda m: m["target_bindings"]["demo-2"].update(item_id=uid(99))),
            lambda f: f.objects.update({f.config["sentinel_uri"]: b"wrong"}),
        ]
        for mutate in mutations:
            with self.subTest(mutate=mutate):
                fixture = Fixture()
                mutate(fixture)
                with self.assertRaises(deploy.Blocked):
                    fixture.run()

    def test_missing_proof_blocks_without_client_calls(self):
        fixture = Fixture()
        with self.assertRaises(deploy.Blocked):
            admission.verify_synthetic_runtime_access(fixture.deployment, fixture.config, {})
        self.assertEqual(fixture.calls, [])

    def test_only_job_timestamps_can_omit_timezone(self):
        fixture = Fixture()
        for value in ("2026-11-24T08:21:48.4417519", "2026-11-24T08:21:48"):
            self.assertEqual(admission._job_utc(value).utcoffset(), timedelta(0))
            with self.assertRaises(deploy.Blocked):
                admission._utc(value)
        for value in ("2026-11-24T08:21:48+01:00", "2026-99-24T08:21:48", "2026-11-24 08:21:48"):
            with self.subTest(value=value), self.assertRaises(deploy.Blocked):
                admission._job_utc(value)
        fixture.preflight["issued_at_utc"] = fixture.preflight["issued_at_utc"].removesuffix("+00:00")
        with self.assertRaises(deploy.Blocked):
            fixture.run()

    def test_generator_loads_actual_producer_source_and_checks_digest(self):
        configured = os.environ.get("CHANEL_PRODUCER_GENERATOR")
        path = Path(configured) if configured else deploy.ROOT / "demo-data-generator" / "generator.py"
        if not path.is_file():
            self.skipTest("Producer generator is not present in this checkout.")
        self.assertEqual((path.parent.name, path.name), ("demo-data-generator", "generator.py"))
        raw = path.read_bytes().decode("utf-8").replace("\r\n", "\n").replace("\r", "\n")
        digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()
        with mock.patch.object(deploy, "ROOT", path.parent.parent):
            generator = admission._generator({"generator_sha256": digest})
            run = generator["RunConfig"](**Fixture().config["producer_config"])
            first = generator["batch_bytes"](run, 1)[1]
            second = generator["batch_bytes"](run, 2)[1]
            self.assertEqual((first["sha256"], first["sales_amount_minor_units"]),
                             ("a99d563a8b3ad25eefb3f748c18bc0ab738240475e83cbb0c7fd70b77a22cd05", 526232))
            self.assertEqual((second["sha256"], second["sales_amount_minor_units"]),
                             ("d90692213e0d10e124109183736fa4576401b470726e535a062c35ecb759ec9a", 351377))
            with self.assertRaises(deploy.Blocked):
                admission._generator({"generator_sha256": "0" * 64})

    def test_queue_requires_exact_source_and_service_operation(self):
        fixture = Fixture()
        identity = ModuleType("azure.identity")
        data = ModuleType("azure.kusto.data")
        ingest = ModuleType("azure.kusto.ingest")
        credential = mock.Mock()
        ingest_client = mock.Mock()
        identity.AzureCliCredential = mock.Mock(return_value=credential)
        data.KustoConnectionStringBuilder = SimpleNamespace(with_token_provider=lambda endpoint, token: endpoint)
        ingest.QueuedIngestClient = mock.Mock(return_value=ingest_client)
        outcomes = [
            ("succeeded", fixture.proof["auth_source_id"], fixture.proof["auth_service_operation_id"], False),
            ("failed", fixture.proof["auth_source_id"], fixture.proof["auth_service_operation_id"], True),
            ("succeeded", uid(99), fixture.proof["auth_service_operation_id"], True),
            ("succeeded", fixture.proof["auth_source_id"], uid(99), True),
            ("pending", fixture.proof["auth_source_id"], None, True),
        ]
        for status, source, operation, blocked in outcomes:
            with self.subTest(status=status, source=source, operation=operation):
                adapter = mock.Mock()
                adapter.ingestion_status.return_value = SimpleNamespace(
                    status=status, source_id=source, service_operation_id=operation)
                with mock.patch.dict(sys.modules, {"azure.identity": identity, "azure.kusto.data": data,
                                                   "azure.kusto.ingest": ingest}), \
                     mock.patch("ingestion.EventhouseAdapter", return_value=adapter):
                    if blocked:
                        with self.assertRaises(deploy.Blocked):
                            admission._queue(fixture.deployment, fixture.database, fixture.proof)
                    else:
                        admission._queue(fixture.deployment, fixture.database, fixture.proof)
                adapter.ingestion_status.assert_called_once_with(fixture.proof["auth_source_id"])
        self.assertEqual(ingest_client.close.call_count, len(outcomes))
        self.assertEqual(credential.close.call_count, len(outcomes))

    def test_installed_sdk_queues_and_cleanup_without_network(self):
        try:
            from azure.identity import AzureCliCredential
            from azure.kusto.ingest import QueuedIngestClient
        except ImportError:
            self.skipTest("Azure identity and Kusto ingestion SDKs are unavailable.")
        fixture = Fixture()
        fixture.database["ingestion_uri"] = "https://ingest-example.kusto.fabric.microsoft.com"
        message = SimpleNamespace(IngestionSourceId=fixture.proof["auth_source_id"],
                                  OperationId=fixture.proof["auth_service_operation_id"], ErrorCode=None)
        failure, success = mock.Mock(), mock.Mock()
        failure.peek.return_value, success.peek.return_value = [], [message]
        queues = SimpleNamespace(failure=failure, success=success)
        original_close = QueuedIngestClient.close
        credential_close = AzureCliCredential.close
        with mock.patch("azure.kusto.ingest.status.KustoIngestStatusQueues", return_value=queues), \
             mock.patch.object(QueuedIngestClient, "close", autospec=True, side_effect=original_close) as close, \
             mock.patch.object(AzureCliCredential, "close", autospec=True, side_effect=credential_close) as cred_close, \
             mock.patch.object(AzureCliCredential, "get_token", side_effect=AssertionError("network token requested")), \
             mock.patch("requests.sessions.Session.request", side_effect=AssertionError("network request")):
            admission._queue(fixture.deployment, fixture.database, fixture.proof)
        failure.peek.assert_called_once_with(32)
        success.peek.assert_called_once_with(32)
        close.assert_called_once()
        cred_close.assert_called_once()

    def test_queue_closes_credential_if_client_creation_fails(self):
        fixture = Fixture()
        identity = ModuleType("azure.identity")
        data = ModuleType("azure.kusto.data")
        ingest = ModuleType("azure.kusto.ingest")
        credential = mock.Mock()
        identity.AzureCliCredential = mock.Mock(return_value=credential)
        data.KustoConnectionStringBuilder = SimpleNamespace(with_token_provider=lambda endpoint, token: endpoint)
        ingest.QueuedIngestClient = mock.Mock(side_effect=RuntimeError("unsupported client"))
        with mock.patch.dict(sys.modules, {"azure.identity": identity, "azure.kusto.data": data,
                                           "azure.kusto.ingest": ingest}), self.assertRaises(deploy.Blocked):
            admission._queue(fixture.deployment, fixture.database, fixture.proof)
        credential.close.assert_called_once()


if __name__ == "__main__":
    unittest.main()
