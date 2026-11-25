import base64
import hashlib
import io
import json
import tempfile
import unittest
from pathlib import Path
from types import ModuleType, SimpleNamespace
from unittest import mock
import sys

from generator import RunConfig, batch_bytes, canonical_bytes
from fabric_runner import run_fabric_batches
from synthetic_fixture import (authorization_run, peek_status_queues, prepare_fixture,
                               stage_fixture, verified_authorization)
from test_fabric_ledger import MemoryStore, TEST_TARGETS
from test_fabric_runner import EventhouseAdapter, FakeClock


WORKSPACE = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
LAKEHOUSE = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
NOTEBOOK = "cccccccc-cccc-4ccc-8ccc-cccccccccccc"
TENANT = "dddddddd-dddd-4ddd-8ddd-dddddddddddd"
IDENTITY = "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee"
TARGET = dict(TEST_TARGETS["demo-2"], fact="SyntheticFactSales")
RUN = "demo2synthetic_38bb0e3e0d0e"
PROBE = "synthetic-probe-1234567890"


def fixture():
    document = {
        "contract_version": 1,
        "common": {"tenant_id": TENANT},
        "shared": {"workspace_id": WORKSPACE, "staging_lakehouse_id": LAKEHOUSE,
                   "identity_id": IDENTITY, "target_bindings": {"demo-2": dict(TARGET)}},
        "data": {"run_id": RUN, "batch_class": "bootstrap", "start_date": "2026-01-01",
                 "seed": 42, "date_count": 4, "product_count": 3, "customer_count": 3,
                 "file_count": 2, "rows_per_file": 5, "interval_seconds": 30,
                 "max_duration_seconds": 300, "currency": "USD"},
    }
    request = {
        "kind": "demo2-synthetic-stage-v1", "nonce": PROBE,
        "config_sha256": hashlib.sha256(canonical_bytes(document)).hexdigest(),
        "tenant_id": TENANT, "identity_id": IDENTITY,
        "producer": {"workspace_id": WORKSPACE, "staging_lakehouse_id": LAKEHOUSE,
                     "notebook_id": NOTEBOOK},
        "target": dict(TARGET),
    }
    context = {"isForPipeline": True, "currentWorkspaceId": WORKSPACE,
               "defaultLakehouseId": LAKEHOUSE, "currentNotebookId": NOTEBOOK,
               "currentRunId": "ffffffff-ffff-4fff-8fff-ffffffffffff"}
    identities = {"storage": (TENANT, IDENTITY), "kusto": (TENANT, IDENTITY)}
    return document, request, context, identities


class FixtureTests(unittest.TestCase):
    def test_exact_bootstrap_manifest_and_replay_stays_pending(self):
        document, request, context, identities = fixture()
        run, targets, _ = prepare_fixture(document, ["demo-2"], PROBE, request, context, identities)
        store = MemoryStore()
        path, manifest = stage_fixture(store, run, targets)
        self.assertEqual(path, f"Files/producer/runs/{RUN}/manifest.json")
        self.assertEqual((manifest["config"]["batch_class"], manifest["destinations"]),
                         ("bootstrap", ["demo-2"]))
        self.assertEqual(manifest["target_bindings"], {"demo-2": TARGET})
        self.assertEqual(set(manifest["batches"]), {"1", "2"})
        for number in (1, 2):
            batch = manifest["batches"][str(number)]
            self.assertEqual(batch["batch_class"], "bootstrap")
            self.assertEqual(batch["row_count"], 5)
            self.assertEqual(batch["deliveries"]["demo-2"]["status"], "pending")
            self.assertNotIn("submission_started_at_utc", batch["deliveries"]["demo-2"])
            self.assertEqual(store.read(f"Files/producer/runs/{RUN}/{batch['filename']}")[0],
                             batch_bytes(run, number)[0])
        self.assertEqual([row.split(b",", 1)[0] for row in store.read(
            f"Files/producer/runs/{RUN}/{manifest['batches']['1']['filename']}")[0].splitlines()[1:]],
                         [b"1", b"2", b"3", b"4", b"5"])
        _, replay = stage_fixture(store, run, targets)
        self.assertEqual(replay, manifest)
        with self.assertRaisesRegex(ValueError, "identity|target|configuration"):
            stage_fixture(store, RunConfig(RUN, batch_class="bootstrap", file_count=2, rows_per_file=5,
                                           seed=43), targets)

    def test_rejects_non_synthetic_selection_context_and_target_before_storage(self):
        document, request, context, identities = fixture()
        changes = [
            (lambda: document["data"].update(batch_class="measured")),
            (lambda: document["data"].update(rows_per_file=6)),
            (lambda: document["data"].update(interval_seconds=0)),
            (lambda: document["data"].update(seed=43)),
            (lambda: document["data"].update(run_id="normal")),
            (lambda: document["shared"]["target_bindings"]["demo-2"].update(fact="FactSales")),
            (lambda: context.update(currentNotebookId=WORKSPACE)),
            (lambda: identities.update(kusto=(TENANT, WORKSPACE))),
        ]
        for change in changes:
            with self.subTest(change=change):
                document, request, context, identities = fixture()
                change()
                with self.assertRaises(ValueError):
                    prepare_fixture(document, ["demo-2"], PROBE, request, context, identities)
        document, request, context, identities = fixture()
        with self.assertRaises(ValueError):
            prepare_fixture(document, ["demo-1", "demo-2"], PROBE, request, context, identities)
        with self.assertRaises(ValueError):
            prepare_fixture(document, ["demo-2"], "other-probe-123456789", request, context, identities)

    def test_schema_catalog_synthetic_fixture_is_still_bound_to_original_staging(self):
        document, request, context, identities = fixture()
        catalog_workspace = "11111111-1111-4111-8111-111111111111"
        catalog_id = "22222222-2222-4222-8222-222222222222"
        document["shared"]["notebook_default_lakehouse"] = {
            "workspace_id": catalog_workspace, "lakehouse_id": catalog_id, "lakehouse_name": "schema_catalog"}
        request["config_sha256"] = hashlib.sha256(canonical_bytes(document)).hexdigest()
        context.update(defaultLakehouseId=catalog_id, defaultLakehouseWorkspaceId=catalog_workspace)
        published = {"workspace_id": WORKSPACE, "staging_lakehouse_id": LAKEHOUSE,
                     "default_lakehouse_workspace_id": catalog_workspace, "default_lakehouse_id": catalog_id}
        with self.assertRaisesRegex(ValueError, "bound producer notebook"):
            prepare_fixture(document, ["demo-2"], PROBE, request, context, identities)
        _, _, producer = prepare_fixture(document, ["demo-2"], PROBE, request, context, identities,
                                          published_binding=published)
        self.assertEqual(producer["staging_lakehouse_id"], LAKEHOUSE)
        with self.assertRaisesRegex(ValueError, "bound producer notebook"):
            prepare_fixture(document, ["demo-2"], PROBE, request, context, identities,
                            published_binding=dict(published, staging_lakehouse_id=catalog_id))

    def test_queue_peek_is_non_destructive_and_does_not_return_message_contents(self):
        calls = []

        class Queue:
            def __init__(self, name):
                self.name = name

            def peek(self, n, raw):
                calls.append((self.name, n, raw))
                return ["private message"]

        class Queues:
            def __init__(self, client):
                self.success, self.failure = Queue("success"), Queue("failure")

        packages = {key: ModuleType(key) for key in
                    ("azure", "azure.kusto", "azure.kusto.ingest", "azure.kusto.ingest.status")}
        packages["azure.kusto.ingest.status"].KustoIngestStatusQueues = Queues
        with mock.patch.dict(sys.modules, packages):
            self.assertEqual(peek_status_queues(object()), {"success": "passed", "failure": "passed"})
        self.assertEqual(calls, [("success", 1, True), ("failure", 1, True)])

    def test_delivery_resumes_only_fully_staged_pending_bootstrap(self):
        document, request, context, identities = fixture()
        run, targets, _ = prepare_fixture(document, ["demo-2"], PROBE, request, context, identities)
        store, clock = MemoryStore(), FakeClock()
        stage_fixture(store, run, targets)
        adapter = EventhouseAdapter(clock, "succeeded")
        kwargs = {"selected_targets": targets, "clock": clock.clock, "sleep": clock.sleep,
                  "poll_seconds": 1}
        with self.assertRaisesRegex(RuntimeError, "predate the scheduler"):
            run_fabric_batches(store, run, {"demo-2": adapter}, 300, 1, **kwargs)
        result = run_fabric_batches(store, run, {"demo-2": adapter}, 300, 1,
                                    allow_staged_pending=True, **kwargs)
        self.assertTrue(result["verified"])
        self.assertEqual([batch_id for batch_id, _ in adapter.calls],
                         [RUN + "_0001", RUN + "_0002"])
        manifest = json.loads(store.read(f"Files/producer/runs/{RUN}/manifest.json")[0])
        self.assertEqual([manifest["batches"][str(n)]["deliveries"]["demo-2"]["status"]
                          for n in (1, 2)], ["verified", "verified"])
        again = run_fabric_batches(store, run, {"demo-2": adapter}, 300, 1,
                                   allow_staged_pending=True, **kwargs)
        self.assertEqual((again, len(adapter.calls)), (result, 2))

    def test_auth_probe_is_one_row_outside_monitored_manifest_and_cannot_repeat(self):
        document, request, context, identities = fixture()
        context.pop("currentRunId")
        request["kind"] = "demo2-synthetic-auth-v1"
        request["kusto"] = {"database": "SalesDatabase", "fact": "SyntheticFactSales",
                            "mapping": "SalesCsvV1",
                            "query_service_uri": "https://query.kusto.fabric.microsoft.com",
                            "ingestion_service_uri": "https://ingest-query.kusto.fabric.microsoft.com"}
        run, targets, _ = prepare_fixture(document, ["demo-2"], PROBE, request, context, identities,
                                          kind="demo2-synthetic-auth-v1")
        self.assertEqual((authorization_run(run).run_id, authorization_run(run).rows_per_file),
                         ("demo2synthetic_auth_38bb0e3e0d0e", 1))
        invalid = dict(request, kusto=dict(request["kusto"], ingestion_service_uri=
                       "https://ingest-other.kusto.fabric.microsoft.com"))
        with self.assertRaisesRegex(ValueError, "endpoints differ"):
            prepare_fixture(document, ["demo-2"], PROBE, invalid, context, identities,
                            kind="demo2-synthetic-auth-v1")
        source = json.loads((Path(__file__).parent / "notebook.ipynb").read_text())
        text = "".join(source["cells"][-1]["source"])
        store, clock = MemoryStore(), FakeClock()
        adapter = EventhouseAdapter(clock, "succeeded")
        claims = base64.urlsafe_b64encode(json.dumps({"tid": TENANT, "oid": IDENTITY}).encode()).decode().rstrip("=")

        class NotebookExit(BaseException):
            pass

        parameters = {"operation": "synthetic-auth", "probe_id": PROBE,
                      "runtime_evidence_json": json.dumps(request),
                      "run_config_json": json.dumps(document),
                      "selected_demos_json": '["demo-2"]',
                      "spark": SimpleNamespace(conf=SimpleNamespace(set=lambda *args: None)),
                      "notebookutils": SimpleNamespace(
                          credentials=SimpleNamespace(getToken=lambda audience: f"header.{claims}.signature"),
                          runtime=SimpleNamespace(context=context),
                          notebook=SimpleNamespace(exit=lambda result: (_ for _ in ()).throw(NotebookExit(result))))}
        with mock.patch("fabric_storage.OneLakeStore", return_value=store), \
                mock.patch("fabric_runtime.build_adapters", return_value={"demo-2": adapter}), \
                mock.patch("fabric_runner.run_fabric_batches",
                           side_effect=lambda *args, **kwargs: run_fabric_batches(
                               *args, **dict(kwargs, clock=clock.clock, sleep=clock.sleep))):
            with self.assertRaises(NotebookExit) as caught:
                exec(text, parameters)
            outcome = json.loads(caught.exception.args[0])
            self.assertEqual((outcome["status"], outcome["batch_id"], outcome["row_count"]),
                             ("verified", "demo2synthetic_auth_38bb0e3e0d0e_0001", 1))
            self.assertEqual(outcome["source_id"], EventhouseAdapter.source_id)
            self.assertEqual(len(adapter.calls), 1)
            self.assertNotIn(f"Files/producer/runs/{RUN}/manifest.json", store.files)
            with self.assertRaisesRegex(RuntimeError, "already initialized"):
                exec(text, parameters)
            self.assertEqual(len(adapter.calls), 1)

    def test_synthetic_delivery_requires_verified_distinct_auth_manifest(self):
        document, request, context, identities = fixture()
        context.pop("currentRunId")
        request["kind"] = "demo2-synthetic-deliver-v1"
        request["kusto"] = {"database": "SalesDatabase", "fact": "SyntheticFactSales",
                            "mapping": "SalesCsvV1",
                            "query_service_uri": "https://query.kusto.fabric.microsoft.com",
                            "ingestion_service_uri": "https://ingest-query.kusto.fabric.microsoft.com"}
        run, targets, _ = prepare_fixture(document, ["demo-2"], PROBE, request, context, identities,
                                          kind="demo2-synthetic-deliver-v1")
        store, clock = MemoryStore(), FakeClock()
        source = json.loads((Path(__file__).parent / "notebook.ipynb").read_text())
        text = "".join(source["cells"][-1]["source"])
        claims = base64.urlsafe_b64encode(json.dumps({"tid": TENANT, "oid": IDENTITY}).encode()).decode().rstrip("=")

        class NotebookExit(BaseException):
            pass

        parameters = {"operation": "synthetic-deliver", "probe_id": PROBE,
                      "runtime_evidence_json": json.dumps(request),
                      "run_config_json": json.dumps(document),
                      "selected_demos_json": '["demo-2"]',
                      "spark": SimpleNamespace(conf=SimpleNamespace(set=lambda *args: None)),
                      "notebookutils": SimpleNamespace(
                          credentials=SimpleNamespace(getToken=lambda audience: f"header.{claims}.signature"),
                          runtime=SimpleNamespace(context=context),
                          notebook=SimpleNamespace(exit=lambda result: (_ for _ in ()).throw(NotebookExit(result))))}
        adapter = EventhouseAdapter(clock, "succeeded")
        with mock.patch("fabric_storage.OneLakeStore", return_value=store), \
                mock.patch("fabric_runtime.build_adapters", return_value={"demo-2": adapter}), \
                mock.patch("fabric_runner.run_fabric_batches",
                           side_effect=lambda *args, **kwargs: run_fabric_batches(
                               *args, **dict(kwargs, clock=clock.clock, sleep=clock.sleep))):
            with self.assertRaises(FileNotFoundError):
                exec(text, parameters)
            self.assertNotIn(f"Files/producer/runs/{RUN}/manifest.json", store.files)
            run_fabric_batches(store, authorization_run(run), {"demo-2": adapter}, 300, 1,
                               selected_targets=targets, clock=clock.clock, sleep=clock.sleep)
            self.assertEqual(verified_authorization(store, run, targets), EventhouseAdapter.source_id)
            auth_path = f"Files/producer/runs/{authorization_run(run).run_id}/manifest.json"
            original = store.files[auth_path]
            altered = json.loads(original[0])
            altered["batches"]["1"]["deliveries"]["demo-2"]["ingestion_outcome"]["status"] = "failed"
            store.files[auth_path] = (json.dumps(altered).encode(), original[1])
            with self.assertRaisesRegex(ValueError, "terminal ingestion"):
                verified_authorization(store, run, targets)
            store.files[auth_path] = original
            stage_fixture(store, run, targets)
            with self.assertRaises(NotebookExit) as caught:
                exec(text, parameters)
        outcome = json.loads(caught.exception.args[0])
        self.assertEqual((outcome["kind"], outcome["status"], outcome["batch_ids"]),
                         ("demo2-synthetic-deliver-v1", "verified", [RUN + "_0001", RUN + "_0002"]))
        self.assertEqual(outcome["authorization_source_id"], EventhouseAdapter.source_id)
        self.assertEqual(len(adapter.calls), 3)

    def test_notebook_synthetic_stage_requires_runtime_binding_and_writes_no_delivery(self):
        document, request, context, _ = fixture()
        context.pop("currentRunId")
        source = json.loads((Path(__file__).parent / "notebook.ipynb").read_text())
        text = "".join(source["cells"][-1]["source"])
        store = MemoryStore()

        def token(audience):
            claims = {"tid": TENANT, "oid": IDENTITY}
            body = base64.urlsafe_b64encode(json.dumps(claims).encode()).decode().rstrip("=")
            return f"header.{body}.signature"

        class NotebookExit(BaseException):
            pass

        def exit_notebook(payload):
            raise NotebookExit(payload)

        parameters = {"operation": "synthetic-stage", "probe_id": PROBE,
                      "runtime_evidence_json": json.dumps(request),
                      "run_config_json": json.dumps(document),
                      "selected_demos_json": '["demo-2"]',
                      "spark": SimpleNamespace(conf=SimpleNamespace(set=lambda *args: None)),
                      "notebookutils": SimpleNamespace(
                          credentials=SimpleNamespace(getToken=token),
                          runtime=SimpleNamespace(context=context),
                          notebook=SimpleNamespace(exit=exit_notebook))}
        with mock.patch("fabric_storage.OneLakeStore", return_value=store):
            with self.assertRaises(NotebookExit) as caught:
                exec(text, parameters)
        outcome = json.loads(caught.exception.args[0])
        self.assertEqual((outcome["kind"], outcome["ready"], outcome["delivery"]),
                         ("demo2-synthetic-stage-v1", False, "pending"))
        self.assertEqual(outcome["batch_ids"], [RUN + "_0001", RUN + "_0002"])
        self.assertEqual(json.loads(store.read(outcome["manifest_path"])[0])["batches"]["2"]
                         ["deliveries"]["demo-2"]["status"], "pending")

    def test_synthetic_preflight_reads_target_without_storage_writes(self):
        source = json.loads((Path(__file__).parent / "notebook.ipynb").read_text())
        request = {
            "nonce": "0123456789abcdef", "run_id": RUN, "tenant_id": TENANT,
            "identity_id": IDENTITY,
            "producer": {"workspace_id": WORKSPACE, "staging_lakehouse_id": LAKEHOUSE,
                         "notebook_id": NOTEBOOK, "environment_id": WORKSPACE,
                         "definition_sha256": "a" * 64},
            "demo-2": {**TARGET, "database": "SalesDatabase", "mapping": "SalesCsvV1",
                       "query_service_uri": "https://query.kusto.fabric.microsoft.com",
                       "ingestion_service_uri": "https://ingest.kusto.fabric.microsoft.com"},
        }
        context = {"isForPipeline": True, "currentWorkspaceId": WORKSPACE,
                   "defaultLakehouseId": LAKEHOUSE, "currentNotebookId": NOTEBOOK}
        claims = base64.urlsafe_b64encode(json.dumps({"oid": IDENTITY, "tid": TENANT}).encode()).decode().rstrip("=")
        evidence = []
        class KustoClient:
            def __init__(self, connection):
                pass

            def execute_query(self, database, query):
                return SimpleNamespace(primary_results=[[{"ProducerRuntimeProbe": 1}]])

            def execute_mgmt(self, database, query):
                return SimpleNamespace(primary_results=[[{"metadata": "available"}]])

        class IngestClient:
            def __init__(self, connection):
                pass

            def close(self):
                pass

        packages = {key: ModuleType(key) for key in ("azure", "azure.kusto", "azure.kusto.data",
                                                    "azure.kusto.ingest", "azure.identity")}
        packages["azure.kusto.data"].KustoClient = KustoClient
        packages["azure.kusto.data"].KustoConnectionStringBuilder = SimpleNamespace(
            with_token_provider=lambda *args: None)
        packages["azure.kusto.ingest"].QueuedIngestClient = IngestClient
        parameters = {"operation": "preflight", "probe_id": PROBE,
                      "runtime_evidence_json": json.dumps(request),
                      "run_config_json": "{}", "selected_demos_json": '["demo-2"]',
                      "spark": SimpleNamespace(conf=SimpleNamespace(set=lambda *args: None)),
                      "notebookutils": SimpleNamespace(
                          credentials=SimpleNamespace(getToken=lambda audience: f"header.{claims}.signature"),
                          runtime=SimpleNamespace(context=context),
                          notebook=SimpleNamespace(exit=evidence.append))}
        with mock.patch.dict(sys.modules, packages), \
                mock.patch("fabric_storage.OneLakeStore", side_effect=AssertionError("Storage write")), \
                mock.patch("synthetic_fixture.peek_status_queues",
                           return_value={"success": "passed", "failure": "passed"}):
            exec("".join(source["cells"][-1]["source"]), parameters)
        checks = json.loads(evidence[0])["targets"]["demo-2"]["checks"]
        self.assertEqual(next(c for c in checks if c["capability"] == "conditional-storage")["result"],
                         "not-run")
        self.assertEqual(next(c for c in checks if c["capability"] == "runtime-binding")["result"],
                         "passed")
        self.assertEqual(next(c for c in checks if c["capability"] == "query")["result"],
                         "passed")
        self.assertEqual(next(c for c in checks if c["capability"] == "status-queue")["result"],
                         "passed")


if __name__ == "__main__":
    unittest.main()
