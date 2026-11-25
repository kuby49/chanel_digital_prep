import ast
import csv
import base64
import hashlib
import io
import json
import os
import subprocess
import sys
import tempfile
import textwrap
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import ModuleType, SimpleNamespace
from unittest.mock import patch
from urllib.parse import urlsplit
from urllib.request import url2pathname

from generator import HEADER, RunConfig, batch_bytes, dimensions, generator_source_sha256, stage_batch
from delivery import begin_submission, record_submission, record_verification
from fabric_files import materialize_staged
import fabric_assets
from fabric_assets import notebook_definition, pipeline_definition
from runner import Submission, run_batches


PREFLIGHT_BINDING = {"workspace_id": "workspace", "staging_lakehouse_id": "lakehouse",
                     "default_lakehouse_workspace_id": "workspace", "default_lakehouse_id": "lakehouse"}
PREFLIGHT_ROOT = "abfss://workspace@onelake.dfs.fabric.microsoft.com/lakehouse/Files/preflight"


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
                ids.add((row["BatchID"], row["SalesLineID"]))
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
            self.assertEqual([int(row["SalesLineID"]) for row in rows], list(range(1, 13)))
            self.assertEqual(meta["batch_class"], "measured")
            self.assertEqual(sum(int(r["SalesAmountMinorUnits"]) for r in rows), meta["sales_amount_minor_units"])
            self.assertEqual(sum(meta["amount_by_date"].values()), meta["sales_amount_minor_units"])
            self.assertEqual(sum(meta["amount_by_product_category"].values()), meta["sales_amount_minor_units"])
            self.assertEqual(sum(meta["amount_by_customer_region"].values()), meta["sales_amount_minor_units"])
            for row in rows:
                self.assertEqual(int(row["SalesAmountMinorUnits"]), int(row["Quantity"]) * int(row["UnitPriceMinorUnits"]))
                self.assertEqual(row["BatchID"], meta["batch_id"])
                self.assertTrue(row["SaleTimestamp"].endswith("Z"))
                key = (row["BatchID"], row["SalesLineID"])
                self.assertNotIn(key, all_ids)
                all_ids.add(key)
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
            self.assertNotIn("sales_line_id_reservation", manifest)
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
        self.assertEqual(set(pipeline["properties"]["parameters"]),
                         {"operation", "probe_id", "runtime_evidence_json", "run_config_json",
                          "selected_demos_json", "activity_timeout"})
        self.assertEqual(set(pipeline["properties"]["activities"][0]["typeProperties"]["parameters"]),
                         {"operation", "probe_id", "runtime_evidence_json", "run_config_json", "selected_demos_json"})
        self.assertEqual(pipeline["properties"]["activities"][0]["policy"]["timeout"], "0.00:15:00")

    def test_deployment_assets_bind_actual_workspace_and_lakehouse(self):
        workspace = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
        lakehouse = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
        notebook_id = "cccccccc-cccc-4ccc-8ccc-cccccccccccc"
        environment_id = "dddddddd-dddd-4ddd-8ddd-dddddddddddd"
        definition = notebook_definition(workspace, lakehouse, "producer_staging", environment_id)
        notebook = json.loads(base64.b64decode(definition["parts"][0]["payload"]))
        for field in ("dependencies", "trident"):
            binding = notebook["metadata"][field]["lakehouse"]
            self.assertEqual(binding["default_lakehouse"], lakehouse)
            self.assertEqual(binding["default_lakehouse_workspace_id"], workspace)
        self.assertEqual(notebook["metadata"]["dependencies"]["environment"],
                         {"environmentId": environment_id, "workspaceId": workspace})
        bundle_source = "".join(notebook["cells"][1]["source"])
        self.assertIn("fabric_ledger.py", bundle_source)
        compile(bundle_source, "producer-bundle", "exec")
        pipeline = json.loads(base64.b64decode(pipeline_definition(workspace, notebook_id)["parts"][0]["payload"]))
        activity = pipeline["properties"]["activities"][0]
        self.assertEqual(activity["typeProperties"]["notebookId"], notebook_id)
        self.assertEqual(activity["typeProperties"]["workspaceId"], workspace)
        with self.assertRaises(ValueError):
            pipeline_definition("not-an-id", notebook_id)

    def test_schema_catalog_binding_keeps_staging_and_environment_explicit(self):
        workspace = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
        staging = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
        catalog_workspace = "cccccccc-cccc-4ccc-8ccc-cccccccccccc"
        catalog_lakehouse = "dddddddd-dddd-4ddd-8ddd-dddddddddddd"
        environment = "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee"
        catalog = {"workspace_id": catalog_workspace, "lakehouse_id": catalog_lakehouse,
                   "lakehouse_name": "schema_lakehouse"}
        definition = notebook_definition(workspace, staging, "producer_staging", environment,
                                         default_lakehouse=catalog)
        notebook = json.loads(base64.b64decode(definition["parts"][0]["payload"]))
        expected = {"default_lakehouse": catalog_lakehouse, "default_lakehouse_name": "schema_lakehouse",
                    "default_lakehouse_workspace_id": catalog_workspace,
                    "known_lakehouses": [{"id": catalog_lakehouse}, {"id": staging}]}
        self.assertEqual(notebook["metadata"]["trident"]["lakehouse"], expected)
        self.assertEqual(notebook["metadata"]["dependencies"]["lakehouse"], expected)
        self.assertEqual(notebook["metadata"]["dependencies"]["environment"]["workspaceId"], workspace)
        source = "".join(notebook["cells"][1]["source"])
        self.assertIn(repr({"workspace_id": workspace, "staging_lakehouse_id": staging,
                            "default_lakehouse_workspace_id": catalog_workspace,
                            "default_lakehouse_id": catalog_lakehouse}), source)
        with self.assertRaises(ValueError):
            notebook_definition(workspace, staging, "producer_staging", environment,
                                default_lakehouse=dict(catalog, lakehouse_id=staging))

    def test_notebook_bundle_loads_the_exact_generator(self):
        definition = notebook_definition("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
                                         "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
                                         "producer_staging")
        notebook = json.loads(base64.b64decode(definition["parts"][0]["payload"]))
        bundle = "".join(notebook["cells"][1]["source"])
        script = bundle + "\nimport generator, fabric_ledger\nprint(generator.generator_source_sha256())\n"
        with tempfile.TemporaryDirectory() as directory:
            environment = dict(os.environ, TEMP=directory, TMP=directory, PYTHONDONTWRITEBYTECODE="1")
            completed = subprocess.run([sys.executable, "-"], input=script, text=True, capture_output=True,
                                       env=environment, timeout=30, check=False)
        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertEqual(completed.stdout.strip(), generator_source_sha256())

    def test_notebook_bundle_is_stable_across_checkout_line_endings(self):
        args = ("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
                "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb", "producer_staging")
        expected = notebook_definition(*args)
        read_bytes = Path.read_bytes

        def windows_checkout(path):
            return read_bytes(path).replace(b"\r\n", b"\n").replace(b"\n", b"\r\n")

        with patch.object(Path, "read_bytes", windows_checkout):
            self.assertEqual(notebook_definition(*args), expected)

    def test_native_bundle_includes_complete_tracks_and_rejects_missing_or_colliding_companions(self):
        companions = {
            "demo-1-materialized-lake-views": ("native_demo1.py",),
            "demo-3-onelake-events-copy": ("native_demo3.py", "native_stop.py", "native_guard.py", "audit.py",
                                           "batch_gate.py", "delivery_runtime.py", "guard_spark.py"),
        }
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            producer = root / "demo-data-generator"
            producer.mkdir()
            for name in fabric_assets.PRODUCER_MODULES:
                (producer / name).write_bytes(f"# producer {name}\n".encode())
            for folder, names in companions.items():
                peer = root / folder
                peer.mkdir()
                for name in names:
                    (peer / name).write_bytes(f"# peer {name}\r\n".encode())
            with patch.object(fabric_assets, "HERE", producer):
                source = fabric_assets._bundle_cell()["source"][4]
                bundled = ast.literal_eval(source.partition("=")[2].strip())
                for names in companions.values():
                    for name in names:
                        self.assertEqual(base64.b64decode(bundled[name]), f"# peer {name}\n".encode())
                missing = root / "demo-3-onelake-events-copy" / "guard_spark.py"
                missing.unlink()
                with self.assertRaisesRegex(ValueError, "incomplete"):
                    fabric_assets._bundle_cell()
                missing.write_text("# restored\n", encoding="utf-8")
                (producer / "audit.py").write_text("# different\n", encoding="utf-8")
                with patch.object(fabric_assets, "PRODUCER_MODULES", fabric_assets.PRODUCER_MODULES + ("audit.py",)):
                    with self.assertRaisesRegex(ValueError, "colliding import names"):
                        fabric_assets._bundle_cell()

    def test_notebook_bundle_imports_available_native_hooks(self):
        root = Path(__file__).parent.parent
        hook_paths = {
            "demo-1": root / "demo-1-materialized-lake-views" / "native_demo1.py",
            "demo-2": root / "demo-2-eventhouse" / "native_demo2.py",
            "demo-3": root / "demo-3-onelake-events-copy" / "native_demo3.py",
        }
        selected = [name for name, path in hook_paths.items() if path.is_file()]
        if not selected:
            self.skipTest("Native track modules are not present in this worktree")
        source = "".join(fabric_assets._bundle_cell()["source"])
        script = source + f"\nimport native_lifecycle\nnative_lifecycle.load_hooks({selected!r})\n"
        with tempfile.TemporaryDirectory() as directory:
            environment = dict(os.environ, TEMP=directory, TMP=directory, PYTHONDONTWRITEBYTECODE="1")
            completed = subprocess.run([sys.executable, "-"], input=script, text=True, capture_output=True,
                                       env=environment, timeout=30, check=False)
        self.assertEqual(completed.returncode, 0, completed.stderr)

    def test_preflight_probe_persists_identity_without_the_token(self):
        notebook = json.loads((Path(__file__).parent / "notebook.ipynb").read_text(encoding="utf-8"))
        claims = {"oid": "test-object", "tid": "test-tenant"}
        token = "header." + base64.urlsafe_b64encode(json.dumps(claims).encode()).decode().rstrip("=") + ".signature"
        files = {}

        class FakeFs:
            def exists(self, path):
                return path in files

            def mkdirs(self, path):
                files[path] = None
                return True

            def put(self, path, value, overwrite):
                self.assert_not_overwrite = not overwrite
                files[path] = value
                return True

            def head(self, path, max_bytes):
                return files[path][:max_bytes]

        utils = SimpleNamespace(
            runtime=SimpleNamespace(context={"isForPipeline": True, "currentWorkspaceId": "workspace",
                                             "defaultLakehouseId": "lakehouse"}),
            credentials=SimpleNamespace(getToken=lambda audience: token),
            fs=FakeFs(),
        )
        scope = {"spark": SimpleNamespace(conf=SimpleNamespace(set=lambda *args: None)),
                 "notebookutils": utils, "operation": "preflight", "probe_id": "probe-1",
                 "runtime_evidence_json": "{}", "run_config_json": '{"probe_marker":"unique"}', "selected_demos_json": "[]",
                 "_producer_runtime_binding": dict(PREFLIGHT_BINDING)}
        exec("".join(notebook["cells"][1]["source"]), scope)
        stored = files[PREFLIGHT_ROOT + "/probe-1.json"]
        self.assertNotIn(token, stored)
        evidence = json.loads(stored)
        self.assertEqual(evidence["storage_principal_oid"], "test-object")
        self.assertEqual(evidence["parameter_marker"], "unique")
        self.assertTrue(evidence["is_for_pipeline"])
        with self.assertRaises(RuntimeError):
            exec("".join(notebook["cells"][1]["source"]), scope)
        scope.update(probe_id="probe-missing-sdk", run_config_json='{"probe_kusto":true}')
        actual_import = __import__

        def missing_kusto(name, *args, **kwargs):
            if name.startswith("azure.kusto"):
                raise ImportError("SDK unavailable")
            return actual_import(name, *args, **kwargs)

        with patch("builtins.__import__", side_effect=missing_kusto):
            with self.assertRaisesRegex(RuntimeError, "Kusto SDK imports"):
                exec("".join(notebook["cells"][1]["source"]), scope)
        evidence = json.loads(files[PREFLIGHT_ROOT + "/probe-missing-sdk.json"])
        self.assertFalse(evidence["kusto_sdk_available"])
        self.assertEqual(evidence["kusto_principal_oid"], "test-object")

    def test_runtime_evidence_is_job_bound_blocked_and_token_free(self):
        from test_fabric_ledger import MemoryStore

        notebook = json.loads((Path(__file__).parent / "notebook.ipynb").read_text(encoding="utf-8"))
        workspace = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
        lakehouse = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
        notebook_id = "cccccccc-cccc-4ccc-8ccc-cccccccccccc"
        tenant = "dddddddd-dddd-4ddd-8ddd-dddddddddddd"
        identity = "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee"
        other = "ffffffff-ffff-4fff-8fff-ffffffffffff"
        claims = {"oid": identity, "tid": tenant}
        token = "header." + base64.urlsafe_b64encode(json.dumps(claims).encode()).decode().rstrip("=") + ".signature"
        mismatch = "header." + base64.urlsafe_b64encode(json.dumps(dict(claims, oid=other)).encode()).decode().rstrip("=") + ".signature"
        producer = {"workspace_id": workspace, "staging_lakehouse_id": lakehouse,
                    "notebook_id": notebook_id, "environment_id": other, "definition_sha256": "a" * 64}
        target = {"workspace_id": workspace, "item_id": other, "database": "Sales",
                  "fact": "FactSales", "mapping": "SalesCsvV1",
                  "query_service_uri": "https://query.kusto.fabric.microsoft.com",
                  "ingestion_service_uri": "https://ingest.kusto.fabric.microsoft.com"}
        request = {"nonce": "0123456789abcdef", "run_id": "run-1", "tenant_id": tenant,
                   "identity_id": identity, "producer": producer, "demo-2": target}
        config = {"arbitrary_config": "private-value-é"}
        captured = []
        token_by_audience = {"pbi": token, "storage": token, "kusto": token}
        utils = SimpleNamespace(
            runtime=SimpleNamespace(context={"isForPipeline": True, "currentWorkspaceId": workspace,
                                             "defaultLakehouseId": lakehouse, "currentNotebookId": notebook_id,
                                             "currentRunId": other}),
            credentials=SimpleNamespace(getToken=lambda audience: token_by_audience[audience]),
            notebook=SimpleNamespace(exit=captured.append))
        scope = {"spark": SimpleNamespace(conf=SimpleNamespace(set=lambda *args: None)),
                 "notebookutils": utils, "operation": "preflight", "probe_id": "probe-1",
                 "runtime_evidence_json": json.dumps(request),
                 "run_config_json": json.dumps(config), "selected_demos_json": '["demo-3","demo-2","demo-1"]'}

        queries = []
        class FakeClient:
            def __init__(self, connection):
                pass

            def execute_query(self, database, query):
                queries.append((database, query))
                return SimpleNamespace(primary_results=[[{"ProducerRuntimeProbe": 1}]])

            def execute_mgmt(self, database, query):
                return SimpleNamespace(primary_results=[[{"metadata": "available"}]])

        fake_data = SimpleNamespace(KustoClient=FakeClient,
                                    KustoConnectionStringBuilder=SimpleNamespace(with_token_provider=lambda *args: None))
        store = MemoryStore()
        with patch.dict(sys.modules, {"azure": SimpleNamespace(), "azure.kusto": SimpleNamespace(),
                                     "azure.kusto.data": fake_data}), patch("fabric_storage.OneLakeStore", return_value=store):
            exec("".join(notebook["cells"][1]["source"]), scope)
            token_by_audience["kusto"] = mismatch
            utils.runtime.context["currentRunId"] = "do-not-emit-secret"
            utils.runtime.context["currentNotebookId"] = "do-not-emit-secret"
            scope["selected_demos_json"] = '["demo-1","demo-3","demo-2"]'
            exec("".join(notebook["cells"][1]["source"]), scope)
        class UnguardedStore(MemoryStore):
            def replace(self, path, value, etag):
                return super().replace(path, value, self.files[path][1])

        token_by_audience["kusto"] = token
        utils.runtime.context["currentRunId"] = other
        utils.runtime.context["currentNotebookId"] = notebook_id
        with patch.dict(sys.modules, {"azure": SimpleNamespace(), "azure.kusto": SimpleNamespace(),
                                     "azure.kusto.data": fake_data}), patch("fabric_storage.OneLakeStore", return_value=UnguardedStore()):
            exec("".join(notebook["cells"][1]["source"]), scope)
        first, second, third = map(json.loads, captured)
        self.assertEqual(len(queries), 2)
        self.assertNotIn(token, captured[0])
        self.assertNotIn("private-value", captured[0])
        self.assertNotIn("do-not-emit-secret", captured[1])
        self.assertEqual(first["status"], "blocked")
        self.assertIsNone(first["producer"]["job_id"])
        self.assertEqual(first["producer"]["observed_run_id"], other)
        self.assertEqual(first["selected_demos"], ["demo-1", "demo-2", "demo-3"])
        self.assertEqual(second["selected_demos"], first["selected_demos"])
        self.assertEqual(second["effective_config_sha256"], first["effective_config_sha256"])
        self.assertEqual(first["effective_config_sha256"],
                         hashlib.sha256(json.dumps(config, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                                                   allow_nan=False).encode()).hexdigest())
        self.assertGreater(datetime.fromisoformat(first["expires_at_utc"]),
                           datetime.fromisoformat(first["issued_at_utc"]))
        checks = first["targets"]["demo-2"]["checks"]
        self.assertEqual(next(x for x in checks if x["capability"] == "conditional-storage")["result"], "passed")
        self.assertEqual(next(x for x in checks if x["capability"] == "effective-ingestion-authorization")["result"], "unverified")
        self.assertEqual(store.files, {})
        checks = second["targets"]["demo-2"]["checks"]
        self.assertEqual(next(x for x in checks if x["capability"] == "runtime-binding")["result"], "failed")
        self.assertEqual(next(x for x in checks if x["capability"] == "token-identity" and x["resource"] == "kusto")["result"], "failed")
        self.assertEqual(next(x for x in third["targets"]["demo-2"]["checks"]
                              if x["capability"] == "conditional-storage")["result"], "failed")

    def test_runtime_preflight_requires_exact_eventhouse_sdks(self):
        import importlib.metadata
        import re
        from fabric_runtime import EVENTHOUSE_SDK_VERSIONS

        notebook = json.loads((Path(__file__).parent / "notebook.ipynb").read_text(encoding="utf-8"))
        source = "".join(notebook["cells"][1]["source"])
        start = source.index("    try:\n        import importlib.metadata\n")
        end = source.index("    # The names are fixed constants", start)
        code = textwrap.dedent(source[start:end])
        names = ("azure", "azure.identity", "azure.kusto", "azure.kusto.ingest", "azure.storage",
                 "azure.storage.queue", "eventhouse_core", "ingestion")
        modules = {name: ModuleType(name) for name in names}
        for parent in ("azure", "azure.kusto", "azure.storage"):
            modules[parent].__path__ = []
        modules["ingestion"].EventhouseAdapter = SimpleNamespace(ingestion_status=lambda: None)
        cases = [({}, True, "passed"), ({}, False, "failed")]
        for name in ("azure-kusto-data", "azure-kusto-ingest", "azure-storage-queue"):
            cases.extend((({name: "99.0.0"}, True, "failed"), ({name: None}, True, "failed")))
        for overrides, import_available, expected in cases:
            with self.subTest(overrides=overrides, import_available=import_available):
                versions = {"azure-identity": "1.27.0", "azure-kusto-data": "6.0.4",
                            "azure-kusto-ingest": "6.0.4", "azure-storage-queue": "12.13.0"}
                versions.update(overrides)
                def version(name):
                    if versions[name] is None:
                        raise importlib.metadata.PackageNotFoundError(name)
                    return versions[name]

                current = dict(modules)
                if not import_available:
                    current["azure.storage.queue"] = None
                checks, evidence = [], {}
                scope = {"EVENTHOUSE_SDK_VERSIONS": EVENTHOUSE_SDK_VERSIONS, "re": re, "evidence": evidence,
                         "producer": {"environment_id": "environment"},
                         "check": lambda capability, resource, method, result: checks.append(result)}
                with patch.dict(sys.modules, current), patch("importlib.metadata.version", side_effect=version):
                    exec(code, scope)
                self.assertEqual(checks, [expected])
                if import_available and all(value is not None for value in versions.values()):
                    self.assertEqual(evidence["runtime_packages"], versions)
                else:
                    self.assertNotIn("runtime_packages", evidence)

    def test_runtime_evidence_rejects_untrusted_target_and_missing_nonce(self):
        notebook = json.loads((Path(__file__).parent / "notebook.ipynb").read_text(encoding="utf-8"))
        identifier = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
        request = {"nonce": "0123456789abcdef", "run_id": "run-1", "tenant_id": identifier,
                   "identity_id": identifier,
                   "producer": {"workspace_id": identifier, "staging_lakehouse_id": identifier,
                                "notebook_id": identifier, "environment_id": identifier, "definition_sha256": "a" * 64},
                   "demo-2": {"workspace_id": identifier, "item_id": identifier, "database": "Sales",
                              "fact": "FactSales", "mapping": "SalesCsvV1",
                              "query_service_uri": "https://query.kusto.fabric.microsoft.com",
                              "ingestion_service_uri": "https://ingest.kusto.fabric.microsoft.com"}}
        scope = {"spark": SimpleNamespace(conf=SimpleNamespace(set=lambda *args: None)),
                 "operation": "preflight", "probe_id": "probe-1", "selected_demos_json": '["demo-2"]',
                 "runtime_evidence_json": json.dumps(request), "run_config_json": "{}"}
        for field in ("query_service_uri", "ingestion_service_uri"):
            original = request["demo-2"][field]
            for endpoint in ("https://attacker.example",
                             "https://query.kusto.fabric.microsoft.com.attacker.example",
                             "https://user:password@query.kusto.fabric.microsoft.com",
                             "https://query.kusto.fabric.microsoft.com:443",
                             "https://query.kusto.fabric.microsoft.com/path",
                             "https://query.kusto.fabric.microsoft.com/?token=secret"):
                request["demo-2"][field] = endpoint
                scope["runtime_evidence_json"] = json.dumps(request)
                with self.subTest(field=field, endpoint=endpoint), self.assertRaisesRegex(ValueError, "Fabric Kusto"):
                    exec("".join(notebook["cells"][1]["source"]), scope)
            request["demo-2"][field] = original
        request["nonce"] = ""
        scope["runtime_evidence_json"] = json.dumps(request)
        with self.assertRaisesRegex(ValueError, "nonce"):
            exec("".join(notebook["cells"][1]["source"]), scope)

    def test_preflight_exercises_conditional_storage_and_removes_sentinel(self):
        from test_fabric_ledger import MemoryStore

        notebook = json.loads((Path(__file__).parent / "notebook.ipynb").read_text(encoding="utf-8"))
        claims = {"oid": "test-object", "tid": "test-tenant"}
        token = "header." + base64.urlsafe_b64encode(json.dumps(claims).encode()).decode().rstrip("=") + ".signature"
        files = {}

        class FakeFs:
            def exists(self, path):
                return path in files

            def mkdirs(self, path):
                files[path] = None
                return True

            def put(self, path, value, overwrite):
                files[path] = value
                return True

            def head(self, path, max_bytes):
                return files[path][:max_bytes]

        utils = SimpleNamespace(
            runtime=SimpleNamespace(context={"isForPipeline": True, "currentWorkspaceId": "workspace",
                                             "defaultLakehouseId": "catalog",
                                             "defaultLakehouseWorkspaceId": "catalog-workspace"}),
            credentials=SimpleNamespace(getToken=lambda audience: token), fs=FakeFs(),
        )
        scope = {"spark": SimpleNamespace(conf=SimpleNamespace(set=lambda *args: None)),
                 "notebookutils": utils, "operation": "preflight", "probe_id": "storage-probe",
                 "runtime_evidence_json": "{}", "run_config_json": '{"probe_storage":true}', "selected_demos_json": "[]",
                 "_producer_runtime_binding": dict(PREFLIGHT_BINDING, default_lakehouse_id="catalog",
                                                   default_lakehouse_workspace_id="catalog-workspace")}
        store = MemoryStore()
        with patch("fabric_storage.OneLakeStore", return_value=store) as create_store:
            exec("".join(notebook["cells"][1]["source"]), scope)
        self.assertEqual(create_store.call_args.args[:2], ("workspace", "lakehouse"))
        evidence = json.loads(files[PREFLIGHT_ROOT + "/storage-probe.json"])["conditional_storage_probe"]
        self.assertTrue(all(evidence[key] for key in ("duplicate_create_rejected", "stale_update_rejected",
                                                      "update_readback_verified", "stale_delete_rejected",
                                                      "deleted")))
        self.assertEqual(store.files, {})
        utils.runtime.context["defaultLakehouseWorkspaceId"] = "wrong-workspace"
        scope["probe_id"] = "wrong-catalog"
        with patch("fabric_storage.OneLakeStore") as create_store:
            with self.assertRaisesRegex(RuntimeError, "binding differs"):
                exec("".join(notebook["cells"][1]["source"]), scope)
        create_store.assert_not_called()
        self.assertNotIn(PREFLIGHT_ROOT + "/wrong-catalog.json", files)
        utils.runtime.context["defaultLakehouseWorkspaceId"] = "catalog-workspace"

        class UnguardedDeleteStore(MemoryStore):
            def delete(self, path, etag):
                del self.files[path]

        scope.update(probe_id="unguarded-delete")
        with patch("fabric_storage.OneLakeStore", return_value=UnguardedDeleteStore()):
            with self.assertRaisesRegex(RuntimeError, "Conditional storage probe"):
                exec("".join(notebook["cells"][1]["source"]), scope)
        evidence = json.loads(files[PREFLIGHT_ROOT + "/unguarded-delete.json"])["conditional_storage_probe"]
        self.assertEqual(evidence["step"], "stale-delete")
        self.assertFalse(evidence.get("stale_delete_rejected", False))
        self.assertFalse(evidence["deleted"])

    def test_preflight_schema_catalog_probe_reads_exact_target_without_sales(self):
        notebook = json.loads((Path(__file__).parent / "notebook.ipynb").read_text(encoding="utf-8"))
        workspace, staging = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
        catalog_workspace, catalog_id = "cccccccc-cccc-4ccc-8ccc-cccccccccccc", "dddddddd-dddd-4ddd-8ddd-dddddddddddd"
        probe = {"workspace_id": catalog_workspace, "workspace_name": "demo-workspace",
                 "lakehouse_id": catalog_id, "lakehouse_name": "DemoLakehouse"}
        binding = {"workspace_id": workspace, "staging_lakehouse_id": staging,
                   "default_lakehouse_workspace_id": catalog_workspace, "default_lakehouse_id": catalog_id}
        claims = base64.urlsafe_b64encode(json.dumps({"oid": "identity", "tid": "tenant"}).encode()).decode().rstrip("=")
        files, calls, queries = {}, [], []

        class FakeFs:
            def exists(self, path):
                return path in files

            def mkdirs(self, path):
                files[path] = None
                return True

            def put(self, path, value, overwrite):
                files[path] = value
                return True

            def head(self, path, size):
                return files[path][:size]

        class FakeClient:
            def __init__(self, token_provider):
                self.token_provider = token_provider

            def get(self, path):
                calls.append(path)
                if path == f"/v1/workspaces/{catalog_workspace}":
                    return {}, {"id": catalog_workspace, "displayName": "demo-workspace", "type": "Workspace"}
                return {}, {"id": catalog_id, "workspaceId": catalog_workspace, "type": "Lakehouse",
                            "displayName": "DemoLakehouse", "properties": {"defaultSchema": "dbo"}}

        companion = ModuleType("native_demo1")
        companion._dimensions = lambda context, base: queries.append((context["spark"], base))
        spark = SimpleNamespace(conf=SimpleNamespace(set=lambda *args: None))
        utils = SimpleNamespace(runtime=SimpleNamespace(context={"isForPipeline": True, "currentWorkspaceId": workspace,
                                "defaultLakehouseId": catalog_id, "defaultLakehouseWorkspaceId": catalog_workspace}),
                                credentials=SimpleNamespace(getToken=lambda audience: f"header.{claims}.signature"),
                                fs=FakeFs())
        scope = {"spark": spark, "notebookutils": utils, "operation": "preflight", "probe_id": "catalog-probe",
                 "runtime_evidence_json": "{}", "run_config_json": json.dumps({"schema_catalog_probe": probe}),
                 "selected_demos_json": "[]", "_producer_runtime_binding": binding}
        with patch.dict(sys.modules, {"native_demo1": companion}), patch("fabric_runtime.RuntimeFabricClient", FakeClient):
            exec("".join(notebook["cells"][1]["source"]), scope)
            self.assertEqual(queries, [(spark, "`demo-workspace`.`DemoLakehouse`")])
            self.assertEqual(len(calls), 2)
            path = f"abfss://{workspace}@onelake.dfs.fabric.microsoft.com/{staging}/Files/preflight/catalog-probe.json"
            self.assertEqual(json.loads(files[path])["schema_catalog_probe"]["status"], "passed")
            scope.update(probe_id="bad-catalog", run_config_json=json.dumps(
                {"schema_catalog_probe": dict(probe, workspace_name="bad`;DROP TABLE x")}))
            with self.assertRaisesRegex(RuntimeError, "Schema catalog probe"):
                exec("".join(notebook["cells"][1]["source"]), scope)
            self.assertEqual(len(calls), 2)
            self.assertEqual(len(queries), 1)

    def test_preflight_verifies_and_cleans_only_unique_destination_sentinel(self):
        notebook = json.loads((Path(__file__).parent / "notebook.ipynb").read_text(encoding="utf-8"))
        claims = {"oid": "test-object", "tid": "test-tenant"}
        token = "header." + base64.urlsafe_b64encode(json.dumps(claims).encode()).decode().rstrip("=") + ".signature"
        workspace = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
        lakehouse = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
        probe_dir = f"abfss://{workspace}@onelake.dfs.fabric.microsoft.com/{lakehouse}/Files/_preflight/producer-access/probe-2"
        files = {"untouched": "existing"}

        class FakeFs:
            def exists(self, path):
                return path in files

            def mkdirs(self, path):
                files[path] = None
                return True

            def put(self, path, value, overwrite):
                if path in files and not overwrite:
                    return False
                files[path] = value
                return True

            def head(self, path, max_bytes):
                return files[path][:max_bytes]

            def rm(self, path, recurse):
                if recurse:
                    raise AssertionError("probe cleanup must not recurse")
                del files[path]
                return True

        utils = SimpleNamespace(
            runtime=SimpleNamespace(context={"isForPipeline": True, "currentWorkspaceId": "workspace",
                                             "defaultLakehouseId": "lakehouse"}),
            credentials=SimpleNamespace(getToken=lambda audience: token),
            fs=FakeFs(),
        )
        config = {"probe_marker": "unique",
                  "destination_access_probe": {"workspace_id": workspace, "lakehouse_id": lakehouse}}
        scope = {"spark": SimpleNamespace(conf=SimpleNamespace(set=lambda *args: None)),
                 "notebookutils": utils, "operation": "preflight", "probe_id": "probe-2",
                 "runtime_evidence_json": "{}", "run_config_json": json.dumps(config), "selected_demos_json": "[]",
                 "_producer_runtime_binding": dict(PREFLIGHT_BINDING)}
        exec("".join(notebook["cells"][1]["source"]), scope)
        evidence = json.loads(files[PREFLIGHT_ROOT + "/probe-2.json"])
        self.assertNotIn(token, json.dumps(evidence))
        access = evidence["destination_access_probe"]
        self.assertEqual(access["path"], probe_dir + "/identity-probe.txt")
        self.assertTrue(all(access[key] for key in ("readback_verified", "sentinel_deleted", "directory_deleted")))
        self.assertNotIn(probe_dir, files)
        self.assertEqual(files["untouched"], "existing")

        files.pop(PREFLIGHT_ROOT + "/probe-2.json")
        files[probe_dir] = None
        files[probe_dir + "/identity-probe.txt"] = "existing sentinel"
        with self.assertRaisesRegex(RuntimeError, "Destination access probe"):
            exec("".join(notebook["cells"][1]["source"]), scope)
        self.assertEqual(files[probe_dir + "/identity-probe.txt"], "existing sentinel")
        evidence = json.loads(files[PREFLIGHT_ROOT + "/probe-2.json"])
        self.assertFalse(evidence["destination_access_probe"]["readback_verified"])

    def test_runner_spaces_batches_and_isolates_failures_on_resume(self):
        class FakeClock:
            now = datetime(2024, 1, 1, tzinfo=timezone.utc)

            def clock(self):
                return self.now

            def sleep(self, seconds):
                self.now += timedelta(seconds=seconds)

        with tempfile.TemporaryDirectory() as root:
            clock = FakeClock()
            calls = []

            def healthy(request, deadline):
                calls.append(("demo-1", request["batch_id"], clock.now))
                if len([c for c in calls if c[0] == "demo-1" and c[1] == request["batch_id"]]) == 1:
                    return Submission("retryable_failure")
                return Submission("submitted", "operation")

            def uncertain(request, deadline):
                calls.append(("demo-2", request["batch_id"], clock.now))
                raise ConnectionError("outcome unknown")

            config = RunConfig("timed", file_count=3, rows_per_file=2, interval_seconds=60)
            adapters = {"demo-1": healthy, "demo-2": uncertain}
            result = run_batches(root, config, adapters, 181, 2, clock=clock.clock, sleep=clock.sleep)
            self.assertFalse(result["submitted"])
            self.assertEqual(len([c for c in calls if c[0] == "demo-1"]), 6)
            self.assertEqual(len([c for c in calls if c[0] == "demo-2"]), 3)
            times = [c[2] for c in calls if c[0] == "demo-2"]
            self.assertEqual([int((t - times[0]).total_seconds()) for t in times], [0, 60, 120])
            previous_calls = len(calls)
            run_batches(root, config, adapters, 181, 2, clock=clock.clock, sleep=clock.sleep)
            self.assertEqual(len(calls), previous_calls)
            with self.assertRaises(ValueError):
                run_batches(root, config, adapters, 200, 2, clock=clock.clock, sleep=clock.sleep)

    def test_runner_stops_new_batches_at_original_deadline(self):
        class FakeClock:
            now = datetime(2024, 1, 1, tzinfo=timezone.utc)

            def clock(self):
                return self.now

            def sleep(self, seconds):
                self.now += timedelta(seconds=seconds)

        with tempfile.TemporaryDirectory() as root:
            clock = FakeClock()

            def slow(request, deadline):
                clock.now += timedelta(seconds=120)
                return Submission("submitted")

            config = RunConfig("deadline", file_count=2, rows_per_file=1, interval_seconds=60)
            result = run_batches(root, config, {"demo-1": slow}, 61, 1, clock=clock.clock, sleep=clock.sleep)
            self.assertFalse(result["submitted"])
            self.assertTrue(any("deadline" in error for error in result["errors"]))
            manifest = json.loads((Path(root) / config.run_id / "manifest.json").read_text(encoding="utf-8"))
            self.assertEqual(list(manifest["batches"]), ["1"])

    def test_runner_rejects_an_existing_lock(self):
        with tempfile.TemporaryDirectory() as root:
            config = RunConfig("locked", file_count=1, rows_per_file=1)
            directory = Path(root) / config.run_id
            directory.mkdir()
            lock = directory / ".producer.lock"
            lock.write_text("", encoding="utf-8")
            with self.assertRaises(RuntimeError):
                run_batches(root, config, {"demo-1": lambda *_: Submission("submitted")}, 1, 1)
            self.assertTrue(lock.exists())
            self.assertFalse((directory / "manifest.json").exists())

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
                if mode != "dry-run":
                    self.assertTrue(any("no cross-workspace destination permissions" in item
                                        for item in data["diagnostics"]))

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
