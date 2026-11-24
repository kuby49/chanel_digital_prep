"""Exercise integration between the actual shared and track entry points."""

import base64
import importlib.machinery
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from unittest import mock


ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "demo-data-generator"))
sys.path.insert(0, str(ROOT / "demo-1-materialized-lake-views"))
from generator import RunConfig, stage_batch
from manifest_validation import measured_batches, validate_manifest


def load_script(name, path):
    loader = importlib.machinery.SourceFileLoader(name, str(path))
    spec = importlib.util.spec_from_loader(name, loader)
    module = importlib.util.module_from_spec(spec)
    loader.exec_module(module)
    return module


class SharedManifestIntegrationTest(unittest.TestCase):
    def test_native_producer_bundle_loads_demo2_with_rendered_fresh_profiles(self):
        import fabric_assets

        deploy = load_script("shared_native_deploy_integration", ROOT / "demo-data-generator" / "deploy.py")
        workspace = "11111111-1111-4111-8111-111111111111"
        lakehouse = "22222222-2222-4222-8222-222222222222"
        notebook_id = "33333333-3333-4333-8333-333333333333"
        identity = "44444444-4444-4444-8444-444444444444"

        target = {"workspace_id": "55555555-5555-4555-8555-555555555555",
                  "item_id": "66666666-6666-4666-8666-666666666666", "fact": "SyntheticFactSales"}
        demo = {"workspace_id": target["workspace_id"], "lakehouse_id": "77777777-7777-4777-8777-777777777777",
                "monitor_notebook_id": "88888888-8888-4888-8888-888888888888",
                "verifier_notebook_id": "99999999-9999-4999-8999-999999999999",
                "monitor_definition_sha256": "a" * 64, "verifier_definition_sha256": "b" * 64,
                "ingestion_endpoint": "https://ingest.example.kusto.fabric.microsoft.com",
                "query_endpoint": "https://example.kusto.fabric.microsoft.com",
                "kql_database_name": "demo", "sales_table": target["fact"], "csv_mapping": "SalesCsvV1",
                "shortcut_name": "EventhouseFactSales", "shortcut_target_path": "Tables/SyntheticFactSales",
                "verification_batch": {"batch_id": "bootstrap_0001"},
                "verification_timeout_seconds": 900, "poll_interval_seconds": 10, "max_concurrent_batches": 2}
        config = json.loads((ROOT / "config.example.json").read_text(encoding="utf-8"))
        config["demo-2"] = demo
        config["shared"].update({"workspace_id": workspace, "staging_lakehouse_id": lakehouse,
                                 "producer_notebook_id": notebook_id, "identity_id": identity,
                                 "target_bindings": {"demo-2": target}, "native_launch": True,
                                 "native_reusable": True, "native_selected_demos": "demo-2"})
        resources = {"workspace": {"id": workspace}, "staging_lakehouse": {"id": lakehouse},
                     "notebook": {"id": notebook_id}}
        profile = deploy._native_profile(config, resources)
        self.assertIsNone(config["data"]["run_id"])
        definition = fabric_assets.notebook_definition(workspace, lakehouse, "producer_staging")
        notebook = json.loads(base64.b64decode(definition["parts"][0]["payload"]))
        bundle = "".join(notebook["cells"][1]["source"])
        script = bundle + """
import json
import sys
from datetime import datetime, timedelta, timezone
from fabric_runtime import native_parameters
from native_lifecycle import load_hooks

profile = json.loads(sys.argv[1])
hook = load_hooks(profile["selected"])["demo-2"]
results = []
for minute in (0, 1):
    now = datetime(2030, 1, 1, tzinfo=timezone.utc) + timedelta(minutes=minute)
    document, selected, binding, nonce, expiry = native_parameters(profile, now=now)
    manifest_path = f"Files/producer/runs/{document['data']['run_id']}/manifest.json"
    context = {
        "document": document, "binding": binding, "store": object(),
        "store_factory": lambda ws, lakehouse: object(), "manifest_path": manifest_path,
        "clock": lambda: now, "stop_requested": lambda: False,
        "preparation_deadline_utc": (now + timedelta(seconds=profile["preparation_timeout_seconds"])).isoformat(),
        "verification_deadline_utc": (datetime.fromisoformat(expiry)
                                      - timedelta(seconds=profile["cleanup_timeout_seconds"])).isoformat(),
        "cleanup_deadline_utc": expiry, "expires_at_utc": expiry,
    }
    run = hook._Run(context, "preparation")
    results.append({"run_id": run.cfg["run_id"], "manifest": run.cfg["manifest_url"],
                    "target": run.target, "producer_config": run.cfg["producer_config"],
                    "binding": run.binding["binding"]})
print(json.dumps(results))
"""
        with tempfile.TemporaryDirectory() as directory:
            environment = dict(os.environ, TEMP=directory, TMP=directory)
            completed = subprocess.run([sys.executable, "-I", "-B", "-", json.dumps(profile)],
                                       input=script, text=True, capture_output=True, env=environment,
                                       timeout=30, check=False)
        self.assertEqual(completed.returncode, 0, completed.stderr)
        first, second = json.loads(completed.stdout)
        self.assertNotEqual(first["run_id"], second["run_id"])
        self.assertNotEqual(first["binding"], second["binding"])
        for result in (first, second):
            self.assertEqual(result["target"], target)
            self.assertEqual(result["producer_config"]["run_id"], result["run_id"])
            self.assertEqual(result["producer_config"]["file_count"], config["data"]["file_count"])
            self.assertTrue(result["manifest"].endswith(f"/{result['run_id']}/manifest.json"))

    def test_native_copy_guard_accepts_producer_admission_and_stop_binding(self):
        import generator
        from fabric_ledger import FabricLedger
        from fabric_runtime import stop_requested
        from test_fabric_ledger import MemoryStore

        sys.path.insert(0, str(ROOT / "demo-3-onelake-events-copy"))
        try:
            guard = load_script("native_guard_integration", ROOT / "demo-3-onelake-events-copy" / "native_guard.py")
        finally:
            sys.path.pop(0)
        config = RunConfig(run_id="integration_native", start_date="2026-01-01",
                           file_count=10, rows_per_file=1000, interval_seconds=60)
        target = {"workspace_id": "11111111-1111-4111-8111-111111111111",
                  "item_id": "22222222-2222-4222-8222-222222222222", "fact": "dbo.factsales"}
        policy = {"version": 1, "max_file_count": 10, "max_rows_per_file": 1000,
                  "max_admission_seconds": 3600, "writer_wait_seconds": 300,
                  "run_template": {key: value for key, value in asdict(config).items()
                                   if key not in ("run_id", "file_count", "rows_per_file")}}
        settings = {"workspace_id": target["workspace_id"], "item_id": target["item_id"],
                    "native_policy": policy}
        now = datetime.now(timezone.utc)
        expiry = (now + timedelta(minutes=45)).isoformat()
        digest = "a" * 64
        store = MemoryStore()
        subject = f"/Files/incoming/sales/sales_{config.run_id}_0001.csv"
        with FabricLedger(store, config, ["demo-3"], selected_targets={"demo-3": target},
                          fresh_only=True) as ledger:
            ledger.open_demo3_admission(expiry, digest)
            ledger.stage_batch(1)
            self.assertEqual(guard.resolve_config(generator, settings, subject, store, now=now), config)
            marker = {"version": 1, "run_id": config.run_id, "nonce": "native-" + digest[:32],
                      "config_sha256": digest}
            store.create(f"Files/producer/runs/{config.run_id}/stop.json", generator.canonical_bytes(marker))
            self.assertTrue(stop_requested(store, config.run_id, marker["nonce"], digest))
            with self.assertRaisesRegex(ValueError, "valid stop request"):
                guard.resolve_config(generator, settings, subject, store, now=now)
            self.assertEqual(guard.resolve_config(generator, settings, subject, store,
                                                  finishing=True, now=now), config)

    def test_demo1_accepts_staged_manifest_and_selects_measured_batch(self):
        with tempfile.TemporaryDirectory() as root:
            config = RunConfig(run_id="integration", file_count=1, rows_per_file=2)
            entry = stage_batch(root, config, 1, ["demo-1"])
            manifest = json.loads(
                (Path(root) / config.run_id / "manifest.json").read_text(encoding="utf-8")
            )
            self.assertEqual(validate_manifest(manifest), [])
            self.assertEqual(measured_batches(manifest), [entry])

    def test_real_producer_accepts_selected_demos_for_blocked_validation(self):
        orchestration = load_script("orchestration_producer_validation", ROOT / "scripts" / "deploy-demos")
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            state = root / "state"
            state.mkdir()
            config = root / "config.json"
            config.write_text((ROOT / "config.example.json").read_text(encoding="utf-8"), encoding="utf-8")
            outcome = orchestration.invoke("shared", "validate-only", config, "chanel", state,
                                           root, 30, ["demo-1", "demo-3"])
            self.assertEqual((outcome["status"], outcome["ready"]), ("blocked", False), outcome)
            self.assertTrue(any("readiness" in message.lower() or "requires" in message.lower()
                                for message in outcome["diagnostics"]))

    def test_real_producer_blocks_selected_validation_without_target_bindings(self):
        orchestration = load_script("orchestration_producer_missing_targets", ROOT / "scripts" / "deploy-demos")
        config = json.loads((ROOT / "config.example.json").read_text(encoding="utf-8"))
        config["capacity"]["id"] = "11111111-1111-4111-8111-111111111111"
        config["shared"]["identity_id"] = "22222222-2222-4222-8222-222222222222"
        config["data"]["run_id"] = "target-check"
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            state = root / "state"
            state.mkdir()
            path = root / "config.json"
            path.write_text(json.dumps(config), encoding="utf-8")
            outcome = orchestration.invoke("shared", "validate-only", path, "chanel", state,
                                           root, 30, ["demo-1", "demo-3"])
            self.assertEqual((outcome["status"], outcome["ready"]), ("blocked", False), outcome)
            self.assertTrue(any("target candidates" in message.lower() for message in outcome["diagnostics"]))
            self.assertFalse(any(state.iterdir()))

    def test_real_producer_recovery_receives_selection_and_remains_blocked(self):
        orchestration = load_script("orchestration_producer_recovery", ROOT / "scripts" / "deploy-demos")
        config = json.loads((ROOT / "config.example.json").read_text(encoding="utf-8"))
        config["capacity"]["id"] = "11111111-1111-4111-8111-111111111111"
        config["shared"]["identity_id"] = "22222222-2222-4222-8222-222222222222"
        config["data"]["run_id"] = "recovery-probe"
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            state = root / "state"
            state.mkdir()
            path = root / "config.json"
            path.write_text(json.dumps(config), encoding="utf-8")
            for mode in ("producer-status", "stop-producer"):
                with self.subTest(mode=mode):
                    outcome = orchestration.invoke("shared", mode, path, "chanel", state,
                                                   root, 30, ["demo-1", "demo-3"])
                    self.assertEqual((outcome["status"], outcome["ready"]), ("blocked", False), outcome)
                    self.assertTrue(any("termination unconfirmed" in message.lower()
                                        for message in outcome["diagnostics"]))
            self.assertFalse(any(state.iterdir()))

    def test_orchestrator_resolves_real_producer_journal_without_cloud_calls(self):
        producer = load_script("producer_deploy_integration", ROOT / "demo-data-generator" / "deploy.py")
        orchestration = load_script("orchestration_integration", ROOT / "scripts" / "deploy-demos")
        config = {
            "contract_version": 1, "prefix": "sample",
            "tenant_id": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
            "capacity": {"id": "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"},
            "shared": {"workspace_name": "{prefix}-producer", "workspace_id": None,
                       "staging_lakehouse_id": None,
                       "identity_id": "cccccccc-cccc-4ccc-8ccc-cccccccccccc"},
        }
        workspace = "11111111-1111-4111-8111-111111111111"
        lakehouse = "22222222-2222-4222-8222-222222222222"
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            state_dir, invocation_dir = root / "state", root / "invocation"
            invocation_dir.mkdir()
            plan = producer.parse_config(config, "sample")
            state = producer.new_state(plan)
            state["deployment_id"] = "66666666-6666-4666-8666-666666666666"
            ids = {"workspace": workspace, "staging_lakehouse": lakehouse,
                   "environment_item": "77777777-7777-4777-8777-777777777777",
                   "notebook": "33333333-3333-4333-8333-333333333333",
                   "pipeline": "44444444-4444-4444-8444-444444444444"}
            for kind, resource_id in ids.items():
                state["resources"][kind] = producer._record(state, plan, kind, resource_id)
            producer.StateStore(state_dir).write(state)
            (invocation_dir / "input-config.json").write_text(json.dumps(config), encoding="utf-8")
            def resolve_target(tracks, mode, *args):
                self.assertEqual((tracks, mode), (["demo-1"], "resolve-target-candidate"))
                candidate = {"contract_version": 1, "track": "demo-1",
                             "tenant_id": config["tenant_id"], "prefix": "sample",
                             "target": {"workspace_id": "dddddddd-dddd-4ddd-8ddd-dddddddddddd",
                                        "item_id": "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee",
                                        "fact": "silver.sales_joined"}}
                (invocation_dir / "demo-1-target-candidate.json").write_text(
                    json.dumps(candidate), encoding="utf-8")
                return {"demo-1": orchestration.result("demo-1", mode, "pass", "candidate only")}
            with mock.patch.object(orchestration, "run_phase", side_effect=resolve_target):
                phases, resolved, path = orchestration.effective_run_config(
                    config, "sample", state_dir, invocation_dir, 30, ["demo-1"], 1)
            self.assertEqual(phases["resolve-candidate-ids"]["shared"]["status"], "pass",
                             (phases, (invocation_dir / "shared-resolve-candidate-ids.log").read_text()))
            self.assertFalse(phases["resolve-candidate-ids"]["shared"]["ready"])
            self.assertEqual(resolved["shared"]["workspace_id"], workspace)
            self.assertEqual(resolved["shared"]["staging_lakehouse_id"], lakehouse)
            self.assertEqual(json.loads(path.read_text(encoding="utf-8")), resolved)
            self.assertIsNone(config["shared"]["workspace_id"])

    def test_monitor_binding_matches_eventhouse_controller(self):
        sys.path.insert(0, str(ROOT / "demo-2-eventhouse"))
        try:
            eventhouse = load_script("eventhouse_deploy_integration", ROOT / "demo-2-eventhouse" / "deploy.py")
        finally:
            sys.path.pop(0)
        orchestration = load_script("orchestration_monitor_integration", ROOT / "scripts" / "deploy-demos")
        config = json.loads((ROOT / "config.example.json").read_text(encoding="utf-8"))
        config["data"]["run_id"] = "integration"
        config["shared"].update({
            "workspace_id": "11111111-1111-4111-8111-111111111111",
            "staging_lakehouse_id": "22222222-2222-4222-8222-222222222222",
            "identity_id": "33333333-3333-4333-8333-333333333333",
            "target_bindings": {
                "demo-1": {"workspace_id": "44444444-4444-4444-8444-444444444444",
                           "item_id": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", "fact": "silver.sales_joined"},
                "demo-2": {"workspace_id": "55555555-5555-4555-8555-555555555555",
                           "item_id": "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb", "fact": "FactSales"},
            },
        })
        config["demo-2"]["workspace_id"] = config["shared"]["target_bindings"]["demo-2"]["workspace_id"]
        config["demo-2"]["kql_database_id"] = config["shared"]["target_bindings"]["demo-2"]["item_id"]
        for concurrency in (1, 2, 4):
            with self.subTest(concurrency=concurrency):
                config["execution"]["max_concurrency"] = concurrency
                settings = eventhouse.validate_config(config, "chanel")
                eventhouse_binding = eventhouse._monitor_config(settings, {"demo-1", "demo-2"})
                root_binding = orchestration.monitor_binding(config)
                for key in ("target_bindings", "generator_sha256", "producer_config", "monitor_binding_sha256"):
                    self.assertEqual(root_binding[key], eventhouse_binding[key])
                duration = orchestration.monitor_config(config)[1]
                self.assertEqual(duration, eventhouse_binding["duration"])
                waves = 5 if concurrency == 1 else 4
                self.assertEqual(duration, waves * config["execution"]["track_timeout_seconds"]
                                 + config["data"]["max_duration_seconds"] + 1170
                                 + config["execution"]["verification_timeout_seconds"])

    def test_orchestrator_binds_mocked_demo1_candidate_without_cloud_calls(self):
        orchestration = load_script("orchestration_demo1_integration", ROOT / "scripts" / "deploy-demos")
        config = json.loads((ROOT / "config.example.json").read_text(encoding="utf-8"))
        config["capacity"]["id"] = "11111111-1111-4111-8111-111111111111"
        config["shared"].update({"workspace_id": "22222222-2222-4222-8222-222222222222",
                                 "staging_lakehouse_id": "33333333-3333-4333-8333-333333333333"})
        config["demo-1"].update({"lakehouse_name": "Demo1Lakehouse", "silver_view": "sales_joined"})
        workspace = "44444444-4444-4444-8444-444444444444"
        lakehouse = "55555555-5555-4555-8555-555555555555"
        target = {"workspace_id": workspace, "item_id": lakehouse, "fact": "silver.sales_joined"}

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            state = root / "state"
            state.mkdir()
            invocation = root / "invocation"
            invocation.mkdir()

            def fake_run_phase(tracks, mode, config_path, prefix, state_dir, invocation_dir,
                               timeout, selected, concurrency, yes):
                self.assertEqual((tracks, mode, selected), (["demo-1"], "resolve-target-candidate", ["demo-1"]))
                candidate = {
                    "contract_version": 1, "track": "demo-1", "tenant_id": config["tenant_id"],
                    "prefix": prefix, "target": target,
                }
                (Path(invocation_dir) / "demo-1-target-candidate.json").write_text(
                    json.dumps(candidate), encoding="utf-8")
                return {"demo-1": orchestration.result(
                    "demo-1", mode, "pass", "mocked candidate for orchestration test")}

            with mock.patch.object(orchestration, "run_phase", side_effect=fake_run_phase) as run_phase:
                phases, resolved, path = orchestration.effective_run_config(
                    config, "chanel", state, invocation, 30, ["demo-1"], 1)
            candidate_result = phases["resolve-target-candidate"]["demo-1"]
            self.assertEqual((candidate_result["status"], candidate_result["ready"]), ("pass", False))
            self.assertEqual(resolved["shared"]["target_bindings"], {"demo-1": target})
            self.assertEqual(json.loads(path.read_text(encoding="utf-8")), resolved)
            self.assertEqual(run_phase.call_count, 1)

            conflict = json.loads(json.dumps(config))
            conflict["shared"]["target_bindings"]["demo-1"] = dict(target, item_id=workspace)
            rejected = root / "rejected"
            rejected.mkdir()
            with mock.patch.object(orchestration, "run_phase", side_effect=fake_run_phase):
                phases, resolved, _ = orchestration.effective_run_config(
                    conflict, "chanel", state, rejected, 30, ["demo-1"], 1)
            self.assertEqual(phases["resolve-target-candidate"]["demo-1"]["status"], "fail")
            self.assertIsNone(resolved)

    def test_orchestrator_resolves_real_eventhouse_candidate_without_cloud_calls(self):
        sys.path.insert(0, str(ROOT / "demo-2-eventhouse"))
        try:
            eventhouse = load_script("eventhouse_candidate_integration", ROOT / "demo-2-eventhouse" / "deploy.py")
        finally:
            sys.path.pop(0)
        orchestration = load_script("orchestration_candidate_integration", ROOT / "scripts" / "deploy-demos")
        config = json.loads((ROOT / "config.example.json").read_text(encoding="utf-8"))
        config["shared"].update({"workspace_id": "11111111-1111-4111-8111-111111111111",
                                 "staging_lakehouse_id": "22222222-2222-4222-8222-222222222222"})
        settings = eventhouse.validate_config(config, "chanel")
        workspace = "33333333-3333-4333-8333-333333333333"
        eventhouse_id = "44444444-4444-4444-8444-444444444444"
        database = "55555555-5555-4555-8555-555555555555"
        target = {"workspace_id": workspace, "item_id": database, "fact": settings["table"]}
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            state = root / "state"
            with eventhouse.Ledger(state, settings, create=True) as ledger:
                ledger.data["resources"].update({
                    "workspace": {"id": workspace, "name": settings["workspace_name"],
                                  "type": "Workspace", "confirmed": True},
                    "eventhouse": {"id": eventhouse_id, "name": settings["eventhouse_name"],
                                   "type": "Eventhouse", "confirmed": True},
                    "database": {"id": database, "name": settings["database_name"], "type": "KQLDatabase",
                                 "parent_eventhouse_id": eventhouse_id, "confirmed": True},
                    "table": {"name": settings["table"], "database_id": database, "confirmed": True},
                })
                ledger.save()
            journal = state / eventhouse.STATE_FILE
            before = journal.read_bytes()
            invocation = root / "invocation"
            invocation.mkdir()
            phases, resolved, path = orchestration.effective_run_config(
                config, "chanel", state, invocation, 30, ["demo-2"], 1)
            candidate_result = phases["resolve-target-candidate"]["demo-2"]
            self.assertEqual((candidate_result["status"], candidate_result["ready"]), ("pass", False))
            self.assertEqual(resolved["shared"]["target_bindings"], {"demo-2": target})
            self.assertEqual(json.loads(path.read_text(encoding="utf-8")), resolved)
            self.assertEqual(journal.read_bytes(), before)
            self.assertEqual(config["shared"]["target_bindings"], {})

            conflict = json.loads(json.dumps(config))
            conflict["demo-2"]["kql_database_id"] = "66666666-6666-4666-8666-666666666666"
            rejected = root / "rejected"
            rejected.mkdir()
            phases, resolved, path = orchestration.effective_run_config(
                conflict, "chanel", state, rejected, 30, ["demo-2"], 1)
            self.assertEqual(phases["resolve-target-candidate"]["demo-2"]["status"], "blocked")
            self.assertIsNone(resolved)
            self.assertIsNone(path)
            self.assertFalse((rejected / "demo-2-target-candidate.json").exists())
            self.assertEqual(journal.read_bytes(), before)

    def test_orchestrator_resolves_real_demo3_candidate_with_exact_fact_case(self):
        demo = load_script("demo3_candidate_integration", ROOT / "demo-3-onelake-events-copy" / "deploy.py")
        orchestration = load_script("orchestration_demo3_integration", ROOT / "scripts" / "deploy-demos")
        config = json.loads((ROOT / "config.example.json").read_text(encoding="utf-8"))
        config["shared"].update({"workspace_id": "11111111-1111-4111-8111-111111111111",
                                 "staging_lakehouse_id": "22222222-2222-4222-8222-222222222222"})
        workspace = "33333333-3333-4333-8333-333333333333"
        lakehouse = "44444444-4444-4444-8444-444444444444"
        owner = "55555555-5555-4555-8555-555555555555"
        marker = f"chanel-demo3-{owner}"
        target = {"workspace_id": workspace, "item_id": lakehouse, "fact": "dbo.factsales"}
        journal = {
            "contract_version": 1, "track": "demo-3", "tenant_id": config["tenant_id"], "prefix": "chanel",
            "owner": {"id": owner, "marker": marker}, "pending_operations": [], "uncertain_operations": [],
            "workspace": {"id": workspace, "type": "Workspace", "status": "confirmed",
                          "created_by_owner": owner, "create_intent_id": "66666666-6666-4666-8666-666666666666",
                          "readback": {"id": workspace, "type": "Workspace", "owner_marker": marker}},
            "lakehouse": {"id": lakehouse, "type": "Lakehouse", "status": "confirmed",
                          "workspace_id": workspace, "created_by_owner": owner,
                          "create_intent_id": "77777777-7777-4777-8777-777777777777",
                          "readback": {"id": lakehouse, "type": "Lakehouse",
                                       "workspace_id": workspace, "owner_marker": marker}},
            "fact": {"status": "confirmed", "workspace_id": workspace,
                     "item_id": lakehouse, "name": "dbo.factsales"},
        }
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            state = root / "state"
            state.mkdir()
            journal_path = state / demo.STATE_FILE
            journal_path.write_text(json.dumps(journal), encoding="utf-8")
            before = journal_path.read_bytes()
            invocation = root / "invocation"
            invocation.mkdir()
            phases, resolved, path = orchestration.effective_run_config(
                config, "chanel", state, invocation, 30, ["demo-3"], 1)
            candidate_result = phases["resolve-target-candidate"]["demo-3"]
            self.assertEqual((candidate_result["status"], candidate_result["ready"]), ("pass", False))
            self.assertEqual(resolved["shared"]["target_bindings"], {"demo-3": target})
            self.assertEqual(json.loads(path.read_text(encoding="utf-8")), resolved)
            self.assertEqual(journal_path.read_bytes(), before)
            self.assertFalse((state / demo.LOCK_FILE).exists())
            self.assertEqual(config["shared"]["target_bindings"], {})

            conflict = json.loads(json.dumps(config))
            conflict["demo-3"]["destination_table"] = "dbo.FactSales"
            rejected = root / "rejected"
            rejected.mkdir()
            phases, resolved, path = orchestration.effective_run_config(
                conflict, "chanel", state, rejected, 30, ["demo-3"], 1)
            self.assertEqual(phases["resolve-target-candidate"]["demo-3"]["status"], "blocked")
            self.assertIsNone(resolved)
            self.assertIsNone(path)
            self.assertFalse((rejected / demo.CANDIDATE_FILE).exists())
            self.assertEqual(journal_path.read_bytes(), before)

    def test_selected_eventhouse_and_demo3_candidates_resolve_concurrently(self):
        sys.path.insert(0, str(ROOT / "demo-2-eventhouse"))
        try:
            eventhouse = load_script("eventhouse_parallel_integration", ROOT / "demo-2-eventhouse" / "deploy.py")
        finally:
            sys.path.pop(0)
        demo3 = load_script("demo3_parallel_integration", ROOT / "demo-3-onelake-events-copy" / "deploy.py")
        orchestration = load_script("orchestration_parallel_integration", ROOT / "scripts" / "deploy-demos")
        config = json.loads((ROOT / "config.example.json").read_text(encoding="utf-8"))
        config["shared"].update({"workspace_id": "11111111-1111-4111-8111-111111111111",
                                 "staging_lakehouse_id": "22222222-2222-4222-8222-222222222222"})
        settings = eventhouse.validate_config(config, "chanel")
        event_workspace = "33333333-3333-4333-8333-333333333333"
        eventhouse_id = "44444444-4444-4444-8444-444444444444"
        database = "55555555-5555-4555-8555-555555555555"
        lake_workspace = "66666666-6666-4666-8666-666666666666"
        lakehouse = "77777777-7777-4777-8777-777777777777"
        owner = "88888888-8888-4888-8888-888888888888"
        marker = f"chanel-demo3-{owner}"
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            state = root / "state"
            with eventhouse.Ledger(state, settings, create=True) as ledger:
                ledger.data["resources"].update({
                    "workspace": {"id": event_workspace, "name": settings["workspace_name"],
                                  "type": "Workspace", "confirmed": True},
                    "eventhouse": {"id": eventhouse_id, "name": settings["eventhouse_name"],
                                   "type": "Eventhouse", "confirmed": True},
                    "database": {"id": database, "name": settings["database_name"], "type": "KQLDatabase",
                                 "parent_eventhouse_id": eventhouse_id, "confirmed": True},
                    "table": {"name": settings["table"], "database_id": database, "confirmed": True},
                })
                ledger.save()
            journal = {
                "contract_version": 1, "track": "demo-3", "tenant_id": config["tenant_id"], "prefix": "chanel",
                "owner": {"id": owner, "marker": marker}, "pending_operations": [], "uncertain_operations": [],
                "workspace": {"id": lake_workspace, "type": "Workspace", "status": "confirmed",
                              "created_by_owner": owner, "create_intent_id": "99999999-9999-4999-8999-999999999999",
                              "readback": {"id": lake_workspace, "type": "Workspace", "owner_marker": marker}},
                "lakehouse": {"id": lakehouse, "type": "Lakehouse", "status": "confirmed",
                              "workspace_id": lake_workspace, "created_by_owner": owner,
                              "create_intent_id": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
                              "readback": {"id": lakehouse, "type": "Lakehouse",
                                           "workspace_id": lake_workspace, "owner_marker": marker}},
                "fact": {"status": "confirmed", "workspace_id": lake_workspace,
                         "item_id": lakehouse, "name": "dbo.factsales"},
            }
            (state / demo3.STATE_FILE).write_text(json.dumps(journal), encoding="utf-8")
            invocation = root / "invocation"
            invocation.mkdir()
            phases, resolved, _ = orchestration.effective_run_config(
                config, "chanel", state, invocation, 30, ["demo-2", "demo-3"], 2)
            self.assertEqual({track: (outcome["status"], outcome["ready"])
                              for track, outcome in phases["resolve-target-candidate"].items()},
                             {"demo-2": ("pass", False), "demo-3": ("pass", False)})
            self.assertEqual(resolved["shared"]["target_bindings"], {
                "demo-2": {"workspace_id": event_workspace, "item_id": database, "fact": "FactSales"},
                "demo-3": {"workspace_id": lake_workspace, "item_id": lakehouse, "fact": "dbo.factsales"},
            })


if __name__ == "__main__":
    unittest.main()
