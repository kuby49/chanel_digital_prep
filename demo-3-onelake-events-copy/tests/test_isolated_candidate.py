import copy
import json
import os
import subprocess
import sys
import tempfile
import unittest
import uuid
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import activator
import customer_preflight as original
import isolated_candidate


class IsolatedCandidateTests(unittest.TestCase):
    def setUp(self):
        uid = lambda: str(uuid.uuid4())
        self.spec = {
            "tenant_id": original.TENANT, "prefix": "isolated",
            "owner_id": uid(), "workspace_id": uid(), "lakehouse_id": uid(),
            "lakehouse_name": "IsolatedLakehouse", "notebook_id": uid(),
            "pipeline_id": uid(), "reflex_id": uid(), "connection_id": uid(),
            "producer_workspace_id": uid(),
            "producer_lakehouse_id": uid(),
            "native_policy": {
                "version": 1, "max_file_count": 10, "max_rows_per_file": 1000,
                "max_admission_seconds": 3600,
                "run_template": {"batch_class": "measured", "start_date": "2026-01-01",
                                 "seed": 42, "date_count": 365, "products": 20,
                                 "customers": 100, "interval_seconds": 60},
            },
        }
        workspace, lakehouse, connection = (self.spec[k] for k in
                                           ("workspace_id", "lakehouse_id", "connection_id"))
        settings = {"properties": {"type": "Lakehouse", "typeProperties": {
            "workspaceId": workspace, "artifactId": lakehouse},
            "externalReferences": {"connection": connection}}}
        columns = ("SalesLineID", "BatchID", "SaleTimestamp", "CustomerID", "ProductID", "Date",
                   "Quantity", "UnitPriceMinorUnits", "SalesAmountMinorUnits")
        self.copy = {"type": "Copy", "typeProperties": {
            "enableStaging": False,
            "translator": {"mappings": [{"source": {"name": name}, "sink": {"name": name}}
                                         for name in columns]},
            "source": {"type": "DelimitedTextSource", "storeSettings": {"recursive": False},
                       "datasetSettings": {"connectionSettings": copy.deepcopy(settings),
                           "typeProperties": {"location": {"type": "LakehouseLocation",
                               "folderPath": "incoming/sales", "fileName": "template.csv"}}}},
            "sink": {"type": "LakehouseTableSink", "tableActionOption": "Upsert",
                     "upsertSettings": {"keys": ["BatchID", "SalesLineID"]},
                     "datasetSettings": {"connectionSettings": copy.deepcopy(settings),
                         "typeProperties": {"schema": "dbo", "table": "factsales"}}},
        }}
        self.sample = {"type": activator.EVENT_TYPE, "id": "offline-test",
                       "subject": "/Files/incoming/sales/sales_example_0001.csv",
                       "source": f"/tenants/{original.TENANT}/workspaces/{workspace}/items/{lakehouse}"}
        source = os.environ.get("DEMO3_PRODUCER_PATH")
        if source:
            self.producer = Path(source).resolve().parent
        else:
            temp = tempfile.TemporaryDirectory()
            self.addCleanup(temp.cleanup)
            self.producer = Path(temp.name)
            for name in ("generator.py", "fabric_storage.py"):
                (self.producer / name).write_text("# offline fixture\n", encoding="utf-8")
        self.resolver = mock.patch.object(
            isolated_candidate, "_producer_ids",
            return_value=(self.spec["producer_workspace_id"], self.spec["producer_lakehouse_id"]))
        self.resolver.start()
        self.addCleanup(self.resolver.stop)

    def build(self):
        return isolated_candidate.build_candidate(
            self.spec, self.producer, self.copy, self.sample,
            producer_config_path=self.producer / "config.json",
            producer_prefix="new", producer_state_dir=self.producer / "state")

    def test_candidate_remains_unready_and_pins_all_bindings(self):
        result = self.build()
        self.assertFalse(result["ready"])
        self.assertEqual(result["status"], "offline-candidate-only")
        notebook, pipeline, graph = (result["definitions"][k] for k in
                                     ("guard_notebook", "pipeline", "activator_graph"))
        self.assertEqual(notebook["metadata"]["trident"]["lakehouse"]["default_lakehouse"],
                         self.spec["lakehouse_id"])
        source = "".join(notebook["cells"][1]["source"])
        self.assertIn(self.spec["producer_workspace_id"], source)
        self.assertIn(self.spec["producer_lakehouse_id"], source)
        self.assertEqual(pipeline["properties"]["concurrency"], 1)
        load = pipeline["properties"]["activities"][1]["typeProperties"]["ifTrueActivities"][0]
        self.assertEqual(load["policy"]["retry"], 0)
        self.assertIn("PrepareCopy", load["typeProperties"]["source"]["datasetSettings"]
                      ["typeProperties"]["location"]["fileName"]["value"])
        rule = next(e for e in graph if e["payload"].get("definition", {}).get("type") == "Rule")
        self.assertFalse(rule["payload"]["definition"]["settings"]["shouldRun"])
        event_source = next(e for e in graph if e["type"] == "realTimeHubSource-v1")
        self.assertEqual(event_source["payload"]["connection"]["artifactId"], self.spec["lakehouse_id"])
        action = next(e for e in graph if e["type"] == "fabricItemAction-v1")
        self.assertEqual(action["payload"]["fabricItem"]["itemId"], self.spec["pipeline_id"])
        self.assertTrue(result["required_live_gates"])

    def test_original_or_inconsistent_bindings_are_rejected(self):
        for field, value in (("lakehouse_id", original.LAKEHOUSE),
                             ("notebook_id", original.NOTEBOOK),
                             ("pipeline_id", original.PIPELINE),
                             ("reflex_id", original.REFLEX),
                             ("producer_workspace_id", str(uuid.uuid4())),
                             ("producer_lakehouse_id", original.STAGING_LAKEHOUSE),
                             ("producer_lakehouse_id", str(uuid.uuid4()))):
            with self.subTest(field=field):
                old = self.spec[field]
                self.spec[field] = value
                with self.assertRaises(ValueError):
                    self.build()
                self.spec[field] = old
        self.copy["typeProperties"]["sink"]["datasetSettings"]["connectionSettings"]["properties"][
            "typeProperties"]["artifactId"] = original.LAKEHOUSE
        with self.assertRaisesRegex(ValueError, "does not match"):
            self.build()

    def test_existing_generic_connection_may_be_a_candidate_only(self):
        self.spec["connection_id"] = original.CONNECTION
        for endpoint in ("source", "sink"):
            self.copy["typeProperties"][endpoint]["datasetSettings"]["connectionSettings"][
                "properties"]["externalReferences"]["connection"] = original.CONNECTION
        result = self.build()
        self.assertFalse(result["ready"])
        self.assertTrue(any("runtime access" in gate for gate in result["required_live_gates"]))

    def test_original_owner_marker_is_rejected(self):
        prefix, owner_id = original.MARKER.split("-demo3-", 1)
        self.spec.update(prefix=prefix, owner_id=owner_id)
        with self.assertRaisesRegex(ValueError, "owner marker"):
            self.build()

    def test_wrong_event_source_is_rejected(self):
        self.sample["source"] = f"/tenants/{original.TENANT}/workspaces/{original.WORKSPACE}/items/{original.LAKEHOUSE}"
        with self.assertRaisesRegex(ValueError, "event source"):
            self.build()

    def test_producer_resolver_uses_actual_owned_journal_and_rejects_pending(self):
        if not (self.producer / "deploy.py").is_file():
            self.skipTest("producer deployment fixture not supplied")
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        root = Path(temp.name)
        config_path, state_dir = root / "config.json", root / "state"
        script = """
import json,sys,uuid
from pathlib import Path
sys.path.insert(0,sys.argv[1])
import deploy
root=Path(sys.argv[2])
config={'contract_version':1,'common':{'tenant_id':sys.argv[3],'capacity_id':str(uuid.uuid4()),
        'prefix':'new'},'shared':{'workspace_name':'{prefix}-producer','identity_id':str(uuid.uuid4())}}
plan=deploy.parse_config(config,'new')
state=deploy.new_state(plan)
for kind in deploy.KINDS:
    state['resources'][kind]=deploy._record(state,plan,kind,str(uuid.uuid4()))
store=deploy.StateStore(root/'state')
store.write(state)
(root/'config.json').write_text(json.dumps(config),encoding='utf-8')
"""
        subprocess.run([sys.executable, "-c", script, str(self.producer), str(root), original.TENANT],
                       check=True, timeout=30)
        self.resolver.stop()
        workspace, lakehouse = isolated_candidate._producer_ids(self.producer, config_path, "new", state_dir)
        state_path = state_dir / "shared-producer-v1.json"
        state = json.loads(state_path.read_text(encoding="utf-8"))
        self.assertEqual((workspace, lakehouse),
                         (state["resources"]["workspace"]["id"], state["resources"]["staging_lakehouse"]["id"]))
        state["pending"] = {"kind": "staging_lakehouse", "operation_id": None, "result_id": None}
        state_path.write_text(json.dumps(state), encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "pending"):
            isolated_candidate._producer_ids(self.producer, config_path, "new", state_dir)
