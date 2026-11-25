import copy
import os
import sys
import tempfile
import unittest
import uuid
from pathlib import Path

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
            "producer_workspace_id": original.STAGING_WORKSPACE,
            "producer_lakehouse_id": original.STAGING_LAKEHOUSE,
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

    def build(self):
        return isolated_candidate.build_candidate(self.spec, self.producer, self.copy, self.sample)

    def test_candidate_remains_unready_and_pins_all_bindings(self):
        result = self.build()
        self.assertFalse(result["ready"])
        self.assertEqual(result["status"], "offline-candidate-only")
        notebook, pipeline, graph = (result["definitions"][k] for k in
                                     ("guard_notebook", "pipeline", "activator_graph"))
        self.assertEqual(notebook["metadata"]["trident"]["lakehouse"]["default_lakehouse"],
                         self.spec["lakehouse_id"])
        source = "".join(notebook["cells"][1]["source"])
        self.assertIn(original.STAGING_WORKSPACE, source)
        self.assertIn(original.STAGING_LAKEHOUSE, source)
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
                             ("connection_id", original.CONNECTION),
                             ("producer_workspace_id", str(uuid.uuid4())),
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

    def test_original_owner_marker_is_rejected(self):
        prefix, owner_id = original.MARKER.split("-demo3-", 1)
        self.spec.update(prefix=prefix, owner_id=owner_id)
        with self.assertRaisesRegex(ValueError, "owner marker"):
            self.build()

    def test_wrong_event_source_is_rejected(self):
        self.sample["source"] = f"/tenants/{original.TENANT}/workspaces/{original.WORKSPACE}/items/{original.LAKEHOUSE}"
        with self.assertRaisesRegex(ValueError, "event source"):
            self.build()
