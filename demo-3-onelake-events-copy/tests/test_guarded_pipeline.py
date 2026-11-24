import copy
import sys
import unittest
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from guarded_pipeline import build_pipeline


class GuardedPipelineTests(unittest.TestCase):
    def test_copy_can_only_use_the_guarded_filename_and_composite_key(self):
        workspace, lakehouse, connection, notebook = (str(uuid.uuid4()) for _ in range(4))
        shared = {"connectionSettings": {"properties": {
            "type": "Lakehouse", "typeProperties": {"workspaceId": workspace, "artifactId": lakehouse},
            "externalReferences": {"connection": connection},
        }}}
        columns = ("SalesLineID", "BatchID", "SaleTimestamp", "CustomerID", "ProductID", "Date",
                   "Quantity", "UnitPriceMinorUnits", "SalesAmountMinorUnits")
        activity = {"name": "Copied", "type": "Copy", "typeProperties": {
            "enableStaging": False,
            "translator": {"mappings": [{"source": {"name": name}, "sink": {"name": name}}
                                         for name in columns]},
            "source": {"type": "DelimitedTextSource", "storeSettings": {"recursive": False},
                       "datasetSettings": {**shared, "typeProperties": {"location": {
                           "type": "LakehouseLocation", "folderPath": "incoming/sales",
                           "fileName": "template.csv",
                       }}}},
            "sink": {"type": "LakehouseTableSink", "tableActionOption": "Upsert",
                     "upsertSettings": {"keys": ["BatchID", "SalesLineID"]},
                     "datasetSettings": {**shared, "typeProperties": {"schema": "dbo", "table": "factsales"}}},
        }}
        original = copy.deepcopy(activity)
        properties = build_pipeline(workspace, lakehouse, connection, notebook, activity)["properties"]
        self.assertEqual(activity, original)
        self.assertEqual(properties["concurrency"], 1)
        prepare, gate = properties["activities"]
        self.assertEqual(gate["dependsOn"], [{"activity": "PrepareCopy", "dependencyConditions": ["Succeeded"]}])
        self.assertEqual(gate["typeProperties"]["ifFalseActivities"], [])
        load, verify = gate["typeProperties"]["ifTrueActivities"]
        self.assertEqual(load["typeProperties"]["sink"]["upsertSettings"]["keys"], ["BatchID", "SalesLineID"])
        self.assertEqual(load["typeProperties"]["source"]["datasetSettings"]["typeProperties"]["location"]["fileName"],
                         {"type": "Expression", "value": "@json(activity('PrepareCopy').output.result.exitValue).filename"})
        self.assertEqual(verify["dependsOn"], [{"activity": "CopyEventFile", "dependencyConditions": ["Succeeded"]}])
        self.assertEqual(prepare["typeProperties"]["sessionTag"], verify["typeProperties"]["sessionTag"])
        activity["typeProperties"]["sink"]["upsertSettings"]["keys"] = ["SalesLineID"]
        with self.assertRaisesRegex(ValueError, "BatchID"):
            build_pipeline(workspace, lakehouse, connection, notebook, activity)
