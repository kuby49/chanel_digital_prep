import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import simple_demo3


class SimpleDemo3Tests(unittest.TestCase):
    def test_pipeline_migrates_legacy_upsert_to_exact_file_append(self):
        config = {"demo-3": {
            "workspace_id": "workspace", "lakehouse_id": "lakehouse", "connection_id": "connection",
            "landing_path": "Files/incoming/sales", "destination_table": "dbo.factsales",
        }}
        connection = {"connectionSettings": {"properties": {
            "type": "Lakehouse", "typeProperties": {"workspaceId": "workspace", "artifactId": "lakehouse"},
            "externalReferences": {"connection": "connection"},
        }}}
        location = {"type": "LakehouseLocation", "folderPath": "incoming/sales",
                    "fileName": "@activity('PrepareCopy').output.result.exitValue"}
        activity = {"name": "LegacyCopy", "type": "Copy", "dependsOn": [{"activity": "PrepareCopy"}],
                    "typeProperties": {
                        "enableStaging": False,
                        "source": {"type": "DelimitedTextSource", "storeSettings": {"recursive": False},
                                   "datasetSettings": {**connection, "typeProperties": {"location": location}}},
                        "sink": {"type": "LakehouseTableSink", "tableActionOption": "Upsert",
                                 "upsertSettings": {"keys": ["BatchID", "SalesLineID"]},
                                 "datasetSettings": {**connection, "typeProperties": {
                                     "schema": "dbo", "table": "factsales"}}},
                    }}
        existing = {"properties": {"activities": [
            {"type": "TridentNotebook", "name": "PrepareCopy"},
            {"type": "IfCondition", "typeProperties": {"ifTrueActivities": [
                activity, {"type": "TridentNotebook", "name": "VerifyCopy"}]}},
        ]}}
        properties = simple_demo3.copy_pipeline(config, existing)["properties"]
        self.assertEqual(list(properties["parameters"]), ["sourceSubject"])
        self.assertEqual([item["type"] for item in properties["activities"]], ["Copy"])
        copy = properties["activities"][0]
        self.assertEqual(copy["dependsOn"], [])
        sink = copy["typeProperties"]["sink"]
        self.assertEqual(sink["tableActionOption"], "Append")
        self.assertNotIn("upsertSettings", sink)
        self.assertNotIn("keyColumns", sink)
        self.assertEqual(
            copy["typeProperties"]["source"]["datasetSettings"]["typeProperties"]["location"]["fileName"],
            {"type": "Expression", "value": "@last(split(pipeline().parameters.sourceSubject, '/'))"},
        )

    def test_stop_waits_for_final_batch_before_disabling_except_on_failure(self):
        context = {"spark": Mock()}
        events = []
        with (patch.object(simple_demo3, "_wait_for_final_batch", side_effect=lambda _: events.append("final")),
              patch.object(simple_demo3, "_trigger", side_effect=lambda _, enabled: events.append(enabled)),
              patch.object(simple_demo3, "_drain", side_effect=lambda _, cancel: events.append({"cancel": cancel}))):
            simple_demo3.stop(context)
            self.assertEqual(events, ["final", False, {"cancel": False}])
            events.clear()
            simple_demo3.stop(context, failed=True)
            self.assertEqual(events, [False, {"cancel": True}])
        self.assertEqual(context["spark"].mock_calls, [])

    def test_reset_waits_for_stopping_before_touching_storage(self):
        context = {"config": {"demo-3": {
            "landing_path": "Files/incoming/sales", "destination_table": "dbo.factsales",
        }}, "spark": Mock()}
        fs = Mock()
        with (patch.dict(sys.modules, {"notebookutils": SimpleNamespace(fs=fs),
                                      "simple_data": SimpleNamespace(dimensions=Mock())}),
              patch.object(simple_demo3, "stop", side_effect=TimeoutError("Copy still active")) as stop):
            with self.assertRaisesRegex(TimeoutError, "Copy still active"):
                simple_demo3.reset(context)
            stop.assert_called_once_with(context, failed=True)
        self.assertEqual(fs.mock_calls, [])
        self.assertEqual(context["spark"].mock_calls, [])


if __name__ == "__main__":
    unittest.main()
