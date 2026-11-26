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

    def test_start_settles_after_enabling(self):
        events = []
        with (patch.object(simple_demo3, "_trigger", side_effect=lambda _, enabled: events.append(enabled)),
              patch.object(simple_demo3.time, "sleep", side_effect=lambda seconds: events.append(seconds))):
            simple_demo3.start({})
        self.assertEqual(events, [True, 180])

    def test_jobs_encodes_token_on_scoped_canonical_endpoint(self):
        path = "/v1/workspaces/workspace/items/pipeline/jobs/instances"
        pages = [
            {"value": [{"id": "first"}], "continuationToken": "a+/=&?",
             "continuationUri": "https://wabi-west-us3-a-primary-redirect.analysis.windows.net" + path},
            {"value": [{"id": "second"}]},
        ]
        with (patch.object(simple_demo3, "_jobs_path", return_value=path),
              patch.object(simple_demo3, "_request", side_effect=[
                  (200, {}, page) for page in pages]) as request):
            self.assertEqual(list(simple_demo3._jobs({})), [{"id": "first"}, {"id": "second"}])
        self.assertEqual(request.call_args_list, [
            unittest.mock.call({}, "GET", path + "?jobType=Pipeline"),
            unittest.mock.call({}, "GET", path + "?jobType=Pipeline&continuationToken=a%2B%2F%3D%26%3F"),
        ])

    def test_jobs_rejects_missing_or_invalid_continuation_token(self):
        for token in (None, "", 123):
            with self.subTest(token=token):
                page = {"value": [], "continuationUri": "https://regional.example/v1/jobs",
                        "continuationToken": token}
                with (patch.object(simple_demo3, "_jobs_path", return_value="/v1/jobs"),
                      patch.object(simple_demo3, "_request", return_value=(200, {}, page)) as request):
                    with self.assertRaisesRegex(RuntimeError, "no valid continuation token"):
                        list(simple_demo3._jobs({}))
                    self.assertEqual(request.call_count, 1)

    def test_jobs_rejects_repeated_continuation_token(self):
        page = {"value": [], "continuationToken": "same"}
        with (patch.object(simple_demo3, "_jobs_path", return_value="/v1/jobs"),
              patch.object(simple_demo3, "_request", return_value=(200, {}, page)) as request):
            with self.assertRaisesRegex(RuntimeError, "unexpected continuation"):
                list(simple_demo3._jobs({}))
            self.assertEqual(request.call_count, 2)

    def test_completion_requires_earlier_batches_even_when_final_batch_is_present(self):
        context = {"config": {"data": {"file_count": 3}, "demo-3": {
            "workspace_id": "744223de-ceab-4c4f-82ba-f811bcb5c7d7",
            "lakehouse_id": "6bba6989-5cfd-4192-8e75-00274afb66c1",
            "landing_path": "Files/incoming/sales", "destination_table": "dbo.factsales",
        }}, "spark": Mock()}
        collect = context["spark"].read.format.return_value.load.return_value.select.return_value.distinct.return_value.collect
        collect.return_value = [{"BatchID": "batch_0003"}]

        def expire(_, message):
            raise TimeoutError(message)

        with (patch.dict(sys.modules, {"simple_data": SimpleNamespace(
                batch_id=lambda number: f"batch_{number:04d}")}),
              patch.object(simple_demo3, "_wait", side_effect=expire)):
            with self.assertRaisesRegex(TimeoutError, "missing batches batch_0001, batch_0002"):
                simple_demo3._wait_for_batches(context)
            collect.return_value = [{"BatchID": f"batch_{number:04d}"} for number in range(1, 4)]
            self.assertIsNone(simple_demo3._wait_for_batches(context))

    def test_stop_waits_for_batches_before_disabling_except_on_failure(self):
        context = {"spark": Mock()}
        events = []
        with (patch.object(simple_demo3, "_wait_for_batches", side_effect=lambda _: events.append("batches")),
              patch.object(simple_demo3, "_trigger", side_effect=lambda _, enabled: events.append(enabled)),
              patch.object(simple_demo3, "_drain", side_effect=lambda _, cancel: events.append({"cancel": cancel}))):
            simple_demo3.stop(context)
            self.assertEqual(events, ["batches", False, {"cancel": False}])
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
