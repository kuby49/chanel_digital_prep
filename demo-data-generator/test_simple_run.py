import base64
import csv
import io
import json
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import fabric_assets
import generator
import publish_simple
import simple_data
import simple_run


ROOT = Path(__file__).resolve().parent
SETTINGS = json.loads((ROOT.parent / "demo-settings.json").read_text(encoding="utf-8"))


class SimpleRunTests(unittest.TestCase):
    def test_interrupted_producer_is_stopped_without_stopping_current_activity(self):
        current = "11111111-1111-4111-8111-111111111111"
        previous = "22222222-2222-4222-8222-222222222222"
        pipeline_run = "33333333-3333-4333-8333-333333333333"
        active = [{"id": job, "jobType": "PipelineRunNotebook", "status": "InProgress",
                   "startTimeUtc": f"2026-11-25T22:5{index}:00Z"}
                  for index, job in enumerate((previous, current))]
        stopped = [{**active[0], "status": "Cancelled"}, active[1]]
        calls = []

        def request(method, url, **kwargs):
            calls.append((method, url))
            if url.endswith("/queryactivityruns"):
                body = {"value": [{"activityRunId": current, "activityName": "RunProducer",
                                    "activityType": "TridentNotebook"}]}
            elif url.endswith("/cancel"):
                body = None
            else:
                body = {"value": active if sum("/cancel" in item[1] for item in calls) == 0 else stopped}
            response = SimpleNamespace(content=b"{}" if body is not None else b"",
                                       json=lambda: body, raise_for_status=lambda: None)
            return response

        with patch("simple_run.requests.request", side_effect=request), patch("simple_run.time.sleep"):
            simple_run.stop_previous_producer({"config": SETTINGS, "get_token": lambda _: "token"}, pipeline_run)
        cancelled = [url for method, url in calls if url.endswith("/cancel")]
        self.assertEqual(len(cancelled), 1)
        self.assertIn(previous, cancelled[0])

    def test_interrupted_producer_never_cancels_a_newer_activity(self):
        current = "11111111-1111-4111-8111-111111111111"
        newer = "22222222-2222-4222-8222-222222222222"
        pipeline_run = "33333333-3333-4333-8333-333333333333"
        jobs = [{"id": job, "jobType": "PipelineRunNotebook", "status": "InProgress",
                 "startTimeUtc": f"2026-11-25T22:5{index}:00Z"}
                for index, job in enumerate((current, newer))]

        def request(method, url, **kwargs):
            self.assertFalse(url.endswith("/cancel"))
            body = ({"value": [{"activityRunId": current, "activityName": "RunProducer",
                                "activityType": "TridentNotebook"}]}
                    if url.endswith("/queryactivityruns") else {"value": jobs})
            return SimpleNamespace(content=b"{}", json=lambda: body, raise_for_status=lambda: None)

        with patch("simple_run.requests.request", side_effect=request):
            simple_run.stop_previous_producer({"config": SETTINGS, "get_token": lambda _: "token"}, pipeline_run)
            jobs[1]["startTimeUtc"] = None
            with self.assertRaisesRegex(RuntimeError, "Cannot determine the order"):
                simple_run.stop_previous_producer({"config": SETTINGS, "get_token": lambda _: "token"}, pipeline_run)

    def test_interrupted_producer_uses_canonical_job_history_pagination(self):
        current = "11111111-1111-4111-8111-111111111111"
        pipeline_run = "33333333-3333-4333-8333-333333333333"
        urls = []

        def request(method, url, **kwargs):
            urls.append(url)
            if url.endswith("/queryactivityruns"):
                body = {"value": [{"activityRunId": current, "activityName": "RunProducer",
                                    "activityType": "TridentNotebook"}]}
            elif "continuationToken=" in url:
                body = {"value": [{"id": current, "jobType": "PipelineRunNotebook",
                                    "status": "InProgress", "startTimeUtc": "2026-11-25T22:51:00Z"}]}
            else:
                body = {"value": [], "continuationToken": "a+b/=",
                        "continuationUri": "https://wabi.example.invalid/v1/ignored"}
            return SimpleNamespace(content=b"{}", json=lambda: body, raise_for_status=lambda: None)

        with patch("simple_run.requests.request", side_effect=request):
            simple_run.stop_previous_producer({"config": SETTINGS, "get_token": lambda _: "token"}, pipeline_run)
        self.assertTrue(urls[-1].startswith("https://api.fabric.microsoft.com/v1/"))
        self.assertIn("continuationToken=a%2Bb%2F%3D", urls[-1])

    def test_dimensions_match_existing_demo_dimensions(self):
        data = SETTINGS["data"]
        original = generator.RunConfig(
            run_id="demo", start_date=data["start_date"], seed=data["seed"],
            date_count=data["date_count"], products=data["product_count"],
            customers=data["customer_count"])
        self.assertEqual(simple_data.dimensions(data), generator.dimensions(original))

    def test_reset_produce_and_stop(self):
        events = []
        modules = []
        for name in ("demo-1", "demo-2", "demo-3"):
            module = SimpleNamespace(
                reset=lambda context, n=name: events.append((n, "reset")),
                start=lambda context, n=name: events.append((n, "start")),
                stop=lambda context, *, failed=False, n=name: events.append((n, "stop", failed)),
            )
            modules.append((name, module))
        submitted = []
        modules[1][1].submit_batch = lambda context, payload: submitted.append(payload)
        settings = {**SETTINGS, "data": {**SETTINGS["data"], "file_count": 2, "rows_per_file": 2}}
        sent = []
        with patch("simple_run.refresh_sql_metadata") as refresh:
            simple_run.run({"config": settings}, modules,
                           send=lambda context, demo, filename, content: sent.append((demo, filename, content)),
                           sleep=lambda seconds: events.append(("sleep", seconds)))
        self.assertEqual([call.args[1] for call in refresh.call_args_list], ["demo-1", "demo-3"])
        self.assertEqual(events[:3], [(name, "stop", True) for name, _ in modules])
        self.assertEqual(events[3:6], [(name, "reset") for name, _ in modules])
        self.assertEqual(events[6:9], [(name, "start") for name, _ in modules])
        self.assertEqual(events[-3:], [(name, "stop", False) for name, _ in modules])
        self.assertIn(("sleep", 60), events)
        self.assertEqual([x[1] for x in sent], ["sales_0001.csv", "sales_0001.csv",
                                                "sales_0002.csv", "sales_0002.csv"])
        self.assertEqual([sent[0][2], sent[2][2]], submitted)
        self.assertEqual(sent[0][2], sent[1][2])
        self.assertEqual(sent[2][2], sent[3][2])
        rows = list(csv.DictReader(io.StringIO(submitted[0].decode())))
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["BatchID"], "batch_0001")

    def test_sql_refresh_waits_for_operation_and_checks_each_table(self):
        endpoint = "44444444-4444-4444-8444-444444444444"
        operation = "55555555-5555-4555-8555-555555555555"
        responses = [
            SimpleNamespace(status_code=200, json=lambda: {
                "properties": {"sqlEndpointProperties": {"id": endpoint}}}),
            SimpleNamespace(status_code=202, headers={"x-ms-operation-id": operation}),
            SimpleNamespace(status_code=200, json=lambda: {"status": "Succeeded"}),
            SimpleNamespace(status_code=200, json=lambda: {"value": [
                {"tableName": "bronze.sales_files", "status": "Success"},
                {"tableName": "silver.sales_joined", "status": "Success"},
            ]}),
        ]
        for response in responses:
            response.raise_for_status = lambda: None
        with patch("simple_run.requests.request", side_effect=responses) as request:
            simple_run.refresh_sql_metadata(
                {"config": SETTINGS, "get_token": lambda _: "token"}, "demo-1",
                [("bronze", "sales_files"), ("silver", "sales_joined")])
        body = request.call_args_list[1].kwargs["json"]
        self.assertFalse(body["recreateTables"])
        self.assertEqual(body["tables"], [
            {"schema": "bronze", "tableNames": ["sales_files"]},
            {"schema": "silver", "tableNames": ["sales_joined"]},
        ])
        self.assertTrue(request.call_args_list[-1].args[1].endswith(f"/{operation}/result"))

    def test_sql_refresh_surfaces_unsuccessful_table(self):
        endpoint = "44444444-4444-4444-8444-444444444444"
        responses = [
            SimpleNamespace(status_code=200, json=lambda: {
                "properties": {"sqlEndpointProperties": {"id": endpoint}}}),
            SimpleNamespace(status_code=200, json=lambda: {"value": [
                {"tableName": "dbo.factsales", "status": "Failure"},
            ]}),
        ]
        for response in responses:
            response.raise_for_status = lambda: None
        with patch("simple_run.requests.request", side_effect=responses):
            with self.assertRaisesRegex(RuntimeError, "factsales: Failure"):
                simple_run.refresh_sql_metadata(
                    {"config": SETTINGS, "get_token": lambda _: "token"}, "demo-3",
                    [("dbo", "factsales")])

    def test_failure_stops_all_without_publishing(self):
        events = []
        def fail(_):
            raise RuntimeError("reset failed")
        modules = [(name, SimpleNamespace(
            reset=fail if name == "demo-2" else lambda _: None,
            start=lambda _: None,
            stop=lambda _, *, failed=False, n=name: events.append((n, failed)),
        )) for name in ("demo-1", "demo-2", "demo-3")]
        with self.assertRaisesRegex(RuntimeError, "reset failed"):
            simple_run.run({"config": SETTINGS}, modules, send=lambda *args: self.fail("published"))
        self.assertEqual(events, [(name, True) for name, _ in modules] * 2)

    def test_bundle_includes_settings_and_demo_helpers(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            producer = root / "demo-data-generator"
            producer.mkdir()
            for name in ("simple_data.py", "simple_run.py", "notebook.ipynb", "pipeline-content.json"):
                (producer / name).write_bytes((ROOT / name).read_bytes())
            (root / "demo-settings.json").write_text(json.dumps(SETTINGS), encoding="utf-8")
            for folder, name in fabric_assets.DEMO_MODULES.items():
                (root / folder).mkdir()
                (root / folder / name).write_text("pass\n", encoding="utf-8")
            (root / "demo-2-eventhouse" / "eventhouse_core.py").write_text("pass\n", encoding="utf-8")
            with patch.object(fabric_assets, "HERE", producer):
                settings = SETTINGS["shared"]
                notebook = fabric_assets.notebook_definition(
                    settings["workspace_id"], settings["staging_lakehouse_id"],
                    settings["staging_lakehouse_name"], settings["producer_environment_id"],
                    default_lakehouse=settings["notebook_default_lakehouse"])
                notebook = json.loads(base64.b64decode(notebook["parts"][0]["payload"]))
                namespace = {}
                bundle_dir = root / "loaded"
                bundle_dir.mkdir()
                with patch("tempfile.mkdtemp", return_value=str(bundle_dir)):
                    try:
                        exec("".join(notebook["cells"][0]["source"]), namespace)
                        self.assertEqual(namespace["_demo_settings"], SETTINGS)
                        for name in fabric_assets.DEMO_MODULES.values():
                            self.assertTrue((namespace["_producer_dir"] / name).is_file())
                        self.assertTrue((namespace["_producer_dir"] / "eventhouse_core.py").is_file())
                    finally:
                        sys.path.remove(str(bundle_dir))
                pipeline = fabric_assets.pipeline_definition(settings["workspace_id"], settings["producer_notebook_id"])
                pipeline = json.loads(base64.b64decode(pipeline["parts"][0]["payload"]))
                self.assertNotIn("parameters", pipeline["properties"])
                internal = pipeline["properties"]["activities"][0]["typeProperties"]["parameters"]
                self.assertEqual(internal["pipeline_run_id"]["value"]["value"], "@pipeline().RunId")

    def test_publisher_builds_both_before_update_and_polls_accepted_operation(self):
        events = []
        class Client:
            def request(self, method, path, payload):
                events.append((method, path, payload))
                return (202, {"x-ms-operation-id": "33333333-3333-4333-8333-333333333333",
                              "Retry-After": "1"}, None) if len(events) == 1 else (200, {}, None)
            def poll(self, operation, delay, **kwargs):
                events.append(("poll", operation))

        with patch.object(fabric_assets, "notebook_definition", return_value={"notebook": 1}), \
             patch.object(fabric_assets, "pipeline_definition", return_value={"pipeline": 1}):
            publish_simple.publish(Client(), SETTINGS)
        shared = SETTINGS["shared"]
        self.assertIn(shared["producer_notebook_id"], events[0][1])
        self.assertEqual(events[1][0], "poll")
        self.assertIn(shared["wrapper_pipeline_id"], events[2][1])


if __name__ == "__main__":
    unittest.main()
