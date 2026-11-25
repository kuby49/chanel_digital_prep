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
        simple_run.run({"config": settings}, modules,
                       send=lambda context, demo, filename, content: sent.append((demo, filename, content)),
                       sleep=lambda seconds: events.append(("sleep", seconds)))
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
                self.assertNotIn("parameters", pipeline["properties"]["activities"][0]["typeProperties"])

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
