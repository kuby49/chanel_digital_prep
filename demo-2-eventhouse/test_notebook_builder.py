import base64
import json
import tempfile
import unittest
from pathlib import Path

from notebook_builder import build_notebook, fabric_definition


class NotebookBuilderTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.generator = Path(self.directory.name) / "generator.py"
        self.generator.write_text("GENERATOR_MARKER = 123\n", encoding="utf-8")
        self.options = {
            "workspace_id": "aab2ed7b-fd2f-4696-84b3-b34ef931681f",
            "lakehouse_id": "04113b0b-8698-4a61-a69d-ad862e96b6e6",
            "lakehouse_name": "SalesLakehouse",
            "data": {"run_id": None, "date_count": 365},
            "shortcut_name": "EventhouseFactSales",
            "verification_timeout_seconds": 37,
            "poll_interval_seconds": 3,
        }

    def test_definition_binds_lakehouse_and_packages_executable_sources(self):
        notebook = build_notebook(self.generator, **self.options)
        lakehouse = notebook["metadata"]["trident"]["lakehouse"]
        self.assertEqual(lakehouse["default_lakehouse"], self.options["lakehouse_id"])
        self.assertEqual(lakehouse["default_lakehouse_workspace_id"], self.options["workspace_id"])
        self.assertEqual(lakehouse["known_lakehouses"], [{"id": self.options["lakehouse_id"]}])
        self.assertEqual(notebook["cells"][0]["metadata"]["tags"], ["parameters"])
        for cell in notebook["cells"]:
            compile("".join(cell["source"]), "notebook", "exec")
        runtime = "".join(notebook["cells"][1]["source"])
        generator_source = self.generator.read_text(encoding="utf-8").encode("utf-8")
        self.assertIn(base64.b64encode(generator_source).decode("ascii"), runtime)
        self.assertIn("verify_batch_with_timeout", runtime)
        self.assertIn("notebookutils.notebook.exit", runtime)
        definition = fabric_definition(notebook)
        self.assertEqual(definition["format"], "ipynb")
        payload = base64.b64decode(definition["parts"][0]["payload"])
        self.assertEqual(json.loads(payload), notebook)

    def test_invalid_identity_and_timeouts_are_rejected(self):
        for key, value in (
            ("workspace_id", "not-a-uuid"),
            ("lakehouse_name", "bad.name"),
            ("verification_timeout_seconds", True),
            ("verification_timeout_seconds", 0),
            ("poll_interval_seconds", -1),
        ):
            options = {**self.options, key: value}
            with self.subTest(key=key, value=value), self.assertRaises(ValueError):
                build_notebook(self.generator, **options)


if __name__ == "__main__":
    unittest.main()
