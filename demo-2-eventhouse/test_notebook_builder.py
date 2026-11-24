import ast
import base64
import builtins
import hashlib
import json
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest import mock

from notebook_builder import build_notebook, fabric_definition


class NotebookBuilderTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.generator = Path(self.directory.name) / "generator.py"
        self.generator.write_text(
            "import hashlib\nfrom pathlib import Path\n"
            "GENERATOR_MARKER = 123\n"
            "def generator_source_sha256():\n"
            "    return hashlib.sha256(Path(__file__).read_bytes()).hexdigest()\n",
            encoding="utf-8",
        )
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

    def test_packaged_generator_has_a_readable_source_file(self):
        notebook = build_notebook(self.generator, **self.options)
        observed = {}
        real_exec = builtins.exec

        def packaged_exec(code, namespace, *args):
            real_exec(code, namespace, *args)
            if Path(getattr(code, "co_filename", "")).name == "generator.py":
                observed["path"] = Path(namespace["__file__"])
                observed["source"] = observed["path"].read_bytes()
            elif getattr(code, "co_filename", "") == "<packaged lakehouse_validation>":
                namespace["preload_dimensions"] = lambda *_: None

        scope = {
            "spark": types.SimpleNamespace(conf=types.SimpleNamespace(set=lambda *_: None)),
            "notebookutils": types.SimpleNamespace(notebook=types.SimpleNamespace(exit=lambda *_: None)),
        }
        with mock.patch.dict(sys.modules), mock.patch.object(builtins, "exec", side_effect=packaged_exec):
            real_exec("".join(notebook["cells"][0]["source"]), scope)
            real_exec("".join(notebook["cells"][1]["source"]), scope)
        self.assertEqual(observed["source"], self.generator.read_text(encoding="utf-8").encode("utf-8"))
        self.assertEqual(hashlib.sha256(observed["source"]).hexdigest(), scope["GENERATOR_SHA256"])
        self.assertFalse(observed["path"].exists())

    def test_temporary_generator_is_removed_when_validation_fails(self):
        self.generator.write_text("def generator_source_sha256():\n    return 'wrong'\n", encoding="utf-8")
        notebook = build_notebook(self.generator, **self.options)
        observed = {}
        real_exec = builtins.exec

        def packaged_exec(code, namespace, *args):
            real_exec(code, namespace, *args)
            if Path(getattr(code, "co_filename", "")).name == "generator.py":
                observed["path"] = Path(namespace["__file__"])

        scope = {"spark": types.SimpleNamespace(conf=types.SimpleNamespace(set=lambda *_: None))}
        with mock.patch.dict(sys.modules), mock.patch.object(builtins, "exec", side_effect=packaged_exec):
            real_exec("".join(notebook["cells"][0]["source"]), scope)
            with self.assertRaisesRegex(ValueError, "generator source"):
                real_exec("".join(notebook["cells"][1]["source"]), scope)
        self.assertFalse(observed["path"].exists())

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

    def test_verifier_definition_is_reusable_across_run_ids(self):
        first_data = {**self.options["data"], "run_id": "candidate_run"}
        next_data = {**first_data, "run_id": "fresh_run"}
        first = build_notebook(self.generator, **{**self.options, "data": first_data})
        second = build_notebook(self.generator, **{**self.options, "data": next_data})
        self.assertEqual(first, second)
        assignments = ast.parse("".join(first["cells"][0]["source"])).body
        data_json = next(
            ast.literal_eval(item.value) for item in assignments
            if isinstance(item, ast.Assign) and item.targets[0].id == "DATA_JSON"
        )
        self.assertIsNone(json.loads(data_json)["run_id"])
        self.assertEqual(first_data["run_id"], "candidate_run")
        runtime = "".join(first["cells"][1]["source"])
        self.assertIn("measured_batches(manifest)", runtime)

    def test_generator_binding_normalizes_only_line_endings(self):
        self.generator.write_bytes(b"first = 1\r\nsecond = 2\rthird = 3\n")
        runtime = "".join(build_notebook(self.generator, **self.options)["cells"][1]["source"])
        normalized = b"first = 1\nsecond = 2\nthird = 3\n"
        self.assertIn(base64.b64encode(normalized).decode("ascii"), runtime)
        self.assertIn(f"GENERATOR_SHA256 = '{hashlib.sha256(normalized).hexdigest()}'", runtime)

    def test_reused_verifier_uses_each_current_manifest_not_prior_run(self):
        notebook = build_notebook(
            self.generator, **{**self.options, "data": {"run_id": "candidate_run", "date_count": 365}}
        )
        generator_digest = hashlib.sha256(self.generator.read_text(encoding="utf-8").encode("utf-8")).hexdigest()
        for run_id in ("candidate_run", "fresh_run"):
            batch_id = f"{run_id}_0001"
            batch = {
                "batch_id": batch_id, "batch_number": 1, "batch_class": "measured", "sha256": "a" * 64,
                "row_count": 1, "sales_amount_minor_units": 5,
                "amount_by_date": {"2026-01-01": 5},
                "amount_by_product_category": {"Beauty": 5},
                "amount_by_customer_region": {"Europe": 5},
            }
            results, verified = [], []
            scope = {
                "spark": types.SimpleNamespace(conf=types.SimpleNamespace(set=lambda *_: None)),
                "notebookutils": types.SimpleNamespace(
                    notebook=types.SimpleNamespace(exit=lambda value: results.append(json.loads(value)))
                ),
            }
            real_exec = builtins.exec

            def verify_keys(_spark, batches, *_args):
                verified.extend(batches)

            def packaged_exec(code, namespace, *args):
                real_exec(code, namespace, *args)
                if getattr(code, "co_filename", "") == "<packaged lakehouse_validation>":
                    namespace["verify_dimensions"] = (
                        lambda _spark, data, *_args: self.assertIsNone(data["run_id"])
                    )
                    namespace["verify_run_keys"] = verify_keys

            with self.subTest(run_id=run_id), mock.patch.dict(sys.modules), \
                    mock.patch.object(builtins, "exec", side_effect=packaged_exec):
                real_exec("".join(notebook["cells"][0]["source"]), scope)
                scope["MODE"] = "verify_run_keys"
                scope["MANIFEST_JSON"] = json.dumps({
                    "contract_version": 1, "run_id": run_id,
                    "generator_sha256": generator_digest,
                    "batches": {"1": batch, "bootstrap": {
                        "batch_id": "bootstrap_0001", "batch_class": "bootstrap",
                    }},
                })
                real_exec("".join(notebook["cells"][1]["source"]), scope)
                scope["MANIFEST_JSON"] = json.dumps({
                    "contract_version": 1, "run_id": run_id,
                    "generator_sha256": "b" * 64, "batches": {"1": batch},
                })
                with self.assertRaisesRegex(ValueError, "packaged generator"):
                    real_exec("".join(notebook["cells"][1]["source"]), scope)
            self.assertEqual([item["batch_id"] for item in verified], [batch_id])
            self.assertEqual(results, [{
                "mode": "verify_run_keys", "status": "verified", "batch_ids": [batch_id],
                "row_count": 1, "sales_amount_minor_units": 5,
            }])


if __name__ == "__main__":
    unittest.main()
