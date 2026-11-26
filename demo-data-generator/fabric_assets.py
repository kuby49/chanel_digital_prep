"""Build the parameter-free Fabric notebook and pipeline definitions."""

import argparse
import base64
import json
import uuid
from pathlib import Path


HERE = Path(__file__).parent
DEMO_MODULES = {
    "demo-1-materialized-lake-views": "simple_demo1.py",
    "demo-2-eventhouse": "simple_demo2.py",
    "demo-3-onelake-events-copy": "simple_demo3.py",
}


def _guid(value):
    try:
        return str(uuid.UUID(value))
    except (ValueError, TypeError, AttributeError) as exc:
        raise ValueError("Fabric artifact IDs must be UUIDs") from exc


def _definition(content, part_path, format_name=None):
    definition = {"parts": [{
        "path": part_path,
        "payload": base64.b64encode(json.dumps(content, separators=(",", ":")).encode()).decode("ascii"),
        "payloadType": "InlineBase64",
    }]}
    if format_name:
        definition["format"] = format_name
    return definition


def _bundle_cell():
    files = {}
    for path in [HERE / "simple_data.py", HERE / "simple_run.py",
                 *(HERE.parent / folder / name for folder, name in DEMO_MODULES.items()),
                 HERE.parent / "demo-2-eventhouse" / "eventhouse_core.py"]:
        if not path.is_file():
            raise FileNotFoundError(f"Required demo helper is missing: {path}")
        files[path.name] = base64.b64encode(path.read_bytes()).decode("ascii")
    settings = json.loads((HERE.parent / "demo-settings.json").read_text(encoding="utf-8"))
    source = [
        "import base64 as _bundle_base64\n",
        "import pathlib as _bundle_pathlib\n",
        "import sys as _bundle_sys\n",
        "import tempfile as _bundle_tempfile\n",
        f"_producer_sources = {files!r}\n",
        "_producer_dir = _bundle_pathlib.Path(_bundle_tempfile.mkdtemp(prefix='demo-modules-'))\n",
        "for _name, _content in _producer_sources.items():\n",
        "    (_producer_dir / _name).write_bytes(_bundle_base64.b64decode(_content))\n",
        "_bundle_sys.path.insert(0, str(_producer_dir))\n",
        f"_demo_settings = {settings!r}\n",
    ]
    return {"cell_type": "code", "execution_count": None, "metadata": {}, "outputs": [], "source": source}


def notebook_definition(workspace_id, lakehouse_id, lakehouse_name, environment_id=None, *, default_lakehouse=None):
    workspace_id, lakehouse_id = _guid(workspace_id), _guid(lakehouse_id)
    if not isinstance(lakehouse_name, str) or not lakehouse_name:
        raise ValueError("Lakehouse name is required")
    catalog = default_lakehouse or {"workspace_id": workspace_id, "lakehouse_id": lakehouse_id,
                                    "lakehouse_name": lakehouse_name}
    binding = {
        "default_lakehouse": _guid(catalog["lakehouse_id"]),
        "default_lakehouse_name": catalog["lakehouse_name"],
        "default_lakehouse_workspace_id": _guid(catalog["workspace_id"]),
        "known_lakehouses": [{"id": _guid(catalog["lakehouse_id"])}],
    }
    if binding["default_lakehouse"] != lakehouse_id:
        binding["known_lakehouses"].append({"id": lakehouse_id})
    notebook = json.loads((HERE / "notebook.ipynb").read_text(encoding="utf-8"))
    notebook["metadata"].setdefault("trident", {})["lakehouse"] = binding
    notebook["metadata"].setdefault("dependencies", {})["lakehouse"] = binding
    if environment_id is not None:
        notebook["metadata"]["dependencies"]["environment"] = {
            "environmentId": _guid(environment_id), "workspaceId": workspace_id,
        }
    notebook["cells"].insert(0, _bundle_cell())
    return _definition(notebook, "notebook-content.ipynb", "ipynb")


def pipeline_definition(workspace_id, notebook_id, *, native_profile=None, measured_duration_seconds=None):
    if native_profile is not None or measured_duration_seconds is not None:
        raise ValueError("Legacy producer profiles are not supported by the reset-and-run pipeline")
    workspace_id, notebook_id = _guid(workspace_id), _guid(notebook_id)
    pipeline = json.loads((HERE / "pipeline-content.json").read_text(encoding="utf-8"))
    activity = pipeline["properties"]["activities"][0]
    activity["typeProperties"].update(workspaceId=workspace_id, notebookId=notebook_id)
    return _definition(pipeline, "pipeline-content.json")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("kind", choices=("notebook", "pipeline"))
    parser.add_argument("--workspace-id", required=True)
    parser.add_argument("--lakehouse-id")
    parser.add_argument("--lakehouse-name")
    parser.add_argument("--environment-id")
    parser.add_argument("--notebook-id")
    args = parser.parse_args()
    if args.kind == "notebook":
        settings = json.loads((HERE.parent / "demo-settings.json").read_text(encoding="utf-8"))
        definition = notebook_definition(args.workspace_id, args.lakehouse_id, args.lakehouse_name,
                                         args.environment_id,
                                         default_lakehouse=settings["shared"]["notebook_default_lakehouse"])
    else:
        definition = pipeline_definition(args.workspace_id, args.notebook_id)
    print(json.dumps({"definition": definition}))


if __name__ == "__main__":
    main()
