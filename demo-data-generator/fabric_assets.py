"""Build reproducible Fabric notebook and pipeline definition payloads."""

import argparse
import base64
import json
import uuid
from pathlib import Path


HERE = Path(__file__).parent
PRODUCER_MODULES = (
    "generator.py", "delivery.py", "runner.py", "fabric_storage.py",
    "fabric_ledger.py", "fabric_delivery.py", "fabric_runner.py", "fabric_runtime.py",
)


def _guid(value):
    try:
        return str(uuid.UUID(value))
    except (ValueError, AttributeError) as exc:
        raise ValueError("Fabric artifact IDs must be UUIDs") from exc


def _definition(content, part_path, format_name=None):
    definition = {
        "parts": [{
            "path": part_path,
            "payload": base64.b64encode(json.dumps(content, separators=(",", ":")).encode("utf-8")).decode("ascii"),
            "payloadType": "InlineBase64",
        }]
    }
    if format_name:
        definition["format"] = format_name
    return definition


def _bundle_cell():
    files = {name: (HERE / name).read_bytes() for name in PRODUCER_MODULES}
    eventhouse = HERE.parent / "demo-2-eventhouse"
    peer_files = [eventhouse / "ingestion.py", eventhouse / "eventhouse_core.py"]
    if any(path.exists() for path in peer_files):
        if not all(path.is_file() for path in peer_files):
            raise ValueError("Both Eventhouse adapter modules are required")
        files.update({path.name: path.read_bytes() for path in peer_files})
    encoded = {name: base64.b64encode(content).decode("ascii") for name, content in files.items()}
    source = [
        "import base64 as _bundle_base64\n",
        "import pathlib as _bundle_pathlib\n",
        "import sys as _bundle_sys\n",
        "import tempfile as _bundle_tempfile\n",
        f"_producer_sources = {encoded!r}\n",
        "_producer_dir = _bundle_pathlib.Path(_bundle_tempfile.mkdtemp(prefix='producer-modules-'))\n",
        "for _name, _content in _producer_sources.items():\n",
        "    (_producer_dir / _name).write_bytes(_bundle_base64.b64decode(_content))\n",
        "_bundle_sys.path.insert(0, str(_producer_dir))\n",
    ]
    return {"cell_type": "code", "execution_count": None, "metadata": {}, "outputs": [], "source": source}


def notebook_definition(workspace_id, lakehouse_id, lakehouse_name, environment_id=None):
    workspace_id, lakehouse_id = _guid(workspace_id), _guid(lakehouse_id)
    if not isinstance(lakehouse_name, str) or not lakehouse_name:
        raise ValueError("Lakehouse name is required")
    notebook = json.loads((HERE / "notebook.ipynb").read_text(encoding="utf-8"))
    binding = {"default_lakehouse": lakehouse_id, "default_lakehouse_name": lakehouse_name,
               "default_lakehouse_workspace_id": workspace_id, "known_lakehouses": [{"id": lakehouse_id}]}
    notebook["metadata"].setdefault("trident", {})["lakehouse"] = binding
    notebook["metadata"].setdefault("dependencies", {})["lakehouse"] = binding
    if environment_id is not None:
        notebook["metadata"]["dependencies"]["environment"] = {
            "environmentId": _guid(environment_id), "workspaceId": workspace_id,
        }
    notebook["cells"].insert(1, _bundle_cell())
    return _definition(notebook, "notebook-content.ipynb", "ipynb")


def pipeline_definition(workspace_id, notebook_id):
    workspace_id, notebook_id = _guid(workspace_id), _guid(notebook_id)
    pipeline = json.loads((HERE / "pipeline-content.json").read_text(encoding="utf-8"))
    activity = pipeline["properties"]["activities"][0]
    activity["typeProperties"]["workspaceId"] = workspace_id
    activity["typeProperties"]["notebookId"] = notebook_id
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
        definition = notebook_definition(args.workspace_id, args.lakehouse_id, args.lakehouse_name,
                                         args.environment_id)
    else:
        definition = pipeline_definition(args.workspace_id, args.notebook_id)
    print(json.dumps({"definition": definition}))


if __name__ == "__main__":
    main()
