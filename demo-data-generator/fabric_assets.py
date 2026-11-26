"""Build reproducible Fabric notebook and pipeline definition payloads."""

import argparse
import base64
import json
import uuid
from pathlib import Path

from fabric_runtime import configured_notebook_catalog
from job_lifecycle import activity_timeout


HERE = Path(__file__).parent
PRODUCER_MODULES = (
    "generator.py", "delivery.py", "runner.py", "fabric_storage.py",
    "fabric_ledger.py", "fabric_delivery.py", "fabric_runner.py", "fabric_runtime.py",
    "synthetic_fixture.py", "target_readback.py",
    "native_lifecycle.py",
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


def _bundle_cell(published_binding=None):
    files = {name: (HERE / name).read_bytes().replace(b"\r\n", b"\n").replace(b"\r", b"\n")
             for name in PRODUCER_MODULES}

    def add(path):
        content = path.read_bytes().replace(b"\r\n", b"\n").replace(b"\r", b"\n")
        if path.name in files and files[path.name] != content:
            raise ValueError("Native companion modules have colliding import names")
        files[path.name] = content

    eventhouse = HERE.parent / "demo-2-eventhouse"
    peer_files = [eventhouse / "ingestion.py", eventhouse / "eventhouse_core.py"]
    if any(path.exists() for path in peer_files):
        if not all(path.is_file() for path in peer_files):
            raise ValueError("Both Eventhouse adapter modules are required")
        for path in peer_files:
            add(path)
    native_companions = {
        "demo-1-materialized-lake-views": ("native_demo1.py",),
        "demo-2-eventhouse": ("native_demo2.py", "monitor_controller.py", "lakehouse_validation.py", "eventhouse_core.py"),
        "demo-3-onelake-events-copy": ("native_demo3.py", "native_stop.py", "native_guard.py", "audit.py",
                                       "batch_gate.py", "delivery_runtime.py", "guard_spark.py"),
    }
    for folder, names in native_companions.items():
        root = HERE.parent / folder
        if not (root / names[0]).is_file():
            continue
        for name in names:
            path = root / name
            if not path.is_file():
                raise ValueError(f"Native companion modules are incomplete for {folder}")
            add(path)
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
    if published_binding is not None:
        source.append(f"_producer_runtime_binding = {published_binding!r}\n")
    return {"cell_type": "code", "execution_count": None, "metadata": {}, "outputs": [], "source": source}


def notebook_definition(workspace_id, lakehouse_id, lakehouse_name, environment_id=None, *, default_lakehouse=None):
    workspace_id, lakehouse_id = _guid(workspace_id), _guid(lakehouse_id)
    if not isinstance(lakehouse_name, str) or not lakehouse_name:
        raise ValueError("Lakehouse name is required")
    catalog = (configured_notebook_catalog({"notebook_default_lakehouse": default_lakehouse})
               if default_lakehouse is not None else
               {"workspace_id": workspace_id, "lakehouse_id": lakehouse_id, "lakehouse_name": lakehouse_name})
    if catalog["lakehouse_id"] == lakehouse_id and catalog["workspace_id"] != workspace_id:
        raise ValueError("The staging Lakehouse cannot belong to a different workspace")
    known = [{"id": catalog["lakehouse_id"]}]
    if catalog["lakehouse_id"] != lakehouse_id:
        known.append({"id": lakehouse_id})
    notebook = json.loads((HERE / "notebook.ipynb").read_text(encoding="utf-8"))
    binding = {"default_lakehouse": catalog["lakehouse_id"], "default_lakehouse_name": catalog["lakehouse_name"],
               "default_lakehouse_workspace_id": catalog["workspace_id"], "known_lakehouses": known}
    notebook["metadata"].setdefault("trident", {})["lakehouse"] = binding
    notebook["metadata"].setdefault("dependencies", {})["lakehouse"] = binding
    if environment_id is not None:
        notebook["metadata"]["dependencies"]["environment"] = {
            "environmentId": _guid(environment_id), "workspaceId": workspace_id,
        }
    published_binding = {"workspace_id": workspace_id, "staging_lakehouse_id": lakehouse_id,
                         "default_lakehouse_workspace_id": catalog["workspace_id"],
                         "default_lakehouse_id": catalog["lakehouse_id"]}
    notebook["cells"].insert(1, _bundle_cell(published_binding))
    return _definition(notebook, "notebook-content.ipynb", "ipynb")


def pipeline_definition(workspace_id, notebook_id, *, native_profile=None, measured_duration_seconds=None):
    workspace_id, notebook_id = _guid(workspace_id), _guid(notebook_id)
    pipeline = json.loads((HERE / "pipeline-content.json").read_text(encoding="utf-8"))
    activity = pipeline["properties"]["activities"][0]
    activity["typeProperties"]["workspaceId"] = workspace_id
    activity["typeProperties"]["notebookId"] = notebook_id
    if native_profile is not None:
        if measured_duration_seconds is not None:
            raise ValueError("A native pipeline cannot have a measured timeout")
        duration = (native_profile["run_window_seconds"] if native_profile.get("version") == 2
                    else native_profile["document"]["data"]["max_duration_seconds"])
        timeout = activity_timeout(duration)
        if native_profile["activity_timeout"] != timeout:
            raise ValueError("Native activity timeout differs from the authorized window")
        params = pipeline["properties"]["parameters"]
        params["operation"]["defaultValue"] = "run-native"
        params["activity_timeout"]["defaultValue"] = timeout
        params["run_config_json"]["defaultValue"] = "{}"
        params["selected_demos_json"]["defaultValue"] = "[]"
        activity["policy"]["timeout"] = timeout
        activity["typeProperties"]["parameters"]["native_profile_json"] = {
            "type": "string", "value": json.dumps(native_profile, separators=(",", ":"), sort_keys=True),
        }
    elif measured_duration_seconds is not None:
        timeout = activity_timeout(measured_duration_seconds)
        pipeline["properties"]["parameters"]["activity_timeout"]["defaultValue"] = timeout
        activity["policy"]["timeout"] = timeout
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
