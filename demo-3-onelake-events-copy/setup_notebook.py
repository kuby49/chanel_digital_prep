"""Package the shared generator and Demo 3 initialization as a Fabric notebook."""

import argparse
import ast
import json
import re
import uuid
from datetime import date, timedelta
from pathlib import Path


def _positive_integer(value, name):
    if type(value) is not int or value <= 0:
        raise ValueError(f"{name} must be a positive integer")
    return value


def build_notebook(generator_source, config, workspace_id, lakehouse_id, lakehouse_name):
    if not isinstance(generator_source, str) or not generator_source.strip():
        raise ValueError("the shared generator source is required")
    source_tree = ast.parse(generator_source)
    if not {"RunConfig", "dimensions"} <= {
        node.name for node in source_tree.body
        if isinstance(node, (ast.ClassDef, ast.FunctionDef))
    }:
        raise ValueError("shared generator must define RunConfig and dimensions")
    if (not isinstance(config, dict) or config.get("contract_version") != 1
            or not isinstance(config.get("data"), dict)):
        raise ValueError("v1 configuration must contain a data object")
    data = config["data"]
    if not isinstance(data.get("start_date"), str):
        raise ValueError("start_date must be an ISO date")
    start = date.fromisoformat(data["start_date"])
    date_count = _positive_integer(data.get("date_count"), "date_count")
    product_count = _positive_integer(data.get("product_count"), "product_count")
    customer_count = _positive_integer(data.get("customer_count"), "customer_count")
    if product_count > 2147483647 or customer_count > 2147483647:
        raise ValueError("dimension identifiers exceed INT")
    try:
        start + timedelta(days=date_count - 1)
    except OverflowError as exc:
        raise ValueError("date range is outside supported dates") from exc
    if type(data.get("seed")) is not int:
        raise ValueError("seed must be an integer")
    if not isinstance(lakehouse_name, str) or not re.fullmatch(r"[A-Za-z][A-Za-z0-9_]*", lakehouse_name):
        raise ValueError("lakehouse name must be a simple Spark identifier")
    workspace_id = str(uuid.UUID(workspace_id))
    lakehouse_id = str(uuid.UUID(lakehouse_id))
    parameters = {
        "setup_workspace_id": workspace_id,
        "setup_lakehouse_id": lakehouse_id,
        "setup_lakehouse_name": lakehouse_name,
        "setup_start_date": start.isoformat(),
        "setup_seed": data["seed"],
        "setup_date_count": date_count,
        "setup_product_count": product_count,
        "setup_customer_count": customer_count,
    }
    parameter_source = "".join(f"{key} = {value!r}\n" for key, value in parameters.items())
    validation_source = (Path(__file__).parent / "view_validation.py").read_text(encoding="utf-8")
    setup_source = (Path(__file__).parent / "setup_spark.py").read_text(encoding="utf-8")
    compile(validation_source, "view_validation.py", "exec")
    compile(setup_source, "setup_spark.py", "exec")

    def cell(cell_id, source, metadata=None):
        return {"cell_type": "code", "execution_count": None, "id": cell_id,
                "metadata": metadata or {}, "outputs": [],
                "source": source.splitlines(keepends=True)}

    generator_loader = f"""\
import importlib.util as _setup_importlib_util
from pathlib import Path as _setup_Path
import sys as _setup_sys
import tempfile as _setup_tempfile
import uuid as _setup_uuid

_setup_generator_dir = _setup_tempfile.TemporaryDirectory(prefix="chanel-demo3-generator-")
_setup_generator_path = _setup_Path(_setup_generator_dir.name) / "generator.py"
_setup_generator_path.write_bytes({generator_source.encode("utf-8")!r})
_setup_generator_name = "chanel_demo3_generator_" + _setup_uuid.uuid4().hex
_setup_generator_spec = _setup_importlib_util.spec_from_file_location(
    _setup_generator_name, _setup_generator_path)
if _setup_generator_spec is None or _setup_generator_spec.loader is None:
    raise RuntimeError("Unable to load the shared generator")
_setup_generator = _setup_importlib_util.module_from_spec(_setup_generator_spec)
_setup_sys.modules[_setup_generator_name] = _setup_generator
_setup_generator_spec.loader.exec_module(_setup_generator)
RunConfig = _setup_generator.RunConfig
dimensions = _setup_generator.dimensions
"""

    return {
        "nbformat": 4,
        "nbformat_minor": 5,
        "metadata": {
            "kernelspec": {"name": "synapse_pyspark", "display_name": "Synapse PySpark"},
            "language_info": {"name": "python"},
            "trident": {"lakehouse": {
                "default_lakehouse": lakehouse_id,
                "default_lakehouse_name": lakehouse_name,
                "default_lakehouse_workspace_id": workspace_id,
                "known_lakehouses": [{"id": lakehouse_id}],
            }},
        },
        "cells": [
            cell("parameters", parameter_source, {"tags": ["parameters"]}),
            cell("shared-generator", generator_loader),
            cell("view-validation", validation_source),
            cell("demo3-setup", setup_source),
        ],
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--generator", type=Path, required=True)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--workspace-id", required=True)
    parser.add_argument("--lakehouse-id", required=True)
    parser.add_argument("--lakehouse-name", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    notebook = build_notebook(
        args.generator.read_text(encoding="utf-8"),
        json.loads(args.config.read_text(encoding="utf-8")),
        args.workspace_id, args.lakehouse_id, args.lakehouse_name,
    )
    args.output.write_text(json.dumps(notebook, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
