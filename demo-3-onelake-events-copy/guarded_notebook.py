"""Package guarded delivery helpers as a deployment-bound Fabric notebook."""

import json
import re
from dataclasses import asdict
from pathlib import Path

from deploy import _guid


def run_config_from_shared(generator, config):
    if (not isinstance(config, dict) or type(config.get("contract_version")) is not int
            or config["contract_version"] != 1 or not isinstance(config.get("data"), dict)):
        raise ValueError("v1 configuration must contain a data object")
    data = config["data"]
    fields = {
        "run_id": "run_id", "start_date": "start_date", "seed": "seed",
        "date_count": "date_count", "products": "product_count", "customers": "customer_count",
        "file_count": "file_count", "rows_per_file": "rows_per_file", "interval_seconds": "interval_seconds",
    }
    if any(key not in data for key in fields.values()):
        raise ValueError("shared producer run configuration is incomplete")
    try:
        run = generator.RunConfig(**{name: data[key] for name, key in fields.items()})
        run.validate()
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError("invalid shared producer run configuration") from exc
    return asdict(run)


def build_notebook(producer_dir, run_config, workspace_id, item_id, lakehouse_name,
                   producer_workspace_id, producer_item_id, *, native_policy=None):
    workspace_id, item_id = _guid(workspace_id), _guid(item_id)
    if not isinstance(lakehouse_name, str) or not re.fullmatch(r"[A-Za-z][A-Za-z0-9_]*", lakehouse_name):
        raise ValueError("invalid Lakehouse name")
    if not isinstance(run_config, dict):
        raise ValueError("expected an approved run configuration")
    approved = json.dumps(run_config, sort_keys=True, allow_nan=False)
    producer = Path(producer_dir)
    local = Path(__file__).parent
    files = {path.name: path.read_bytes() for path in (
        *(producer / name for name in ("generator.py", "fabric_storage.py")),
        *(local / name for name in ("audit.py", "batch_gate.py", "delivery_runtime.py", "guard_spark.py")),
    )}
    legacy_allocator = producer / "sales_ids.py"
    if legacy_allocator.is_file():
        files[legacy_allocator.name] = legacy_allocator.read_bytes()
    settings = {
        "workspace_id": workspace_id, "item_id": item_id, "lakehouse_name": lakehouse_name,
        "producer_workspace_id": _guid(producer_workspace_id),
        "producer_item_id": _guid(producer_item_id), "approved_config": approved,
    }
    entry_point = "guard_spark"
    if native_policy is not None:
        from native_guard import validate_policy

        if run_config:
            raise ValueError("native policy and fixed run configuration are mutually exclusive")
        settings["native_policy"] = json.loads(json.dumps(
            validate_policy(native_policy), sort_keys=True, allow_nan=False))
        files["native_guard.py"] = (local / "native_guard.py").read_bytes()
        entry_point = "native_guard"

    def cell(source, tags=()):
        return {"cell_type": "code", "execution_count": None,
                "metadata": {"tags": list(tags)} if tags else {},
                "outputs": [], "source": source.splitlines(keepends=True)}

    parameters = "".join(f"{key} = ''\n" for key in
                         ("phase", "job_id", "subject", "event_id", "origin", "prepared_json", "copy_json"))
    loader = f"""\
import sys as _guard_sys
from pathlib import Path as _GuardPath
from tempfile import TemporaryDirectory as _GuardTemp

_guard_modules = {files!r}
_guard_inputs = dict(phase=phase, job_id=job_id, subject=subject, event_id=event_id,
                     origin=origin, prepared_json=prepared_json, copy_json=copy_json)
with _GuardTemp(prefix="demo3-guard-") as _guard_dir:
    _guard_names = [name.removesuffix(".py") for name in _guard_modules]
    if any(name in _guard_sys.modules for name in _guard_names):
        raise RuntimeError("Demo 3 module names already loaded")
    for _guard_name, _guard_content in _guard_modules.items():
        (_GuardPath(_guard_dir) / _guard_name).write_bytes(_guard_content)
    _guard_sys.path.insert(0, _guard_dir)
    try:
        from {entry_point} import execute
        _guard_result = execute(spark, notebookutils, {settings!r}, _guard_inputs)
    finally:
        _guard_sys.path.remove(_guard_dir)
        for _guard_name in _guard_names:
            _guard_sys.modules.pop(_guard_name, None)
notebookutils.notebook.exit(_guard_result)
"""
    binding = {"default_lakehouse": item_id, "default_lakehouse_name": lakehouse_name,
               "default_lakehouse_workspace_id": workspace_id, "known_lakehouses": [{"id": item_id}]}
    return {"nbformat": 4, "nbformat_minor": 4, "metadata": {
        "kernelspec": {"name": "synapse_pyspark", "display_name": "Synapse PySpark"},
        "language_info": {"name": "python"},
        "trident": {"lakehouse": binding}, "dependencies": {"lakehouse": binding},
    }, "cells": [cell(parameters, ("parameters",)), cell(loader)]}


def build_native_notebook(producer_dir, policy, workspace_id, item_id, lakehouse_name,
                          producer_workspace_id, producer_item_id):
    return build_notebook(
        producer_dir, {}, workspace_id, item_id, lakehouse_name,
        producer_workspace_id, producer_item_id, native_policy=policy)
