"""Package the Demo 2 monitor and its checked-in runtime dependencies."""

import base64
from pathlib import Path

from notebook_builder import _cell, _uuid
from eventhouse_core import identifier


_HERE = Path(__file__).resolve().parent


def build_monitor_notebook(*, workspace_id, lakehouse_id, lakehouse_name):
    workspace_id = _uuid(workspace_id, "workspace_id")
    lakehouse_id = _uuid(lakehouse_id, "lakehouse_id")
    lakehouse_name = identifier(lakehouse_name, "lakehouse")
    sources = {
        name: base64.b64encode((_HERE / f"{name}.py").read_bytes()).decode("ascii")
        for name in ("eventhouse_core", "lakehouse_validation", "monitor_controller")
    }
    parameters = """\
RUN_ID = ""
MANIFEST_URL = ""
SENTINEL_URL = ""
CONTROLLER_ID = ""
STATE_URL = ""
DEADLINE_UTC = ""
VERIFIER_WORKSPACE_ID = ""
VERIFIER_NOTEBOOK_ID = ""
EXPECTED_IDENTITY_ID = ""
EXPECTED_TENANT_ID = ""
EXPECTED_BATCH_COUNT = 0
VERIFICATION_TIMEOUT_SECONDS = 0
POLL_INTERVAL_SECONDS = 0
MAX_CONCURRENT_BATCHES = 0
"""
    runtime = f"""\
import base64
import json
import sys
import types

for name, payload in {sources!r}.items():
    module = types.ModuleType(name)
    module.__file__ = "<packaged " + name + ">"
    sys.modules[name] = module
    exec(compile(base64.b64decode(payload), module.__file__, "exec"), module.__dict__)

from monitor_controller import Controller, FabricClient, MonitorError, settings

context = notebookutils.runtime.context
if (context.get("currentWorkspaceId") or "").lower() != {workspace_id!r}:
    raise MonitorError("Monitor notebook is running in an unexpected workspace")
if (context.get("defaultLakehouseId") or "").lower() != {lakehouse_id!r}:
    raise MonitorError("Monitor notebook is not bound to the expected Lakehouse")
spark.conf.set("spark.fabric.session.highConcurrency.enabled", "true")
names = [
    "RUN_ID", "MANIFEST_URL", "SENTINEL_URL", "CONTROLLER_ID", "STATE_URL",
    "DEADLINE_UTC", "VERIFIER_WORKSPACE_ID", "VERIFIER_NOTEBOOK_ID",
    "EXPECTED_IDENTITY_ID", "EXPECTED_TENANT_ID", "EXPECTED_BATCH_COUNT",
    "VERIFICATION_TIMEOUT_SECONDS", "POLL_INTERVAL_SECONDS", "MAX_CONCURRENT_BATCHES",
]
cfg = settings({{name: globals()[name] for name in names}}, lakehouse_id={lakehouse_id!r})
if cfg["workspace_id"] != {workspace_id!r}:
    raise MonitorError("Verifier and monitor workspaces must match")
result = Controller(cfg, FabricClient(notebookutils, cfg)).run()
notebookutils.notebook.exit(json.dumps(result, sort_keys=True))
"""
    return {
        "nbformat": 4,
        "nbformat_minor": 5,
        "metadata": {
            "kernelspec": {
                "display_name": "Synapse PySpark",
                "language": "python",
                "name": "synapse_pyspark",
            },
            "language_info": {"name": "python"},
            "trident": {
                "lakehouse": {
                    "default_lakehouse": lakehouse_id,
                    "default_lakehouse_name": lakehouse_name,
                    "default_lakehouse_workspace_id": workspace_id,
                    "known_lakehouses": [{"id": lakehouse_id}],
                }
            },
        },
        "cells": [_cell(parameters, parameters=True), _cell(runtime)],
    }
