"""Package the Demo 2 verification notebook with its checked-in dependencies."""

import base64
import json
import uuid
from pathlib import Path

from eventhouse_core import identifier


_HERE = Path(__file__).resolve().parent


def _positive_integer(value, name):
    if type(value) is not int or value <= 0:
        raise ValueError(f"{name} must be a positive integer")
    return value


def _uuid(value, name):
    try:
        return str(uuid.UUID(value))
    except (ValueError, TypeError, AttributeError) as exc:
        raise ValueError(f"{name} must be a UUID") from exc


def _cell(source, *, parameters=False):
    return {
        "cell_type": "code",
        "execution_count": None,
        "metadata": {"tags": ["parameters"]} if parameters else {},
        "outputs": [],
        "source": source.splitlines(keepends=True),
    }


def build_notebook(
    generator_path,
    *,
    workspace_id,
    lakehouse_id,
    lakehouse_name,
    data,
    shortcut_name,
    schema=None,
    verification_timeout_seconds=900,
    poll_interval_seconds=15,
):
    """Build an ipynb; source modules are embedded without a runtime install."""
    workspace_id = _uuid(workspace_id, "workspace_id")
    lakehouse_id = _uuid(lakehouse_id, "lakehouse_id")
    lakehouse_name = identifier(lakehouse_name, "lakehouse")
    shortcut_name = identifier(shortcut_name, "shortcut")
    if schema:
        identifier(schema, "lakehouse schema")
    if not isinstance(data, dict):
        raise ValueError("data must be an object")
    data = {**data, "run_id": None}
    verification_timeout_seconds = _positive_integer(
        verification_timeout_seconds, "verification_timeout_seconds"
    )
    poll_interval_seconds = _positive_integer(poll_interval_seconds, "poll_interval_seconds")

    sources = {
        "eventhouse_core": (_HERE / "eventhouse_core.py").read_text(encoding="utf-8"),
        "lakehouse_validation": (_HERE / "lakehouse_validation.py").read_text(encoding="utf-8"),
        "chanel_generator": Path(generator_path).read_text(encoding="utf-8"),
    }
    encoded = {
        name: base64.b64encode(source.encode("utf-8")).decode("ascii")
        for name, source in sources.items()
    }
    parameters = "\n".join(
        (
            'MODE = "preload"',
            f"LAKEHOUSE_NAME = {lakehouse_name!r}",
            f"LAKEHOUSE_SCHEMA = {(schema or '')!r}",
            f"SHORTCUT_NAME = {shortcut_name!r}",
            f"DATA_JSON = {json.dumps(data, sort_keys=True)!r}",
            'BATCH_JSON = ""',
            'MANIFEST_JSON = ""',
            f"VERIFICATION_TIMEOUT_SECONDS = {verification_timeout_seconds}",
            f"POLL_INTERVAL_SECONDS = {poll_interval_seconds}",
            "",
        )
    )
    runtime = f"""\
import base64
import json
import sys
import types

for name, payload in {encoded!r}.items():
    module = types.ModuleType(name)
    module.__file__ = "<packaged " + name + ">"
    sys.modules[name] = module
    exec(compile(base64.b64decode(payload), module.__file__, "exec"), module.__dict__)

import chanel_generator as generator
from lakehouse_validation import (
    measured_batches, preload_dimensions, verify_batch_with_timeout,
    verify_dimensions, verify_run_keys,
)

spark.conf.set("spark.fabric.session.highConcurrency.enabled", "true")
if type(VERIFICATION_TIMEOUT_SECONDS) is not int or VERIFICATION_TIMEOUT_SECONDS <= 0:
    raise ValueError("VERIFICATION_TIMEOUT_SECONDS must be a positive integer")
if type(POLL_INTERVAL_SECONDS) is not int or POLL_INTERVAL_SECONDS <= 0:
    raise ValueError("POLL_INTERVAL_SECONDS must be a positive integer")
data = json.loads(DATA_JSON)
schema = LAKEHOUSE_SCHEMA or None

def verify_batch(batch):
    return verify_batch_with_timeout(
        spark, batch, LAKEHOUSE_NAME, SHORTCUT_NAME, schema,
        VERIFICATION_TIMEOUT_SECONDS, POLL_INTERVAL_SECONDS,
    )

if MODE == "preload":
    preload_dimensions(spark, data, generator, LAKEHOUSE_NAME, schema)
    result = {{"mode": MODE, "status": "verified"}}
elif MODE == "verify_batch":
    verify_dimensions(spark, data, generator, LAKEHOUSE_NAME, schema)
    result = {{"mode": MODE, "status": "verified", "batch": verify_batch(json.loads(BATCH_JSON))}}
elif MODE == "verify_run_keys":
    verify_dimensions(spark, data, generator, LAKEHOUSE_NAME, schema)
    batches = measured_batches(json.loads(MANIFEST_JSON))
    verify_run_keys(spark, batches, LAKEHOUSE_NAME, SHORTCUT_NAME, schema)
    result = {{
        "mode": MODE, "status": "verified",
        "batch_ids": sorted(batch["batch_id"] for batch in batches),
        "row_count": sum(batch["row_count"] for batch in batches),
        "sales_amount_minor_units": sum(batch["sales_amount_minor_units"] for batch in batches),
    }}
elif MODE == "verify_run":
    verify_dimensions(spark, data, generator, LAKEHOUSE_NAME, schema)
    batches = measured_batches(json.loads(MANIFEST_JSON))
    results = [verify_batch(batch) for batch in batches]
    verify_run_keys(spark, batches, LAKEHOUSE_NAME, SHORTCUT_NAME, schema)
    result = {{"mode": MODE, "status": "verified", "batches": results}}
else:
    raise ValueError("Unsupported Demo 2 notebook mode")
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


def fabric_definition(notebook):
    payload = json.dumps(notebook, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    return {
        "format": "ipynb",
        "parts": [
            {
                "path": "notebook-content.ipynb",
                "payload": base64.b64encode(payload).decode("ascii"),
                "payloadType": "InlineBase64",
            }
        ],
    }
