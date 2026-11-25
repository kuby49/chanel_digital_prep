"""Prepare a setup notebook only after fresh owned-shell readback."""

import hashlib
from pathlib import Path

import customer_preflight as original
import isolated_preflight
import ownership
import setup_notebook
from deploy import CandidateBlocked


def prepare(config, prefix, state_dir, api, producer_dir):
    """Read live ownership and build an unexecuted setup notebook in memory."""
    state = ownership.provision(config, prefix, Path(state_dir), api, create=False)
    workspace_id = state["workspace"]["id"]
    lakehouse_id = state["lakehouse"]["id"]
    if lakehouse_id == original.LAKEHOUSE or state["owner"]["marker"] == original.MARKER:
        raise CandidateBlocked("Replacement setup cannot bind the original target or owner marker.")
    isolated_preflight.require_f32(api, state["deployment_spec"]["capacity_id"])
    source = (Path(producer_dir) / "generator.py").read_bytes()
    notebook = setup_notebook.build_notebook(
        source.decode("utf-8"), config, workspace_id, lakehouse_id,
        state["deployment_spec"]["lakehouse_name"])
    guard = (
        "_setup_context = notebookutils.runtime.context\n"
        "if (_setup_context.get('currentWorkspaceId') != setup_workspace_id or\n"
        "        _setup_context.get('defaultLakehouseId') != setup_lakehouse_id):\n"
        "    raise RuntimeError('Replacement setup context differs from its owned target')\n"
    )
    notebook["cells"][-1]["source"] = guard.splitlines(keepends=True) + notebook["cells"][-1]["source"]
    return {
        "status": "unexecuted-setup-candidate",
        "ready": False,
        "workspace_id": workspace_id,
        "lakehouse_id": lakehouse_id,
        "owner_marker": state["owner"]["marker"],
        "generator_sha256": hashlib.sha256(source).hexdigest(),
        "notebook": notebook,
    }
