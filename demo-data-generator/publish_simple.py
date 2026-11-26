"""Publish the reset-and-run producer definitions to the existing Fabric items."""

import json
import shutil
import subprocess
from pathlib import Path

import fabric_assets
from deploy import FabricClient, _header, _retry_after


ROOT = Path(__file__).resolve().parent.parent


def publish(client, settings):
    shared = settings["shared"]
    workspace = shared["workspace_id"]
    notebook = shared["producer_notebook_id"]
    pipeline = shared["wrapper_pipeline_id"]
    definitions = (
        ("notebooks", notebook, fabric_assets.notebook_definition(
            workspace, shared["staging_lakehouse_id"], shared["staging_lakehouse_name"],
            shared["producer_environment_id"], default_lakehouse=shared["notebook_default_lakehouse"])),
        ("dataPipelines", pipeline, fabric_assets.pipeline_definition(workspace, notebook)),
    )
    for kind, item_id, definition in definitions:
        path = f"/v1/workspaces/{workspace}/items/{item_id}/updateDefinition"
        status, headers, _ = client.request("POST", path, {"definition": definition})
        if status == 202:
            operation = _header(headers, "x-ms-operation-id")
            location = _header(headers, "Location")
            if not operation:
                raise RuntimeError(f"{kind} update was accepted without an operation ID; inspect Fabric before retrying")
            print(f"{kind} update accepted; operation {operation}", flush=True)
            client.poll(operation, _retry_after(headers), require_result=False, location=location)
        elif status != 200:
            raise RuntimeError(f"{kind} update returned unexpected HTTP {status}")
        print(f"{kind} definition updated.", flush=True)


def main():
    settings = json.loads((ROOT / "demo-settings.json").read_text(encoding="utf-8"))
    executable = shutil.which("az")
    if not executable:
        raise RuntimeError("Azure CLI is required to obtain the signed-in Fabric token")
    result = subprocess.run(
        [executable, "account", "get-access-token", "--tenant", settings["tenant_id"],
         "--resource", "https://api.fabric.microsoft.com", "--query", "accessToken", "-o", "tsv"],
        text=True, capture_output=True, check=True, timeout=120)
    token = result.stdout.strip()
    if not token:
        raise RuntimeError("Azure CLI returned no Fabric token")
    publish(FabricClient(token), settings)


if __name__ == "__main__":
    main()
