"""Opt-in, isolated Workspace and schema-enabled Lakehouse provisioning."""

import argparse
import base64
from http.client import HTTPException
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import time
from urllib.error import HTTPError, URLError
from urllib.request import HTTPRedirectHandler, Request, build_opener

from fresh_deploy import (
    begin, binding, confirm, record_intent, record_lro_result, record_response, verify_readback,
)
from ownership import (
    FABRIC_ORIGIN, STATE_FILE, CandidateBlocked, _resource, _state_lock, _unique_pairs, _uuid,
)


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


class FabricClient:
    def __init__(self, tenant, principal, timeout, azure_config):
        self.end = time.monotonic() + timeout
        self.requester = build_opener(_NoRedirect())
        az = shutil.which("az")
        if not az:
            raise CandidateBlocked("Azure CLI is required for synthetic provisioning.")
        try:
            process = subprocess.run(
                [az, "account", "get-access-token", "--resource", FABRIC_ORIGIN, "--tenant",
                 tenant, "--output", "json", "--only-show-errors"],
                env={**os.environ, "AZURE_CONFIG_DIR": str(azure_config)},
                capture_output=True, timeout=min(45, self.remaining()), check=False,
            )
        except (OSError, subprocess.TimeoutExpired):
            raise CandidateBlocked("Azure CLI could not acquire a Fabric token.") from None
        if process.returncode:
            raise CandidateBlocked("Azure CLI has no usable Fabric token for the configured tenant.")
        try:
            credential = json.loads(process.stdout, object_pairs_hook=_unique_pairs)
            token = credential["accessToken"]
            parts = token.split(".")
            if len(parts) != 3 or not all(re.fullmatch(r"[A-Za-z0-9_-]+", part) for part in parts):
                raise ValueError
            claims = json.loads(base64.urlsafe_b64decode(parts[1] + "=" * (-len(parts[1]) % 4)),
                                object_pairs_hook=_unique_pairs)
            if (credential["tenant"] != tenant or claims["tid"] != tenant
                    or claims["oid"] != principal or type(claims["exp"]) is not int
                    or claims["exp"] <= time.time() + self.remaining()):
                raise ValueError
        except (AttributeError, KeyError, TypeError, ValueError):
            raise CandidateBlocked(
                "Azure CLI Fabric token did not match the expected tenant/principal or deadline."
            ) from None
        self._token = token

    def remaining(self):
        seconds = self.end - time.monotonic()
        if seconds <= 0:
            raise CandidateBlocked("Synthetic provisioning deadline expired; no create was retried.")
        return seconds

    def wait(self, header):
        delay = int(header) if isinstance(header, str) and re.fullmatch(r"\d{1,6}", header, re.ASCII) else 2
        delay = max(1, delay)
        if delay >= self.remaining():
            raise CandidateBlocked("Fabric polling delay exceeds the provisioning deadline.")
        time.sleep(delay)

    def request(self, method, path, body=None):
        if not re.fullmatch(r"/v1/[A-Za-z0-9/-]+", path):
            raise ValueError("Invalid Fabric control-plane path")
        payload = json.dumps(body).encode("utf-8") if body is not None else None
        headers = {"Authorization": f"Bearer {self._token}", "Accept": "application/json",
                   "x-ms-fabric-skill": "spark-cli"}
        if payload is not None:
            headers["Content-Type"] = "application/json"
        request = Request(FABRIC_ORIGIN + path, data=payload, headers=headers, method=method)
        try:
            with self.requester.open(request, timeout=min(45, self.remaining())) as response:
                status = response.status
                headers = {key.lower(): value for key, value in response.headers.items()}
                if any(len(response.headers.get_all(key, [])) > 1
                       for key in ("Location", "x-ms-operation-id", "Retry-After")):
                    return status, {}, None, False
                try:
                    raw = response.read(1024 * 1024 + 1)
                    if len(raw) > 1024 * 1024:
                        return status, headers, None, False
                    parsed = json.loads(raw, object_pairs_hook=_unique_pairs) if raw else None
                    return status, headers, parsed, True
                except (ValueError, HTTPException, OSError):
                    return status, headers, None, False
        except HTTPError as error:
            with error:
                return error.code, {}, None, False
        except (URLError, HTTPException, OSError):
            raise CandidateBlocked("Fabric response was not obtained; a pending create cannot be retried.") from None


def _object_response(client, path):
    status, headers, body, readable = client.request("GET", path)
    if status != 200 or not readable or not isinstance(body, dict):
        raise CandidateBlocked(f"Fabric readback did not return an object (HTTP {status}).")
    return headers, body


def _request_for(journal, kind):
    expected = journal["expected"]
    marker = journal["owner"]["marker"]
    if kind == "Workspace":
        return "/v1/workspaces", {"displayName": expected["workspace_name"], "description": marker,
                                  "capacityId": expected["capacity_id"]}
    workspace_id = _uuid(journal["workspace"]["id"])
    return f"/v1/workspaces/{workspace_id}/lakehouses", {
        "displayName": expected["lakehouse_name"], "description": marker,
        "creationPayload": {"enableSchemas": True},
    }


def _retain_response(state_dir, journal, intent_id, response):
    status, headers, body, readable = response
    operation_id = headers.get("x-ms-operation-id")
    location = headers.get("location")
    valid_location = (isinstance(location, str)
                      and re.fullmatch(re.escape(FABRIC_ORIGIN) + r"/v1/[a-z0-9/-]+", location))
    retry_after = headers.get("retry-after")
    record_response(
        state_dir, journal, intent_id, status, body if status in (201, 202) else None,
        operation_id if isinstance(operation_id, str) and re.fullmatch(r"[0-9a-f-]{36}", operation_id) else None,
        location if valid_location else None,
        retry_after if isinstance(retry_after, str) and re.fullmatch(r"[0-9]{1,6}", retry_after) else None,
        body_read=readable and (location is None or bool(valid_location)),
    )


def _finish_pending(state_dir, journal, client, intent):
    kind = intent.get("kind")
    if kind not in ("Workspace", "Lakehouse") or kind.lower() in journal:
        raise CandidateBlocked("Pending create kind conflicts with the journal.")
    intent_id = _uuid(intent.get("id"))
    path, request = _request_for(journal, kind)
    if intent.get("request") != request or intent.get("path") != path:
        raise CandidateBlocked("Pending create request does not match the deployment binding.")
    response = intent.get("response")
    if not isinstance(response, dict):
        raise CandidateBlocked("Pending create has no retained HTTP response; manual reconciliation is required.")
    status = response.get("status")
    if response.get("body_read") is not True or (status != 201 and (kind != "Lakehouse" or status != 202)):
        raise CandidateBlocked(f"Pending {kind} create has unusable HTTP {status} evidence; no POST was retried.")
    operation_state = None
    if status == 201:
        result = response.get("body")
    else:
        operation_id = _uuid(response.get("operation_id"))
        operation_path = f"/v1/operations/{operation_id}"
        if response.get("location") != FABRIC_ORIGIN + operation_path or response.get("body") is not None:
            raise CandidateBlocked("Lakehouse LRO acceptance metadata does not match its operation ID.")
        retained = intent.get("lro_result")
        if retained is None:
            client.wait(response.get("retry_after"))
            while True:
                headers, state = _object_response(client, operation_path)
                if state.get("status") == "Succeeded":
                    break
                if state.get("status") not in ("Running", "NotStarted"):
                    raise CandidateBlocked("Lakehouse LRO did not return a recoverable or successful state.")
                client.wait(headers.get("retry-after"))
            _, result = _object_response(client, operation_path + "/result")
            operation_state = {**state, "operation_id": operation_id}
            record_lro_result(state_dir, journal, intent_id, result, operation_state)
        elif isinstance(retained, dict):
            result, operation_state = retained.get("body"), retained.get("operation_state")
        else:
            raise CandidateBlocked("Retained Lakehouse LRO result is invalid.")
    if not isinstance(result, dict):
        raise CandidateBlocked("Pending create response has no resource result.")
    resource_id = _uuid(result.get("id"))
    if status == 201 and response.get("location") not in (None, f"{FABRIC_ORIGIN}{path}/{resource_id}"):
        raise CandidateBlocked("Create result ID disagrees with its Location header.")
    _, live = _object_response(client, f"{path}/{resource_id}")
    confirm(state_dir, journal, intent_id, result, live, operation_state)


def _verify_confirmed_receipt(record, kind, path, resource_id):
    response = record.get("accepted_response")
    result = record.get("create_response")
    if not isinstance(response, dict) or response.get("body_read") is not True:
        raise CandidateBlocked(f"Confirmed {kind} has no readable create receipt.")
    status = response.get("status")
    if status == 201:
        if (response.get("body") != result
                or response.get("location") not in (None, f"{FABRIC_ORIGIN}{path}/{resource_id}")):
            raise CandidateBlocked(f"Confirmed {kind} create receipt is inconsistent.")
    elif status == 202 and kind == "Lakehouse":
        try:
            operation_id = _uuid(response.get("operation_id"))
        except ValueError as exc:
            raise CandidateBlocked("Confirmed Lakehouse operation ID is invalid.") from exc
        operation = record.get("operation_state")
        if (response.get("body") is not None
                or response.get("location") != f"{FABRIC_ORIGIN}/v1/operations/{operation_id}"
                or not isinstance(operation, dict)
                or operation.get("operation_id") != operation_id
                or operation.get("status") != "Succeeded"):
            raise CandidateBlocked("Confirmed Lakehouse operation receipt is inconsistent.")
    else:
        raise CandidateBlocked(f"Confirmed {kind} has no accepted create receipt.")
    if (not isinstance(result, dict) or result.get("id") != resource_id
            or result.get("type") != kind
            or result.get("displayName") != record["create_request"]["displayName"]):
        raise CandidateBlocked(f"Confirmed {kind} create result is inconsistent.")
    if kind == "Lakehouse" and result.get("workspaceId") != record.get("workspace_id"):
        raise CandidateBlocked("Confirmed Lakehouse create result has a mismatched parent.")


def _load(state_dir, config, prefix, principal):
    path = state_dir / STATE_FILE
    if path.is_symlink() or not path.is_file():
        raise CandidateBlocked("No regular synthetic provisioning journal is available for recovery.")
    with path.open(encoding="utf-8") as file:
        journal = json.load(file, object_pairs_hook=_unique_pairs)
    if not isinstance(journal, dict) or not isinstance(journal.get("owner"), dict):
        raise CandidateBlocked("Synthetic provisioning journal is malformed.")
    expected = binding(config, prefix, principal, _uuid(journal["owner"].get("id")))
    if (type(journal.get("contract_version")) is not int
            or any(journal.get(key) != value for key, value in expected.items())
            or journal.get("uncertain_operations") != []
            or not isinstance(journal.get("pending_operations"), list)
            or len(journal["pending_operations"]) > 1
            or any(not isinstance(intent, dict) for intent in journal["pending_operations"])):
        raise CandidateBlocked("Synthetic journal does not match the configuration and authenticated principal.")
    return journal


def provision(config, prefix, state_dir, principal, azure_config, timeout, *, resume=False):
    if (not isinstance(config, dict) or type(config.get("contract_version")) is not int
            or config["contract_version"] != 1):
        raise ValueError("Expected a v1 Demo 1 configuration.")
    tenant, principal = _uuid(config.get("tenant_id")), _uuid(principal)
    state_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    if state_dir.is_symlink() or getattr(state_dir, "is_junction", lambda: False)():
        raise CandidateBlocked("Synthetic provisioner requires a local, operator-protected state directory.")
    with _state_lock(state_dir):
        path = state_dir / STATE_FILE
        if not resume and (path.exists() or path.is_symlink()):
            raise CandidateBlocked("Existing journal will not be replaced; use --resume for exact-evidence recovery.")
        journal = _load(state_dir, config, prefix, principal) if resume else None
        client = FabricClient(tenant, principal, timeout, azure_config)
        if journal is None:
            journal = begin(config, prefix, state_dir, principal)
        pending = journal["pending_operations"]
        if pending and pending[0].get("kind") not in ("Workspace", "Lakehouse"):
            raise CandidateBlocked("Synthetic journal has an unrecognized pending create.")
        for kind in ("Workspace", "Lakehouse"):
            key = kind.lower()
            path, request = _request_for(journal, kind)
            if key in journal:
                if pending and pending[0].get("kind") == kind:
                    raise CandidateBlocked(f"Confirmed {kind} also has a pending create.")
                resource_id = _resource(journal[key], kind, journal["owner"]["id"],
                                        request["displayName"], request["description"])
                if journal[key].get("create_request") != request or journal[key].get("create_path") != path:
                    raise CandidateBlocked(f"Confirmed {kind} create intent does not match the deployment.")
                _verify_confirmed_receipt(journal[key], kind, path, resource_id)
                _, live = _object_response(client, f"{path}/{resource_id}")
                verify_readback(journal, kind, resource_id, live)
            else:
                if pending:
                    if pending[0].get("kind") != kind:
                        raise CandidateBlocked("Pending create is missing an earlier confirmed resource.")
                else:
                    client.remaining()
                    intent_id = record_intent(state_dir, journal, kind, request, path=path)
                    _retain_response(state_dir, journal, intent_id, client.request("POST", path, request))
                _finish_pending(state_dir, journal, client, journal["pending_operations"][0])
                pending = journal["pending_operations"]
    return {"workspace_id": journal["workspace"]["id"], "item_id": journal["lakehouse"]["id"]}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--prefix", required=True)
    parser.add_argument("--state-dir", required=True, type=Path)
    parser.add_argument("--principal-id", required=True, help="Expected tenant-local Entra object ID.")
    parser.add_argument("--timeout", type=int, default=300)
    parser.add_argument("--yes-create-synthetic", action="store_true", required=True)
    parser.add_argument("--resume", action="store_true")
    args = parser.parse_args(argv)
    azure_config = Path(os.environ.get("AZURE_CONFIG_DIR", ""))
    try:
        if not azure_config.is_absolute() or not azure_config.is_dir():
            raise ValueError("AZURE_CONFIG_DIR must select an existing isolated Azure CLI directory.")
        if not args.config.is_absolute() or not args.state_dir.is_absolute() or not 1 <= args.timeout <= 900:
            raise ValueError("Config and state paths must be absolute, and timeout must be 1–900 seconds.")
        with args.config.open(encoding="utf-8") as file:
            config = json.load(file, object_pairs_hook=_unique_pairs)
        resources = provision(config, args.prefix, args.state_dir, args.principal_id,
                              azure_config, args.timeout, resume=args.resume)
    except (CandidateBlocked, OSError, ValueError) as exc:
        print(f"Synthetic provisioning blocked: {exc}", file=sys.stderr)
        return 1
    print(json.dumps({"status": "pass", "ready": False, "synthetic_resources": resources}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
