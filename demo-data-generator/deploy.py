"""Deploy and inspect the shared Fabric producer without starting a data job.

The v1 config accepts tenant_id, capacity_id and prefix either at the top level
or in ``common``. ``shared`` supplies the workspace name, identity and optional
resource IDs. The Environment can be specified with producer_environment_id,
producer_environment_name and producer_environment_description (or the
environment_id/name/description and environment_item_id/name/description
aliases). Conflicting aliases are rejected. Optional item
names default to producer_staging, producer_environment, producer_notebook and
producer_wrapper; names may contain ``{prefix}``.

New resources and Environment publishes are journaled in
state-dir/shared-producer-v1.json; an uncertain POST is never repeated. The
older deployment-mcap.json is read-only and requires exact recorded or
configured descriptions plus live ownership checks before reuse. Environment
libraries must match the pinned environment.yml in both staging and published
readback. Cleanup and data execution remain blocked until their safety and
readiness requirements can be demonstrated.
"""

import argparse
import base64
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import time
import uuid
from contextlib import contextmanager
from pathlib import Path
from urllib import error, parse, request

from generator import RunConfig, strict_json_loads
from target_readback import TargetReadbackBlocked, read_selected_targets, selected_candidates


HERE = Path(__file__).resolve().parent
ENVIRONMENT_YML = HERE / "environment.yml"
API_ROOT = "https://api.fabric.microsoft.com"
STATE_NAME = "shared-producer-v1.json"
LOCK_NAME = ".shared-producer-v1.lock"
CANDIDATE_NAME = "shared-producer-candidate-ids.json"
OWNER = "chanel_digital_prep/demo-data-generator"
MCAP_TENANT_ID = "4526cee1-4399-4e4b-9134-46fabd583527"
KINDS = ("workspace", "staging_lakehouse", "environment_item", "notebook", "pipeline")
ITEM_TYPES = {"staging_lakehouse": "Lakehouse", "environment_item": "Environment",
              "notebook": "Notebook", "pipeline": "DataPipeline"}
ID_FIELDS = {"workspace": "workspace_id", "staging_lakehouse": "staging_lakehouse_id",
             "environment_item": "producer_environment_id",
             "notebook": "producer_notebook_id", "pipeline": "wrapper_pipeline_id"}
NAME_FIELDS = {"workspace": "workspace_name", "staging_lakehouse": "staging_lakehouse_name",
               "environment_item": "producer_environment_name",
               "notebook": "producer_notebook_name", "pipeline": "wrapper_pipeline_name"}
DESCRIPTION_FIELDS = {"workspace": "workspace_description",
                      "staging_lakehouse": "staging_lakehouse_description",
                      "environment_item": "producer_environment_description",
                      "notebook": "producer_notebook_description",
                      "pipeline": "wrapper_pipeline_description"}
DEFAULT_NAMES = {"staging_lakehouse": "producer_staging", "environment_item": "producer_environment",
                 "notebook": "producer_notebook",
                 "pipeline": "producer_wrapper"}
ENVIRONMENT_PART = "Libraries/PublicLibraries/environment.yml"
REQUIRED_ENVIRONMENT_PACKAGES = {"azure-identity", "azure-kusto-data", "azure-kusto-ingest"}
ENVIRONMENT_ALIASES = {
    "producer_environment_id": ("environment_id", "environment_item_id"),
    "producer_environment_name": ("environment_name", "environment_item_name"),
    "producer_environment_description": ("environment_description", "environment_item_description"),
}
BLOCKERS = (
    "Blocked: no cross-workspace destination permissions or connections have been verified by this entrypoint.",
    "Blocked: this entrypoint has not verified producer staging, destination transport or file-event finalization.",
    "Blocked: this entrypoint cannot yet establish fresh readiness from runtime identity and pipeline-binding evidence.",
)


class ContractError(Exception):
    """A locally invalid invocation or configuration."""


class Blocked(Exception):
    """An operation cannot safely proceed with the available evidence."""


class FabricHttpError(Exception):
    def __init__(self, method, status, retry_after=None):
        self.status = status
        self.retry_after = retry_after
        super().__init__(f"Fabric {method} request failed (HTTP {status}).")


class FabricTransportError(Exception):
    pass


def _uuid(value, field, exception=ContractError):
    if not isinstance(value, str):
        raise exception(f"{field} must be a UUID.")
    try:
        parsed = uuid.UUID(value)
    except (ValueError, AttributeError):
        raise exception(f"{field} must be a UUID.") from None
    if parsed.int == 0:
        raise exception(f"{field} must be a nonzero UUID.")
    return str(parsed)


def _same_uuid(actual, expected):
    try:
        return _uuid(actual, "ID") == expected
    except ContractError:
        return False


def _one_id(config, common, field):
    values = []
    for source in (config, common):
        for key in (field + "_id", field):
            value = source.get(key)
            if isinstance(value, dict):
                value = value.get("id")
            if value is not None:
                values.append(_uuid(value, field + "_id"))
    if len(set(values)) > 1:
        raise ContractError(f"Conflicting {field}_id values in config.")
    return values[0] if values else None


def _name(value, prefix, field):
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip() or len(value) > 256:
        raise ContractError(f"{field} must be a nonempty name of at most 256 characters.")
    rendered = value.replace("{prefix}", prefix)
    if ("{" in rendered or "}" in rendered or len(rendered) > 256
            or any(ord(char) < 32 or ord(char) == 127 for char in rendered)):
        raise ContractError(f"{field} contains an unsupported placeholder or character.")
    return rendered


def _shared_value(shared, field):
    values = [shared[key] for key in (field, *ENVIRONMENT_ALIASES.get(field, ())) if shared.get(key) is not None]
    if not values:
        return None
    if field.endswith("_id"):
        values = [_uuid(value, field) for value in values]
    if any(value != values[0] for value in values[1:]):
        raise ContractError(f"Conflicting {field} values in shared config.")
    return values[0]


def parse_config(config, prefix):
    if not isinstance(config, dict) or type(config.get("contract_version")) is not int or config["contract_version"] != 1:
        raise ContractError("config contract_version must be 1.")
    if (not isinstance(prefix, str) or not prefix.strip() or len(prefix) > 128
            or prefix != prefix.strip() or any(ord(char) < 32 or ord(char) == 127 for char in prefix)):
        raise ContractError("prefix must be a nonempty printable value.")
    common = config.get("common", {})
    shared = config.get("shared", {})
    if not isinstance(common, dict) or not isinstance(shared, dict):
        raise ContractError("common and shared must be configuration objects.")
    for configured in (config.get("prefix"), common.get("prefix")):
        if configured is not None and configured != prefix:
            raise ContractError("config prefix does not match --prefix.")

    names = {}
    for kind, field in NAME_FIELDS.items():
        value = _shared_value(shared, field)
        names[kind] = _name(value if value is not None else DEFAULT_NAMES.get(kind), prefix, field)
    if not re.fullmatch(r"[A-Za-z][A-Za-z0-9_]{0,122}", names["staging_lakehouse"]):
        raise ContractError("staging_lakehouse_name must start with a letter and use only letters, digits or underscores.")
    item_names = [names[kind].casefold() for kind in ITEM_TYPES]
    if len(item_names) != len(set(item_names)):
        raise ContractError("producer item names must be distinct.")
    ids = {}
    for kind, field in ID_FIELDS.items():
        value = _shared_value(shared, field)
        ids[kind] = _uuid(value, field) if value is not None else None
    if len([value for value in ids.values() if value]) != len(set(value for value in ids.values() if value)):
        raise ContractError("workspace and item IDs must be distinct.")
    descriptions = {}
    for kind, field in DESCRIPTION_FIELDS.items():
        value = _shared_value(shared, field)
        if value is not None and not isinstance(value, str):
            raise ContractError(f"{field} must be a string.")
        descriptions[kind] = value
    identity = shared.get("identity_id")
    identity_type = shared.get("identity_type")
    if identity_type is not None and identity_type not in ("User", "ServicePrincipal"):
        raise ContractError("identity_type must be User or ServicePrincipal.")
    return {
        "tenant_id": _one_id(config, common, "tenant"),
        "capacity_id": _one_id(config, common, "capacity"),
        "identity_id": _uuid(identity, "identity_id") if identity is not None else None,
        "identity_type": identity_type,
        "prefix": prefix,
        "names": names,
        "ids": ids,
        "descriptions": descriptions,
    }


def _environment_spec():
    try:
        content = ENVIRONMENT_YML.read_bytes()
        if len(content) > 1024 * 1024:
            raise ValueError
        lines = [line.rstrip() for line in content.decode("utf-8").splitlines()
                 if line.strip() and not line.lstrip().startswith("#")]
        if lines and re.fullmatch(r"name: [A-Za-z0-9][A-Za-z0-9_.-]*", lines[0]):
            lines = lines[1:]
        if len(lines) < 3 or lines[:2] != ["dependencies:", "  - pip:"]:
            raise ValueError
        pins = {}
        for line in lines[2:]:
            match = re.fullmatch(r"      - ([A-Za-z0-9][A-Za-z0-9_.-]*)==([0-9][A-Za-z0-9.+_-]*)", line)
            if not match:
                raise ValueError
            name = re.sub(r"[-_.]+", "-", match.group(1)).lower()
            if name in pins:
                raise ValueError
            pins[name] = match.group(2)
        if not REQUIRED_ENVIRONMENT_PACKAGES.issubset(pins):
            raise ValueError
    except (OSError, UnicodeError, ValueError):
        raise Blocked("environment.yml must contain only explicitly pinned pip dependencies, including the required Azure SDKs.") from None
    return {"content": content, "pins": pins, "sha256": hashlib.sha256(content).hexdigest()}


def _environment_definition(spec):
    return {"parts": [{"path": ENVIRONMENT_PART, "payloadType": "InlineBase64",
                       "payload": base64.b64encode(spec["content"]).decode("ascii")}]}


def _selected_demos(selected_demos):
    selected = selected_demos.split(",") if selected_demos else []
    if (not selected or len(selected) != len(set(selected))
            or any(name not in ("demo-1", "demo-2", "demo-3") for name in selected)):
        raise ContractError("Distinct --selected-demos are required (demo-1,demo-2,demo-3).")
    return selected


def _validate_run(config, selected_demos):
    selected = _selected_demos(selected_demos)
    data = config.get("data")
    required = ("run_id", "seed", "start_date", "date_count", "product_count", "customer_count",
                "rows_per_file", "file_count", "interval_seconds", "max_duration_seconds", "currency")
    if not isinstance(data, dict) or any(key not in data or data[key] is None for key in required):
        raise ContractError("run-data requires all data parameters.")
    if not isinstance(data["run_id"], str) or not isinstance(data["start_date"], str):
        raise ContractError("run_id and start_date must be strings.")
    run = RunConfig(run_id=data["run_id"], start_date=data["start_date"], seed=data["seed"],
                    date_count=data["date_count"], products=data["product_count"],
                    customers=data["customer_count"], file_count=data["file_count"],
                    rows_per_file=data["rows_per_file"], interval_seconds=data["interval_seconds"])
    try:
        run.validate()
    except (ValueError, TypeError, OverflowError):
        raise ContractError("Invalid data parameters; check the run ID, date and numeric limits.") from None
    duration = data["max_duration_seconds"]
    if (not isinstance(duration, int) or isinstance(duration, bool)
            or duration <= (run.file_count - 1) * run.interval_seconds):
        raise ContractError("max_duration_seconds must exceed the final scheduled batch offset.")
    if data["currency"] != "USD":
        raise ContractError("producer currently supports USD only.")
    return selected


def _atomic_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    sibling = path.with_name(f".{path.name}.{uuid.uuid4().hex}.write")
    fd = None
    try:
        fd = os.open(str(sibling), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as file:
            fd = None
            json.dump(value, file, indent=2, sort_keys=True)
            file.write("\n")
            file.flush()
            os.fsync(file.fileno())
        os.replace(sibling, path)
        if os.name != "nt":
            directory_fd = os.open(str(path.parent), os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
    finally:
        if fd is not None:
            os.close(fd)
        try:
            sibling.unlink(missing_ok=True)
        except OSError:
            pass


def _atomic_new_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    sibling = path.with_name(f".{path.name}.{uuid.uuid4().hex}.write")
    fd = None
    try:
        fd = os.open(str(sibling), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as file:
            fd = None
            json.dump(value, file, indent=2, sort_keys=True)
            file.write("\n")
            file.flush()
            os.fsync(file.fileno())
        os.link(sibling, path)
        if os.name != "nt":
            directory_fd = os.open(str(path.parent), os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
    finally:
        if fd is not None:
            os.close(fd)
        sibling.unlink(missing_ok=True)


class StateStore:
    def __init__(self, directory):
        self.directory = Path(directory)
        self.path = self.directory / STATE_NAME
        self.lock_path = self.directory / LOCK_NAME

    def read(self):
        if not self.path.exists():
            return None
        try:
            state = strict_json_loads(self.path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, ValueError):
            raise Blocked("Owned deployment state is unreadable; do not create or adopt resources.") from None
        if not isinstance(state, dict):
            raise Blocked("Owned deployment state is invalid; do not create or adopt resources.")
        return state

    def write(self, state):
        try:
            _atomic_json(self.path, state)
        except OSError:
            raise Blocked("Cannot persist deployment state; reconcile before retrying.") from None

    @contextmanager
    def lock(self):
        try:
            self.directory.mkdir(parents=True, exist_ok=True)
            nonce = uuid.uuid4().hex
            fd = os.open(str(self.lock_path), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        except FileExistsError:
            raise Blocked("Deployment state is locked; inspect any interrupted deployment before retrying.") from None
        except OSError:
            raise Blocked("Cannot acquire the deployment state lock.") from None
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as file:
                file.write(nonce)
                file.flush()
                os.fsync(file.fileno())
            yield
        finally:
            try:
                if self.lock_path.read_text(encoding="utf-8") == nonce:
                    self.lock_path.unlink()
            except OSError:
                pass


def _description(deployment_id, kind):
    return f"{OWNER}; contract_version=1; deployment={deployment_id}; resource={kind}"


def new_state(plan):
    return {"contract_version": 1, "track": "shared", "owner": OWNER,
            "tenant_id": plan["tenant_id"], "capacity_id": plan["capacity_id"],
            "identity_id": plan["identity_id"], "identity_type": plan["identity_type"],
            "prefix": plan["prefix"], "deployment_id": str(uuid.uuid4()),
            "resources": {}, "pending": None}


def validate_state(state, plan):
    if (not isinstance(state, dict) or type(state.get("contract_version")) is not int
            or state["contract_version"] != 1 or state.get("track") != "shared"
            or state.get("owner") != OWNER):
        raise Blocked("Deployment state is not a recognized owned producer manifest.")
    for field in ("tenant_id", "capacity_id", "identity_id", "prefix"):
        if state.get(field) != plan[field]:
            raise Blocked(f"Deployment state {field} does not match the configuration.")
    state_type = state.get("identity_type")
    if state_type not in (None, "User", "ServicePrincipal") or (
            plan["identity_type"] is not None and state_type != plan["identity_type"]):
        raise Blocked("Deployment state identity type does not match the configuration.")
    deployment_id = _uuid(state.get("deployment_id"), "deployment ID", Blocked)
    if state["deployment_id"] != deployment_id:
        raise Blocked("Deployment state has a noncanonical deployment ID.")
    for kind, configured_description in plan["descriptions"].items():
        if configured_description is not None and configured_description != _description(deployment_id, kind):
            raise Blocked("A configured description differs from owned deployment state.")
    resources = state.get("resources")
    pending = state.get("pending")
    if not isinstance(resources, dict) or any(kind not in KINDS for kind in resources):
        raise Blocked("Deployment state has invalid resource records.")
    if pending is not None and (not isinstance(pending, dict) or pending.get("kind") not in KINDS):
        raise Blocked("Deployment state has an invalid pending operation.")
    workspace = resources.get("workspace")
    resource_ids = set()
    for kind, record in resources.items():
        if not isinstance(record, dict):
            raise Blocked("Deployment state has an invalid resource record.")
        resource_id = _uuid(record.get("id"), "resource ID", Blocked)
        if resource_id in resource_ids:
            raise Blocked("Deployment state has duplicate resource IDs.")
        resource_ids.add(resource_id)
        expected_type = "Workspace" if kind == "workspace" else ITEM_TYPES[kind]
        if (record.get("id") != resource_id or record.get("name") != plan["names"][kind]
                or record.get("description") != _description(deployment_id, kind)
                or record.get("type") != expected_type):
            raise Blocked("Deployment state resource name, description or type does not match.")
        if kind != "workspace" and (not isinstance(workspace, dict)
                                      or record.get("workspace_id") != workspace.get("id")):
            raise Blocked("Deployment state item parent does not match its workspace.")
        if kind == "environment_item":
            if record.get("published_version") is not None:
                _uuid(record["published_version"], "published Environment version", Blocked)
            if record.get("staging_compute_sha256") is not None and (
                    not isinstance(record["staging_compute_sha256"], str)
                    or not re.fullmatch(r"[0-9a-f]{64}", record["staging_compute_sha256"])):
                raise Blocked("Deployment state has an invalid Environment compute fingerprint.")
    if pending:
        kind = pending["kind"]
        action = pending.get("action", "create")
        if action == "create":
            if kind in resources or (kind != "workspace" and not isinstance(workspace, dict)):
                raise Blocked("Deployment state pending creation has an invalid parent or resource.")
            if type(pending.get("operation_succeeded", False)) is not bool:
                raise Blocked("Deployment state has an invalid completed creation operation.")
            if pending.get("operation_succeeded") and not pending.get("operation_id"):
                raise Blocked("Deployment state has an invalid completed creation operation.")
            if pending.get("result_id") and _uuid(pending["result_id"], "pending result ID", Blocked) in resource_ids:
                raise Blocked("Deployment state pending creation duplicates an existing resource ID.")
            known_result = pending.get("result_id")
        elif action == "publish":
            if kind != "environment_item" or kind not in resources or pending.get("result_id") is not None:
                raise Blocked("Deployment state pending publish has an invalid Environment.")
            if (not isinstance(pending.get("spec_sha256"), str)
                    or not re.fullmatch(r"[0-9a-f]{64}", pending["spec_sha256"])
                    or type(pending.get("operation_succeeded", False)) is not bool):
                raise Blocked("Deployment state has an invalid Environment publish journal.")
            known_result = pending.get("target_version")
            if pending.get("operation_succeeded") and not pending.get("operation_id"):
                raise Blocked("Deployment state has an invalid completed publish operation.")
        else:
            raise Blocked("Deployment state has an unknown pending operation.")
        for field in ("operation_id", "result_id", "target_version"):
            if pending.get(field) is not None:
                _uuid(pending[field], "pending " + field, Blocked)
        retry_after = pending.get("retry_after", 1)
        if type(retry_after) is not int or not 1 <= retry_after <= 999999999:
            raise Blocked("Deployment state has an invalid operation retry delay.")
        if not pending.get("operation_id") and not known_result:
            raise Blocked("A prior POST has an unknown outcome; reconcile it manually before retrying.")
    for kind, configured_id in plan["ids"].items():
        if configured_id:
            recorded_id = resources.get(kind, {}).get("id")
            if pending and pending["kind"] == kind and pending.get("action", "create") == "create":
                recorded_id = pending.get("result_id")
            if not recorded_id or not _same_uuid(recorded_id, configured_id):
                raise Blocked("A configured resource ID is absent from or differs from owned deployment state.")
    return state


def _legacy_manifest(plan, manifest):
    if not isinstance(manifest, dict) or type(manifest.get("contract_version")) is not int or manifest["contract_version"] != 1:
        raise Blocked("The existing producer manifest is invalid; do not adopt resources.")
    if manifest.get("prefix") != plan["prefix"]:
        return None
    tenant = manifest.get("tenant_id")
    if tenant is None and manifest.get("environment") == "MCAP":
        tenant = MCAP_TENANT_ID
    if not _same_uuid(tenant, plan["tenant_id"]):
        raise Blocked("The existing producer manifest tenant does not match the configuration.")
    identity = manifest.get("producer_identity")
    capacity = manifest.get("capacity")
    if (not isinstance(identity, dict) or not _same_uuid(identity.get("object_id"), plan["identity_id"])
            or identity.get("type") not in ("User", "ServicePrincipal")
            or (plan["identity_type"] is not None and identity["type"] != plan["identity_type"])
            or not isinstance(capacity, dict) or not _same_uuid(capacity.get("id"), plan["capacity_id"])):
        raise Blocked("The existing producer manifest identity or capacity does not match.")
    records = {}
    for kind in KINDS:
        source = manifest.get(kind)
        if not isinstance(source, dict) or not isinstance(source.get("ownership"), str) or not source["ownership"].strip():
            raise Blocked("The existing producer manifest lacks explicit resource ownership.")
        resource_id = _uuid(source.get("id"), "manifest resource ID", Blocked)
        if source.get("name") != plan["names"][kind]:
            raise Blocked("The existing producer manifest resource name does not match the configuration.")
        if plan["ids"][kind] is not None and resource_id != plan["ids"][kind]:
            raise Blocked("A configured resource ID differs from the existing producer manifest.")
        if kind != "workspace" and not _same_uuid(source.get("workspace_id"), records["workspace"]["id"]):
            raise Blocked("The existing producer manifest item parent does not match its workspace.")
        recorded_description = source.get("description")
        configured_description = plan["descriptions"][kind]
        if recorded_description is not None and not isinstance(recorded_description, str):
            raise Blocked("The existing producer manifest has an invalid description.")
        if (recorded_description is not None and configured_description is not None
                and recorded_description != configured_description):
            raise Blocked("The configured resource description differs from the existing manifest.")
        records[kind] = {"id": resource_id, "name": source["name"],
                         "description": recorded_description if recorded_description is not None else configured_description,
                         "type": "Workspace" if kind == "workspace" else ITEM_TYPES[kind]}
        if kind != "workspace":
            records[kind]["workspace_id"] = records["workspace"]["id"]
        if kind == "environment_item" and source.get("published_version") is not None:
            records[kind]["published_version"] = _uuid(source["published_version"], "manifest published version", Blocked)
    if len({record["id"] for record in records.values()}) != len(records):
        raise Blocked("The existing producer manifest has duplicate resource IDs.")
    return {"legacy": True, "identity_type": identity["type"], "resources": records, "pending": None}


def load_owned_state(store, plan):
    state = store.read()
    if state is not None:
        return validate_state(state, plan)
    legacy_path = HERE / "deployment-mcap.json"
    if legacy_path.exists():
        try:
            manifest = json.loads(legacy_path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, ValueError):
            raise Blocked("The existing producer manifest is unreadable; do not adopt resources.") from None
        state = _legacy_manifest(plan, manifest)
        if state is not None:
            return state
    if any(plan["ids"].values()):
        raise Blocked("Configured resource IDs require matching owned deployment state or manifest.")
    return None


def resolve_candidate_ids(config, prefix, state_dir, *, candidate_path=None):
    """Return journal IDs as candidates, not as evidence of live readiness."""
    plan = parse_config(config, prefix)
    if any(not plan[field] for field in ("tenant_id", "capacity_id", "identity_id")) or not plan["names"]["workspace"]:
        raise ContractError("Candidate IDs require tenant, capacity, identity and workspace name.")
    if not Path(state_dir).is_absolute():
        raise ContractError("state-dir must be an absolute path.")
    store = StateStore(state_dir)
    with store.lock():
        state = store.read()
        if state is None:
            raise Blocked("No owned producer deployment journal exists.")
        state = validate_state(state, plan)
        if state.get("pending") is not None:
            raise Blocked("Producer deployment has a pending operation; reconcile before resolving IDs.")
        resources = state["resources"]
        if "workspace" not in resources or "staging_lakehouse" not in resources:
            raise Blocked("Producer deployment journal does not contain both candidate IDs.")
        result = {"workspace_id": resources["workspace"]["id"],
                  "staging_lakehouse_id": resources["staging_lakehouse"]["id"]}
        if candidate_path is not None:
            candidate_path = Path(candidate_path)
            if not candidate_path.is_absolute():
                raise ContractError("Candidate path must be absolute.")
            _atomic_new_json(candidate_path, {"version": 1, **result})
        return result


def _token_claims(token):
    try:
        parts = token.split(".")
        if len(parts) != 3:
            raise ValueError
        payload = parts[1] + "=" * (-len(parts[1]) % 4)
        claims = json.loads(base64.urlsafe_b64decode(payload).decode("utf-8"))
        if not isinstance(claims, dict):
            raise ValueError
        return claims
    except (ValueError, UnicodeError, TypeError):
        raise Blocked("Azure CLI did not return a token with verifiable tenant and identity claims.") from None


def azure_cli_token(tenant_id, identity_id):
    command = [shutil.which("az") or "az", "account", "get-access-token", "--resource", API_ROOT,
               "--tenant", tenant_id, "--output", "json", "--only-show-errors"]
    try:
        completed = subprocess.run(command, capture_output=True, text=True, encoding="utf-8",
                                   timeout=30, check=False)
    except FileNotFoundError:
        raise Blocked("Azure CLI is unavailable; sign in using the intended isolated configuration.") from None
    except (OSError, UnicodeError, subprocess.SubprocessError):
        raise Blocked("Azure CLI token acquisition failed or timed out.") from None
    if completed.returncode != 0:
        raise Blocked("Azure CLI token acquisition failed; verify the intended tenant login.") from None
    try:
        response = json.loads(completed.stdout)
        token = response["accessToken"]
        if not isinstance(token, str) or not token or any(ord(char) < 33 or ord(char) > 126 for char in token):
            raise ValueError
    except (KeyError, TypeError, ValueError):
        raise Blocked("Azure CLI returned an invalid Fabric token response.") from None
    # This is a routing guard; Fabric authenticates the token, and job identity needs runtime evidence.
    claims = _token_claims(token)
    if not _same_uuid(claims.get("tid"), tenant_id) or not _same_uuid(claims.get("oid"), identity_id):
        raise Blocked("Azure CLI token tenant or principal does not match the configured identity.")
    return token


class _NoRedirect(request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def _header(headers, name):
    for key, value in (headers or {}).items():
        if key.lower() == name.lower():
            return value
    return None


def _retry_after(headers, default=1):
    value = _header(headers, "Retry-After")
    if value is None:
        return default
    if not isinstance(value, str) or not re.fullmatch(r"[0-9]{1,9}", value.strip()):
        raise Blocked("Fabric returned an invalid Retry-After value.")
    return max(1, int(value.strip()))


class FabricClient:
    def __init__(self, token, opener=None, clock=None, sleep=None, *, skill="spark-cli"):
        if skill not in ("spark-cli", "eventhouse-cli"):
            raise ContractError("Unsupported Fabric telemetry skill.")
        self._token = token
        self._skill = skill
        self._opener = opener or request.build_opener(_NoRedirect())
        self._clock = clock or time.monotonic
        self._sleep = sleep or time.sleep

    @staticmethod
    def _url(path):
        if not isinstance(path, str) or any(ord(char) < 32 or ord(char) == 127 for char in path) or "\\" in path:
            raise Blocked("Fabric returned an unsafe API URL.")
        url = API_ROOT + path if path.startswith("/v1/") else path
        parts = parse.urlsplit(url)
        if (parts.scheme != "https" or parts.netloc != "api.fabric.microsoft.com"
                or not parts.path.startswith("/v1/") or parts.fragment):
            raise Blocked("Fabric returned an unsafe API URL.")
        return url

    def request(self, method, path, payload=None, timeout=30):
        if method not in ("GET", "POST"):
            raise Blocked("Unsupported Fabric request method.")
        url = self._url(path)
        data = None if method == "GET" else (
            b"" if payload is None else json.dumps(payload, separators=(",", ":")).encode("utf-8"))
        headers = {"Accept": "application/json", "Authorization": "Bearer " + self._token,
                   "x-ms-fabric-skill": self._skill}
        if payload is not None:
            headers["Content-Type"] = "application/json"
        req = request.Request(url, data=data, headers=headers, method=method)
        try:
            with self._opener.open(req, timeout=timeout) as response:
                status = response.status
                response_headers = response.headers
                body = response.read(8 * 1024 * 1024 + 1)
        except error.HTTPError as exc:
            status, retry_after = int(exc.code), _header(exc.headers, "Retry-After")
            exc.close()
            raise FabricHttpError(method, status, retry_after) from None
        except (error.URLError, OSError, TimeoutError):
            raise FabricTransportError(f"Fabric {method} request failed or timed out.") from None
        if not 200 <= status < 300:
            raise FabricHttpError(method, status, _header(response_headers, "Retry-After"))
        if len(body) > 8 * 1024 * 1024:
            raise FabricTransportError("Fabric response exceeded the size limit.")
        if not body:
            parsed = None
        else:
            try:
                parsed = json.loads(body.decode("utf-8"))
            except (UnicodeError, ValueError):
                raise FabricTransportError("Fabric returned an invalid JSON response.") from None
        return status, response_headers, parsed

    def _wait(self, seconds, deadline):
        if seconds > deadline - self._clock():
            raise Blocked("Fabric operation or retry exceeded its deadline; reconcile before retrying.")
        self._sleep(seconds)
        if self._clock() >= deadline:
            raise Blocked("Fabric operation or retry exceeded its deadline; reconcile before retrying.")

    def get(self, path, deadline=None):
        deadline = deadline if deadline is not None else self._clock() + 60
        for attempt in range(3):
            remaining = deadline - self._clock()
            if remaining <= 0:
                raise Blocked("Fabric read exceeded its deadline.")
            try:
                status, headers, body = self.request("GET", path, timeout=min(30, remaining))
            except FabricHttpError as exc:
                if exc.status != 429 or attempt == 2:
                    raise
                self._wait(_retry_after({"Retry-After": exc.retry_after} if exc.retry_after is not None else {}),
                           deadline)
                continue
            if status != 200:
                raise FabricTransportError("Fabric returned an unexpected status for a read.")
            return headers, body
        raise FabricTransportError("Fabric read could not be completed.")

    def list_all(self, path, max_pages=1000, collection_key="value"):
        if collection_key not in ("value", "libraries"):
            raise Blocked("Unsupported Fabric pagination collection.")
        original = self._url(path)
        original_parts = parse.urlsplit(original)
        original_query = [(key, value) for key, value in parse.parse_qsl(
            original_parts.query, keep_blank_values=True) if key != "continuationToken"]
        next_url = original
        seen = set()
        values = []
        deadline = self._clock() + 600
        for _ in range(max_pages):
            if next_url in seen:
                raise Blocked("Fabric pagination repeated a continuation URL.")
            seen.add(next_url)
            _, page = self.get(next_url, deadline)
            if not isinstance(page, dict) or not isinstance(page.get(collection_key), list):
                raise FabricTransportError("Fabric returned an invalid paginated response.")
            values.extend(page[collection_key])
            if len(values) > 100000:
                raise Blocked("Fabric pagination exceeded its item limit.")
            uri, token = page.get("continuationUri"), page.get("continuationToken")
            # Some published API examples use the string "null" instead of JSON null.
            uri = None if uri == "null" else uri
            token = None if token == "null" else token
            if uri:
                continuation = parse.urlsplit(self._url(uri))
                if continuation.path != original_parts.path:
                    raise Blocked("Fabric pagination changed the requested API path.")
                query = parse.parse_qsl(continuation.query, keep_blank_values=True)
                continuation_tokens = [(key, value) for key, value in query if key == "continuationToken"]
                if (len(continuation_tokens) > 1
                        or any(pair not in original_query for pair in query if pair[0] != "continuationToken")):
                    raise Blocked("Fabric pagination changed the requested API parameters.")
                next_url = parse.urlunsplit((original_parts.scheme, original_parts.netloc,
                                             original_parts.path,
                                             parse.urlencode(original_query + continuation_tokens), ""))
            elif token:
                if not isinstance(token, str):
                    raise FabricTransportError("Fabric returned an invalid continuation token.")
                next_url = parse.urlunsplit((original_parts.scheme, original_parts.netloc,
                                             original_parts.path,
                                             parse.urlencode(original_query + [("continuationToken", parse.unquote(token))]),
                                             ""))
            else:
                return values
        raise Blocked("Fabric pagination exceeded its page limit.")

    def poll(self, operation_id, retry_after=1, timeout=600, require_result=True):
        operation_id = _uuid(operation_id, "operation ID", Blocked)
        deadline = self._clock() + timeout
        path = f"/v1/operations/{operation_id}"
        delay = retry_after
        while True:
            self._wait(delay, deadline)
            headers, body = self.get(path, deadline)
            if not isinstance(body, dict):
                raise FabricTransportError("Fabric returned an invalid operation state.")
            status = body.get("status")
            if status == "Succeeded":
                if require_result:
                    _, result = self.get(path + "/result", deadline)
                    return result
                return None
            if status == "Failed":
                raise Blocked("Fabric operation failed; reconcile its resource before retrying.")
            if status not in ("NotStarted", "Running"):
                raise Blocked("Fabric returned an unknown operation state; reconcile before retrying.")
            delay = _retry_after(headers)

    def post_operation(self, path, payload=None, on_accepted=None, require_result=True):
        status, headers, body = self.request("POST", path, payload)
        if status != 202:
            return status, body
        operation_id = _uuid(_header(headers, "x-ms-operation-id"), "operation ID", Blocked)
        delay = _retry_after(headers)
        if on_accepted is not None:
            on_accepted(operation_id, delay)
        return 202, self.poll(operation_id, delay, require_result=require_result)


def _workspace_principal(client, state, plan):
    record = state["resources"]["workspace"]
    _, live = client.get(f"/v1/workspaces/{record['id']}")
    if (not isinstance(live, dict) or not _same_uuid(live.get("id"), record["id"])
            or live.get("displayName") != record["name"] or live.get("type") != "Workspace"):
        raise Blocked("Live workspace ID, name or type does not match owned state.")
    if record.get("description") is None or not record["description"]:
        raise Blocked("Owned workspace state lacks the exact live description needed for reuse.")
    if live.get("description") != record["description"]:
        raise Blocked("Live workspace description does not match owned state.")
    if (not _same_uuid(live.get("capacityId"), plan["capacity_id"])
            or live.get("capacityAssignmentProgress") != "Completed"):
        raise Blocked("Live workspace capacity assignment is not the configured completed assignment.")
    roles = client.list_all(f"/v1/workspaces/{record['id']}/roleAssignments")
    for role in roles:
        if not isinstance(role, dict) or not isinstance(role.get("principal"), dict):
            continue
        principal = role["principal"]
        principal_type = principal.get("type")
        if (_same_uuid(principal.get("id"), plan["identity_id"]) and role.get("role") == "Admin"
                and principal_type in ("User", "ServicePrincipal")
                and (state.get("identity_type") is None or principal_type == state["identity_type"])):
            return principal_type
    raise Blocked("Configured identity is not a directly assigned workspace Admin of the expected type.")


def _verify_item(client, state, plan, kind, principal_type):
    record = state["resources"][kind]
    workspace_id = state["resources"]["workspace"]["id"]
    _, live = client.get(f"/v1/workspaces/{workspace_id}/items/{record['id']}?include=DefaultIdentity")
    if (not isinstance(live, dict) or not _same_uuid(live.get("id"), record["id"])
            or not _same_uuid(live.get("workspaceId"), workspace_id)
            or live.get("displayName") != record["name"] or live.get("type") != ITEM_TYPES[kind]):
        raise Blocked("Live item ID, name, type or workspace parent does not match owned state.")
    if record.get("description") is None:
        raise Blocked("Owned item state lacks the exact live description needed for reuse.")
    if live.get("description") != record["description"]:
        raise Blocked("Live item description does not match owned state.")
    if kind in ("notebook", "pipeline") or (kind == "environment_item" and live.get("defaultIdentity") is not None):
        identity = live.get("defaultIdentity")
        if identity is None:
            raise Blocked("Live item default identity is not exposed; runtime identity must be independently verified.")
        if (not isinstance(identity, dict) or not _same_uuid(identity.get("id"), plan["identity_id"])
                or identity.get("type") != principal_type):
            raise Blocked("Live item default identity does not match the configured principal.")


def _environment_path(state):
    resources = state["resources"]
    return f"/v1/workspaces/{resources['workspace']['id']}/environments/{resources['environment_item']['id']}"


def _read_environment(client, state, deadline=None):
    record = state["resources"]["environment_item"]
    headers, live = client.get(_environment_path(state), deadline)
    if (not isinstance(live, dict) or not _same_uuid(live.get("id"), record["id"])
            or not _same_uuid(live.get("workspaceId"), state["resources"]["workspace"]["id"])
            or live.get("type") != "Environment" or live.get("displayName") != record["name"]
            or record.get("description") is None or live.get("description") != record["description"]):
        raise Blocked("Live Environment metadata does not match owned state.")
    return headers, live


def _publish_details(live):
    properties = live.get("properties")
    if properties is None:
        return None
    if not isinstance(properties, dict):
        raise Blocked("Environment publish metadata is invalid.")
    details = properties.get("publishDetails")
    if details is None:
        return None
    if not isinstance(details, dict) or details.get("state") not in (
            "Waiting", "Running", "Cancelling", "Cancelled", "Failed", "Success"):
        raise Blocked("Environment publish state is unknown; reconcile before proceeding.")
    version = _uuid(details.get("targetVersion"), "Environment publish version", Blocked)
    return details["state"], version, details.get("componentPublishInfo")


def _success_version(live):
    details = _publish_details(live)
    if details is None or details[0] != "Success":
        raise Blocked("Environment publication has not been confirmed successful.")
    components = details[2]
    if components is not None:
        if not isinstance(components, dict):
            raise Blocked("Environment component publication metadata is invalid.")
        for component in ("sparkLibraries", "sparkSettings"):
            if component in components and (
                    not isinstance(components[component], dict)
                    or components[component].get("state") != "Success"):
                raise Blocked("Environment components have not been published successfully.")
    return details[1]


def _verify_libraries(client, state, spec, published):
    stage = "" if published else "/staging"
    path = _environment_path(state) + stage + "/libraries?beta=false"
    libraries = client.list_all(path, collection_key="libraries")
    actual = {}
    for library in libraries:
        if (not isinstance(library, dict) or library.get("libraryType") != "External"
                or not isinstance(library.get("name"), str)
                or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", library["name"])
                or not isinstance(library.get("version"), str)):
            raise Blocked("Environment libraries contain an unexpected or invalid entry; no definition update was attempted.")
        name = re.sub(r"[-_.]+", "-", library["name"]).lower()
        if name in actual:
            raise Blocked("Environment library readback contains duplicate package names.")
        actual[name] = library["version"]
    if actual != spec["pins"]:
        phase = "Published" if published else "Staging"
        raise Blocked(f"{phase} Environment libraries differ from pinned environment.yml; no definition update was attempted.")


def _compute_fingerprint(client, state):
    _, compute = client.get(_environment_path(state) + "/staging/sparkcompute?beta=false")
    if not isinstance(compute, dict) or not compute:
        raise Blocked("Environment staging Spark compute readback is missing or invalid.")
    try:
        data = json.dumps(compute, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")
    except (TypeError, ValueError):
        raise Blocked("Environment staging Spark compute readback is invalid.") from None
    return hashlib.sha256(data).hexdigest()


def _verify_published_environment(client, state, spec, expected_version=None):
    _verify_libraries(client, state, spec, published=False)
    _, first = _read_environment(client, state)
    version = _success_version(first)
    recorded_version = state["resources"]["environment_item"].get("published_version")
    for expected in (expected_version, recorded_version):
        if expected is not None and not _same_uuid(expected, version):
            raise Blocked("Published Environment version differs from the owned or pending version.")
    _verify_libraries(client, state, spec, published=True)
    _, last = _read_environment(client, state)
    if _success_version(last) != version:
        raise Blocked("Environment publication changed during library readback; retry validation.")
    return version


def _wait_for_environment_publish(client, state, target_version, timeout=600):
    deadline = client._clock() + timeout
    while True:
        if client._clock() >= deadline:
            raise Blocked("Environment publish exceeded its deadline; resume the recorded operation.")
        headers, live = _read_environment(client, state, deadline)
        details = _publish_details(live)
        if details is not None:
            status, version, _ = details
            if target_version is not None and version != target_version:
                raise Blocked("Environment publish version changed; reconcile the recorded operation.")
            if status == "Success":
                return _success_version(live)
            if status not in ("Waiting", "Running"):
                raise Blocked("Environment publish did not succeed; reconcile the recorded operation.")
        client._wait(_retry_after(headers, default=5), deadline)


def _finish_environment_publish(client, store, state, spec):
    pending = state["pending"]
    if pending.get("spec_sha256") != spec["sha256"]:
        raise Blocked("environment.yml changed during a pending publish; reconcile before proceeding.")
    if pending.get("operation_id") and not pending.get("operation_succeeded"):
        client.poll(pending["operation_id"], pending.get("retry_after", 1), require_result=False)
        pending["operation_succeeded"] = True
        store.write(state)
    version = _wait_for_environment_publish(client, state, pending.get("target_version"))
    version = _verify_published_environment(client, state, spec, expected_version=version)
    state["resources"]["environment_item"]["published_version"] = version
    state["pending"] = None
    store.write(state)


def _ensure_environment(client, store, state, spec):
    _, live = _read_environment(client, state)
    details = _publish_details(live)
    record = state["resources"]["environment_item"]
    if details is not None:
        if details[0] != "Success":
            raise Blocked("Environment has an unjournaled or unsuccessful publish; reconcile before proceeding.")
        version = _verify_published_environment(client, state, spec)
        if not state.get("legacy") and record.get("published_version") is None:
            record["published_version"] = version
            store.write(state)
        return
    if state.get("legacy") or record.get("published_version") is not None:
        raise Blocked("An existing Environment without a verified publication will not be published automatically.")
    if not record.get("staging_compute_sha256"):
        raise Blocked("Environment staging compute baseline is missing; publishing is blocked.")
    _verify_libraries(client, state, spec, published=False)
    if _compute_fingerprint(client, state) != record["staging_compute_sha256"]:
        raise Blocked("Environment staging compute changed since creation; publishing is blocked.")
    _, live = _read_environment(client, state)
    if _publish_details(live) is not None:
        raise Blocked("Environment publication changed before the publish request; reconcile before proceeding.")
    state["pending"] = {"kind": "environment_item", "action": "publish", "spec_sha256": spec["sha256"],
                        "operation_id": None, "target_version": None, "operation_succeeded": False}
    store.write(state)

    def accepted(operation_id, delay):
        state["pending"].update(operation_id=operation_id, retry_after=delay)
        store.write(state)

    try:
        status, response = client.post_operation(_environment_path(state) + "/staging/publish?beta=false",
                                                 on_accepted=accepted, require_result=False)
    except (FabricHttpError, FabricTransportError, Blocked) as exc:
        raise Blocked(f"Environment publish was not confirmed; no POST was retried. {exc}") from None
    if status == 200 and isinstance(response, dict):
        details = _publish_details({"properties": response})
        if details is None:
            raise Blocked("Environment publish returned no version; reconcile before retrying.")
        state["pending"]["target_version"] = details[1]
    elif status == 202 and state["pending"].get("operation_id"):
        state["pending"]["operation_succeeded"] = True
    else:
        raise Blocked("Environment publish returned no confirmed operation; reconcile before retrying.")
    store.write(state)
    _finish_environment_publish(client, store, state, spec)


def _definition(state, kind):
    try:
        import fabric_assets

        resources = state["resources"]
        workspace_id = resources["workspace"]["id"]
        if kind == "notebook":
            return fabric_assets.notebook_definition(workspace_id, resources["staging_lakehouse"]["id"],
                                                     resources["staging_lakehouse"]["name"],
                                                     resources["environment_item"]["id"])
        return fabric_assets.pipeline_definition(workspace_id, resources["notebook"]["id"])
    except (ImportError, OSError, ValueError, TypeError, KeyError, IndexError):
        raise Blocked("Local notebook or pipeline definition is unavailable or invalid.") from None


def _parts(definition):
    if not isinstance(definition, dict) or not isinstance(definition.get("parts"), list):
        raise Blocked("A notebook or pipeline definition is invalid.")
    result = {}
    try:
        for part in definition["parts"]:
            name = part["path"]
            if not isinstance(name, str) or name in result or part["payloadType"] != "InlineBase64":
                raise ValueError
            result[name] = json.loads(base64.b64decode(part["payload"], validate=True).decode("utf-8"))
    except (KeyError, TypeError, ValueError, UnicodeError):
        raise Blocked("A notebook or pipeline definition is invalid.") from None
    return result


def _verify_definition(client, state, kind):
    expected_definition = _definition(state, kind)
    expected = _parts(expected_definition)
    workspace_id = state["resources"]["workspace"]["id"]
    item_id = state["resources"][kind]["id"]
    path = f"/v1/workspaces/{workspace_id}/items/{item_id}/getDefinition"
    if kind == "notebook":
        if expected_definition.get("format") != "ipynb":
            raise Blocked("Local notebook definition does not use the supported ipynb format.")
        path += "?format=ipynb"
    _, response = client.post_operation(path)
    if not isinstance(response, dict):
        raise Blocked("Fabric did not return the item definition.")
    actual = _parts(response.get("definition"))
    if set(actual) - set(expected) - {".platform"} or any(actual.get(key) != value for key, value in expected.items()):
        raise Blocked("Live item definition differs from local assets; no overwrite was attempted.")


def _record(state, plan, kind, resource_id):
    record = {"id": _uuid(resource_id, "created resource ID", Blocked),
              "name": plan["names"][kind], "description": _description(state["deployment_id"], kind),
              "type": "Workspace" if kind == "workspace" else ITEM_TYPES[kind]}
    if kind != "workspace":
        record["workspace_id"] = state["resources"]["workspace"]["id"]
    return record


def _finish_pending(client, store, state, plan):
    pending = state["pending"]
    kind = pending["kind"]
    if not pending.get("result_id"):
        if not pending.get("operation_id"):
            raise Blocked("A prior POST has an unknown outcome; reconcile it manually before retrying.")
        if not pending.get("operation_succeeded"):
            client.poll(pending["operation_id"], pending.get("retry_after", 1), require_result=False)
            pending["operation_succeeded"] = True
            store.write(state)
        path = ("/v1/workspaces" if kind == "workspace" else
                f"/v1/workspaces/{state['resources']['workspace']['id']}/items")
        expected_type = "Workspace" if kind == "workspace" else ITEM_TYPES[kind]
        matches = [item for item in client.list_all(path)
                   if isinstance(item, dict) and item.get("displayName") == plan["names"][kind]
                   and item.get("type") == expected_type]
        if len(matches) != 1:
            raise Blocked("Created resource was not uniquely discoverable; reconcile before retrying.")
        pending["result_id"] = _uuid(matches[0].get("id"), "created resource ID", Blocked)
        store.write(state)
    record = _record(state, plan, kind, pending["result_id"])
    state["resources"][kind] = record
    try:
        if kind == "workspace":
            _workspace_principal(client, state, plan)
        else:
            principal_type = _workspace_principal(client, state, plan)
            _verify_item(client, state, plan, kind, principal_type)
            if kind == "environment_item":
                record["staging_compute_sha256"] = _compute_fingerprint(client, state)
    except Exception:
        del state["resources"][kind]
        raise
    state["pending"] = None
    store.write(state)


def _create_resource(client, store, state, plan, kind, spec=None):
    if kind == "workspace":
        path = "/v1/workspaces"
        existing = client.list_all(path)
        payload = {"displayName": plan["names"][kind],
                   "description": _description(state["deployment_id"], kind),
                   "capacityId": plan["capacity_id"]}
    else:
        _workspace_principal(client, state, plan)
        workspace_path = f"/v1/workspaces/{state['resources']['workspace']['id']}"
        existing = client.list_all(workspace_path + "/items")
        path = workspace_path + ("/environments" if kind == "environment_item" else "/items")
        payload = {"displayName": plan["names"][kind], "type": ITEM_TYPES[kind],
                   "description": _description(state["deployment_id"], kind)}
        if kind == "environment_item":
            del payload["type"]
            payload["definition"] = _environment_definition(spec or _environment_spec())
        elif kind in ("notebook", "pipeline"):
            payload["definition"] = _definition(state, kind)
    if any(isinstance(item, dict) and isinstance(item.get("displayName"), str)
           and item["displayName"].casefold() == plan["names"][kind].casefold() for item in existing):
        raise Blocked("A resource with the requested name exists without an owned state ID; it was not adopted.")

    state["pending"] = {"kind": kind, "operation_id": None, "result_id": None}
    store.write(state)

    def accepted(operation_id, delay):
        state["pending"].update(operation_id=operation_id, retry_after=delay)
        store.write(state)

    try:
        status, response = client.post_operation(path, payload, on_accepted=accepted, require_result=False)
    except (FabricHttpError, FabricTransportError, Blocked) as exc:
        raise Blocked(f"Creation was not confirmed; no POST was retried. {exc}") from None
    if status == 201 and isinstance(response, dict):
        state["pending"]["result_id"] = _uuid(response.get("id"), "created resource ID", Blocked)
    elif status == 202 and state["pending"].get("operation_id"):
        state["pending"]["operation_succeeded"] = True
    else:
        raise Blocked("Creation returned no confirmed resource ID; reconcile before retrying.")
    store.write(state)
    _finish_pending(client, store, state, plan)


def _verify_all(client, state, plan, spec=None):
    spec = spec or _environment_spec()
    if state.get("pending"):
        raise Blocked("Deployment has a pending operation; reconcile or resume it before validation.")
    if any(kind not in state["resources"] for kind in KINDS):
        raise Blocked("Owned deployment state does not yet contain all producer resources.")
    principal_type = _workspace_principal(client, state, plan)
    for kind in ITEM_TYPES:
        _verify_item(client, state, plan, kind, principal_type)
    _verify_published_environment(client, state, spec)
    for kind in ("notebook", "pipeline"):
        _verify_definition(client, state, kind)


def _check_assets(plan):
    spec = _environment_spec()
    dummy = {"resources": {
        "workspace": {"id": "11111111-1111-4111-8111-111111111111"},
        "staging_lakehouse": {"id": "22222222-2222-4222-8222-222222222222",
                              "name": plan["names"]["staging_lakehouse"]},
        "environment_item": {"id": "44444444-4444-4444-8444-444444444444"},
        "notebook": {"id": "33333333-3333-4333-8333-333333333333"},
    }}
    for kind in ("notebook", "pipeline"):
        parts = _parts(_definition(dummy, kind))
        if not parts:
            raise Blocked("Local notebook or pipeline definition has no parts.")
    return spec


def perform(mode, config, prefix, state_dir, selected_demos=None, yes=False, *, candidate_path=None):
    plan = parse_config(config, prefix)
    if mode == "cleanup" and not yes:
        raise ContractError("cleanup requires --yes.")
    if mode == "run-data":
        _validate_run(config, selected_demos)
        return ["Blocked: fresh readiness for the shared producer and every selected destination is not implemented; no job was started.", *BLOCKERS]
    if mode == "dry-run":
        return ["Plan only; no resources were changed.",
                "Plan: verify owned state, tenant, identity, workspace and item readbacks before any reuse.",
                "Plan: provision only missing owned producer resources, publish the pinned Environment, then verify definitions; data execution and cleanup remain blocked."]
    if mode == "resolve-candidate-ids":
        if candidate_path is None:
            raise ContractError("Candidate output path is required.")
        resolve_candidate_ids(config, prefix, state_dir, candidate_path=candidate_path)
        return ["Candidate IDs resolved from owned journal only; live ownership and readiness remain unproven."]
    if mode == "cleanup":
        return ["Blocked: cleanup is disabled until deletion ownership and dependencies can be verified; nothing was deleted.", *BLOCKERS]
    missing = [field for field in ("tenant_id", "capacity_id", "identity_id") if not plan[field]]
    if not plan["names"]["workspace"]:
        missing.append("shared.workspace_name")
    if missing:
        return ["Blocked: deployment/readback requires " + ", ".join(missing) + ".", *BLOCKERS]
    store = StateStore(state_dir)
    if mode == "validate-only":
        selected = _selected_demos(selected_demos) if selected_demos is not None else None
        if selected is not None:
            selected_candidates(config, selected)
        state = load_owned_state(store, plan)
        if state is None:
            return ["Blocked: no owned producer deployment state exists.", *BLOCKERS]
        if state.get("pending"):
            return ["Blocked: deployment has a pending operation; reconcile or resume it before validation.", *BLOCKERS]
        token = azure_cli_token(plan["tenant_id"], plan["identity_id"])
        client = FabricClient(token)
        _verify_all(client, state, plan)
        observations = []
        if selected is not None:
            clients = {name: client for name in selected}
            if "demo-2" in selected:
                clients["demo-2"] = FabricClient(token, skill="eventhouse-cli")
            observations = read_selected_targets(config, selected, clients)
        return ["Owned workspace, item metadata, published Environment libraries and local definitions were read back successfully; runtime readiness remains unproven.", *observations, *BLOCKERS]
    if mode != "deploy":
        raise ContractError("Unsupported mode.")
    with store.lock():
        state = load_owned_state(store, plan)
        spec = _check_assets(plan)
        client = FabricClient(azure_cli_token(plan["tenant_id"], plan["identity_id"]))
        if state is None:
            if any(value is not None for value in plan["descriptions"].values()):
                raise Blocked("Description overrides are only accepted when verifying an existing manifest.")
            state = new_state(plan)
            store.write(state)
        if state.get("legacy"):
            _verify_all(client, state, plan, spec)
        else:
            for kind in KINDS:
                if state.get("pending") and state["pending"]["kind"] == kind:
                    if state["pending"].get("action", "create") == "publish":
                        principal_type = _workspace_principal(client, state, plan)
                        _verify_item(client, state, plan, kind, principal_type)
                        _finish_environment_publish(client, store, state, spec)
                    else:
                        _finish_pending(client, store, state, plan)
                if kind not in state["resources"]:
                    if state.get("pending"):
                        raise Blocked("A previous operation is still pending; reconcile before proceeding.")
                    _create_resource(client, store, state, plan, kind, spec)
                if kind == "workspace":
                    _workspace_principal(client, state, plan)
                else:
                    principal_type = _workspace_principal(client, state, plan)
                    _verify_item(client, state, plan, kind, principal_type)
                    if kind == "environment_item":
                        _ensure_environment(client, store, state, spec)
                    elif kind in ("notebook", "pipeline"):
                        _verify_definition(client, state, kind)
        return ["Owned producer resources and published Environment libraries were read back; no data job was started and runtime readiness remains unproven.", *BLOCKERS]


class _ArgumentParser(argparse.ArgumentParser):
    def error(self, message):
        raise ContractError("Invalid or missing command-line arguments.")


def _result_target(args):
    if not Path(args.result).is_absolute():
        raise ContractError("config, state-dir and result must be absolute paths.")
    config, directory, result = (Path(path).resolve() for path in (args.config, args.state_dir, args.result))
    protected = {config, directory / STATE_NAME, directory / LOCK_NAME}
    protected.update((HERE / name).resolve() for name in (
        "deploy.py", "test_deploy.py", "deployment-mcap.json", "generator.py", "fabric_assets.py",
        "notebook.ipynb", "pipeline-content.json", "delivery.py", "runner.py", "fabric_files.py",
        "environment.yml", "fabric_storage.py", "fabric_ledger.py", "fabric_delivery.py", "fabric_runner.py",
        "README.md"))
    source_path = result.is_relative_to(HERE) and result.suffix.lower() in (
        ".py", ".ipynb", ".yml", ".yaml", ".md")
    if (result in protected or source_path or result.name.casefold() in
            (STATE_NAME.casefold(), LOCK_NAME.casefold(), CANDIDATE_NAME.casefold())
            or config in (directory / STATE_NAME, directory / LOCK_NAME)):
        raise ContractError("result and state paths must not overwrite config, source files or owned state.")
    return result


def _envelope(mode, status="blocked", diagnostics=None):
    return {"contract_version": 1, "track": "shared", "mode": mode, "status": status,
            "ready": False, "diagnostics": diagnostics or []}


def _raw_options(argv, name):
    values = []
    for index, value in enumerate(argv):
        if value == name and index + 1 < len(argv):
            values.append(argv[index + 1])
        elif value.startswith(name + "="):
            values.append(value.split("=", 1)[1])
    return values


def _write_argument_error(argv, diagnostic):
    modes = _raw_options(argv, "--mode")
    mode = modes[0] if len(modes) == 1 else "unknown"
    if mode not in ("dry-run", "validate-only", "deploy", "cleanup", "run-data", "resolve-candidate-ids"):
        mode = "unknown"
    result = _envelope(mode, "fail", [diagnostic])
    targets = _raw_options(argv, "--result")
    configs = _raw_options(argv, "--config")
    state_dirs = _raw_options(argv, "--state-dir")
    if len(targets) != 1 or len(configs) > 1 or len(state_dirs) > 1:
        print(json.dumps(result), file=sys.stderr)
        return
    target = targets[0]
    config = configs[0] if configs else None
    state_dir = state_dirs[0] if state_dirs else None
    if target and Path(target).is_absolute():
        try:
            candidate = argparse.Namespace(result=target, config=config or str(HERE / "__missing_config__"),
                                           state_dir=state_dir or str(HERE / "__missing_state__"))
            _atomic_json(_result_target(candidate), result)
            return
        except (ContractError, OSError):
            pass
    print(json.dumps(result), file=sys.stderr)


def main(argv=None):
    parser = _ArgumentParser(add_help=False, allow_abbrev=False)
    parser.add_argument("--mode", required=True, choices=("dry-run", "validate-only", "deploy", "cleanup",
                                                          "run-data", "resolve-candidate-ids"))
    parser.add_argument("--config", required=True)
    parser.add_argument("--prefix", required=True)
    parser.add_argument("--state-dir", required=True)
    parser.add_argument("--result", required=True)
    parser.add_argument("--selected-demos")
    parser.add_argument("--yes", action="store_true")
    raw_args = list(sys.argv[1:] if argv is None else argv)
    try:
        args = parser.parse_args(raw_args)
    except ContractError as exc:
        _write_argument_error(raw_args, str(exc))
        return 1
    result = _envelope(args.mode, diagnostics=["Operation did not complete; no readiness has been established."])
    result_path = None
    try:
        result_path = _result_target(args)
        _atomic_json(result_path, result)
        if not Path(args.config).is_absolute() or not Path(args.state_dir).is_absolute():
            raise ContractError("config, state-dir and result must be absolute paths.")
        candidate_path = result_path.parent / CANDIDATE_NAME if args.mode == "resolve-candidate-ids" else None
        if candidate_path is not None and candidate_path.parent == Path(args.state_dir).resolve():
            raise ContractError("Candidate output must be outside the durable state directory.")
        if candidate_path is not None and candidate_path.resolve() == Path(args.config).resolve():
            raise ContractError("Candidate output must not overwrite the input config.")
        try:
            config = json.loads(Path(args.config).read_text(encoding="utf-8"))
        except (OSError, UnicodeError, ValueError):
            raise ContractError("Config is unreadable or is not valid JSON.") from None
        diagnostics = perform(args.mode, config, args.prefix, args.state_dir, args.selected_demos, args.yes,
                              candidate_path=candidate_path)
        result = _envelope(args.mode, "pass" if args.mode in ("dry-run", "resolve-candidate-ids") else "blocked",
                           diagnostics)
    except ContractError as exc:
        result = _envelope(args.mode, "fail", [str(exc)])
    except (Blocked, TargetReadbackBlocked) as exc:
        result = _envelope(args.mode, "blocked", [str(exc), *BLOCKERS])
    except (FabricHttpError, FabricTransportError) as exc:
        result = _envelope(args.mode, "fail", [str(exc)])
    except OSError:
        result = _envelope(args.mode, "fail", ["Unable to persist the result or deployment state."])
    except Exception:
        result = _envelope(args.mode, "fail", ["Unexpected deployment error; no readiness has been established."])
    if result_path is not None:
        try:
            _atomic_json(result_path, result)
        except OSError:
            result = _envelope(args.mode, "fail", ["Unable to persist the result; inspect deployment state before retrying."])
            print(json.dumps(result), file=sys.stderr)
            return 1
    else:
        print(json.dumps(result), file=sys.stderr)
    return 0 if result["status"] == "pass" else 1


if __name__ == "__main__":
    sys.exit(main())
