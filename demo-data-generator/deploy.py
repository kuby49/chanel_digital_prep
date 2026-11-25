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
import concurrent.futures
import copy
import hashlib
import importlib.util
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib import error, parse, request

from fabric_ledger import _validate_ingestion_outcome
from fabric_runtime import (QUEUE_SDK_VERSION, configured_notebook_catalog, _kusto_endpoint, _tenant, native_parameters,
                            require_write_roles, run_config, validate_native_template)
from fabric_storage import OneLakeStore, StorageConflict, StorageError, StorageOutcomeUnknown
from generator import (RunConfig, batch_bytes, batch_metadata_matches, canonical_bytes, new_manifest,
                       strict_json_loads, validate_run_manifest)
from job_lifecycle import (JOURNAL_DIR_NAME, STARTUP_HEADROOM_SECONDS, TERMINAL, LifecycleBlocked,
                           PipelineJob, _read_journal, activity_timeout)
from target_readback import (OneLakeTableClient, TargetReadbackBlocked, _configured_landing,
                             read_selected_targets, selected_candidates)


HERE = Path(__file__).resolve().parent
ENVIRONMENT_YML = HERE / "environment.yml"
API_ROOT = "https://api.fabric.microsoft.com"
STORAGE_RESOURCE = "https://storage.azure.com/"
AZURE_CLI_TOKEN_TIMEOUT_SECONDS = 120
STATE_NAME = "shared-producer-v1.json"
DEFINITION_UPDATE_NAME = "shared-producer-definition-update-v1.json"
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
REQUIRED_ENVIRONMENT_PACKAGES = {"azure-identity", "azure-kusto-data", "azure-kusto-ingest", "azure-storage-queue"}
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
TRACK_SCRIPTS = {
    "demo-1": HERE.parent / "demo-1-materialized-lake-views" / "deploy.py",
    "demo-2": HERE.parent / "demo-2-eventhouse" / "deploy.py",
    "demo-3": HERE.parent / "demo-3-onelake-events-copy" / "deploy.py",
    "shared": HERE / "deploy.py",
}


class ContractError(Exception):
    """A locally invalid invocation or configuration."""


class Blocked(Exception):
    """An operation cannot safely proceed with the available evidence."""


class FabricHttpError(Exception):
    def __init__(self, method, status, retry_after=None, *, error_code=None, url=None):
        self.status = status
        self.retry_after = retry_after
        self.error_code = error_code
        self.url = url
        super().__init__(f"Fabric {method} request failed (HTTP {status}).")


class FabricTransportError(Exception):
    pass


class FabricTransientTransportError(FabricTransportError):
    """A recognized transport failure that may be retried for an idempotent read."""


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
    try:
        configured_notebook_catalog(shared)
    except ValueError as exc:
        raise ContractError(str(exc)) from None
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
        if not REQUIRED_ENVIRONMENT_PACKAGES.issubset(pins) or pins["azure-storage-queue"] != QUEUE_SDK_VERSION:
            raise ValueError
    except (OSError, UnicodeError, ValueError):
        raise Blocked("environment.yml must contain only explicitly pinned pip dependencies, including the "
                      f"required Azure SDKs and azure-storage-queue=={QUEUE_SDK_VERSION}.") from None
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
    if data.get("batch_class", "measured") != "measured":
        raise ContractError("run-data requires measured batches.")
    try:
        activity_timeout(duration)
    except LifecycleBlocked as exc:
        raise ContractError(str(exc)) from None
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


def azure_cli_token(tenant_id, identity_id, *, resource=API_ROOT):
    if resource not in (API_ROOT, STORAGE_RESOURCE):
        raise ContractError("Unsupported Azure token resource.")
    executable = shutil.which("az")
    environment = None
    command = [executable or "az"]
    if sys.platform == "win32":
        if not executable:
            raise Blocked("Azure CLI is unavailable; sign in using the intended isolated configuration.")
        launcher = Path(executable)
        if launcher.suffix.lower() in (".cmd", ".bat"):
            runtime = launcher.parent.parent / "python.exe"
            if launcher.name.lower() != "az.cmd" or not runtime.is_file():
                raise Blocked("Azure CLI Windows launcher has no supported bundled Python runtime; "
                              "repair the MSI installation.")
            # A CMD child can retain captured pipes after the wrapper's timeout.
            command = [str(runtime), "-IBm", "azure.cli"]
            environment = {**os.environ, "AZ_INSTALLER": "MSI"}
    command += ["account", "get-access-token", "--resource", resource,
                "--tenant", tenant_id, "--output", "json", "--only-show-errors"]
    try:
        completed = subprocess.run(command, capture_output=True, text=True, encoding="utf-8",
                                   env=environment, timeout=AZURE_CLI_TOKEN_TIMEOUT_SECONDS, check=False)
    except subprocess.TimeoutExpired:
        raise Blocked(f"Azure CLI token acquisition timed out after {AZURE_CLI_TOKEN_TIMEOUT_SECONDS}s; "
                      "no automatic retry was made.") from None
    except FileNotFoundError:
        raise Blocked("Azure CLI is unavailable; sign in using the intended isolated configuration.") from None
    except (OSError, UnicodeError, subprocess.SubprocessError):
        raise Blocked("Azure CLI token acquisition failed; no automatic retry was made.") from None
    if completed.returncode != 0:
        raise Blocked(f"Azure CLI token acquisition exited with code {completed.returncode}; "
                      "verify the intended tenant login. Command output is withheld.") from None
    try:
        response = json.loads(completed.stdout)
        token = response["accessToken"]
        if not isinstance(token, str) or not token or any(ord(char) < 33 or ord(char) > 126 for char in token):
            raise ValueError
    except (KeyError, TypeError, ValueError):
        raise Blocked("Azure CLI returned an invalid token response.") from None
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


def _fabric_error_code(body):
    if not isinstance(body, bytes) or len(body) > 65536:
        return None
    try:
        response = strict_json_loads(body.decode("utf-8"))
    except (UnicodeError, ValueError):
        return None
    code = response.get("errorCode") if isinstance(response, dict) else None
    return code if isinstance(code, str) and re.fullmatch(r"[A-Za-z0-9_]{1,128}", code) else None


def _retry_after(headers, default=1):
    value = _header(headers, "Retry-After")
    if value is None:
        return default
    if not isinstance(value, str) or not re.fullmatch(r"[0-9]{1,9}", value.strip()):
        raise Blocked("Fabric returned an invalid Retry-After value.")
    return max(1, int(value.strip()))


def _operation_location(location, operation_id):
    operation_id = _uuid(operation_id, "operation ID", Blocked)
    if not isinstance(location, str) or any(ord(char) < 33 or ord(char) == 127 for char in location):
        raise Blocked("Fabric operation Location is missing or unsafe; reconcile without retry.")
    try:
        parts = parse.urlsplit(location)
        hostname = parts.hostname
        if (parts.scheme != "https" or parts.username is not None or parts.password is not None
                or parts.port is not None or parts.netloc != hostname or parts.query or parts.fragment
                or parts.path != f"/v1/operations/{operation_id}"
                or not (hostname == "api.fabric.microsoft.com" or
                        re.fullmatch(r"wabi-[a-z0-9-]+\.analysis\.windows\.net", hostname or ""))):
            raise ValueError
    except ValueError:
        raise Blocked("Fabric operation Location is missing or unsafe; reconcile without retry.") from None
    return location


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
        if method == "GET" and isinstance(path, str) and path.startswith("https://wabi-"):
            match = re.fullmatch(r"https://wabi-[a-z0-9-]+\.analysis\.windows\.net/v1/operations/"
                                 r"([a-f0-9-]{36})", path)
            if not match:
                raise Blocked("Fabric operation Location is missing or unsafe; reconcile without retry.")
            url = _operation_location(path, match.group(1))
        else:
            url = self._url(path)
        data = None if method == "GET" or payload is None else json.dumps(
            payload, separators=(",", ":")).encode("utf-8")
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
            try:
                body = exc.read(65537) if status == 404 else b""
            except (OSError, ValueError):
                body = b""
            finally:
                exc.close()
            raise FabricHttpError(method, status, retry_after, error_code=_fabric_error_code(body), url=url) from None
        except (error.URLError, OSError, TimeoutError) as exc:
            reason = exc.reason if isinstance(exc, error.URLError) else exc
            if isinstance(reason, (TimeoutError, ConnectionError)):
                raise FabricTransientTransportError(f"Fabric {method} request failed or timed out.") from None
            raise FabricTransportError(f"Fabric {method} request failed or timed out.") from None
        if not 200 <= status < 300:
            raise FabricHttpError(method, status, _header(response_headers, "Retry-After"),
                                  error_code=_fabric_error_code(body) if status == 404 else None, url=url)
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
        transport_retried = False
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
            except FabricTransientTransportError:
                if transport_retried or attempt == 2:
                    raise
                transport_retried = True
                self._wait(1, deadline)
                continue
            if status != 200:
                raise FabricTransportError("Fabric returned an unexpected status for a read.")
            if self._clock() >= deadline:
                raise Blocked("Fabric read exceeded its deadline.")
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
                if not isinstance(uri, str) or "\\" in uri or any(ord(char) < 32 or ord(char) == 127 for char in uri):
                    raise Blocked("Fabric returned an unsafe API URL.")
                continuation = parse.urlsplit(uri)
                wabi_mlv = (re.fullmatch(r"/v1/workspaces/[a-f0-9-]{36}/lakehouses/[a-f0-9-]{36}"
                                          r"/jobs/refreshMaterializedLakeViews/instances", original_parts.path)
                            and continuation.scheme == "https"
                            and re.fullmatch(r"wabi-[a-z0-9-]+\.analysis\.windows\.net", continuation.netloc)
                            and not continuation.fragment)
                if not wabi_mlv:
                    continuation = parse.urlsplit(self._url(uri))
                if continuation.path != original_parts.path:
                    raise Blocked("Fabric pagination changed the requested API path.")
                query = parse.parse_qsl(continuation.query, keep_blank_values=True)
                continuation_tokens = [(key, value) for key, value in query if key == "continuationToken"]
                if (len(continuation_tokens) > 1
                        or any(pair not in original_query for pair in query if pair[0] != "continuationToken")):
                    raise Blocked("Fabric pagination changed the requested API parameters.")
                if wabi_mlv and (len({key for key, _ in query}) != len(query)
                                 or not isinstance(token, str) or not token or len(continuation_tokens) != 1
                                 or continuation_tokens[0][1] != parse.unquote(token)):
                    raise Blocked("Fabric pagination returned conflicting continuation tokens.")
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

    def poll(self, operation_id, retry_after=1, timeout=600, require_result=True, *, location=None):
        operation_id = _uuid(operation_id, "operation ID", Blocked)
        deadline = self._clock() + timeout
        if location is not None and require_result:
            raise Blocked("A definition-update operation cannot request a result endpoint.")
        path = _operation_location(location, operation_id) if location is not None else f"/v1/operations/{operation_id}"
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

    def post_operation(self, path, payload=None, on_accepted=None, require_result=True, *, exact_location=False):
        if exact_location and (on_accepted is None or require_result):
            raise Blocked("Definition updates require a durable operation receipt before polling.")
        status, headers, body = self.request("POST", path, payload)
        if status != 202:
            return status, body
        operation_id = _uuid(_header(headers, "x-ms-operation-id"), "operation ID", Blocked)
        delay = _retry_after(headers)
        if exact_location:
            try:
                location = _operation_location(_header(headers, "Location"), operation_id)
            except Blocked:
                if on_accepted is not None:
                    on_accepted(operation_id, delay, None)
                raise
            if on_accepted is not None:
                on_accepted(operation_id, delay, location)
            return 202, self.poll(operation_id, delay, require_result=require_result, location=location)
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
    if kind in ("notebook", "pipeline", "environment_item") and live.get("defaultIdentity") is not None:
        identity = live["defaultIdentity"]
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


def _verify_libraries(client, state, spec, published, *, allow_missing=False):
    stage = "" if published else "/staging"
    path = _environment_path(state) + stage + "/libraries?beta=false"
    try:
        libraries = client.list_all(path, collection_key="libraries")
    except FabricHttpError as exc:
        if (published and allow_missing and exc.status == 404
                and exc.error_code == "EnvironmentLibrariesNotFound" and exc.url == FabricClient._url(path)):
            return False
        raise
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
    return True


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
    empty_baseline_version = None
    if details is not None:
        if details[0] != "Success":
            raise Blocked("Environment has an unjournaled or unsuccessful publish; reconcile before proceeding.")
        if not state.get("legacy") and record.get("published_version") is None:
            empty_baseline_version = _success_version(live)
            if _verify_libraries(client, state, spec, published=True, allow_missing=True):
                version = _verify_published_environment(client, state, spec, expected_version=empty_baseline_version)
                record["published_version"] = version
                store.write(state)
                return
        else:
            _verify_published_environment(client, state, spec)
            return
    if state.get("legacy") or record.get("published_version") is not None:
        raise Blocked("An existing Environment without a verified publication will not be published automatically.")
    if not record.get("staging_compute_sha256"):
        raise Blocked("Environment staging compute baseline is missing; publishing is blocked.")
    _verify_libraries(client, state, spec, published=False)
    if _compute_fingerprint(client, state) != record["staging_compute_sha256"]:
        raise Blocked("Environment staging compute changed since creation; publishing is blocked.")
    _, live = _read_environment(client, state)
    if empty_baseline_version is None:
        if _publish_details(live) is not None:
            raise Blocked("Environment publication changed before the publish request; reconcile before proceeding.")
    elif (_success_version(live) != empty_baseline_version
          or _verify_libraries(client, state, spec, published=True, allow_missing=True)):
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


def _native_profile(config, resources):
    shared = config.get("shared", {})
    if shared.get("native_launch", False) is False:
        return None
    if shared["native_launch"] is not True:
        raise ValueError("shared.native_launch must be a boolean")
    selection = shared.get("native_selected_demos", "demo-1,demo-2,demo-3")
    if not isinstance(selection, str):
        raise ValueError("Native launch selection must be a comma-delimited string")
    reusable = shared.get("native_reusable", False)
    if type(reusable) is not bool:
        raise ValueError("shared.native_reusable must be a boolean")
    profile_config = config
    if reusable and isinstance(config.get("data"), dict):
        profile_config = {**config, "data": {**config["data"], "run_id": "native_template"}}
    selected = sorted(_validate_run(profile_config, selection))
    targets = selected_candidates(config, selected)
    run, duration = run_config(profile_config)
    producer = {field: resources[kind]["id"] for field, kind in (
        ("workspace_id", "workspace"), ("staging_lakehouse_id", "staging_lakehouse"), ("notebook_id", "notebook"))}
    if any(shared.get(field) != producer[key] for field, key in (
            ("workspace_id", "workspace_id"), ("staging_lakehouse_id", "staging_lakehouse_id"),
            ("producer_notebook_id", "notebook_id"))):
        raise ValueError("Native launch requires the exact owned producer IDs")
    tenant = _tenant(config)
    identity = _uuid(shared.get("identity_id"), "native identity")
    runtime = {"contract_version": 1, "tenant_id": tenant,
               "shared": {"workspace_id": producer["workspace_id"],
                          "staging_lakehouse_id": producer["staging_lakehouse_id"],
                          "producer_notebook_id": producer["notebook_id"], "identity_id": identity,
                          "target_bindings": targets},
               "data": {"run_id": run.run_id, "seed": run.seed, "start_date": run.start_date,
                        "date_count": run.date_count, "product_count": run.products, "customer_count": run.customers,
                        "rows_per_file": run.rows_per_file, "file_count": run.file_count,
                        "interval_seconds": run.interval_seconds, "max_duration_seconds": duration,
                        "currency": "USD"}}
    catalog = configured_notebook_catalog(shared)
    if catalog is not None:
        runtime["shared"]["notebook_default_lakehouse"] = catalog
    for name in selected:
        keys = ("ingestion_endpoint", "query_endpoint", "kql_database_name", "sales_table", "csv_mapping") \
            if name == "demo-2" else ("landing_uri",)
        runtime[name] = {key: config[name][key] for key in keys}
        if name == "demo-2":
            for key in ("ingestion_endpoint", "query_endpoint"):
                _kusto_endpoint(runtime[name][key])
            if (runtime[name]["sales_table"] != targets[name]["fact"]
                    or runtime[name]["csv_mapping"] != "SalesCsvV1"
                    or not isinstance(runtime[name]["kql_database_name"], str)
                    or not runtime[name]["kql_database_name"].strip()):
                raise ValueError("Native Eventhouse target does not match the CSV v1 binding")
        else:
            _configured_landing(runtime, name, targets[name])
    if reusable:
        native_fields = {
            "demo-1": ("workspace_id", "workspace_name", "lakehouse_id", "lakehouse_name",
                       "bronze_schema", "bronze_view", "silver_schema", "silver_view", "native_schedule"),
            "demo-2": ("workspace_id", "lakehouse_id", "monitor_notebook_id", "verifier_notebook_id",
                       "monitor_definition_sha256", "verifier_definition_sha256", "shortcut_name",
                       "shortcut_target_path", "verification_batch", "verification_timeout_seconds",
                       "poll_interval_seconds", "max_concurrent_batches"),
            "demo-3": ("native_proof",),
        }
        for name in selected:
            runtime[name].update({key: config[name][key] for key in native_fields[name] if key in config[name]})
        window = shared.get("native_run_window_seconds", 7200)
        profile = {"version": 2, "document": runtime, "selected": selected, "producer": producer,
                   "run_window_seconds": window,
                   "preparation_timeout_seconds": shared.get("native_preparation_timeout_seconds", 1200),
                   "cleanup_timeout_seconds": shared.get("native_cleanup_timeout_seconds", 300),
                   "activity_timeout": activity_timeout(window)}
        validate_native_template(profile)
        return profile
    digest = hashlib.sha256(canonical_bytes(runtime)).hexdigest()
    profile = {"document": runtime, "selected": selected, "activity_timeout": activity_timeout(duration),
               "expires_at_utc": shared["native_launch_expires_at_utc"],
               "binding": {"nonce": "native-" + digest[:32], "run_id": run.run_id, "config_sha256": digest,
                           "tenant_id": tenant, "identity_id": identity, "producer": producer}}
    native_parameters(profile, now=None)
    return profile


def _definition(state, kind, config=None):
    try:
        import fabric_assets

        resources = state["resources"]
        workspace_id = resources["workspace"]["id"]
        if kind == "notebook":
            catalog = configured_notebook_catalog((config or {}).get("shared", {}))
            return fabric_assets.notebook_definition(workspace_id, resources["staging_lakehouse"]["id"],
                                                     resources["staging_lakehouse"]["name"],
                                                     resources["environment_item"]["id"],
                                                     default_lakehouse=catalog)
        profile = _native_profile(config or {}, resources)
        duration = None
        if profile is None:
            data = (config or {}).get("data", {})
            if not isinstance(data, dict):
                raise ValueError("Data configuration must be an object")
            duration = data.get("max_duration_seconds")
            if "max_duration_seconds" in data and duration is None:
                raise ValueError("Measured duration is required")
        return fabric_assets.pipeline_definition(workspace_id, resources["notebook"]["id"], native_profile=profile,
                                                 measured_duration_seconds=duration)
    except (ImportError, OSError, ValueError, TypeError, KeyError, IndexError, ContractError, LifecycleBlocked):
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


def _definition_path(state, kind):
    workspace_id = state["resources"]["workspace"]["id"]
    item_id = state["resources"][kind]["id"]
    return f"/v1/workspaces/{workspace_id}/items/{item_id}"


def _read_definition(client, state, kind):
    path = _definition_path(state, kind) + "/getDefinition"
    if kind == "notebook":
        path += "?format=ipynb"
    _, response = client.post_operation(path)
    if not isinstance(response, dict):
        raise Blocked("Fabric did not return the item definition.")
    definition = response.get("definition")
    return definition, _parts(definition)


def _merge_notebook_content(expected, live, *, allow_missing_trident, previous_binding=None):
    try:
        if not isinstance(expected, dict) or not isinstance(live, dict):
            raise ValueError
        if any(live.get(key) != expected[key] for key in ("nbformat", "nbformat_minor")):
            raise ValueError
        target_metadata, live_metadata = expected["metadata"], live["metadata"]
        if not isinstance(target_metadata, dict) or not isinstance(live_metadata, dict):
            raise ValueError
        for key, value in target_metadata.items():
            if key not in ("dependencies", "trident") and live_metadata.get(key) != value:
                raise ValueError
        target_dependencies = target_metadata["dependencies"]
        live_dependencies = live_metadata["dependencies"]
        if not isinstance(target_dependencies, dict) or not isinstance(live_dependencies, dict):
            raise ValueError
        if any(live_dependencies.get(key) != value for key, value in target_dependencies.items()
               if key != "lakehouse"):
            raise ValueError
        binding = target_metadata["trident"]["lakehouse"]
        if target_dependencies.get("lakehouse") != binding:
            raise ValueError
        trident = live_metadata.get("trident")
        if trident is not None and not isinstance(trident, dict):
            raise ValueError
        if isinstance(trident, dict) and "lakehouse" in trident and trident["lakehouse"] is None:
            raise ValueError
        existing_binding = trident.get("lakehouse") if trident is not None else None
        dependency_binding = live_dependencies.get("lakehouse")
        if dependency_binding == binding:
            if existing_binding != binding and not (allow_missing_trident and existing_binding is None):
                raise ValueError
        elif (previous_binding is None or dependency_binding != previous_binding
              or existing_binding != previous_binding):
            raise ValueError
        if "trident" in live_metadata and trident is None:
            raise ValueError
        expected_cells, live_cells = expected["cells"], live["cells"]
        if (not isinstance(expected_cells, list) or not isinstance(live_cells, list)
                or len(expected_cells) != len(live_cells)):
            raise ValueError
        merged = copy.deepcopy(live)
        merged["metadata"].setdefault("trident", {})["lakehouse"] = copy.deepcopy(binding)
        merged["metadata"]["dependencies"]["lakehouse"] = copy.deepcopy(binding)
        for target_cell, live_cell, result_cell in zip(expected_cells, live_cells, merged["cells"]):
            if not isinstance(target_cell, dict) or not isinstance(live_cell, dict):
                raise ValueError
            if any(live_cell.get(key) != value for key, value in target_cell.items()
                   if key not in ("source", "metadata", "outputs", "execution_count")):
                raise ValueError
            target_cell_metadata = target_cell["metadata"]
            live_cell_metadata = live_cell["metadata"]
            if not isinstance(target_cell_metadata, dict) or not isinstance(live_cell_metadata, dict):
                raise ValueError
            if any(live_cell_metadata.get(key) != value for key, value in target_cell_metadata.items()):
                raise ValueError
            for source in (target_cell["source"], live_cell["source"]):
                if not isinstance(source, list) or any(not isinstance(line, str) for line in source):
                    raise ValueError
            result_cell["source"] = copy.deepcopy(target_cell["source"])
        return merged
    except (KeyError, TypeError, ValueError):
        raise Blocked("Notebook layout, metadata or owned binding cannot be preserved safely; "
                      "no overwrite was attempted.") from None


def _verify_schema_catalog(client, config):
    catalog = configured_notebook_catalog((config or {}).get("shared", {}))
    if catalog is None:
        return
    _, live = client.get(f"/v1/workspaces/{catalog['workspace_id']}/lakehouses/{catalog['lakehouse_id']}")
    properties = live.get("properties") if isinstance(live, dict) else None
    if (not isinstance(live, dict) or live.get("id") != catalog["lakehouse_id"]
            or live.get("workspaceId") != catalog["workspace_id"] or live.get("type") != "Lakehouse"
            or live.get("displayName") != catalog["lakehouse_name"] or not isinstance(properties, dict)
            or not isinstance(properties.get("defaultSchema"), str) or not properties["defaultSchema"].strip()):
        raise Blocked("Configured notebook default Lakehouse is not the exact schema-enabled catalog.")


def _verify_definition(client, state, kind, config=None):
    if kind == "notebook":
        _verify_schema_catalog(client, config)
    expected_definition = _definition(state, kind, config)
    expected = _parts(expected_definition)
    if kind == "notebook" and expected_definition.get("format") != "ipynb":
        raise Blocked("Local notebook definition does not use the supported ipynb format.")
    _, actual = _read_definition(client, state, kind)
    if kind == "notebook" and "notebook-content.ipynb" in actual and "notebook-content.ipynb" in expected:
        expected["notebook-content.ipynb"] = _merge_notebook_content(
            expected["notebook-content.ipynb"], actual["notebook-content.ipynb"], allow_missing_trident=False)
    if set(actual) - set(expected) - {".platform"} or any(actual.get(key) != value for key, value in expected.items()):
        raise Blocked("Live item definition differs from local assets; no overwrite was attempted.")


def _definition_digest(parts):
    return hashlib.sha256(canonical_bytes(parts)).hexdigest()


def _platform_part(definition):
    return next((part for part in definition["parts"] if part["path"] == ".platform"), None)


def _platform_digest(definition):
    part = _platform_part(definition)
    return hashlib.sha256(base64.b64decode(part["payload"], validate=True)).hexdigest() if part else None


def _definition_update_record(store):
    path = store.directory / DEFINITION_UPDATE_NAME
    try:
        raw = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return None
    except (OSError, UnicodeError):
        raise Blocked("Definition-update journal is unreadable; reconcile before continuing.") from None
    try:
        value = strict_json_loads(raw)
        binding = value["binding"]
        required = {"version", "binding", "phase", "operation_id", "retry_after"}
        optional = {"response_status", "location_required", "operation_location"}
        if (not isinstance(binding, dict)
                or not required <= set(value) or set(value) - required - optional
                or type(value["version"]) is not int or value["version"] != 1
                or value["phase"] not in ("pending", "completed")
                or set(binding) != {"kind", "item_id", "workspace_id", "tenant_id", "before_sha256",
                                    "after_sha256", "platform_sha256"}
                or binding["kind"] not in ("notebook", "pipeline")
                or any(not _same_uuid(binding[key], binding[key]) for key in ("item_id", "workspace_id", "tenant_id"))
                or any(not re.fullmatch(r"[a-f0-9]{64}", binding[key]) for key in ("before_sha256", "after_sha256"))
                or (binding["platform_sha256"] is not None
                    and not re.fullmatch(r"[a-f0-9]{64}", binding["platform_sha256"]))
                or (value["operation_id"] is None and value["retry_after"] is not None)
                or (value["operation_id"] is not None
                    and (not _same_uuid(value["operation_id"], value["operation_id"])
                         or type(value["retry_after"]) is not int or value["retry_after"] < 0))
                or ("response_status" in value
                    and (type(value["response_status"]) is not int or value["response_status"] != 200
                         or value["operation_id"] is not None))
                or ("location_required" in value and value["location_required"] is not True)
                or (value.get("location_required") is True and value["phase"] == "completed"
                    and value["operation_id"] is not None and "operation_location" not in value)
                or (value.get("location_required") is True and value["phase"] == "completed"
                    and value["operation_id"] is None and value.get("response_status") != 200)
                or ("operation_location" in value
                    and (value.get("location_required") is not True or value["operation_id"] is None
                         or _operation_location(value["operation_location"], value["operation_id"])
                         != value["operation_location"]))):
            raise ValueError
    except (Blocked, KeyError, TypeError, ValueError, RecursionError):
        raise Blocked("Definition-update journal is invalid; reconcile before continuing.") from None
    return value


def _require_idle_producer(client, store, state, tenant_id):
    resources = state["resources"]
    for kind in ("pipeline", "notebook"):
        seen = set()
        for row in client.list_all(_definition_path(state, kind) + "/jobs/instances"):
            if not isinstance(row, dict) or row.get("itemId") != resources[kind]["id"]:
                raise Blocked(f"Producer {kind} job history is ambiguous; no definition was updated.")
            job_id = _uuid(row.get("id"), "job ID", Blocked)
            if job_id in seen or not isinstance(row.get("status"), str) or row["status"] not in TERMINAL:
                raise Blocked(f"Producer {kind} has active or uncertain jobs; no definition was updated.")
            seen.add(job_id)
    for path in (store.directory / JOURNAL_DIR_NAME).glob("*.json"):
        record = _read_journal(path)
        binding = record.get("binding") if isinstance(record, dict) else None
        job = PipelineJob(store.directory, binding, client)
        if (path != job.path or binding["workspace_id"] != resources["workspace"]["id"]
                or binding["pipeline_id"] != resources["pipeline"]["id"]
                or binding["notebook_id"] != resources["notebook"]["id"]
                or binding["tenant_id"] != tenant_id):
            raise Blocked("Producer job journal is outside the owned scope; no definition was updated.")
        if job.status_detail()["status"] not in TERMINAL:
            raise Blocked("An exact journaled producer job is active; no definition was updated.")


def _update_producer_definition(config, plan, state_dir, kind, before_sha256):
    store = StateStore(state_dir)
    with store.lock():
        state = load_owned_state(store, plan)
        if state is None or state.get("pending") or any(key not in state["resources"] for key in KINDS):
            raise Blocked("A complete owned producer deployment with no pending operation is required.")
        record = _definition_update_record(store)
        target = _definition(state, kind, config)
        target_parts = _parts(target)
        client = FabricClient(azure_cli_token(plan["tenant_id"], plan["identity_id"]))
        principal_type = _workspace_principal(client, state, plan)
        for owned_kind in ITEM_TYPES:
            _verify_item(client, state, plan, owned_kind, principal_type)
        if kind == "notebook":
            _verify_schema_catalog(client, config)
        if kind == "pipeline":
            _verify_definition(client, state, "notebook", config)
        live, actual = _read_definition(client, state, kind)
        if kind == "notebook":
            if set(target_parts) != {"notebook-content.ipynb"} or "notebook-content.ipynb" not in actual:
                raise Blocked("Live notebook definition differs from the supported layout; no overwrite was attempted.")
            previous_binding = None
            if configured_notebook_catalog(config.get("shared", {})) is not None:
                staging = state["resources"]["staging_lakehouse"]
                previous_binding = {"default_lakehouse": staging["id"], "default_lakehouse_name": staging["name"],
                                    "default_lakehouse_workspace_id": state["resources"]["workspace"]["id"],
                                    "known_lakehouses": [{"id": staging["id"]}]}
            merged = _merge_notebook_content(target_parts["notebook-content.ipynb"],
                                             actual["notebook-content.ipynb"], allow_missing_trident=True,
                                             previous_binding=previous_binding)
            target["parts"][0]["payload"] = base64.b64encode(canonical_bytes(merged)).decode("ascii")
            target_parts = _parts(target)
        if set(actual) - {".platform"} != set(target_parts):
            raise Blocked("Live definition differs from the supported part layout; no overwrite was attempted.")
        after = dict(target_parts)
        if ".platform" in actual:
            after[".platform"] = actual[".platform"]
            target["parts"].append(_platform_part(live))
        binding = {"kind": kind, "workspace_id": state["resources"]["workspace"]["id"],
                   "item_id": state["resources"][kind]["id"], "tenant_id": plan["tenant_id"],
                   "before_sha256": before_sha256, "after_sha256": _definition_digest(after),
                   "platform_sha256": _platform_digest(live)}
        matches = record is not None and record["binding"] == binding
        if record and record["phase"] == "pending" and not matches:
            raise Blocked("Another definition update is unresolved or its binding changed; no POST was attempted.")
        if (matches and record["phase"] == "pending" and record["operation_id"] is None
                and record.get("response_status") != 200):
            raise Blocked("Definition update POST outcome unknown; manual reconciliation is required. "
                          "No POST was retried.")
        if (matches and record["phase"] == "pending" and record.get("location_required") is True
                and record["operation_id"] is not None and "operation_location" not in record):
            raise Blocked("Definition update operation Location is unconfirmed; manual reconciliation is required. "
                          "No POST was retried.")
        _require_idle_producer(client, store, state, plan["tenant_id"])
        path = store.directory / DEFINITION_UPDATE_NAME

        def persist():
            _atomic_json(path, record)

        if matches:
            if record["phase"] == "pending" and record["operation_id"] is not None:
                if record.get("location_required") is True:
                    client.poll(record["operation_id"], record["retry_after"], require_result=False,
                                location=record["operation_location"])
                else:
                    client.poll(record["operation_id"], record["retry_after"], require_result=False)
        elif _definition_digest(actual) != before_sha256:
            raise Blocked(f"Live {kind} baseline differs from approved SHA-256; observed "
                          f"{_definition_digest(actual)}. No POST was attempted.")
        elif before_sha256 != binding["after_sha256"]:
            current, current_parts = _read_definition(client, state, kind)
            if _definition_digest(current_parts) != before_sha256 or _platform_digest(current) != binding["platform_sha256"]:
                raise Blocked("Live definition changed during admission; no POST was attempted.")
            record = {"version": 1, "binding": binding, "phase": "pending",
                      "operation_id": None, "retry_after": None, "location_required": True}
            persist()

            def accepted(operation_id, delay, location):
                record.update(operation_id=operation_id, retry_after=delay)
                if location is not None:
                    record["operation_location"] = location
                persist()

            try:
                status, _ = client.post_operation(_definition_path(state, kind) + "/updateDefinition?updateMetadata=false",
                                                  {"definition": target}, on_accepted=accepted, require_result=False,
                                                  exact_location=True)
            except (FabricHttpError, FabricTransportError, Blocked, OSError):
                raise Blocked("Definition update is unconfirmed; reconcile the same intent. No POST was retried.") from None
            if (status not in (200, 202) or status == 202 and (
                    record["operation_id"] is None or "operation_location" not in record)
                    or status == 200 and record["operation_id"] is not None):
                raise Blocked("Definition update returned an unconfirmed response; no POST will be retried.")
            if status == 200:
                record["response_status"] = 200
                persist()
            matches = True
        if matches:
            live, actual = _read_definition(client, state, kind)
        if _definition_digest(actual) != binding["after_sha256"] or _platform_digest(live) != binding["platform_sha256"]:
            raise Blocked("Exact definition or .platform readback is unconfirmed; no POST was retried.")
        if matches and record["phase"] == "pending":
            record["phase"] = "completed"
            persist()
        return [f"Owned {kind} definition and .platform read back exactly; no data job was started."]


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


def _create_resource(client, store, state, plan, kind, spec=None, config=None):
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
            if kind == "notebook":
                _verify_schema_catalog(client, config)
            payload["definition"] = _definition(state, kind, config)
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


def _verify_all(client, state, plan, spec=None, config=None):
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
        _verify_definition(client, state, kind, config)


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


STOP_CHILD_WAIT_SECONDS = 120


def _track_timeout(config):
    execution = config.get("execution", {})
    if not isinstance(execution, dict):
        raise ContractError("execution must be a configuration object.")
    timeout = execution.get("track_timeout_seconds", 1800)
    if type(timeout) is not int or timeout < 1:
        raise ContractError("execution.track_timeout_seconds must be a positive integer.")
    return timeout


def _admission_concurrency(config):
    execution = config.get("execution", {})
    if not isinstance(execution, dict):
        raise ContractError("execution must be a configuration object.")
    concurrency = execution.get("max_concurrency", 4)
    if type(concurrency) is not int or not 1 <= concurrency <= 4:
        raise ContractError("execution.max_concurrency must be an integer between 1 and 4.")
    return concurrency


def _monitor_contract():
    path = HERE.parent / "scripts" / "monitor_contract.py"
    if not path.is_file():
        raise Blocked("The shared Demo 2 monitor contract is unavailable; no job was started.")
    try:
        spec = importlib.util.spec_from_file_location("producer_monitor_contract", path)
        if spec is None or spec.loader is None:
            raise ImportError("monitor contract loader unavailable")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module
    except (ImportError, OSError):
        raise Blocked("The shared Demo 2 monitor contract could not be loaded; no job was started.") from None


def _config_snapshot(path, expected):
    try:
        actual = strict_json_loads(path.read_text(encoding="utf-8"))
        if canonical_bytes(actual) != canonical_bytes(expected):
            raise ValueError
    except (OSError, UnicodeError, ValueError, TypeError, RecursionError):
        raise Blocked("Effective configuration changed during admission; no job was started.") from None


def _invoke_fresh(track, mode, config_path, prefix, state_dir, directory, timeout, selected):
    script = TRACK_SCRIPTS[track]
    if not script.is_file():
        raise Blocked(f"{track} entry point is unavailable; no job was started.")
    result_path = directory / f"{track}-{mode}.json"
    args = [sys.executable, str(script), "--mode", mode, "--config", str(config_path),
            "--prefix", prefix, "--state-dir", str(state_dir), "--result", str(result_path)]
    if track == "shared" or mode == "monitor-status":
        args += ["--selected-demos", ",".join(selected)]
    if track == "demo-2" and mode == "validate-only":
        args.append("--require-existing-proof")
    try:
        completed = subprocess.run(args, cwd=HERE.parent, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                                   timeout=timeout, check=False)
    except subprocess.TimeoutExpired:
        raise Blocked(f"{track} {mode} timed out; no job was started.") from None
    except OSError:
        raise Blocked(f"{track} {mode} could not be invoked; no job was started.") from None
    try:
        raw = result_path.read_bytes()
        if len(raw) > 1024 * 1024:
            raise ValueError
        result = strict_json_loads(raw.decode("utf-8"))
        if (not isinstance(result, dict) or set(result) !=
                {"contract_version", "track", "mode", "status", "ready", "diagnostics"}
                or type(result["contract_version"]) is not int or result["contract_version"] != 1
                or result["track"] != track or result["mode"] != mode
                or result["status"] not in ("pass", "fail", "blocked")
                or type(result["ready"]) is not bool or not isinstance(result["diagnostics"], list)
                or any(not isinstance(item, str) for item in result["diagnostics"])
                or (completed.returncode == 0) != (result["status"] == "pass")):
            raise ValueError
    except (OSError, UnicodeError, ValueError, TypeError, RecursionError):
        raise Blocked(f"{track} {mode} returned an invalid result; no job was started.") from None
    if result["status"] != "pass" or not result["ready"]:
        diagnostics = result["diagnostics"]
        reason = " ".join(item[:500] for item in diagnostics[:20]) if diagnostics else "readiness was not established"
        if len(diagnostics) > 20:
            reason += " Additional diagnostics omitted."
        raise Blocked(f"{track} {mode} is not ready: {reason} No job was started.")
    return result


def _fresh_admission(config, config_path, prefix, state_dir, directory, selected, token_ready):
    timeout = _track_timeout(config)
    helper = _monitor_contract()
    demo2_scope = None
    try:
        scopes = {track: helper.deployment_scope(config, track, prefix, state_dir) for track in selected}
        demo2_scope = scopes.get("demo-2")
    except (ValueError, OSError, TypeError) as exc:
        raise Blocked(f"Demo deployment scope is invalid: {exc} No job was started.") from None
    _config_snapshot(config_path, config)
    tracks = [*selected, "shared"]
    with concurrent.futures.ThreadPoolExecutor(max_workers=min(len(tracks), _admission_concurrency(config))) as executor:
        futures = []
        for track in tracks:
            track_prefix, track_state = scopes.get(track, (prefix, state_dir))
            futures.append(executor.submit(_invoke_fresh, track, "validate-only", config_path,
                                           track_prefix, track_state, directory, timeout, selected))
        for future in futures:
            future.result()
    _config_snapshot(config_path, config)
    token_ready()
    if demo2_scope is not None:
        track_prefix, track_state = demo2_scope
        _invoke_fresh("demo-2", "monitor-status", config_path, track_prefix, track_state,
                      directory, timeout, selected)
        try:
            manifest_uri, duration = helper.monitor_config(config)
            binding = helper.monitor_binding(config)
            start = helper.monitor_evidence(Path(track_state) / "demo-2-monitor-start.json",
                                            config["data"]["run_id"], manifest_uri, duration, "start", binding)
            current = helper.monitor_evidence(Path(track_state) / "demo-2-monitor.json",
                                              config["data"]["run_id"], manifest_uri, duration, "running", binding)
            helper.matching_monitor_identity(start, current)
            deadline = datetime.fromisoformat(current["deadline_utc"].replace("Z", "+00:00"))
            verification = config["demo-2"].get("verification_timeout_seconds")
            if verification is None:
                verification = config["execution"].get("verification_timeout_seconds")
            if type(verification) is not int or verification < 1:
                raise ValueError("invalid verification timeout")
            required = config["data"]["max_duration_seconds"] + STARTUP_HEADROOM_SECONDS + \
                STOP_CHILD_WAIT_SECONDS + verification
            if deadline <= datetime.now(timezone.utc) + timedelta(seconds=required):
                raise ValueError("monitor deadline does not cover producer execution and verification")
        except (OSError, ValueError, TypeError, KeyError, AttributeError, OverflowError) as exc:
            raise Blocked(f"Demo 2 running monitor is not bound to this run: {exc} No job was started.") from None
    _config_snapshot(config_path, config)


def _measured_payload(config, binding, identity_id, lakehouse_id):
    runtime = {"nonce": binding["nonce"], "run_id": binding["run_id"],
               "config_sha256": binding["config_sha256"], "tenant_id": binding["tenant_id"],
               "identity_id": identity_id,
               "producer": {"workspace_id": binding["workspace_id"], "staging_lakehouse_id": lakehouse_id,
                            "notebook_id": binding["notebook_id"]}}
    return {"executionData": {"parameters": {
        "operation": "run-data", "probe_id": binding["nonce"],
        "runtime_evidence_json": canonical_bytes(runtime).decode("utf-8"),
        "run_config_json": canonical_bytes(config).decode("utf-8"),
        "selected_demos_json": canonical_bytes(binding["selected_demos"]).decode("utf-8"),
        "activity_timeout": activity_timeout(config["data"]["max_duration_seconds"]),
    }}}


def _manifest_summary(store, config, selected):
    run, _ = run_config(config)
    targets = selected_candidates(config, selected)
    path = f"Files/producer/runs/{run.run_id}/manifest.json"
    try:
        raw, _ = store.read(path)
    except FileNotFoundError:
        return False, ["Producer manifest is not yet available."]
    try:
        manifest = strict_json_loads(raw.decode("utf-8"))
        expected = new_manifest(run, selected)
        expected["target_bindings"] = targets
        validate_run_manifest(manifest, expected)
    except (UnicodeError, ValueError, TypeError, RecursionError):
        raise Blocked("Producer manifest identity or configuration does not match the run.") from None
    batches = manifest["batches"]
    if set(batches) - {str(number) for number in range(1, run.file_count + 1)}:
        raise Blocked("Producer manifest contains unexpected batches.")
    diagnostics = []
    all_verified = len(batches) == run.file_count
    for number in range(1, run.file_count + 1):
        batch = batches.get(str(number))
        if batch is None:
            diagnostics.append(f"Batch {number}: not staged.")
            all_verified = False
            continue
        _, expected_batch = batch_bytes(run, number)
        if not batch_metadata_matches(batch, expected_batch):
            raise Blocked(f"Producer batch {number} metadata differs from deterministic input.")
        staged, _ = store.read(f"Files/producer/runs/{run.run_id}/{expected_batch['filename']}")
        if hashlib.sha256(staged).hexdigest() != expected_batch["sha256"]:
            raise Blocked(f"Producer batch {number} staged checksum differs.")
        deliveries = batch.get("deliveries")
        if not isinstance(deliveries, dict) or set(deliveries) != set(selected):
            raise Blocked(f"Producer batch {number} delivery targets differ from the run.")
        for destination in selected:
            delivery = deliveries[destination]
            if not isinstance(delivery, dict) or not isinstance(delivery.get("status"), str):
                raise Blocked(f"Producer batch {number} has an invalid {destination} delivery.")
            status = delivery["status"]
            operation = delivery.get("operation_id")
            detail = f"{batch['batch_id']} {destination}: {status}"
            if isinstance(operation, str):
                detail += f", source={operation[:256]}"
            if destination == "demo-2" and isinstance(delivery.get("ingestion_outcome"), dict):
                detail += f", ingestion={delivery['ingestion_outcome'].get('status')}"
            lag = delivery.get("staged_to_submission_ack_seconds")
            if type(lag) in (int, float):
                detail += f", staging-to-submission acknowledgement={lag:g}s"
            diagnostics.append(detail + ".")
            if status != "verified":
                all_verified = False
                continue
            values = ("verified_row_count", "verified_total_minor_units", "verified_distinct_sales_line_ids")
            try:
                verified_at = datetime.fromisoformat(delivery["verified_at_utc"].replace("Z", "+00:00"))
                valid_time = verified_at.tzinfo is not None and verified_at.utcoffset() == timedelta(0)
            except (KeyError, AttributeError, TypeError, ValueError, OverflowError):
                valid_time = False
            if (any(type(delivery.get(name)) is not int for name in values)
                    or delivery.get("verified_row_count") != expected_batch["row_count"]
                    or delivery.get("verified_total_minor_units") != expected_batch["sales_amount_minor_units"]
                    or delivery.get("verified_distinct_sales_line_ids") != expected_batch["row_count"]
                    or not valid_time):
                raise Blocked(f"Producer batch {number} has invalid {destination} verification evidence.")
            if destination == "demo-2":
                try:
                    _validate_ingestion_outcome(delivery.get("ingestion_outcome"), operation)
                except ValueError:
                    raise Blocked(f"Producer batch {number} has invalid Eventhouse ingestion evidence.") from None
                if delivery["ingestion_outcome"]["status"] != "succeeded":
                    raise Blocked(f"Producer batch {number} lacks successful Eventhouse ingestion.")
    scheduler = manifest.get("scheduler")
    result = scheduler.get("result") if isinstance(scheduler, dict) else None
    if (not isinstance(scheduler, dict) or scheduler.get("status") != "completed"
            or not isinstance(result, dict) or result.get("verified") is not True or result.get("errors") != []):
        all_verified = False
        diagnostics.insert(0, "Producer scheduler has not reported a verified completion.")
    return all_verified, diagnostics


def _wait_for_measured(job, job_id, config, selected, plan, lakehouse_id):
    deadline_utc = datetime.fromisoformat(job.binding["deadline_utc"].replace("Z", "+00:00"))
    deadline = time.monotonic() + max(0, (deadline_utc - datetime.now(timezone.utc)).total_seconds())
    refresh_at = time.monotonic() + 300
    parent_terminal_at = None
    parent_terminal_status = None
    observed_child = None
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        if parent_terminal_at is not None:
            child_remaining = parent_terminal_at + STOP_CHILD_WAIT_SECONDS - time.monotonic()
            if child_remaining <= 0:
                raise Blocked(f"Producer job {job_id} is {parent_terminal_status}, but its exact notebook termination "
                              "is unconfirmed; reconcile before retrying.")
            remaining = min(remaining, child_remaining)
        if time.monotonic() >= refresh_at:
            job.client = FabricClient(azure_cli_token(plan["tenant_id"], plan["identity_id"]))
            refresh_at = time.monotonic() + 300
        try:
            snapshot = job.status_detail(timeout=min(30, max(0.01, remaining)),
                                         include_notebook=True)
        except LifecycleBlocked as exc:
            if "initial polling delay has not elapsed" not in str(exc):
                raise Blocked(f"Producer job {job_id} status is unconfirmed: {exc}") from None
            time.sleep(min(1, max(0, deadline - time.monotonic())))
            continue
        except (FabricHttpError, FabricTransportError) as exc:
            raise Blocked(f"Producer job {job_id} status is unconfirmed: {exc}") from None
        child = snapshot.get("notebook")
        if child:
            if observed_child is not None and child["job_id"] != observed_child:
                raise Blocked(f"Producer job {job_id} linked notebook identity changed; termination is unconfirmed.")
            observed_child = child["job_id"]
        if parent_terminal_status is not None and snapshot["status"] != parent_terminal_status:
            raise Blocked(f"Producer job {job_id} terminal status changed; termination is unconfirmed.")
        if snapshot["status"] in ("Completed", "Failed", "Cancelled", "Deduped"):
            if parent_terminal_at is None:
                parent_terminal_at = time.monotonic()
                parent_terminal_status = snapshot["status"]
            if child and child["status"] in ("Completed", "Failed", "Cancelled"):
                storage_token = azure_cli_token(plan["tenant_id"], plan["identity_id"], resource=STORAGE_RESOURCE)
                store = OneLakeStore(job.binding["workspace_id"], lakehouse_id, lambda: storage_token)
                verified, diagnostics = _manifest_summary(store, config, selected)
                identities = (f"Pipeline job {job_id}: {snapshot['status']}; notebook job "
                              f"{child['job_id']}: {child['status']}.")
                if snapshot["status"] != "Completed" or child["status"] != "Completed" or not verified:
                    raise Blocked(identities + " Delivery is not fully verified; reconcile the manifest. "
                                  + " ".join(diagnostics[:20]))
                return [identities, "Producer delivery is verified; file landing does not prove downstream facts.",
                        *diagnostics]
        wait = deadline - time.monotonic()
        if parent_terminal_at is not None:
            wait = min(wait, parent_terminal_at + STOP_CHILD_WAIT_SECONDS - time.monotonic())
        time.sleep(min(5, max(0, wait)))
    raise Blocked(f"Producer job {job_id} exceeded the bounded wait; termination is unconfirmed.")


def _run_measured(config, plan, state_dir, selected_demos):
    if config.get("shared", {}).get("native_launch") is True:
        raise Blocked("This run ID is reserved for the prepared native pipeline; no CLI job was started.")
    selected = _validate_run(config, selected_demos)
    if any(not plan[field] for field in ("tenant_id", "capacity_id", "identity_id")):
        raise Blocked("Producer tenant, capacity and identity are required; no job was started.")
    if not Path(state_dir).is_absolute() or not Path(state_dir).is_dir():
        raise Blocked("An existing absolute producer state directory is required; no job was started.")
    selected_candidates(config, selected)
    state = load_owned_state(StateStore(state_dir), plan)
    if state is None or state.get("pending") or any(kind not in state["resources"] for kind in KINDS):
        raise Blocked("The owned producer deployment is incomplete; no job was started.")
    resources = state["resources"]
    workspace_id = resources["workspace"]["id"]
    lakehouse_id = resources["staging_lakehouse"]["id"]
    shared = config["shared"]
    if (shared.get("workspace_id") != workspace_id or shared.get("staging_lakehouse_id") != lakehouse_id
            or shared.get("identity_id") != plan["identity_id"]):
        raise Blocked("Effective producer IDs differ from owned state; no job was started.")
    root = f"https://onelake.blob.fabric.microsoft.com/{workspace_id}/{lakehouse_id}/Files/producer/runs"
    location = shared.get("run_state_location")
    if location is not None and (not isinstance(location, str) or location.rstrip("/") != root):
        raise Blocked("Configured producer state location differs from owned staging; no job was started.")
    timeout = _track_timeout(config)
    concurrency = _admission_concurrency(config)
    duration = config["data"]["max_duration_seconds"]
    validator_count = len(selected) + 1
    admission_waves = (validator_count + concurrency - 1) // concurrency
    total = timeout * (admission_waves + ("demo-2" in selected)) + duration + \
        STARTUP_HEADROOM_SECONDS + STOP_CHILD_WAIT_SECONDS + AZURE_CLI_TOKEN_TIMEOUT_SECONDS
    try:
        deadline = datetime.now(timezone.utc) + timedelta(seconds=total)
    except OverflowError:
        raise ContractError("The bounded producer deadline is too large.") from None
    binding = {"run_id": config["data"]["run_id"], "nonce": "run-" + uuid.uuid4().hex,
               "tenant_id": plan["tenant_id"], "prefix": plan["prefix"],
               "config_sha256": hashlib.sha256(canonical_bytes(config)).hexdigest(),
               "selected_demos": sorted(selected), "workspace_id": workspace_id,
               "pipeline_id": resources["pipeline"]["id"], "notebook_id": resources["notebook"]["id"],
               "manifest_uri": f"{root}/{config['data']['run_id']}/manifest.json",
               "deadline_utc": deadline.isoformat()}
    job = PipelineJob(state_dir, binding, None)
    payload = _measured_payload(config, binding, plan["identity_id"], lakehouse_id)
    with tempfile.TemporaryDirectory(prefix="producer-admission-") as directory:
        directory = Path(directory)
        config_path = directory / "effective-config.json"
        config_path.write_bytes(canonical_bytes(config))

        def token_ready():
            live_state = load_owned_state(StateStore(state_dir), plan)
            if live_state is None or live_state.get("pending") or live_state["resources"] != resources:
                raise Blocked("Owned producer state changed during admission; no job was started.")
            job.client = FabricClient(azure_cli_token(plan["tenant_id"], plan["identity_id"]))

        try:
            with StateStore(state_dir).lock():
                definition_update = _definition_update_record(StateStore(state_dir))
                if definition_update and definition_update["phase"] == "pending":
                    raise Blocked("A producer definition update is unresolved; no job was started.")
                job_id = job.submit(payload, admission=lambda: _fresh_admission(
                    config, config_path, plan["prefix"], state_dir, directory, selected, token_ready))
        except (FabricHttpError, FabricTransportError, OSError) as exc:
            if job.path.exists():
                raise Blocked("Producer submission outcome may be unknown; reconcile its journal before retrying.") from None
            raise Blocked(f"Producer admission failed before submission: {exc}") from None
    print(f"Producer pipeline job {job_id} submitted; inspect its journal before any retry.", file=sys.stderr,
          flush=True)
    try:
        return _wait_for_measured(job, job_id, config, selected, plan, lakehouse_id)
    except (StorageError, OSError) as exc:
        raise Blocked(f"Producer job {job_id} delivery or termination remains unconfirmed: {exc}") from None


def _write_stop_request(store, binding):
    marker = canonical_bytes({"version": 1, "run_id": binding["run_id"], "nonce": binding["nonce"],
                              "config_sha256": binding["config_sha256"]})
    path = f"Files/producer/runs/{binding['run_id']}/stop.json"
    try:
        try:
            store.create(path, marker)
        except (StorageConflict, StorageOutcomeUnknown):
            pass
        actual, _ = store.read(path)
    except (FileNotFoundError, StorageError):
        raise Blocked("Cooperative stop request outcome is unconfirmed; reconcile before retrying.") from None
    if actual != marker:
        raise Blocked("A different cooperative stop request exists; reconcile before proceeding.")


def _recover_measured(mode, job, config, selected, plan, lakehouse_id):
    snapshot = job.status_detail(include_notebook=True)
    child = snapshot.get("notebook")
    terminal = snapshot["status"] in ("Completed", "Failed", "Cancelled", "Deduped") and child is not None \
        and child["status"] in ("Completed", "Failed", "Cancelled")
    store = None
    action = "No cancellation was requested."
    if mode == "stop-producer" and not terminal:
        storage_token = azure_cli_token(plan["tenant_id"], plan["identity_id"], resource=STORAGE_RESOURCE)
        store = OneLakeStore(job.binding["workspace_id"], lakehouse_id, lambda: storage_token)
        _write_stop_request(store, job.binding)
        action = "A cooperative stop was requested; blocking calls and in-flight deliveries are not interrupted."
        deadline = time.monotonic() + STOP_CHILD_WAIT_SECONDS
        while time.monotonic() < deadline:
            snapshot = job.status_detail(timeout=min(30, max(0.01, deadline - time.monotonic())),
                                         include_notebook=True)
            child = snapshot.get("notebook")
            terminal = snapshot["status"] in ("Completed", "Failed", "Cancelled", "Deduped") \
                and child is not None and child["status"] in ("Completed", "Failed", "Cancelled")
            if terminal:
                break
            time.sleep(min(5, max(0, deadline - time.monotonic())))
    observations = [f"Recorded pipeline job {snapshot['job_id']} has live status {snapshot['status']}."]
    if child:
        observations.append(f"Linked notebook job {child['job_id']} has live status {child['status']}.")
    else:
        observations.append("Notebook linkage unconfirmed: " + snapshot.get("notebook_diagnostic", "not observed"))
    observations.append(action)
    if not terminal:
        observations.append("Producer termination unconfirmed; in-flight deliveries require reconciliation.")
        return observations
    try:
        if store is None:
            storage_token = azure_cli_token(plan["tenant_id"], plan["identity_id"], resource=STORAGE_RESOURCE)
            store = OneLakeStore(job.binding["workspace_id"], lakehouse_id, lambda: storage_token)
        verified, diagnostics = _manifest_summary(store, config, selected)
    except (StorageError, OSError, Blocked, ContractError) as exc:
        observations.append("Producer termination confirmed; delivery reconciliation is unconfirmed: " + str(exc))
        return observations
    observations.append("Producer termination confirmed; " +
                        ("recorded deliveries are verified." if verified else "in-flight deliveries require reconciliation."))
    return [*observations, *diagnostics]


def _recover_pipeline(mode, config, plan, state_dir, selected_demos):
    selected = _validate_run(config, selected_demos)
    if any(not plan[field] for field in ("tenant_id", "capacity_id", "identity_id")):
        raise Blocked("Recovery requires the original tenant, capacity and identity; termination unconfirmed.")
    state = load_owned_state(StateStore(state_dir), plan)
    if state is None or state.get("pending"):
        raise Blocked("Owned producer deployment is missing or pending; termination unconfirmed.")
    resources = state.get("resources", {})
    if any(kind not in resources for kind in ("workspace", "staging_lakehouse", "pipeline", "notebook")):
        raise Blocked("Owned producer resource identities are incomplete; termination unconfirmed.")
    workspace_id = resources["workspace"]["id"]
    lakehouse_id = resources["staging_lakehouse"]["id"]
    run_id = config["data"]["run_id"]
    try:
        fingerprint = hashlib.sha256(canonical_bytes(config)).hexdigest()
    except (TypeError, ValueError, UnicodeError):
        raise ContractError("The original effective configuration is not valid canonical JSON.") from None
    expected = {"run_id": run_id, "tenant_id": plan["tenant_id"], "prefix": plan["prefix"],
                "config_sha256": fingerprint, "selected_demos": sorted(selected),
                "workspace_id": workspace_id, "pipeline_id": resources["pipeline"]["id"],
                "notebook_id": resources["notebook"]["id"],
                "manifest_uri": (f"https://onelake.blob.fabric.microsoft.com/{workspace_id}/{lakehouse_id}/"
                                 f"Files/producer/runs/{run_id}/manifest.json")}
    job = PipelineJob.recover(state_dir, expected)
    try:
        client = FabricClient(azure_cli_token(plan["tenant_id"], plan["identity_id"]))
        principal_type = _workspace_principal(client, state, plan)
        _verify_item(client, state, plan, "pipeline", principal_type)
        _verify_item(client, state, plan, "notebook", principal_type)
        job.client = client
        try:
            measured = job.matches_payload(_measured_payload(config, job.binding, plan["identity_id"], lakehouse_id))
        except LifecycleBlocked:
            measured = False
        if measured:
            return _recover_measured(mode, job, config, selected, plan, lakehouse_id)
        snapshot = job.status_detail(
            timeout=STOP_CHILD_WAIT_SECONDS if mode == "stop-producer" else 30,
            include_notebook=True, wait_for_terminal_child=mode == "stop-producer")
    except (FabricHttpError, FabricTransportError) as exc:
        raise Blocked(f"{exc} Termination unconfirmed.") from None
    notebook = snapshot.get("notebook")
    observation = (
        f"Linked notebook job {notebook['job_id']} has live status {notebook['status']}."
        if notebook else f"Notebook linkage unconfirmed: {snapshot['notebook_diagnostic']}"
    )
    if notebook and snapshot.get("notebook_diagnostic"):
        observation += " " + snapshot["notebook_diagnostic"]
    action = ("Cancellation was not requested because complete stop safety is not yet verified."
              if mode == "stop-producer" else "No cancellation was requested.")
    blocker = ("Blocked: in-flight delivery reconciliation is unproven; producer termination unconfirmed."
               if notebook and all(job["status"] in ("Completed", "Failed", "Cancelled")
                                   for job in (snapshot, notebook))
               else "Blocked: notebook-child termination and in-flight delivery reconciliation are unproven; "
                    "termination unconfirmed.")
    return [f"Recorded pipeline job {snapshot['job_id']} has live status {snapshot['status']}.", observation,
            action, blocker]


def perform(mode, config, prefix, state_dir, selected_demos=None, yes=False, *, candidate_path=None,
            definition_kind=None, expected_definition_sha256=None):
    plan = parse_config(config, prefix)
    if mode == "update-definition":
        if (not yes or definition_kind not in ("notebook", "pipeline")
                or not re.fullmatch(r"[a-f0-9]{64}", expected_definition_sha256 or "") or selected_demos is not None):
            raise ContractError("update-definition requires --yes, --definition-kind and --expected-definition-sha256.")
        if not all(plan[field] for field in ("tenant_id", "capacity_id", "identity_id")):
            raise ContractError("update-definition requires the configured tenant, capacity and identity.")
        return _update_producer_definition(config, plan, state_dir, definition_kind, expected_definition_sha256)
    if definition_kind is not None or expected_definition_sha256 is not None:
        raise ContractError("Definition update arguments require --mode update-definition.")
    if mode == "cleanup" and not yes:
        raise ContractError("cleanup requires --yes.")
    if mode == "run-data":
        return _run_measured(config, plan, state_dir, selected_demos)
    if mode in ("producer-status", "stop-producer"):
        try:
            return _recover_pipeline(mode, config, plan, state_dir, selected_demos)
        except (Blocked, LifecycleBlocked) as exc:
            message = str(exc)
            if "termination unconfirmed" not in message.lower():
                message += " Termination unconfirmed."
            raise Blocked(message) from None
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
        if mode == "validate-only":
            raise Blocked("Deployment/readback requires " + ", ".join(missing) + ".")
        return ["Blocked: deployment/readback requires " + ", ".join(missing) + ".", *BLOCKERS]
    store = StateStore(state_dir)
    if mode == "validate-only":
        if selected_demos is None:
            raise Blocked("Selected destinations are required for producer readiness.")
        selected = _validate_run(config, selected_demos)
        targets = selected_candidates(config, selected)
        state = load_owned_state(store, plan)
        if state is None:
            raise Blocked("No owned producer deployment state exists.")
        if state.get("pending"):
            raise Blocked("Deployment has a pending operation; reconcile or resume it before validation.")
        if any(kind not in state["resources"] for kind in KINDS):
            raise Blocked("Owned deployment state does not yet contain all producer resources.")
        shared = config["shared"]
        if (shared.get("workspace_id") != state["resources"].get("workspace", {}).get("id")
                or shared.get("staging_lakehouse_id") != state["resources"].get("staging_lakehouse", {}).get("id")
                or shared.get("identity_id") != plan["identity_id"]):
            raise Blocked("Effective producer IDs differ from owned state.")
        token = azure_cli_token(plan["tenant_id"], plan["identity_id"])
        client = FabricClient(token)
        _verify_all(client, state, plan, config=config)
        clients = {name: client for name in selected}
        if "demo-2" in selected:
            clients["demo-2"] = FabricClient(token, skill="eventhouse-cli")
        table_client = None
        if "demo-3" in selected:
            storage_token = azure_cli_token(plan["tenant_id"], plan["identity_id"], resource=STORAGE_RESOURCE)
            table_client = OneLakeTableClient(storage_token)
        observations = read_selected_targets(config, selected, clients, table_client=table_client)
        try:
            require_write_roles(client, config, selected, targets, shared["workspace_id"], plan["identity_id"])
        except (ValueError, RuntimeError) as exc:
            raise Blocked(f"Producer write-role preflight failed: {exc}") from None
        return ["Producer definitions, selected target metadata and direct write roles verified. "
                "The notebook checks its actual runtime identity and access before writing.", *observations]
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
            _verify_all(client, state, plan, spec, config)
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
                    _create_resource(client, store, state, plan, kind, spec, config)
                if kind == "workspace":
                    _workspace_principal(client, state, plan)
                else:
                    principal_type = _workspace_principal(client, state, plan)
                    _verify_item(client, state, plan, kind, principal_type)
                    if kind == "environment_item":
                        _ensure_environment(client, store, state, spec)
                    elif kind in ("notebook", "pipeline"):
                        _verify_definition(client, state, kind, config)
        return ["Owned producer resources and published Environment libraries were read back; no data job was started and runtime readiness remains unproven.", *BLOCKERS]


class _ArgumentParser(argparse.ArgumentParser):
    def error(self, message):
        raise ContractError("Invalid or missing command-line arguments.")


def _result_target(args):
    if not Path(args.result).is_absolute():
        raise ContractError("config, state-dir and result must be absolute paths.")
    paths = tuple(Path(path) for path in (args.config, args.state_dir, args.result))
    config, directory, result = (path.resolve() for path in paths)
    protected = {config, directory / STATE_NAME, directory / LOCK_NAME, directory / DEFINITION_UPDATE_NAME}
    protected.update((HERE / name).resolve() for name in (
        "deploy.py", "test_deploy.py", "deployment-mcap.json", "generator.py", "fabric_assets.py",
        "notebook.ipynb", "pipeline-content.json", "delivery.py", "runner.py", "fabric_files.py",
        "environment.yml", "fabric_storage.py", "fabric_ledger.py", "fabric_delivery.py", "fabric_runner.py",
        "README.md"))
    source_path = result.is_relative_to(HERE) and result.suffix.lower() in (
        ".py", ".ipynb", ".yml", ".yaml", ".md")
    in_job_directory = any(
        part.casefold() == JOURNAL_DIR_NAME for path in (*paths, config, directory, result) for part in path.parts
    )
    if (result in protected or source_path or in_job_directory or result.name.casefold() in
            (STATE_NAME.casefold(), LOCK_NAME.casefold(), CANDIDATE_NAME.casefold(), DEFINITION_UPDATE_NAME.casefold())
            or config in (directory / STATE_NAME, directory / LOCK_NAME, directory / DEFINITION_UPDATE_NAME)):
        raise ContractError("result and state paths must not overwrite config, source files or owned state.")
    return result


def _envelope(mode, status="blocked", diagnostics=None):
    entries = list(diagnostics or [])
    if (mode in ("producer-status", "stop-producer") and status != "pass"
            and not any("termination unconfirmed" in entry.lower() or "producer termination confirmed" in entry.lower()
                        for entry in entries)):
        entries.append("Producer termination unconfirmed.")
    return {"contract_version": 1, "track": "shared", "mode": mode, "status": status,
            "ready": mode == "validate-only" and status == "pass", "diagnostics": entries}


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
    if mode not in ("dry-run", "validate-only", "deploy", "cleanup", "run-data", "resolve-candidate-ids",
                    "producer-status", "stop-producer"):
        mode = "unknown"
    result = _envelope(mode, "fail", [diagnostic])
    targets = _raw_options(argv, "--result")
    configs = _raw_options(argv, "--config")
    state_dirs = _raw_options(argv, "--state-dir")
    if (len(targets) != 1 or len(configs) != 1 or len(state_dirs) != 1
            or any(not Path(path).is_absolute() for path in (*targets, *configs, *state_dirs))):
        print(json.dumps(result), file=sys.stderr)
        return
    target = targets[0]
    if target:
        try:
            candidate = argparse.Namespace(result=target, config=configs[0], state_dir=state_dirs[0])
            _atomic_json(_result_target(candidate), result)
            return
        except (ContractError, OSError):
            pass
    print(json.dumps(result), file=sys.stderr)


def main(argv=None):
    parser = _ArgumentParser(add_help=False, allow_abbrev=False)
    parser.add_argument("--mode", required=True, choices=("dry-run", "validate-only", "deploy", "cleanup",
                                                          "run-data", "resolve-candidate-ids",
                                                          "producer-status", "stop-producer", "update-definition"))
    parser.add_argument("--config", required=True)
    parser.add_argument("--prefix", required=True)
    parser.add_argument("--state-dir", required=True)
    parser.add_argument("--result", required=True)
    parser.add_argument("--selected-demos")
    parser.add_argument("--definition-kind", choices=("notebook", "pipeline"))
    parser.add_argument("--expected-definition-sha256")
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
            config = strict_json_loads(Path(args.config).read_text(encoding="utf-8"))
        except (OSError, UnicodeError, ValueError):
            raise ContractError("Config is unreadable or is not valid JSON.") from None
        diagnostics = perform(args.mode, config, args.prefix, args.state_dir, args.selected_demos, args.yes,
                              candidate_path=candidate_path, definition_kind=args.definition_kind,
                              expected_definition_sha256=args.expected_definition_sha256)
        passed = args.mode in ("dry-run", "resolve-candidate-ids", "run-data", "update-definition", "validate-only")
        result = _envelope(args.mode, "pass" if passed else "blocked", diagnostics)
    except ContractError as exc:
        result = _envelope(args.mode, "fail", [str(exc)])
    except (Blocked, TargetReadbackBlocked, LifecycleBlocked) as exc:
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
