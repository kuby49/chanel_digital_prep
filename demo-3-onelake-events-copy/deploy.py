"""Fail-closed Demo 3 orchestration; owned-shell provisioning is explicit."""

import argparse
import json
import os
import re
import sys
import uuid
from contextlib import contextmanager
from pathlib import Path


TRACK = "demo-3"
FACT = "dbo.factsales"
STATE_FILE = "demo-3-state.json"
LOCK_FILE = "demo-3-state.lock"
CANDIDATE_FILE = "demo-3-target-candidate.json"


class CandidateBlocked(Exception):
    pass


def _unique_pairs(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON field")
        result[key] = value
    return result


def _load_json(path):
    with path.open(encoding="utf-8") as file:
        return json.load(file, object_pairs_hook=_unique_pairs)


def _guid(value):
    if not isinstance(value, str):
        raise ValueError("identifier must be a canonical UUID")
    try:
        parsed = uuid.UUID(value)
    except ValueError as exc:
        raise ValueError("identifier must be a canonical UUID") from exc
    if parsed.int == 0 or str(parsed) != value:
        raise ValueError("identifier must be a nonzero canonical UUID")
    return value


@contextmanager
def _state_lock(state_dir):
    lock = state_dir / LOCK_FILE
    token = str(uuid.uuid4())
    try:
        with lock.open("x", encoding="utf-8") as file:
            file.write(token + "\n")
            file.flush()
            os.fsync(file.fileno())
    except FileExistsError as exc:
        raise CandidateBlocked("Demo 3 state is locked; stale locks require manual recovery.") from exc
    except OSError as exc:
        raise CandidateBlocked("Could not acquire the Demo 3 state lock.") from exc
    try:
        yield
    finally:
        try:
            if lock.read_text(encoding="utf-8") != token + "\n":
                raise OSError("lock ownership changed")
            lock.unlink()
        except OSError as exc:
            raise CandidateBlocked("Could not safely release the Demo 3 state lock.") from exc


def _confirmed_resource(record, resource_type, owner_id, marker):
    if not isinstance(record, dict) or record.get("status") != "confirmed":
        raise CandidateBlocked(f"{resource_type} ownership is not confirmed.")
    if record.get("type") != resource_type or record.get("created_by_owner") != owner_id:
        raise CandidateBlocked(f"{resource_type} was not created by this owner.")
    try:
        resource_id = _guid(record.get("id"))
        _guid(record.get("create_intent_id"))
    except ValueError as exc:
        raise CandidateBlocked(f"{resource_type} ownership identifiers are invalid.") from exc
    readback = record.get("readback")
    if (not isinstance(readback, dict) or readback.get("id") != resource_id
            or readback.get("type") != resource_type
            or readback.get("owner_marker") != marker):
        raise CandidateBlocked(f"{resource_type} ownership readback is missing or inconsistent.")
    return resource_id


def resolve_target_candidate(config, prefix, state_dir, result_path):
    try:
        tenant = _guid(config.get("tenant_id"))
    except ValueError as exc:
        raise ValueError("tenant_id must be a nonzero canonical UUID") from exc
    if not state_dir.is_dir():
        raise CandidateBlocked("Demo 3 ownership state directory does not exist.")
    with _state_lock(state_dir):
        try:
            journal = _load_json(state_dir / STATE_FILE)
        except (OSError, ValueError, json.JSONDecodeError) as exc:
            raise CandidateBlocked("Demo 3 ownership journal is missing or invalid.") from exc
        if (not isinstance(journal, dict) or type(journal.get("contract_version")) is not int
                or journal["contract_version"] != 1
                or journal.get("track") != TRACK or journal.get("tenant_id") != tenant
                or journal.get("prefix") != prefix):
            raise CandidateBlocked("Demo 3 ownership journal identity does not match this invocation.")
        if journal.get("pending_operations") != [] or journal.get("uncertain_operations") != []:
            raise CandidateBlocked("Demo 3 ownership journal has pending or uncertain operations.")
        owner = journal.get("owner")
        if not isinstance(owner, dict):
            raise CandidateBlocked("Demo 3 ownership identity is missing.")
        try:
            owner_id = _guid(owner.get("id"))
        except ValueError as exc:
            raise CandidateBlocked("Demo 3 ownership identity is invalid.") from exc
        marker = f"{prefix}-demo3-{owner_id}"
        if owner.get("marker") != marker:
            raise CandidateBlocked("Demo 3 ownership marker does not match its owner.")
        workspace = journal.get("workspace")
        lakehouse = journal.get("lakehouse")
        workspace_id = _confirmed_resource(workspace, "Workspace", owner_id, marker)
        item_id = _confirmed_resource(lakehouse, "Lakehouse", owner_id, marker)
        if (lakehouse.get("workspace_id") != workspace_id
                or lakehouse["readback"].get("workspace_id") != workspace_id):
            raise CandidateBlocked("Lakehouse parent does not match the owned workspace.")
        fact = journal.get("fact")
        expected_fact = {"status": "confirmed", "workspace_id": workspace_id,
                         "item_id": item_id, "name": FACT}
        if not isinstance(fact, dict) or any(
            fact.get(key) != value for key, value in expected_fact.items()
        ):
            raise CandidateBlocked("Demo 3 fact binding is missing or inconsistent.")
        target = {"workspace_id": workspace_id, "item_id": item_id, "fact": FACT}
        demo = config[TRACK]
        for field, expected in (("workspace_id", workspace_id),
                                ("lakehouse_id", item_id), ("destination_table", FACT)):
            supplied = demo.get(field)
            if supplied is not None and supplied != expected:
                raise CandidateBlocked(f"Configured {field} conflicts with the owned target.")
        candidate = {"contract_version": 1, "track": TRACK, "tenant_id": tenant,
                     "prefix": prefix, "target": target}
        candidate_path = result_path.parent / CANDIDATE_FILE
        try:
            with candidate_path.open("x", encoding="utf-8") as file:
                json.dump(candidate, file, indent=2)
                file.write("\n")
                file.flush()
                os.fsync(file.fileno())
        except FileExistsError as exc:
            raise CandidateBlocked("Target candidate already exists; it was not overwritten.") from exc
        except OSError as exc:
            raise CandidateBlocked("Could not persist the target candidate.") from exc


def verification_timeout(config):
    execution = config.get("execution", {})
    if not isinstance(execution, dict):
        raise ValueError("execution must be an object")
    demo = config["demo-3"]
    for settings in (execution, demo):
        if "verification_timeout_seconds" in settings:
            value = settings["verification_timeout_seconds"]
            if type(value) is not int or value <= 0:
                raise ValueError("verification_timeout_seconds must be a positive integer")
    return demo.get("verification_timeout_seconds", execution.get("verification_timeout_seconds", 900))


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", required=True, choices=(
        "dry-run", "validate-only", "deploy", "cleanup", "resolve-target-candidate",
        "provision-owned", "verify-owned"))
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--prefix", required=True)
    parser.add_argument("--state-dir", required=True, type=Path)
    parser.add_argument("--result", required=True, type=Path)
    parser.add_argument("--yes", action="store_true")
    args = parser.parse_args(argv)
    result = {
        "contract_version": 1, "track": TRACK, "mode": args.mode,
        "status": "blocked", "ready": False, "diagnostics": [],
    }
    if not args.result.is_absolute():
        print("Result path must be absolute", file=sys.stderr)
        return 1
    protected = (args.config, args.state_dir / STATE_FILE, args.state_dir / LOCK_FILE,
                 args.result.parent / CANDIDATE_FILE,
                 args.state_dir / "demo3-customer-binding-state.json",
                 args.state_dir / "demo3-native-binding-state.json",
                 args.state_dir / "demo3-native-rule-state.json",
                 args.state_dir / "demo3-staging-access-state.json")
    if args.result.resolve() in {path.resolve() for path in protected}:
        print("Result path conflicts with input, state, or candidate", file=sys.stderr)
        return 1
    try:
        if not all(path.is_absolute() for path in (args.config, args.state_dir)):
            raise ValueError("config and state-dir paths must be absolute")
        if not re.fullmatch(r"[a-z0-9](?:[a-z0-9-]*[a-z0-9])?", args.prefix):
            raise ValueError("prefix must contain lowercase letters, digits, or interior hyphens")
        config = _load_json(args.config)
        if (not isinstance(config, dict) or type(config.get("contract_version")) is not int
                or config["contract_version"] != 1):
            raise ValueError("configuration contract_version must be 1")
        if not isinstance(config.get("demo-3"), dict):
            raise ValueError("configuration must contain a demo-3 object")
        for field in ("customer_binding_journal", "native_binding_journal"):
            selected_journal = config["demo-3"].get(field)
            if isinstance(selected_journal, str):
                if args.result.resolve() == (args.state_dir / selected_journal).resolve():
                    print("Result path conflicts with customer binding journal", file=sys.stderr)
                    return 1
        verification_timeout(config)
        if args.mode == "cleanup" and not args.yes:
            raise ValueError("cleanup requires --yes; no resources were removed")
        if args.mode == "dry-run":
            result["status"] = "pass"
            result["diagnostics"].append(
                "Offline contract check only; deployment and runtime readiness are not established."
            )
        elif args.mode == "resolve-target-candidate":
            args.result.parent.mkdir(parents=True, exist_ok=True)
            resolve_target_candidate(config, args.prefix, args.state_dir, args.result)
            result["status"] = "pass"
            result["diagnostics"].append(
                "Local ownership candidate only; live identity, access, and readiness are not established."
            )
        elif args.mode == "validate-only" and any(
            config["demo-3"].get(field) is True for field in ("customer_preflight", "native_preflight")
        ):
            from customer_preflight import validate

            result["diagnostics"].extend(validate(config, args.prefix, args.state_dir))
            result.update(status="pass", ready=True)
        elif args.mode in ("provision-owned", "verify-owned"):
            from ownership import FabricControl, provision

            create = args.mode == "provision-owned"
            if create and not args.yes:
                raise ValueError("provision-owned requires --yes")
            api = FabricControl.from_cli(_guid(config.get("tenant_id")))
            if create:
                args.state_dir.mkdir(parents=True, exist_ok=True)
            provision(config, args.prefix, args.state_dir, api, create=create)
            result["diagnostics"].append(
                "Owned workspace and Lakehouse verified. No fact table, writer fence, or "
                "production pipeline readiness established."
            )
        else:
            result["diagnostics"].append(
                "Blocked: production exact-file Copy, serialized writes, monitoring, event binding, data correctness, "
                "duplicate-retry safety, safe stopping and query-surface readiness are not implemented or verified. "
                "The no-missed-notification assumption is unmeasured; missed-notification recovery is not a "
                "prerequisite, but any visible failed run must be reported. No Fabric changes made."
            )
    except CandidateBlocked as exc:
        result["diagnostics"].append(str(exc))
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        result["status"] = "fail"
        result["diagnostics"].append(f"Invalid local input: {type(exc).__name__}")
    try:
        args.result.parent.mkdir(parents=True, exist_ok=True)
        args.result.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    except OSError as exc:
        print(f"Could not write result: {type(exc).__name__}", file=sys.stderr)
        return 1
    print(json.dumps(result))
    return 0 if result["status"] == "pass" else 1


if __name__ == "__main__":
    sys.modules.setdefault("deploy", sys.modules[__name__])
    sys.exit(main())
