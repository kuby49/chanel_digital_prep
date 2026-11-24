"""Presenter controls for one owned, bounded native MLV lineage schedule."""

import argparse
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import re
import sys

from definition_plan import build_refresh_schedule_request
from fresh_deploy import _persist
from ownership import (
    STATE_FILE, CandidateBlocked, _resource, _state_lock, _unique_pairs, _uuid,
)
from provision_synthetic import FabricClient, _object_response, _verify_confirmed_receipt


TERMINAL = ("Completed", "Failed", "Cancelled", "Deduped")
MARGIN_MINUTES = 5


def recurring_run(data):
    shape = (data.get("file_count"), data.get("rows_per_file"))
    if (any(type(data.get(key)) is not int for key in ("file_count", "rows_per_file"))
            or data.get("currency") != "USD"):
        raise CandidateBlocked("Customer runs require explicit integer file/row counts and USD.")
    if shape == (1, 10):
        return False
    if shape != (10, 1000) or type(data.get("interval_seconds")) is not int or data["interval_seconds"] != 60:
        raise CandidateBlocked("Supported customer runs are 1x10 or recurring 10x1000 at 60-second source intervals.")
    return True


def schedule_file(state_dir, config):
    run = config["data"].get("run_id")
    if not isinstance(run, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}", run):
        raise ValueError("A safe run_id is required for the schedule journal.")
    return state_dir / f"demo-1-refresh-{run}.json"


def _read(path):
    if path.is_symlink() or not path.is_file():
        raise CandidateBlocked(f"Missing regular local journal: {path.name}")
    with path.open(encoding="utf-8") as file:
        value = json.load(file, object_pairs_hook=_unique_pairs)
    if not isinstance(value, dict):
        raise CandidateBlocked("Schedule/ownership journal must be an object.")
    return value


def _binding(config, prefix, journal):
    demo, shared = config["demo-1"], config["shared"]
    owner = journal.get("owner")
    if (type(journal.get("contract_version")) is not int or journal["contract_version"] != 1
            or journal.get("track") != "demo-1"
            or journal.get("tenant_id") != config.get("tenant_id") or journal.get("prefix") != prefix
            or journal.get("pending_operations") != [] or journal.get("uncertain_operations") != []
            or not isinstance(owner, dict)):
        raise CandidateBlocked("Original ownership journal is mismatched or unresolved.")
    owner_id = _uuid(owner.get("id"))
    principal = _uuid(shared.get("identity_id"))
    if owner.get("principal_id") != principal or owner.get("marker") != f"{prefix}-demo1-{owner_id}":
        raise CandidateBlocked("Schedule principal/marker does not match the original owner.")
    workspace, item = _uuid(demo.get("workspace_id")), _uuid(demo.get("lakehouse_id"))
    for kind, resource_id, path, name in (
        ("Workspace", workspace, "/v1/workspaces", demo.get("workspace_name")),
        ("Lakehouse", item, f"/v1/workspaces/{workspace}/lakehouses", demo.get("lakehouse_name")),
    ):
        resource = journal.get(kind.lower())
        _resource(resource, kind, owner_id, name, owner["marker"])
        if (resource.get("id") != resource_id or resource.get("create_path") != path
                or not isinstance(resource.get("create_request"), dict)):
            raise CandidateBlocked("Schedule target conflicts with retained resource ownership.")
        _verify_confirmed_receipt(resource, kind, path, resource_id)
        if kind == "Lakehouse" and resource.get("workspace_id") != workspace:
            raise CandidateBlocked("Schedule Lakehouse parent is mismatched.")
    return {
        "owner_id": owner_id, "principal_id": principal, "tenant_id": _uuid(config.get("tenant_id")),
        "prefix": prefix, "workspace_id": workspace, "lakehouse_id": item,
        "data": config["data"], "demo": demo,
    }


def _time(value):
    if not isinstance(value, str):
        raise CandidateBlocked("Schedule start/end time is missing.")
    try:
        # Fabric can return UTC timestamps without the Z suffix.
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        if parsed.utcoffset() != timedelta(0):
            raise ValueError
        return parsed
    except ValueError:
        raise CandidateBlocked("Schedule timestamps must be valid UTC times.") from None


def _check_payload(config, request):
    if not isinstance(request, dict) or not isinstance(request.get("configuration"), dict):
        raise CandidateBlocked("Retained schedule request is missing.")
    settings = request["configuration"]
    start, end = _time(settings.get("startDateTime")), _time(settings.get("endDateTime"))
    if end - start > timedelta(minutes=60):
        raise CandidateBlocked("Demo schedules must end within 60 minutes of their start.")
    expected = build_refresh_schedule_request(
        start.isoformat(), end.isoformat(), file_count=config["data"]["file_count"],
        interval_seconds=config["data"]["interval_seconds"], processing_margin_minutes=MARGIN_MINUTES,
    )
    if (request.get("enabled") is not True or set(request) != set(expected)
            or {**settings, "startDateTime": expected["configuration"]["startDateTime"],
                "endDateTime": expected["configuration"]["endDateTime"]} != expected["configuration"]):
        raise CandidateBlocked("Only the explicitly bounded five-minute full-lineage request is supported.")
    return start, end


def _check_live(live, record, enabled):
    if not isinstance(live, dict) or not isinstance(live.get("configuration"), dict):
        raise CandidateBlocked("Native schedule readback is missing.")
    expected, actual = record["request"]["configuration"], live["configuration"]
    if (live.get("id") != record.get("schedule_id") or live.get("enabled") is not enabled
            or not isinstance(live.get("owner"), dict)
            or live["owner"].get("id") != record["binding"]["principal_id"]
            or live.get("executionData") not in (None, {})
            or set(actual) != set(expected)
            or any(actual.get(key) != expected[key] for key in ("type", "interval", "localTimeZoneId"))
            or any(_time(actual.get(key)) != _time(expected[key]) for key in ("startDateTime", "endDateTime"))):
        raise CandidateBlocked("Native schedule owner, scope, enabled state or bounds do not match its receipt.")


def _record(config, prefix, state_dir, journal):
    record = _read(schedule_file(state_dir, config))
    if (type(record.get("contract_version")) is not int or record["contract_version"] != 1
            or record.get("binding") != _binding(config, prefix, journal)
            or record.get("status") not in ("confirmed", "stop-pending", "stopped")
            or record.get("create_status") != 201):
        raise CandidateBlocked("Native schedule has no matching confirmed create evidence; reconcile manually.")
    _uuid(record.get("schedule_id"))
    _check_payload(config, record.get("request"))
    _check_live(record.get("create_response"), record, True)
    return record


def _page(client, path):
    _, body = _object_response(client, path)
    if (not isinstance(body.get("value"), list) or body.get("continuationToken")
            or body.get("continuationUri") or any(not isinstance(row, dict) for row in body["value"])):
        raise CandidateBlocked("Native schedule/job listing is incomplete or paginated.")
    return body["value"]


def check_idle(history, item):
    if not history or any(row.get("status") not in TERMINAL for row in history):
        raise CandidateBlocked("Native MLV history is missing or a lineage refresh is already active.")
    latest = max(history, key=lambda row: row.get("startTimeUtc") or "")
    if latest.get("status") != "Completed" or latest.get("itemId") != item:
        raise CandidateBlocked("The latest native MLV lineage refresh did not complete successfully.")


def check_schedule(config, prefix, state_dir, journal, schedules, *, preparing=False):
    path = schedule_file(state_dir, config)
    if preparing and not path.exists():
        if any(row.get("enabled") is not False for row in schedules):
            raise CandidateBlocked("Cannot prepare while an unowned/unknown schedule is enabled.")
        return
    record = _record(config, prefix, state_dir, journal)
    matches = [row for row in schedules if row.get("id") == record["schedule_id"]]
    if len(matches) != 1 or any(row.get("enabled") is not False for row in schedules if row not in matches):
        raise CandidateBlocked("Owned schedule is missing or a competing schedule is enabled.")
    _check_live(matches[0], record, True)
    start, end = _check_payload(config, record["request"])
    now = datetime.now(timezone.utc)
    minimum_remaining = timedelta(seconds=9 * 60, minutes=5 + MARGIN_MINUTES)
    if record["status"] != "confirmed" or start <= now or end - now < minimum_remaining:
        raise CandidateBlocked("Admission needs the owned schedule's future start and sufficient bounded run window.")


def plan(config, start_delay_minutes=10, duration_minutes=30):
    if not recurring_run(config["data"]):
        raise CandidateBlocked("Managed recurring schedules require the exact 10x1000/60s customer run.")
    if type(start_delay_minutes) is not int or not 1 <= start_delay_minutes <= 30:
        raise ValueError("start-delay-minutes must be between 1 and 30.")
    if type(duration_minutes) is not int or not 1 <= duration_minutes <= 60:
        raise ValueError("duration-minutes must be between 1 and 60.")
    start = datetime.now(timezone.utc).replace(microsecond=0) + timedelta(minutes=start_delay_minutes)
    end = start + timedelta(minutes=duration_minutes)
    payload = build_refresh_schedule_request(
        start.isoformat(), end.isoformat(), file_count=10, interval_seconds=60,
        processing_margin_minutes=MARGIN_MINUTES,
    )
    return payload


def manage(config, prefix, state_dir, action, *, yes=False, start_delay_minutes=10, duration_minutes=30):
    if action == "plan":
        return {"action": action, "status": "pass", "request": plan(config, start_delay_minutes, duration_minutes)}
    if action not in ("start", "status", "stop", "export-native"):
        raise ValueError("Unknown native schedule action.")
    if action in ("start", "stop") and not yes:
        raise ValueError(f"{action} requires --yes; no cloud mutation was attempted.")
    if not recurring_run(config["data"]):
        raise CandidateBlocked("Managed recurring schedules require the exact 10x1000/60s customer run.")
    if action == "start":
        if schedule_file(state_dir, config).exists():
            raise CandidateBlocked("A journal already exists for this run; use status/stop, never repeat its POST.")
        from customer_validation import validate_customer

        validate_customer(config, prefix, state_dir, _preparing_schedule=True)
    with _state_lock(state_dir):
        journal = _read(state_dir / STATE_FILE)
        binding = _binding(config, prefix, journal)
        if action == "export-native":
            record = _record(config, prefix, state_dir, journal)
            if (record["status"] != "confirmed"
                    or record["create_response"]["owner"].get("type")
                    not in ("User", "ServicePrincipal", "ManagedIdentity")):
                raise CandidateBlocked("Native export requires a confirmed setup schedule and known owner type.")
            receipt = {
                "contract_version": 1, "track": "demo-1", "status": "confirmed",
                "setup_run_id": config["data"]["run_id"], "schedule_id": record["schedule_id"],
                **{key: binding[key] for key in ("tenant_id", "principal_id", "workspace_id", "lakehouse_id")},
                "request": record["request"],
                "create_response": {key: value for key, value in record["create_response"].items() if key in (
                    "id", "enabled", "configuration", "owner", "executionData")},
            }
            return {"action": action, "status": "pass", "native_schedule": receipt,
                    "diagnostics": ["Local schedule receipt export only; no live readiness or creator attestation."]}
        azure_config = Path(os.environ.get("AZURE_CONFIG_DIR", ""))
        if not azure_config.is_absolute() or not azure_config.is_dir():
            raise CandidateBlocked("Select the existing isolated AZURE_CONFIG_DIR.")
        client = FabricClient(binding["tenant_id"], binding["principal_id"], 300, azure_config)
        root = (f"/v1/workspaces/{binding['workspace_id']}/lakehouses/{binding['lakehouse_id']}"
                "/jobs/refreshMaterializedLakeViews")
        path = schedule_file(state_dir, config)
        if action == "start":
            if path.exists():
                raise CandidateBlocked("Schedule state appeared concurrently; no POST was attempted.")
            check_idle(_page(client, root + "/instances"), binding["lakehouse_id"])
            check_schedule(config, prefix, state_dir, journal, _page(client, root + "/schedules"), preparing=True)
            record = {"contract_version": 1, "binding": binding, "status": "pending",
                      "request": plan(config, start_delay_minutes, duration_minutes)}
            _persist(path, record)
            status, _, body, readable = client.request("POST", root + "/schedules", record["request"])
            record.update(create_status=status, create_response=body if readable else None)
            _persist(path, record)
            if status == 201 and readable and isinstance(body, dict):
                record["schedule_id"] = _uuid(body.get("id"))
            _persist(path, record)
            if status != 201 or "schedule_id" not in record:
                raise CandidateBlocked("Schedule create was not confirmed; preserve pending state and reconcile, no retry.")
            _check_live(body, record, True)
            _, live = _object_response(client, root + "/schedules/" + record["schedule_id"])
            _check_live(live, record, True)
            record["status"] = "confirmed"
            _persist(path, record)
        else:
            record = _record(config, prefix, state_dir, journal)
            _, live = _object_response(client, root + "/schedules/" + record["schedule_id"])
            if type(live.get("enabled")) is not bool:
                raise CandidateBlocked("Native schedule enabled state is unknown.")
            _check_live(live, record, live["enabled"])
            if action == "stop":
                if live["enabled"]:
                    if record["status"] != "confirmed":
                        raise CandidateBlocked("Unresolved stop; reconcile the retained schedule before any retry.")
                    record["status"] = "stop-pending"
                    _persist(path, record)
                    request = {**record["request"], "enabled": False}
                    status, _, _, _ = client.request("PATCH", root + "/schedules/" + record["schedule_id"], request)
                    if status != 200:
                        raise CandidateBlocked(f"Schedule stop returned HTTP {status}; no automatic retry.")
                    _, live = _object_response(client, root + "/schedules/" + record["schedule_id"])
                _check_live(live, record, False)
                record["status"] = "stopped"
                _persist(path, record)
        jobs = _page(client, root + "/instances")
        active = [row.get("id") for row in jobs if row.get("status") not in TERMINAL]
        return {
            "action": action, "status": "blocked" if action == "stop" and active else "pass",
            "schedule_id": record["schedule_id"], "request": record["request"],
            "enabled": live["enabled"], "active_lineage_jobs": active,
            "lineage_jobs": [
                {key: row.get(key) for key in ("id", "itemId", "status", "invokeType", "startTimeUtc", "endTimeUtc")}
                for row in jobs
            ],
            "diagnostics": [
                "Schedule disabled; wait for active/unknown lineage jobs to become terminal." if action == "stop" and active
                else "Owned native schedule readback verified; this is not producer admission.",
                "Source arrivals are 60 seconds apart; downstream scheduled refresh is five minutes plus processing.",
                "No refresh job was submitted or cancelled by this command; stop does not cancel unattributed jobs.",
                "Reconcile only customer BatchIDs against the producer manifest; exclude bootstrap/synthetic baseline.",
            ],
        }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("plan", "start", "status", "stop", "export-native"))
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--state-dir", type=Path)
    parser.add_argument("--prefix")
    parser.add_argument("--start-delay-minutes", type=int, default=10)
    parser.add_argument("--duration-minutes", type=int, default=30)
    parser.add_argument("--yes", action="store_true")
    args = parser.parse_args()
    try:
        if not args.config.is_absolute():
            raise ValueError("config must be absolute.")
        config = _read(args.config)
        if (type(config.get("contract_version")) is not int or config["contract_version"] != 1
                or any(not isinstance(config.get(key), dict) for key in ("demo-1", "shared", "data"))):
            raise ValueError("A complete version-1 customer configuration is required.")
        selected_state = args.state_dir or config["demo-1"].get("deployment_state_dir")
        if not isinstance(selected_state, (str, Path)):
            raise ValueError("The original deployment_state_dir or --state-dir is required.")
        state = Path(selected_state)
        prefix = args.prefix or config.get("prefix")
        if (not state.is_absolute() or not state.is_dir() or not isinstance(prefix, str)
                or not prefix or prefix != config.get("prefix")):
            raise ValueError("Original absolute state-dir and matching prefix are required.")
        result = manage(config, prefix, state, args.action, yes=args.yes,
                        start_delay_minutes=args.start_delay_minutes, duration_minutes=args.duration_minutes)
    except CandidateBlocked as error:
        result = {"action": args.action, "status": "blocked", "diagnostics": [str(error)]}
    except (OSError, ValueError) as error:
        result = {"action": args.action, "status": "fail", "diagnostics": [str(error)]}
    print(json.dumps(result))
    return 0 if result["status"] == "pass" else 1


if __name__ == "__main__":
    sys.exit(main())
