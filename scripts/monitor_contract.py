"""Shared configuration and evidence checks for the Demo 2 monitor."""

import datetime
import hashlib
import json
from pathlib import Path
import re
import uuid


ROOT = Path(__file__).resolve().parent.parent
PRODUCER_HEADROOM_SECONDS = 900 + 120 + 30


def strict_json(path):
    def object_pairs(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError(f"duplicate JSON key: {key}")
            result[key] = value
        return result

    def invalid_constant(value):
        raise ValueError(f"invalid JSON constant: {value}")

    with path.open(encoding="utf-8") as file:
        return json.load(file, object_pairs_hook=object_pairs, parse_constant=invalid_constant)


def deployment_scope(config, track, prefix, state_dir):
    if not isinstance(config, dict):
        raise ValueError("configuration must be an object")
    if track not in ("demo-1", "demo-2", "demo-3"):
        raise ValueError("invalid demo deployment scope")
    settings = config.get(track)
    if settings is None:
        settings = {}
    if not isinstance(settings, dict):
        raise ValueError(f"{track} must be an object")
    owner_prefix = settings.get("deployment_prefix")
    owner_dir = settings.get("deployment_state_dir")
    if owner_prefix is None and owner_dir is None:
        return prefix, Path(state_dir)
    if not isinstance(owner_prefix, str) or not re.fullmatch(r"[a-z0-9][a-z0-9-]{0,39}", owner_prefix):
        raise ValueError(f"{track}.deployment_prefix must be a safe prefix and paired with deployment_state_dir")
    if not isinstance(owner_dir, str) or not owner_dir.strip():
        raise ValueError(f"{track}.deployment_state_dir must be an existing absolute directory")
    directory = Path(owner_dir)
    if not directory.is_absolute() or not directory.is_dir():
        raise ValueError(f"{track}.deployment_state_dir must be an existing absolute directory")
    return owner_prefix, directory.resolve()


def demo2_deployment_scope(config, prefix, state_dir):
    return deployment_scope(config, "demo-2", prefix, state_dir)


def monitor_config(config):
    shared = config.get("shared")
    if not isinstance(shared, dict):
        raise ValueError("shared must contain the producer workspace and staging lakehouse IDs")
    try:
        workspace_id = str(uuid.UUID(shared["workspace_id"]))
        lakehouse_id = str(uuid.UUID(shared["staging_lakehouse_id"]))
    except (KeyError, ValueError, TypeError, AttributeError) as error:
        raise ValueError("shared workspace_id and staging_lakehouse_id must be UUIDs") from error
    root = (f"https://onelake.blob.fabric.microsoft.com/{workspace_id}/{lakehouse_id}"
            "/Files/producer/runs")
    location = shared.get("run_state_location")
    if location is not None and (not isinstance(location, str) or location.rstrip("/") != root):
        raise ValueError("shared.run_state_location must match the derived producer runs root")
    demo = config.get("demo-2") or {}
    verification_timeout = demo.get("verification_timeout_seconds")
    if verification_timeout is None:
        verification_timeout = config["execution"].get("verification_timeout_seconds")
    if type(verification_timeout) is not int or verification_timeout < 1:
        raise ValueError("Demo 2 verification timeout must be a positive integer")
    concurrency = config["execution"].get("max_concurrency", 4)
    if type(concurrency) is not int or not 1 <= concurrency <= 4:
        raise ValueError("execution.max_concurrency must be an integer between 1 and 4")
    waves = max(4, (4 + concurrency - 1) // concurrency + 1)
    duration = (waves * config["execution"]["track_timeout_seconds"]
                + config["data"]["max_duration_seconds"] + PRODUCER_HEADROOM_SECONDS + verification_timeout)
    try:
        datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(seconds=duration)
    except OverflowError as error:
        raise ValueError("Demo 2 monitor deadline bound is too large") from error
    return f"{root}/{config['data']['run_id']}/manifest.json", duration


def monitor_binding(config):
    data = config["data"]
    producer_config = {
        "run_id": data.get("run_id"), "batch_class": "measured", "start_date": data.get("start_date"),
        "seed": data.get("seed"), "date_count": data.get("date_count"), "products": data.get("product_count"),
        "customers": data.get("customer_count"), "file_count": data.get("file_count"),
        "rows_per_file": data.get("rows_per_file"), "interval_seconds": data.get("interval_seconds"),
    }
    source = (ROOT / "demo-data-generator" / "generator.py").read_bytes().decode("utf-8")
    generator_digest = hashlib.sha256(source.replace("\r\n", "\n").replace("\r", "\n").encode("utf-8")).hexdigest()
    targets = config["shared"]["target_bindings"]
    binding = {"targets": targets, "generator_sha256": generator_digest, "config": producer_config}
    encoded = json.dumps(binding, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                         allow_nan=False).encode("utf-8")
    return {"target_bindings": targets, "generator_sha256": generator_digest,
            "producer_config": producer_config, "monitor_binding_sha256": hashlib.sha256(encoded).hexdigest()}


def monitor_evidence(path, run_id, manifest_uri, max_duration, state, binding, require_future=True):
    evidence = strict_json(path)
    if not isinstance(evidence, dict) or type(evidence.get("contract_version")) is not int or evidence["contract_version"] != 1:
        raise ValueError("invalid monitor evidence version")
    for key in ("run_id", "job_id", "state", "manifest_uri", "deadline_utc"):
        if not isinstance(evidence.get(key), str) or not evidence[key].strip():
            raise ValueError(f"monitor evidence requires {key}")
    if evidence["run_id"] != run_id:
        raise ValueError("monitor is observing a different run")
    if evidence["manifest_uri"] != manifest_uri:
        raise ValueError("monitor is observing a different manifest")
    for key, expected in binding.items():
        if json.dumps(evidence.get(key), sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                      allow_nan=False) != json.dumps(expected, sort_keys=True, separators=(",", ":"),
                                                     ensure_ascii=False, allow_nan=False):
            raise ValueError(f"monitor {key} does not match the effective configuration")
    states = {"start": {"submitted", "running"}, "running": {"running"}, "completed": {"completed"}}
    if evidence["state"] not in states[state]:
        raise ValueError("monitor has an invalid lifecycle state")
    try:
        deadline = datetime.datetime.fromisoformat(evidence["deadline_utc"].replace("Z", "+00:00"))
    except ValueError as error:
        raise ValueError("invalid monitor deadline") from error
    now = datetime.datetime.now(datetime.timezone.utc)
    if deadline.tzinfo is None or (require_future and deadline <= now):
        raise ValueError("monitor deadline must be a timezone-aware timestamp and not expired during startup")
    if deadline > now + datetime.timedelta(seconds=max_duration):
        raise ValueError("monitor deadline exceeds the configured bound")
    return evidence


def matching_monitor_identity(first, second):
    for key in ("contract_version", "run_id", "job_id", "manifest_uri", "deadline_utc",
                "target_bindings", "generator_sha256", "producer_config", "monitor_binding_sha256"):
        if json.dumps(first[key], sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                      allow_nan=False) != json.dumps(second[key], sort_keys=True, separators=(",", ":"),
                                                     ensure_ascii=False, allow_nan=False):
            raise ValueError(f"monitor {key} does not match the start record")
