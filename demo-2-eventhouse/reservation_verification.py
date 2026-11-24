"""Verify producer registry bindings without treating a manifest as an allocator."""

import hashlib
import json
import re
import uuid
from dataclasses import asdict


OWNER = "chanel_digital_prep/demo-data-generator"
DEMOS = frozenset(("demo-1", "demo-2", "demo-3"))
MAX_ID = 2**63 - 1


class ReservationError(ValueError):
    pass


def _canonical(value):
    try:
        return json.dumps(
            value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
        ).encode("utf-8")
    except (TypeError, ValueError, UnicodeError, RecursionError) as exc:
        raise ReservationError("Reservation evidence cannot be canonically encoded") from exc


def _digest(value):
    return hashlib.sha256(_canonical(value)).hexdigest()


def _uuid(value):
    try:
        parsed = uuid.UUID(value)
        return isinstance(value, str) and str(parsed) == value and parsed.int != 0
    except (ValueError, TypeError, AttributeError):
        return False


def _targets(value, *, allow_empty=False):
    if not isinstance(value, dict) or (not value and not allow_empty) or any(
        name not in DEMOS for name in value
    ):
        raise ReservationError("Allocator target bindings are invalid")
    for target in value.values():
        if (
            not isinstance(target, dict)
            or set(target) != {"workspace_id", "item_id", "fact"}
            or not _uuid(target["workspace_id"])
            or not _uuid(target["item_id"])
            or not isinstance(target["fact"], str)
            or not re.fullmatch(r"[A-Za-z0-9_][A-Za-z0-9_.-]{0,255}", target["fact"])
        ):
            raise ReservationError("Allocator target identity is invalid")
    return value


def _integer(value, *, signed):
    pattern = r"(?:0|-?[1-9][0-9]*)" if signed else r"[1-9][0-9]*"
    if (
        not isinstance(value, str)
        or len(value.lstrip("-")) > 19
        or not re.fullmatch(pattern, value)
    ):
        raise ReservationError("Allocator range must use canonical BIGINT strings")
    number = int(value)
    if not -(2**63) <= number <= MAX_ID or (not signed and number < 1):
        raise ReservationError("Allocator range exceeds BIGINT")
    return number


def strict_json_object(raw):
    def unique_pairs(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("duplicate JSON key")
            result[key] = value
        return result

    def invalid_constant(value):
        raise ValueError(f"invalid JSON constant: {value}")

    try:
        if isinstance(raw, bytes):
            if len(raw) > 16 * 1024 * 1024:
                raise ValueError("JSON object is too large")
            raw = raw.decode("utf-8")
        elif not isinstance(raw, str) or len(raw.encode("utf-8")) > 16 * 1024 * 1024:
            raise ValueError("JSON object is invalid or too large")
        value = json.loads(raw, object_pairs_hook=unique_pairs, parse_constant=invalid_constant)
    except (TypeError, ValueError, UnicodeError, RecursionError) as exc:
        raise ReservationError("Allocator evidence is not strict JSON") from exc
    if not isinstance(value, dict):
        raise ReservationError("Allocator evidence must be a JSON object")
    return value


def verify_registry_binding(
    registry, manifest, *, expected_scope, expected_config, expected_targets,
    expected_generator_sha256, generator
):
    """Validate a registry snapshot and its selected run against independent expectations.

    The caller must separately establish registry ownership and a quiesced retained-key
    inventory, and must bind a versioned read to the monitor's immutable run state.
    """
    scope_fields = {
        "version", "kind", "owner", "tenant_id", "producer_workspace_id", "staging_lakehouse_id"
    }
    if (
        not isinstance(expected_scope, dict)
        or set(expected_scope) != scope_fields
        or type(expected_scope["version"]) is not int
        or expected_scope["version"] != 1
        or expected_scope["kind"] != "fabric"
        or expected_scope["owner"] != OWNER
        or any(not _uuid(expected_scope[key]) for key in
               ("tenant_id", "producer_workspace_id", "staging_lakehouse_id"))
    ):
        raise ReservationError("Expected allocator scope is invalid")
    fields = {
        "version", "scope", "allocator_id", "target_bindings", "retained_ranges",
        "reservations", "integrity_sha256",
    }
    if (
        not isinstance(registry, dict)
        or set(registry) != fields
        or type(registry["version"]) is not int
        or registry["version"] != 1
        or _canonical(registry["scope"]) != _canonical(expected_scope)
        or registry["allocator_id"] != _digest(expected_scope)
        or registry["integrity_sha256"] != _digest({
            key: value for key, value in registry.items() if key != "integrity_sha256"
        })
    ):
        raise ReservationError("Allocator registry identity or integrity does not match")
    targets = _targets(registry["target_bindings"], allow_empty=True)
    expected_targets = _targets(expected_targets)
    if "demo-2" not in expected_targets:
        raise ReservationError("The selected allocator targets must include Demo 2")
    ranges = []
    if not isinstance(registry["retained_ranges"], list):
        raise ReservationError("Retained-key inventory is invalid")
    for record in registry["retained_ranges"]:
        if (
            not isinstance(record, dict)
            or set(record) != {"source", "start", "end"}
            or not isinstance(record["source"], str)
            or not 1 <= len(record["source"]) <= 512
            or any(ord(char) < 32 for char in record["source"])
        ):
            raise ReservationError("Retained-key inventory entry is invalid")
        start, end = _integer(record["start"], signed=True), _integer(record["end"], signed=True)
        if start > end:
            raise ReservationError("Retained-key range is invalid")
        ranges.append((start, end))

    try:
        generator_digest = generator.generator_source_sha256()
        config_fields = set(asdict(generator.RunConfig("sample")))
    except (AttributeError, OSError, TypeError, ValueError, RuntimeError) as exc:
        raise ReservationError("The bound generator source cannot be verified") from exc
    records = registry["reservations"]
    if (
        not isinstance(records, dict)
        or not isinstance(expected_config, dict)
        or set(expected_config) != config_fields
        or not isinstance(expected_config.get("run_id"), str)
    ):
        raise ReservationError("Allocator reservations or expected configuration are invalid")
    if (
        not isinstance(expected_generator_sha256, str)
        or not re.fullmatch(r"[a-f0-9]{64}", expected_generator_sha256)
        or generator_digest != expected_generator_sha256
    ):
        raise ReservationError("Approved generator digest does not match the bound source")
    for run_id, record in records.items():
        if (
            not isinstance(record, dict)
            or set(record) != {"reservation", "binding"}
            or not isinstance(record["binding"], dict)
            or set(record["binding"]) != {"config", "selected_demos", "targets", "generator_sha256"}
        ):
            raise ReservationError("Allocator reservation record is invalid")
        binding = record["binding"]
        if not isinstance(binding["config"], dict) or set(binding["config"]) != config_fields:
            raise ReservationError("Reserved generator configuration is invalid")
        try:
            config = generator.RunConfig(**binding["config"])
            config.validate()
            start, end = generator.sales_line_id_range(config)
        except (TypeError, ValueError, OverflowError, AttributeError) as exc:
            raise ReservationError("Reserved generator configuration or range is invalid") from exc
        selected = _targets(binding["targets"])
        if (
            run_id != config.run_id
            or binding["selected_demos"] != sorted(selected)
            or any(_canonical(targets.get(name)) != _canonical(target) for name, target in selected.items())
            or not isinstance(binding["generator_sha256"], str)
            or not re.fullmatch(r"[a-f0-9]{64}", binding["generator_sha256"])
        ):
            raise ReservationError("Reserved run or target binding does not match")
        expected = {
            "version": 1,
            "allocator_id": registry["allocator_id"],
            "run_id": config.run_id,
            "batch_class": config.batch_class,
            "start": str(start),
            "end": str(end),
            "count": str(config.file_count * config.rows_per_file),
            "generator_sha256": binding["generator_sha256"],
            "fingerprint": _digest({"allocator_id": registry["allocator_id"], "binding": binding}),
        }
        if _canonical(record["reservation"]) != _canonical(expected):
            raise ReservationError("Reserved range or fingerprint does not match its binding")
        ranges.append((_integer(expected["start"], signed=False), _integer(expected["end"], signed=False)))
    ranges.sort()
    if any(left[1] >= right[0] for left, right in zip(ranges, ranges[1:])):
        raise ReservationError("Allocator contains overlapping reserved or retained ranges")

    if (
        not isinstance(manifest, dict)
        or type(manifest.get("contract_version")) is not int
        or manifest["contract_version"] != 1
        or manifest.get("run_id") != expected_config.get("run_id")
        or _canonical(manifest.get("config")) != _canonical(expected_config)
        or manifest.get("destinations") != sorted(expected_targets)
        or expected_config.get("batch_class") != "measured"
    ):
        raise ReservationError("Producer manifest does not match the expected run configuration")
    record = records.get(expected_config["run_id"])
    if (
        not isinstance(record, dict)
        or _canonical(record["binding"]["config"]) != _canonical(expected_config)
        or _canonical(record["binding"]["targets"]) != _canonical(expected_targets)
        or record["binding"]["selected_demos"] != sorted(expected_targets)
        or record["binding"]["generator_sha256"] != generator_digest
        or _canonical(record["reservation"]) != _canonical(manifest.get("sales_line_id_reservation"))
    ):
        raise ReservationError("Producer manifest does not match the authoritative run reservation")
    return {
        "reservation": dict(record["reservation"]),
        "record_sha256": _digest(record),
        "registry_sha256": registry["integrity_sha256"],
    }
