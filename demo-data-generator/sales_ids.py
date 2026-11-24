"""Durable, collision-checked SalesLineID reservations."""

import copy
import hashlib
import json
import os
import re
import uuid
from contextlib import contextmanager
from dataclasses import asdict
from pathlib import Path

from fabric_storage import StorageConflict, StorageError, StorageOutcomeUnknown
from generator import (MAX_SALES_LINE_ID, RunConfig, generator_source_sha256,
                       sales_line_id_range, strict_json_loads)


REGISTRY_PATH = "Files/producer/allocations/sales-line-id-v1.json"
LOCK_PATH = "Files/producer/allocations/sales-line-id-v1.lock"
OWNER = "chanel_digital_prep/demo-data-generator"
DEMOS = ("demo-1", "demo-2", "demo-3")


class AllocationError(RuntimeError):
    pass


class AllocationCollision(AllocationError):
    pass


class AllocationUncertain(AllocationError):
    pass


def canonical_bytes(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                      allow_nan=False).encode("utf-8")


def _digest(value):
    return hashlib.sha256(canonical_bytes(value)).hexdigest()


def _canonical_uuid(value):
    try:
        return isinstance(value, str) and str(uuid.UUID(value)) == value and uuid.UUID(value).int != 0
    except (ValueError, AttributeError):
        return False


def validate_scope(scope):
    if not isinstance(scope, dict) or type(scope.get("version")) is not int or scope["version"] != 1:
        raise AllocationError("Invalid allocator scope version")
    if scope.get("kind") == "local":
        if (set(scope) != {"version", "kind", "root"} or not isinstance(scope["root"], str)
                or not Path(scope["root"]).is_absolute()):
            raise AllocationError("Invalid local allocator scope")
    elif scope.get("kind") == "fabric":
        fields = {"version", "kind", "owner", "tenant_id", "producer_workspace_id", "staging_lakehouse_id"}
        if (set(scope) != fields or scope["owner"] != OWNER
                or any(not _canonical_uuid(scope[key]) for key in
                       ("tenant_id", "producer_workspace_id", "staging_lakehouse_id"))):
            raise AllocationError("Invalid Fabric allocator scope")
    else:
        raise AllocationError("Invalid allocator scope kind")
    return copy.deepcopy(scope)


def _targets(value, scope, *, allow_empty=False):
    if not isinstance(value, dict) or (not value and not allow_empty) or any(k not in DEMOS for k in value):
        raise AllocationError("Invalid allocator target bindings")
    for binding in value.values():
        if scope["kind"] == "local":
            if (not isinstance(binding, dict) or set(binding) != {"identity"}
                    or not isinstance(binding["identity"], str) or not binding["identity"]):
                raise AllocationError("Invalid local target binding")
        elif (not isinstance(binding, dict) or set(binding) != {"workspace_id", "item_id", "fact"}
              or not _canonical_uuid(binding["workspace_id"]) or not _canonical_uuid(binding["item_id"])
              or not isinstance(binding["fact"], str)
              or not re.fullmatch(r"[A-Za-z0-9_][A-Za-z0-9_.-]{0,255}", binding["fact"])):
            raise AllocationError("Invalid Fabric target binding")
    return copy.deepcopy(value)


def validate_fabric_targets(value):
    """Validate candidate identities without establishing ownership or access."""
    return _targets(value, {"kind": "fabric"})


def _decimal(value, *, signed=False):
    pattern = r"0|-?[1-9][0-9]*" if signed else r"[1-9][0-9]*"
    if not isinstance(value, str) or not re.fullmatch(pattern, value):
        raise AllocationError("Invalid canonical BIGINT value")
    if len(value.lstrip("-")) > 19:
        raise AllocationError("SalesLineID range exceeds BIGINT")
    number = int(value)
    if number < -9223372036854775808 or number > MAX_SALES_LINE_ID or (not signed and number < 1):
        raise AllocationError("SalesLineID range exceeds BIGINT")
    return number


def _retained_ranges(value):
    if not isinstance(value, list):
        raise AllocationError("Invalid retained-key inventory")
    result = []
    for record in value:
        if (not isinstance(record, dict) or set(record) != {"source", "start", "end"}
                or not isinstance(record["source"], str) or not record["source"]
                or len(record["source"]) > 512 or any(ord(c) < 32 for c in record["source"])):
            raise AllocationError("Invalid retained-key inventory entry")
        start, end = _decimal(record["start"], signed=True), _decimal(record["end"], signed=True)
        if start > end:
            raise AllocationError("Invalid retained-key range")
        result.append((start, end))
    return result


def _binding(config, targets, generator_sha256):
    return {"config": asdict(config), "selected_demos": sorted(targets),
            "targets": copy.deepcopy(targets), "generator_sha256": generator_sha256}


def _reservation(scope, config, targets, generator_sha256):
    if not isinstance(generator_sha256, str) or not re.fullmatch(r"[0-9a-f]{64}", generator_sha256):
        raise AllocationError("Invalid generator fingerprint")
    start, end = sales_line_id_range(config)
    allocator_id = _digest(scope)
    binding = _binding(config, targets, generator_sha256)
    reservation = {"version": 1, "allocator_id": allocator_id, "run_id": config.run_id,
                   "batch_class": config.batch_class, "start": str(start), "end": str(end),
                   "count": str(config.file_count * config.rows_per_file),
                   "generator_sha256": generator_sha256,
                   "fingerprint": _digest({"allocator_id": allocator_id, "binding": binding})}
    return reservation, binding


def _integrity(registry):
    return _digest({key: value for key, value in registry.items() if key != "integrity_sha256"})


def _validate_registry(registry, expected_scope):
    fields = {"version", "scope", "allocator_id", "target_bindings", "retained_ranges",
              "reservations", "integrity_sha256"}
    if (not isinstance(registry, dict) or set(registry) != fields or type(registry["version"]) is not int
            or registry["version"] != 1 or validate_scope(registry["scope"]) != expected_scope
            or registry["allocator_id"] != _digest(expected_scope)
            or registry["integrity_sha256"] != _integrity(registry)):
        raise AllocationError("Allocator registry identity or integrity differs")
    targets = _targets(registry["target_bindings"], expected_scope, allow_empty=True)
    ranges = _retained_ranges(registry["retained_ranges"])
    records = registry["reservations"]
    if not isinstance(records, dict):
        raise AllocationError("Invalid allocator reservations")
    config_fields = set(asdict(RunConfig("sample")))
    for run_id, record in records.items():
        if (not isinstance(record, dict) or set(record) != {"reservation", "binding"}
                or not isinstance(record["binding"], dict)
                or set(record["binding"]) != {"config", "selected_demos", "targets", "generator_sha256"}):
            raise AllocationError("Invalid allocator reservation record")
        binding = record["binding"]
        if not isinstance(binding["config"], dict) or set(binding["config"]) != config_fields:
            raise AllocationError("Invalid reserved generator configuration")
        try:
            config = RunConfig(**binding["config"])
            config.validate()
        except (ValueError, TypeError, OverflowError):
            raise AllocationError("Invalid reserved generator configuration") from None
        selected = _targets(binding["targets"], expected_scope)
        if (run_id != config.run_id or binding["selected_demos"] != sorted(selected)
                or any(targets.get(name) != target for name, target in selected.items())):
            raise AllocationError("Reserved run or target binding differs")
        expected, _ = _reservation(expected_scope, config, selected, binding["generator_sha256"])
        if canonical_bytes(record["reservation"]) != canonical_bytes(expected):
            raise AllocationError("Reserved range or fingerprint differs")
        ranges.append((_decimal(expected["start"]), _decimal(expected["end"])))
    ranges.sort()
    if any(left[1] >= right[0] for left, right in zip(ranges, ranges[1:])):
        raise AllocationError("Allocator registry contains overlapping ranges")
    return registry


def registry_bytes(scope, target_bindings, retained_ranges):
    """Build a registry document; callers must separately establish a quiesced baseline."""
    scope = validate_scope(scope)
    targets = _targets(target_bindings, scope, allow_empty=True)
    registry = {"version": 1, "scope": scope, "allocator_id": _digest(scope),
                "target_bindings": targets, "retained_ranges": copy.deepcopy(retained_ranges),
                "reservations": {}}
    registry["integrity_sha256"] = _integrity(registry)
    _validate_registry(registry, scope)
    return canonical_bytes(registry) + b"\n"


def _read_registry(store, scope):
    try:
        raw, etag = store.read(REGISTRY_PATH)
    except FileNotFoundError:
        raise AllocationError("Allocator registry is missing; reconcile and initialize it explicitly") from None
    if not isinstance(raw, bytes) or len(raw) > 16 * 1024 * 1024:
        raise AllocationError("Allocator registry is invalid or too large")
    try:
        registry = strict_json_loads(raw.decode("utf-8"))
    except (UnicodeError, ValueError, RecursionError):
        raise AllocationError("Allocator registry is unreadable or corrupt") from None
    if not isinstance(etag, str) or not etag:
        raise AllocationError("Allocator registry has no usable conditional-write ETag")
    try:
        return _validate_registry(registry, scope), etag
    except AllocationError:
        raise
    except (TypeError, ValueError, UnicodeError, OverflowError, RecursionError):
        raise AllocationError("Allocator registry contains invalid values") from None


def reserve(store, scope, config, selected_targets, *, require_existing=False, require_new=False):
    if type(require_existing) is not bool or type(require_new) is not bool or require_existing and require_new:
        raise ValueError("Choose either an existing or a new reservation")
    scope = validate_scope(scope)
    config.validate()
    targets = _targets(selected_targets, scope)
    expected, binding = _reservation(scope, config, targets, generator_source_sha256())
    for _ in range(8):
        registry, etag = _read_registry(store, scope)
        if any(registry["target_bindings"].get(name) != target for name, target in targets.items()):
            raise AllocationError("Selected target is not registered with the allocator")
        previous = registry["reservations"].get(config.run_id)
        if previous is not None:
            if canonical_bytes(previous) != canonical_bytes({"reservation": expected, "binding": binding}):
                raise AllocationError("Existing reservation differs from this run")
            if require_new:
                raise AllocationError("Reservation exists without a run manifest; manual recovery required")
            return copy.deepcopy(expected)
        if require_existing:
            raise AllocationError("Existing run has no allocator reservation")
        start, end = _decimal(expected["start"]), _decimal(expected["end"])
        ranges = _retained_ranges(registry["retained_ranges"])
        ranges.extend((_decimal(record["reservation"]["start"]), _decimal(record["reservation"]["end"]))
                      for record in registry["reservations"].values())
        if any(start <= other_end and other_start <= end for other_start, other_end in ranges):
            raise AllocationCollision("SalesLineID range overlaps an existing reservation or retained fact")
        registry["reservations"][config.run_id] = {"reservation": expected, "binding": binding}
        registry["integrity_sha256"] = _integrity(registry)
        try:
            store.replace(REGISTRY_PATH, canonical_bytes(registry) + b"\n", etag)
        except StorageConflict:
            continue
        except StorageOutcomeUnknown:
            pass
        except StorageError:
            raise AllocationUncertain("Allocator conditional write was not confirmed; reconcile before retrying") from None
        try:
            readback, _ = _read_registry(store, scope)
            if canonical_bytes(readback["reservations"].get(config.run_id)) == canonical_bytes(
                    {"reservation": expected, "binding": binding}):
                return copy.deepcopy(expected)
        except (AllocationError, StorageError):
            pass
        raise AllocationUncertain("Allocator write outcome is unknown; reconcile before retrying")
    raise AllocationError("Allocator is contended; retry after reconciliation")


class _LocalStore:
    def __init__(self, root):
        self.root = Path(root).resolve()
        self.path = self.root / ".sales-line-id-v1.json"
        self.lock_path = self.root / ".sales-line-id-v1.lock"

    def _sync_directory(self):
        if os.name != "nt":
            fd = os.open(self.root, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
            try:
                os.fsync(fd)
            finally:
                os.close(fd)

    @contextmanager
    def lock(self):
        self.root.mkdir(parents=True, exist_ok=True)
        nonce = uuid.uuid4().hex
        try:
            fd = os.open(self.lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        except FileExistsError:
            raise AllocationError("Local allocator is locked; reconcile the previous writer") from None
        keep = True
        try:
            with os.fdopen(fd, "w", encoding="ascii") as file:
                file.write(nonce)
                file.flush()
                os.fsync(file.fileno())
            self._sync_directory()
            try:
                yield
            except AllocationUncertain:
                raise
            except (AllocationError, ValueError, TypeError):
                keep = False
                raise
            else:
                keep = False
        finally:
            if not keep:
                if self.lock_path.read_text(encoding="ascii") != nonce:
                    raise AllocationError("Local allocator lock changed")
                self.lock_path.unlink()
                self._sync_directory()

    def read(self, path):
        if path != REGISTRY_PATH:
            raise ValueError("Invalid local allocator path")
        raw = self.path.read_bytes()
        return raw, hashlib.sha256(raw).hexdigest()

    def create(self, path, data):
        if path != REGISTRY_PATH:
            raise ValueError("Invalid local allocator path")
        try:
            with self.path.open("xb") as file:
                file.write(data)
                file.flush()
                os.fsync(file.fileno())
            self._sync_directory()
        except FileExistsError:
            raise StorageConflict("Local allocator already exists") from None
        except OSError:
            raise StorageOutcomeUnknown("Local allocator creation outcome unknown") from None
        return hashlib.sha256(data).hexdigest()

    def replace(self, path, data, etag):
        _, current_etag = self.read(path)
        if current_etag != etag:
            raise StorageConflict("Local allocator changed")
        temporary = self.root / (".sales-line-id-" + uuid.uuid4().hex + ".tmp")
        try:
            with temporary.open("xb") as file:
                file.write(data)
                file.flush()
                os.fsync(file.fileno())
            os.replace(temporary, self.path)
            self._sync_directory()
        except OSError:
            raise StorageOutcomeUnknown("Local allocator write outcome unknown") from None
        finally:
            temporary.unlink(missing_ok=True)
        return hashlib.sha256(data).hexdigest()


def local_reservation(root, config, destinations, *, require_existing=False, require_new=False):
    if (not isinstance(destinations, (list, tuple)) or not destinations
            or len(destinations) != len(set(destinations)) or any(name not in DEMOS for name in destinations)):
        raise ValueError("select distinct demo destinations")
    store = _LocalStore(root)
    scope = {"version": 1, "kind": "local", "root": str(store.root)}
    targets = {name: {"identity": name} for name in destinations}
    with store.lock():
        if not store.path.exists():
            run_dir = store.root / config.run_id
            others = [path for path in store.root.iterdir()
                      if path != store.lock_path and path != run_dir]
            if (require_existing or others or
                    run_dir.exists() and any(path.name != ".producer.lock" for path in run_dir.iterdir())):
                raise AllocationError("Local allocator is missing from a nonempty staging root")
            all_targets = {name: {"identity": name} for name in DEMOS}
            store.create(REGISTRY_PATH, registry_bytes(scope, all_targets, []))
        return reserve(store, scope, config, targets, require_existing=require_existing, require_new=require_new)
