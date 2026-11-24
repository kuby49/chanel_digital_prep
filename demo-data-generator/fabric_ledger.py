"""Single-writer, conditional OneLake ledger for producer runs."""

import copy
import hashlib
import json
import re
import uuid
from datetime import datetime, timedelta, timezone

from delivery import finish_delivery_submission, start_delivery, verify_delivery
from fabric_storage import StorageConflict, StorageError
from generator import (batch_bytes, batch_metadata_matches, canonical_bytes, new_manifest, strict_json_loads,
                       validate_fabric_targets, validate_run_manifest)


def _encoded(value):
    return (json.dumps(value, sort_keys=True, indent=2) + "\n").encode("utf-8")


def _validate_ingestion_outcome(outcome, operation_id):
    fields = {"status", "timestamp_utc", "source_id", "service_operation_id", "error_code", "diagnostic"}
    if not isinstance(outcome, dict) or set(outcome) != fields:
        raise ValueError("invalid ingestion status evidence")
    if outcome["status"] not in ("succeeded", "failed", "pending", "unknown"):
        raise ValueError("invalid ingestion status")
    try:
        source_id = uuid.UUID(outcome["source_id"])
        submitted_id = uuid.UUID(operation_id)
        timestamp = datetime.fromisoformat(outcome["timestamp_utc"])
    except (ValueError, TypeError, AttributeError):
        raise ValueError("invalid ingestion status identity or timestamp") from None
    if source_id != submitted_id or timestamp.tzinfo is None or timestamp.utcoffset() != timedelta(0):
        raise ValueError("ingestion status does not match the submission")
    service_id, error_code, diagnostic = (outcome["service_operation_id"],
                                           outcome["error_code"], outcome["diagnostic"])
    if (service_id is not None and (not isinstance(service_id, str)
                                    or not re.fullmatch(r"[A-Za-z0-9_.:-]{1,128}", service_id))):
        raise ValueError("invalid ingestion service operation ID")
    if (error_code is not None and (not isinstance(error_code, str)
                                    or not re.fullmatch(r"[A-Za-z0-9_.-]{1,100}", error_code))):
        raise ValueError("invalid ingestion error code")
    if diagnostic is not None and (not isinstance(diagnostic, str) or len(diagnostic) > 500):
        raise ValueError("invalid ingestion diagnostic")
    if outcome["status"] == "succeeded" and error_code is not None:
        raise ValueError("successful ingestion has a conflicting error")


class FabricLedger:
    def __init__(self, store, config, destinations, *, selected_targets, fresh_only=False):
        self.store = store
        self.config = config
        self.fresh_only = fresh_only
        self.expected = new_manifest(config, destinations)
        validate_fabric_targets(selected_targets)
        if set(selected_targets) != set(destinations):
            raise ValueError("selected destinations and Fabric target bindings must match")
        self.expected["target_bindings"] = copy.deepcopy(selected_targets)
        self.directory = f"Files/producer/runs/{config.run_id}"
        self.lock_path = f"{self.directory}/writer.lock"
        self.started_path = f"{self.directory}/initialized"
        self.manifest_path = f"{self.directory}/manifest.json"
        self.lock_value = None
        self.lock_etag = None
        self.manifest_etag = None
        self.manifest = None
        self.unsafe = False

    def __enter__(self):
        self.lock_value = uuid.uuid4().hex.encode("ascii")
        try:
            self.lock_etag = self.store.create(self.lock_path, self.lock_value)
        except StorageConflict:
            raise RuntimeError("run is locked; confirm the previous writer is stopped before recovery") from None
        try:
            self._assert_lock()
            self._initialize()
        except BaseException:
            self._release()
            raise
        return self

    def __exit__(self, exc_type, exc, traceback):
        try:
            if exc_type is not None and not self.unsafe:
                self.stop_demo3_admission()
        finally:
            self._release()

    def _release(self):
        if not self.unsafe:
            try:
                if self.lock_etag is not None:
                    self.store.delete(self.lock_path, self.lock_etag)
                    self.lock_etag = None
            except BaseException:
                self.unsafe = True
                raise

    def _assert_lock(self):
        try:
            raw, etag = self.store.read(self.lock_path)
            if raw != self.lock_value or etag != self.lock_etag:
                raise RuntimeError("Run writer lock changed; reconcile before continuing")
        except (FileNotFoundError, RuntimeError):
            self.unsafe = True
            raise

    def _write(self, path, data, etag=None):
        try:
            if etag is None:
                written_etag = self.store.create(path, data)
            else:
                written_etag = self.store.replace(path, data, etag)
            actual, actual_etag = self.store.read(path)
            if actual != data or actual_etag != written_etag:
                raise StorageError("OneLake write readback did not match")
            return written_etag
        except BaseException:
            self.unsafe = True
            raise

    def _initialize(self):
        try:
            raw, etag = self.store.read(self.manifest_path)
        except FileNotFoundError:
            raw, etag = None, None
        marker = _encoded({key: value for key, value in self.expected.items() if key != "batches"})
        try:
            initialized, _ = self.store.read(self.started_path)
        except FileNotFoundError:
            initialized = None
        if self.fresh_only and (raw is not None or initialized is not None):
            raise RuntimeError("Native run ID has already been initialized; inspect the existing manifest")
        if raw is None:
            if initialized is not None:
                raise RuntimeError("initialized run has no manifest; manual recovery required")
            self._write(self.started_path, marker)
            etag = self._write(self.manifest_path, _encoded(self.expected))
            manifest = self.expected
        else:
            manifest = strict_json_loads(raw.decode("utf-8"))
            validate_run_manifest(manifest, self.expected)
            if initialized != marker:
                raise RuntimeError("run initialization marker is missing or differs; manual recovery required")
        self.manifest, self.manifest_etag = manifest, etag

    def _save(self, manifest):
        self._assert_lock()
        self.manifest_etag = self._write(self.manifest_path, _encoded(manifest), self.manifest_etag)
        self.manifest = manifest

    def open_demo3_admission(self, expires_at_utc, stop_config_sha256):
        target = self.expected["target_bindings"]["demo-3"]
        if (not self.fresh_only or self.manifest["batches"] or "demo3_admission" in self.manifest
                or not re.fullmatch(r"[a-f0-9]{64}", stop_config_sha256)):
            raise ValueError("Demo 3 admission requires a fresh native run and exact stop binding")
        updated = copy.deepcopy(self.manifest)
        updated["demo3_admission"] = {
            "version": 1, "state": "open", "workspace_id": target["workspace_id"],
            "lakehouse_id": target["item_id"], "expires_at_utc": expires_at_utc,
            "config_sha256": hashlib.sha256(canonical_bytes(self.expected["config"])).hexdigest(),
            "stop_config_sha256": stop_config_sha256,
        }
        self._save(updated)

    def stop_demo3_admission(self):
        admission = self.manifest.get("demo3_admission")
        if self.fresh_only and admission is not None and admission["state"] == "open":
            updated = copy.deepcopy(self.manifest)
            updated["demo3_admission"]["state"] = "stopped"
            self._save(updated)

    def stage_batch(self, batch_number, *, clock=None):
        self._assert_lock()
        payload, expected = batch_bytes(self.config, batch_number)
        key = str(batch_number)
        entry = self.manifest["batches"].get(key)
        path = f"{self.directory}/{expected['filename']}"
        if entry is not None:
            if not batch_metadata_matches(entry, expected):
                raise ValueError("staged batch metadata does not match deterministic input")
            actual, _ = self.store.read(path)
            if hashlib.sha256(actual).hexdigest() != expected["sha256"]:
                raise RuntimeError("staged batch checksum differs; manual recovery required")
            return entry
        try:
            self._write(path, payload)
        except StorageConflict:
            # A previous writer may have persisted the deterministic file before its manifest.
            self.unsafe = False
            actual, _ = self.store.read(path)
            if actual != payload:
                raise RuntimeError("untracked staged batch differs; manual recovery required") from None
        entry = dict(expected)
        entry["generated_at_utc"] = (clock() if clock else datetime.now(timezone.utc)).isoformat()
        entry["deliveries"] = {name: {"status": "pending", "timestamp_utc": None}
                               for name in self.expected["destinations"]}
        updated = copy.deepcopy(self.manifest)
        updated["batches"][key] = entry
        self._save(updated)
        return entry

    def staged_bytes(self, batch_number):
        self._assert_lock()
        entry = self.manifest["batches"][str(batch_number)]
        _, expected = batch_bytes(self.config, batch_number)
        if not batch_metadata_matches(entry, expected):
            raise RuntimeError("staged batch metadata differs from deterministic input")
        raw, _ = self.store.read(f"{self.directory}/{expected['filename']}")
        if hashlib.sha256(raw).hexdigest() != expected["sha256"]:
            raise RuntimeError("staged batch checksum differs; manual recovery required")
        return raw

    def begin_submission(self, batch_number, destination, max_attempts):
        self.staged_bytes(batch_number)
        updated = copy.deepcopy(self.manifest)
        batch = updated["batches"][str(batch_number)]
        delivery = batch["deliveries"][destination]
        start_delivery(delivery, max_attempts)
        self._save(updated)
        return {"staged_path": f"{self.directory}/{batch['filename']}",
                "batch_id": batch["batch_id"], "sha256": batch["sha256"],
                "row_count": batch["row_count"],
                "sales_amount_minor_units": batch["sales_amount_minor_units"]}

    def record_submission(self, batch_number, destination, status, operation_id=None, diagnostic=None, *,
                          observed_at=None):
        updated = copy.deepcopy(self.manifest)
        batch = updated["batches"][str(batch_number)]
        delivery = batch["deliveries"][destination]
        finish_delivery_submission(delivery, status, operation_id, diagnostic, observed_at=observed_at)
        if observed_at is not None and status == "submitted":
            staged = datetime.fromisoformat(batch["generated_at_utc"])
            delivery["staged_to_submission_ack_seconds"] = round((observed_at - staged).total_seconds(), 3)
        self._save(updated)

    def record_verification(self, batch_number, destination, row_count, total_minor_units, distinct_sales_line_ids):
        updated = copy.deepcopy(self.manifest)
        batch = updated["batches"][str(batch_number)]
        delivery = batch["deliveries"][destination]
        if destination == "demo-2":
            outcome = delivery.get("ingestion_outcome")
            _validate_ingestion_outcome(outcome, delivery.get("operation_id"))
            if outcome["status"] != "succeeded":
                raise RuntimeError("Eventhouse ingestion has no acceptable terminal status")
        verify_delivery(batch, delivery, row_count, total_minor_units, distinct_sales_line_ids)
        delivery.update(last_verification_status="verified",
                        last_verification_at_utc=delivery["verified_at_utc"],
                        verification_diagnostic=None)
        self._save(updated)

    def record_ingestion_outcome(self, batch_number, destination, outcome):
        if destination != "demo-2":
            raise ValueError("ingestion status evidence applies only to Eventhouse")
        updated = copy.deepcopy(self.manifest)
        delivery = updated["batches"][str(batch_number)]["deliveries"][destination]
        if delivery["status"] not in ("submitted", "unknown", "submitting"):
            raise RuntimeError("delivery has no submitted or uncertain operation to reconcile")
        _validate_ingestion_outcome(outcome, delivery.get("operation_id"))
        delivery["ingestion_outcome"] = copy.deepcopy(outcome)
        self._save(updated)

    def record_observation(self, batch_number, destination, status, diagnostic=None):
        if status not in ("pending", "unknown", "failed"):
            raise ValueError("invalid verification observation")
        if diagnostic is not None and not isinstance(diagnostic, str):
            raise ValueError("diagnostic must be a sanitized string")
        updated = copy.deepcopy(self.manifest)
        delivery = updated["batches"][str(batch_number)]["deliveries"][destination]
        if delivery["status"] not in ("submitted", "unknown", "submitting"):
            raise RuntimeError("delivery has no submitted or uncertain operation to reconcile")
        delivery.update(last_verification_status=status,
                        last_verification_at_utc=datetime.now(timezone.utc).isoformat(),
                        verification_diagnostic=diagnostic)
        if status == "failed":
            delivery["status"] = "verification_failed"
        self._save(updated)

    def set_scheduler(self, scheduler):
        updated = copy.deepcopy(self.manifest)
        updated["scheduler"] = copy.deepcopy(scheduler)
        self._save(updated)
