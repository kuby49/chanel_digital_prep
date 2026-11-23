"""Single-writer, conditional OneLake ledger for producer runs."""

import copy
import hashlib
import json
import re
import uuid
from datetime import datetime, timedelta, timezone

from delivery import finish_delivery_submission, start_delivery, verify_delivery
from fabric_storage import StorageConflict, StorageError
from generator import batch_bytes, new_manifest


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
    def __init__(self, store, config, destinations):
        self.store = store
        self.config = config
        self.expected = new_manifest(config, destinations)
        self.directory = f"Files/producer/runs/{config.run_id}"
        self.lock_path = f"{self.directory}/writer.lock"
        self.manifest_path = f"{self.directory}/manifest.json"
        self.lock_etag = None
        self.manifest_etag = None
        self.manifest = None
        self.unsafe = False

    def __enter__(self):
        try:
            self.lock_etag = self.store.create(self.lock_path, uuid.uuid4().hex.encode("ascii"))
        except StorageConflict:
            raise RuntimeError("run is locked; confirm the previous writer is stopped before recovery") from None
        try:
            self._initialize()
        except Exception:
            self._release()
            raise
        return self

    def __exit__(self, exc_type, exc, traceback):
        self._release()

    def _release(self):
        if self.lock_etag is not None and not self.unsafe:
            self.store.delete(self.lock_path, self.lock_etag)
            self.lock_etag = None

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
            raw = _encoded(self.expected)
            etag = self._write(self.manifest_path, raw)
        manifest = json.loads(raw.decode("utf-8"))
        for key in ("contract_version", "run_id", "config", "destinations"):
            if manifest.get(key) != self.expected[key]:
                raise ValueError("existing run identity, configuration or destinations differ")
        if not isinstance(manifest.get("batches"), dict):
            raise ValueError("existing run manifest is invalid")
        self.manifest, self.manifest_etag = manifest, etag

    def _save(self, manifest):
        self.manifest_etag = self._write(self.manifest_path, _encoded(manifest), self.manifest_etag)
        self.manifest = manifest

    def stage_batch(self, batch_number):
        payload, expected = batch_bytes(self.config, batch_number)
        key = str(batch_number)
        entry = self.manifest["batches"].get(key)
        path = f"{self.directory}/{expected['filename']}"
        if entry is not None:
            if any(entry.get(name) != value for name, value in expected.items()):
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
        entry["generated_at_utc"] = datetime.now(timezone.utc).isoformat()
        entry["deliveries"] = {name: {"status": "pending", "timestamp_utc": None}
                               for name in self.expected["destinations"]}
        updated = copy.deepcopy(self.manifest)
        updated["batches"][key] = entry
        self._save(updated)
        return entry

    def staged_bytes(self, batch_number):
        entry = self.manifest["batches"][str(batch_number)]
        raw, _ = self.store.read(f"{self.directory}/{entry['filename']}")
        if hashlib.sha256(raw).hexdigest() != entry["sha256"]:
            raise RuntimeError("staged batch checksum differs; manual recovery required")
        return raw

    def begin_submission(self, batch_number, destination, max_attempts):
        updated = copy.deepcopy(self.manifest)
        batch = updated["batches"][str(batch_number)]
        delivery = batch["deliveries"][destination]
        start_delivery(delivery, max_attempts)
        self._save(updated)
        return {"staged_path": f"{self.directory}/{batch['filename']}",
                "batch_id": batch["batch_id"], "sha256": batch["sha256"],
                "row_count": batch["row_count"],
                "sales_amount_minor_units": batch["sales_amount_minor_units"]}

    def record_submission(self, batch_number, destination, status, operation_id=None, diagnostic=None):
        updated = copy.deepcopy(self.manifest)
        delivery = updated["batches"][str(batch_number)]["deliveries"][destination]
        finish_delivery_submission(delivery, status, operation_id, diagnostic)
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
