"""Persisted, fail-closed delivery transitions for a single-writer run."""

import hashlib
from datetime import datetime, timezone
from pathlib import Path

from generator import (RunConfig, _atomic_json, batch_bytes, batch_metadata_matches,
                       new_manifest, strict_json_loads, validate_run_manifest)


def _load(root, run_id, batch_number, destination):
    RunConfig(run_id).validate()
    if not isinstance(batch_number, int) or isinstance(batch_number, bool) or batch_number < 1:
        raise ValueError("batch_number must be a positive integer")
    path = Path(root) / run_id / "manifest.json"
    manifest = strict_json_loads(path.read_text(encoding="utf-8"))
    if (not isinstance(manifest, dict) or type(manifest.get("contract_version")) is not int
            or manifest["contract_version"] != 1 or manifest.get("run_id") != run_id):
        raise ValueError("manifest identity or version mismatch")
    config = RunConfig(**manifest["config"])
    if config.run_id != run_id:
        raise ValueError("manifest configuration has a different run ID")
    validate_run_manifest(manifest, new_manifest(config, manifest["destinations"]))
    batch = manifest["batches"][str(batch_number)]
    _, expected = batch_bytes(config, batch_number)
    if not batch_metadata_matches(batch, expected):
        raise ValueError("manifest batch metadata differs from deterministic input")
    if destination not in batch["deliveries"]:
        raise ValueError("destination is not selected for this run")
    staged = path.parent / batch["filename"]
    if not staged.is_file() or hashlib.sha256(staged.read_bytes()).hexdigest() != expected["sha256"]:
        raise RuntimeError("staged batch missing or corrupt")
    return path, manifest, batch, batch["deliveries"][destination]


def _now():
    return datetime.now(timezone.utc).isoformat()


def start_delivery(delivery, max_attempts):
    if not isinstance(max_attempts, int) or isinstance(max_attempts, bool) or max_attempts < 1:
        raise ValueError("max_attempts must be a positive integer")
    if delivery["status"] not in ("pending", "retryable_failure"):
        raise RuntimeError("delivery must be reconciled before another submission")
    attempts = delivery.get("attempts", 0)
    if attempts >= max_attempts:
        raise RuntimeError("delivery retry limit reached")
    timestamp = _now()
    delivery.update(status="submitting", timestamp_utc=timestamp, submission_started_at_utc=timestamp, attempts=attempts + 1,
                    submission_status=None, submission_result_at_utc=None, operation_id=None, diagnostic=None)


def begin_submission(root, run_id, batch_number, destination, max_attempts):
    """Persist intent before an external call. A stranded intent is ambiguous."""
    path, manifest, batch, delivery = _load(root, run_id, batch_number, destination)
    start_delivery(delivery, max_attempts)
    _atomic_json(path, manifest)
    return {"staged_path": str(path.parent / batch["filename"]), "batch_id": batch["batch_id"],
            "sha256": batch["sha256"], "row_count": batch["row_count"],
            "sales_amount_minor_units": batch["sales_amount_minor_units"]}


def finish_delivery_submission(delivery, status, operation_id=None, diagnostic=None, *, observed_at=None):
    if status not in ("submitted", "retryable_failure", "unknown", "permanent_failure"):
        raise ValueError("invalid submission outcome")
    if operation_id is not None and not isinstance(operation_id, str):
        raise ValueError("operation_id must be a string")
    if diagnostic is not None and not isinstance(diagnostic, str):
        raise ValueError("diagnostic must be a sanitized string")
    if delivery["status"] != "submitting":
        raise RuntimeError("no submission is in progress")
    timestamp = observed_at.isoformat() if observed_at is not None else _now()
    delivery.update(status=status, timestamp_utc=timestamp, submission_status=status,
                    submission_result_at_utc=timestamp, operation_id=operation_id, diagnostic=diagnostic)


def record_submission(root, run_id, batch_number, destination, status, operation_id=None, diagnostic=None):
    """Record the adapter's outcome; retryable_failure means proven not accepted."""
    path, manifest, _, delivery = _load(root, run_id, batch_number, destination)
    finish_delivery_submission(delivery, status, operation_id, diagnostic)
    _atomic_json(path, manifest)


def verify_delivery(batch, delivery, row_count, total_minor_units, distinct_sales_line_ids):
    if delivery["status"] not in ("submitted", "unknown", "submitting"):
        raise RuntimeError("delivery has no submitted or uncertain operation to reconcile")
    observed = (row_count, total_minor_units, distinct_sales_line_ids)
    if any(not isinstance(value, int) or isinstance(value, bool) or value < 0 for value in observed):
        raise ValueError("verification values must be nonnegative integers")
    if observed != (batch["row_count"], batch["sales_amount_minor_units"], batch["row_count"]):
        raise ValueError("destination counts or exact total do not match the manifest")
    timestamp = _now()
    delivery.update(status="verified", timestamp_utc=timestamp, verified_at_utc=timestamp, verified_row_count=row_count,
                    verified_total_minor_units=total_minor_units,
                    verified_distinct_sales_line_ids=distinct_sales_line_ids)


def record_verification(root, run_id, batch_number, destination, row_count, total_minor_units, distinct_sales_line_ids):
    """Mark verified only after the destination independently proves the counts and total."""
    path, manifest, batch, delivery = _load(root, run_id, batch_number, destination)
    verify_delivery(batch, delivery, row_count, total_minor_units, distinct_sales_line_ids)
    _atomic_json(path, manifest)
