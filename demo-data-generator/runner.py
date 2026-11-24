"""Cooperative bounded runner for injected delivery adapters; not wired to Fabric."""

import os
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable

from delivery import begin_submission, record_submission
from generator import _atomic_json, initialize_run, stage_batch, strict_json_loads


@dataclass(frozen=True)
class Submission:
    status: str
    operation_id: str | None = None
    diagnostic: str | None = None


def run_batches(root, config, adapters: dict[str, Callable], max_duration_seconds,
                max_attempts, *, clock=None, sleep=None):
    """Run staged batches with per-destination failure isolation.

    Adapter receives the immutable submission request and a UTC deadline and
    must enforce its own I/O timeout. Completion means submission, not ingestion.
    A local exclusive lock rejects overlapping runners for the same run ID.
    The storage backend must independently guarantee exclusive file creation.
    """
    config.validate()
    run_root = Path(root) / config.run_id
    run_root.mkdir(parents=True, exist_ok=True)
    lock_path = run_root / ".producer.lock"
    try:
        fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    except FileExistsError as exc:
        raise RuntimeError("run is locked; confirm the previous writer is stopped before recovery") from exc
    try:
        os.close(fd)
        return _run_batches_locked(root, config, adapters, max_duration_seconds, max_attempts,
                                   clock=clock, sleep=sleep)
    finally:
        lock_path.unlink()


def _run_batches_locked(root, config, adapters, max_duration_seconds, max_attempts, *, clock, sleep):
    clock = clock or (lambda: datetime.now(timezone.utc))
    sleep = sleep or time.sleep
    config.validate()
    if not adapters:
        raise ValueError("at least one destination is required")
    if not isinstance(max_duration_seconds, int) or isinstance(max_duration_seconds, bool) or (
        max_duration_seconds <= (config.file_count - 1) * config.interval_seconds
    ):
        raise ValueError("max_duration_seconds must exceed the final scheduled batch offset")
    if not isinstance(max_attempts, int) or isinstance(max_attempts, bool) or max_attempts < 1:
        raise ValueError("max_attempts must be a positive integer")
    if any(not callable(adapter) for adapter in adapters.values()):
        raise ValueError("each destination must have a callable adapter")
    now = clock()
    if now.tzinfo is None or now.utcoffset() != timedelta(0):
        raise ValueError("clock must return a UTC-aware datetime")
    run_root, manifest = initialize_run(root, config, list(adapters))
    manifest_path = run_root / "manifest.json"
    scheduler = manifest.get("scheduler")
    if scheduler is None:
        if manifest["batches"]:
            raise RuntimeError("staged batches predate the scheduler; manual recovery required")
        scheduler = {"started_at_utc": now.isoformat(), "max_duration_seconds": max_duration_seconds,
                     "max_attempts": max_attempts, "batch_started_at_utc": {}}
        manifest["scheduler"] = scheduler
        _atomic_json(manifest_path, manifest)
    elif scheduler["max_duration_seconds"] != max_duration_seconds or scheduler["max_attempts"] != max_attempts:
        raise ValueError("existing run bounds differ; use a new run ID")
    start = datetime.fromisoformat(scheduler["started_at_utc"])
    deadline = start + timedelta(seconds=max_duration_seconds)
    errors = []
    for batch_number in range(1, config.file_count + 1):
        key = str(batch_number)
        starts = scheduler["batch_started_at_utc"]
        if key not in starts:
            due = start + timedelta(seconds=(batch_number - 1) * config.interval_seconds)
            if batch_number > 1:
                previous = datetime.fromisoformat(starts[str(batch_number - 1)])
                due = max(due, previous + timedelta(seconds=config.interval_seconds))
            remaining = (due - clock()).total_seconds()
            if remaining > 0:
                if due >= deadline:
                    errors.append("deadline reached before next batch")
                    break
                sleep(remaining)
            if clock() >= deadline:
                errors.append("deadline reached before next batch")
                break
            starts[key] = clock().isoformat()
            manifest = strict_json_loads(manifest_path.read_text(encoding="utf-8"))
            manifest["scheduler"] = scheduler
            _atomic_json(manifest_path, manifest)
        if clock() >= deadline:
            errors.append("deadline reached before batch delivery")
            break
        stage_batch(root, config, batch_number, list(adapters))
        for destination, adapter in adapters.items():
            while True:
                manifest = strict_json_loads(manifest_path.read_text(encoding="utf-8"))
                delivery = manifest["batches"][key]["deliveries"][destination]
                status = delivery["status"]
                if status in ("submitted", "verified"):
                    break
                if status not in ("pending", "retryable_failure"):
                    errors.append(f"{destination} batch {batch_number}: {status}; reconciliation required")
                    break
                if delivery.get("attempts", 0) >= max_attempts:
                    errors.append(f"{destination} batch {batch_number}: retry limit reached")
                    break
                if clock() >= deadline:
                    errors.append(f"{destination} batch {batch_number}: deadline reached")
                    break
                request = begin_submission(root, config.run_id, batch_number, destination, max_attempts)
                try:
                    outcome = adapter(request, deadline)
                    if not isinstance(outcome, Submission):
                        raise TypeError("adapter returned an invalid outcome")
                    record_submission(root, config.run_id, batch_number, destination,
                                      outcome.status, outcome.operation_id, outcome.diagnostic)
                except Exception:
                    # The external operation may have succeeded before raising.
                    record_submission(root, config.run_id, batch_number, destination,
                                      "unknown", diagnostic="Adapter outcome unknown; reconcile before retry.")
                    errors.append(f"{destination} batch {batch_number}: adapter outcome unknown")
                    break
                if clock() >= deadline:
                    errors.append(f"{destination} batch {batch_number}: deadline reached during submission")
                    break
    final = strict_json_loads(Path(manifest_path).read_text(encoding="utf-8"))
    return {"submitted": not errors and len(final["batches"]) == config.file_count,
            "errors": errors, "deadline_utc": deadline.isoformat()}
