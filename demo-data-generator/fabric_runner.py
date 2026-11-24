"""Bounded producer orchestration over the conditional OneLake ledger."""

import tempfile
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from fabric_ledger import FabricLedger


def run_fabric_batches(store, config, adapters, max_duration_seconds, max_attempts, *,
                       selected_targets, clock=None, sleep=None, poll_seconds=5,
                       allow_staged_pending=False, stop_requested=None):
    config.validate()
    if not adapters or any(not callable(getattr(adapter, "submit", None))
                           or not callable(getattr(adapter, "verify", None)) for adapter in adapters.values()):
        raise ValueError("each selected destination requires submit and verify methods")
    if "demo-2" in adapters and not callable(getattr(adapters["demo-2"], "ingestion_status", None)):
        raise ValueError("Eventhouse requires terminal ingestion status readback")
    if (not isinstance(max_duration_seconds, int) or isinstance(max_duration_seconds, bool)
            or max_duration_seconds <= (config.file_count - 1) * config.interval_seconds):
        raise ValueError("max_duration_seconds must exceed the final scheduled batch offset")
    if not isinstance(max_attempts, int) or isinstance(max_attempts, bool) or max_attempts < 1:
        raise ValueError("max_attempts must be a positive integer")
    if not isinstance(poll_seconds, (int, float)) or isinstance(poll_seconds, bool) or poll_seconds <= 0:
        raise ValueError("poll_seconds must be positive")
    if stop_requested is not None and not callable(stop_requested):
        raise ValueError("stop_requested must be callable")
    clock = clock or (lambda: datetime.now(timezone.utc))
    sleep = sleep or time.sleep
    now = clock()
    if now.tzinfo is None or now.utcoffset() != timedelta(0):
        raise ValueError("clock must return a UTC-aware datetime")
    errors = []
    stopped = False

    def should_stop():
        nonlocal stopped
        if not stopped and stop_requested is not None and stop_requested():
            stopped = True
            errors.append("stop requested; unresolved deliveries require reconciliation")
        return stopped

    with FabricLedger(store, config, list(adapters), selected_targets=selected_targets) as ledger:
        scheduler = ledger.manifest.get("scheduler")
        if scheduler is None:
            if ledger.manifest["batches"]:
                if (not allow_staged_pending or set(ledger.manifest["batches"]) !=
                        {str(n) for n in range(1, config.file_count + 1)}):
                    raise RuntimeError("staged batches predate the scheduler; manual recovery required")
                for number in range(1, config.file_count + 1):
                    batch = ledger.manifest["batches"][str(number)]
                    if batch["deliveries"] != {
                            name: {"status": "pending", "timestamp_utc": None}
                            for name in adapters}:
                        raise RuntimeError("staged batch has delivery state without a scheduler")
                    ledger.staged_bytes(number)
            scheduler = {"started_at_utc": now.isoformat(), "max_duration_seconds": max_duration_seconds,
                         "max_attempts": max_attempts, "batch_started_at_utc": {},
                         "status": "running", "finished_at_utc": None}
            ledger.set_scheduler(scheduler)
        elif (scheduler["max_duration_seconds"] != max_duration_seconds
              or scheduler["max_attempts"] != max_attempts):
            raise ValueError("existing run bounds differ; use a new run ID")
        start = datetime.fromisoformat(scheduler["started_at_utc"])
        deadline = start + timedelta(seconds=max_duration_seconds)
        if scheduler.get("status") in ("completed", "failed"):
            result = scheduler.get("result")
            if (not isinstance(result, dict) or set(result) != {"verified", "errors", "deadline_utc"}
                    or not isinstance(result["verified"], bool) or not isinstance(result["errors"], list)
                    or any(not isinstance(error, str) for error in result["errors"])
                    or result["deadline_utc"] != deadline.isoformat()
                    or result["verified"] != (scheduler["status"] == "completed")
                    or not isinstance(scheduler.get("finished_at_utc"), str)):
                raise RuntimeError("terminal run record is invalid; manual recovery required")
            return result
        if scheduler.get("status", "running") != "running":
            raise RuntimeError("run lifecycle state is invalid")

        def verify(number, destination):
            batch = ledger.manifest["batches"][str(number)]
            delivery = batch["deliveries"][destination]
            if destination == "demo-2":
                operation_id = delivery.get("operation_id")
                if not operation_id:
                    ledger.record_observation(number, destination, "unknown",
                                              "Ingestion source ID is unavailable; reconcile before retry")
                    return
                try:
                    outcome = adapters[destination].ingestion_status(operation_id)
                    evidence = {field: getattr(outcome, field) for field in
                                ("status", "timestamp_utc", "source_id", "service_operation_id",
                                 "error_code", "diagnostic")}
                except Exception:
                    ledger.record_observation(number, destination, "unknown", "Ingestion status outcome unknown")
                    return
                previous = delivery.get("ingestion_outcome")
                if (evidence["status"] == "pending" and isinstance(previous, dict)
                        and previous.get("status") == "succeeded"):
                    evidence = previous
                try:
                    ledger.record_ingestion_outcome(number, destination, evidence)
                except (TypeError, ValueError):
                    ledger.record_observation(number, destination, "unknown", "Invalid ingestion status evidence")
                    return
                status = evidence["status"]
                if status in ("pending", "unknown"):
                    ledger.record_observation(number, destination, status, evidence["diagnostic"])
                    return
                if status == "failed":
                    diagnostic = ("Skipped ingestion requires prior successful-source evidence; manual reconciliation required"
                                  if evidence["error_code"] == "Skipped_IngestByTagAlreadyExists"
                                  else evidence["diagnostic"] or "Eventhouse ingestion failed")
                    ledger.record_observation(number, destination, "failed", diagnostic)
                    return
            try:
                result = adapters[destination].verify(
                    batch["batch_id"], batch["sha256"], batch["row_count"],
                    batch["sales_amount_minor_units"], delivery.get("operation_id"))
            except Exception:
                # The readback is not proof of failure; keep the prior submission state.
                ledger.record_observation(number, destination, "unknown", "Verification outcome unknown")
                return
            status = getattr(result, "status", None)
            if status == "verified":
                try:
                    ledger.record_verification(number, destination, result.row_count, result.total_minor_units,
                                               result.distinct_sales_line_ids)
                except (AttributeError, RuntimeError, TypeError, ValueError):
                    ledger.record_observation(number, destination, "failed", "Invalid verification evidence")
            elif status in ("pending", "unknown", "failed"):
                diagnostic = getattr(result, "diagnostic", None)
                if diagnostic is not None and not isinstance(diagnostic, str):
                    diagnostic = "Invalid verification diagnostic"
                ledger.record_observation(number, destination, status, diagnostic)
            else:
                ledger.record_observation(number, destination, "unknown", "Invalid verification outcome")

        for number in range(1, config.file_count + 1):
            if should_stop():
                break
            key = str(number)
            starts = scheduler["batch_started_at_utc"]
            if key not in starts:
                due = start + timedelta(seconds=(number - 1) * config.interval_seconds)
                if number > 1:
                    previous = datetime.fromisoformat(starts[str(number - 1)])
                    due = max(due, previous + timedelta(seconds=config.interval_seconds))
                remaining = (due - clock()).total_seconds()
                if remaining > 0:
                    if due >= deadline:
                        errors.append("deadline reached before next batch")
                        break
                    while remaining > 0:
                        if should_stop():
                            break
                        sleep(min(remaining, poll_seconds) if stop_requested else remaining)
                        remaining = (due - clock()).total_seconds()
                if should_stop():
                    break
                if clock() >= deadline:
                    errors.append("deadline reached before next batch")
                    break
                starts[key] = clock().isoformat()
                ledger.set_scheduler(scheduler)
            if clock() >= deadline:
                errors.append("deadline reached before batch delivery")
                break
            ledger.stage_batch(number)
            for destination, adapter in adapters.items():
                while clock() < deadline:
                    if should_stop():
                        break
                    delivery = ledger.manifest["batches"][key]["deliveries"][destination]
                    status = delivery["status"]
                    if status in ("submitted", "unknown", "submitting", "verified", "verification_failed",
                                  "permanent_failure"):
                        break
                    if status not in ("pending", "retryable_failure"):
                        errors.append(f"{destination} batch {number}: invalid delivery state")
                        break
                    if delivery.get("attempts", 0) >= max_attempts:
                        errors.append(f"{destination} batch {number}: retry limit reached")
                        break
                    with tempfile.TemporaryDirectory(prefix="producer-") as directory:
                        local = Path(directory) / "batch.csv"
                        local.write_bytes(ledger.staged_bytes(number))
                        request = ledger.begin_submission(number, destination, max_attempts)
                        try:
                            result = adapter.submit(str(local), request["batch_id"], request["sha256"],
                                                    request["row_count"], request["sales_amount_minor_units"])
                        except Exception:
                            # The external operation may have succeeded before raising.
                            result = None
                        status = getattr(result, "status", None)
                        operation_id = getattr(result, "operation_id", None)
                        diagnostic = getattr(result, "diagnostic", None)
                        if (status not in ("submitted", "retryable_failure", "unknown", "permanent_failure")
                                or operation_id is not None and not isinstance(operation_id, str)
                                or diagnostic is not None and not isinstance(diagnostic, str)):
                            status, operation_id, diagnostic = "unknown", None, "Adapter outcome unknown; reconcile before retry"
                        ledger.record_submission(number, destination, status, operation_id, diagnostic)
                        if status == "unknown":
                            break
                if should_stop():
                    break
                delivery = ledger.manifest["batches"][key]["deliveries"][destination]
                if delivery["status"] in ("submitted", "unknown", "submitting") and clock() < deadline:
                    verify(number, destination)
            if stopped:
                break

        while clock() < deadline:
            if should_stop():
                break
            pending = [(int(number), destination)
                       for number, batch in ledger.manifest["batches"].items()
                       for destination, delivery in batch["deliveries"].items()
                       if delivery["status"] in ("submitted", "unknown", "submitting")]
            if not pending:
                break
            sleep(min(poll_seconds, max(0, (deadline - clock()).total_seconds())))
            for number, destination in pending:
                if clock() >= deadline or should_stop():
                    break
                verify(number, destination)
        final = ledger.manifest
        for number, batch in final["batches"].items():
            for destination, delivery in batch["deliveries"].items():
                if delivery["status"] != "verified":
                    errors.append(f"{destination} batch {number}: {delivery['status']}")
        verified = not errors and len(final["batches"]) == config.file_count
        result = {"verified": verified, "errors": errors, "deadline_utc": deadline.isoformat()}
        scheduler.update(status="completed" if verified else "failed",
                         finished_at_utc=clock().isoformat(), result=result)
        ledger.set_scheduler(scheduler)
        return result
