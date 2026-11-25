"""Bounded, durable controller for Demo 2 shortcut verification."""

import base64
import hashlib
import json
import math
import re
import time
import uuid
from datetime import datetime, timedelta, timezone
from email.utils import format_datetime
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener

from lakehouse_validation import VerificationError, measured_batches


SENTINEL_SHA256 = "aea6faa895eae3072bb28cfacf732a93cfd9968fc8d2c6d2675f415278d6fd55"
FABRIC = "https://api.fabric.microsoft.com"
RUN_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}\Z")
TERMINAL_JOBS = {"Completed", "Failed", "Cancelled", "Deduped"}


class MonitorError(RuntimeError):
    pass


class RetryLater(MonitorError):
    def __init__(self, seconds):
        self.seconds = seconds
        super().__init__("The service requested a later retry")


def _uuid(value, name):
    try:
        return str(uuid.UUID(value))
    except (ValueError, TypeError, AttributeError) as exc:
        raise MonitorError(f"{name} must be a UUID") from exc


def _utc(value, name):
    if not isinstance(value, str) or "T" not in value:
        raise MonitorError(f"{name} must be a UTC timestamp")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise MonitorError(f"{name} must be a UTC timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0):
        raise MonitorError(f"{name} must be a UTC timestamp")
    return parsed.astimezone(timezone.utc)


def _stamp(value):
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _positive(value, name):
    if type(value) is not int or not 1 <= value <= 2**31 - 1:
        raise MonitorError(f"{name} must be a positive integer")
    return value


def _canonical_json(value):
    try:
        return json.dumps(
            value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
        ).encode("utf-8")
    except (TypeError, ValueError, UnicodeError, RecursionError) as exc:
        raise MonitorError("Monitor binding is not valid JSON") from exc


def _blob_parts(value, name):
    if not isinstance(value, str):
        raise MonitorError(f"{name} must be an HTTPS OneLake Blob URL")
    try:
        parsed = urlsplit(value)
        if (
            parsed.scheme != "https"
            or parsed.hostname != "onelake.blob.fabric.microsoft.com"
            or parsed.username is not None
            or parsed.password is not None
            or parsed.port not in (None, 443)
            or parsed.query
            or parsed.fragment
            or "%" in parsed.path
        ):
            raise ValueError
    except ValueError as exc:
        raise MonitorError(f"{name} must be an HTTPS OneLake Blob URL") from exc
    parts = parsed.path.lstrip("/").split("/")
    if len(parts) < 3 or parts[2] != "Files":
        raise MonitorError(f"{name} must identify a Lakehouse file")
    return [_uuid(parts[0], name), _uuid(parts[1], name), *parts[2:]]


def settings(parameters, *, lakehouse_id, reused_synthetic_target=None):
    run_id = parameters.get("RUN_ID")
    if not isinstance(run_id, str) or not RUN_ID.fullmatch(run_id):
        raise MonitorError("RUN_ID is invalid")
    workspace_id = _uuid(parameters.get("VERIFIER_WORKSPACE_ID"), "VERIFIER_WORKSPACE_ID")
    lakehouse_id = _uuid(lakehouse_id, "lakehouse_id")
    manifest = _blob_parts(parameters.get("MANIFEST_URL"), "MANIFEST_URL")
    sentinel = _blob_parts(parameters.get("SENTINEL_URL"), "SENTINEL_URL")
    state = _blob_parts(parameters.get("STATE_URL"), "STATE_URL")
    if manifest[2:] != ["Files", "producer", "runs", run_id, "manifest.json"]:
        raise MonitorError("MANIFEST_URL does not identify this producer run")
    if sentinel[:2] != manifest[:2] or sentinel[2:] != [
        "Files", "preflight", "producer-monitor-heartbeat-v1.txt"
    ]:
        raise MonitorError("SENTINEL_URL does not identify the producer readiness object")
    if state != [
        workspace_id, lakehouse_id, "Files", "demo2-verification", "runs", run_id, "state.json"
    ]:
        raise MonitorError("STATE_URL does not identify the Demo 2 run state")
    concurrency = _positive(parameters.get("MAX_CONCURRENT_BATCHES"), "MAX_CONCURRENT_BATCHES")
    if concurrency > 64:
        raise MonitorError("MAX_CONCURRENT_BATCHES cannot exceed 64")
    generator_sha256 = parameters.get("EXPECTED_GENERATOR_SHA256")
    if not isinstance(generator_sha256, str) or not re.fullmatch(r"[a-f0-9]{64}", generator_sha256):
        raise MonitorError("EXPECTED_GENERATOR_SHA256 must be a lowercase SHA-256 digest")
    count = _positive(parameters.get("EXPECTED_BATCH_COUNT"), "EXPECTED_BATCH_COUNT")
    try:
        producer_config = json.loads(parameters["EXPECTED_PRODUCER_CONFIG_JSON"])
        targets = json.loads(parameters["EXPECTED_TARGET_BINDINGS_JSON"])
    except (KeyError, TypeError, ValueError) as exc:
        raise MonitorError("Expected producer configuration or targets are invalid JSON") from exc
    if (
        not isinstance(producer_config, dict)
        or producer_config.get("run_id") != run_id
        or type(producer_config.get("file_count")) is not int
        or producer_config["file_count"] != count
        or not isinstance(targets, dict)
        or "demo-2" not in targets
        or any(name not in ("demo-1", "demo-2", "demo-3") or not isinstance(target, dict)
               for name, target in targets.items())
    ):
        raise MonitorError("Expected producer configuration or targets do not match this run")
    synthetic = run_id.casefold().startswith("demo2synthetic_")
    if synthetic:
        target = targets["demo-2"]
        if (reused_synthetic_target is not None
                or not re.fullmatch(r"demo2synthetic_[0-9a-f]{12}", run_id)
                or producer_config.get("batch_class") != "bootstrap"
                or set(targets) != {"demo-2"} or target.get("workspace_id") != workspace_id
                or target.get("fact") != "SyntheticFactSales" or manifest[0] == workspace_id):
            raise MonitorError("Synthetic monitor must bind an isolated bootstrap target")
    else:
        synthetic_targets = [name for name, target in targets.items()
                             if str(target.get("fact", "")).casefold() == "syntheticfactsales"]
        if producer_config.get("batch_class", "measured") != "measured":
            raise MonitorError("Production monitor requires a measured run")
        if synthetic_targets or reused_synthetic_target is not None:
            if (synthetic_targets != ["demo-2"] or not isinstance(reused_synthetic_target, dict)
                    or set(reused_synthetic_target) != {"workspace_id", "item_id", "fact"}
                    or targets["demo-2"] != reused_synthetic_target
                    or reused_synthetic_target.get("workspace_id") != workspace_id
                    or reused_synthetic_target.get("fact") != "SyntheticFactSales"
                    or manifest[0] == workspace_id):
                raise MonitorError("Measured reuse requires the notebook's exact isolated synthetic target")
            for field in ("workspace_id", "item_id"):
                value = reused_synthetic_target[field]
                if _uuid(value, field) != value or uuid.UUID(value).int == 0:
                    raise MonitorError("Measured reuse target identity must be canonical and nonzero")
    binding = hashlib.sha256(_canonical_json({
        "targets": targets, "generator_sha256": generator_sha256, "config": producer_config,
    })).hexdigest()
    if parameters.get("EXPECTED_MONITOR_BINDING_SHA256") != binding:
        raise MonitorError("Expected monitor binding does not match the producer configuration and targets")
    return {
        "run_id": run_id,
        "manifest_url": parameters["MANIFEST_URL"],
        "sentinel_url": parameters["SENTINEL_URL"],
        "state_url": parameters["STATE_URL"],
        "lock_url": parameters["STATE_URL"].rsplit("/", 1)[0] + "/writer.lock",
        "controller_id": _uuid(parameters.get("CONTROLLER_ID"), "CONTROLLER_ID"),
        "workspace_id": workspace_id,
        "notebook_id": _uuid(parameters.get("VERIFIER_NOTEBOOK_ID"), "VERIFIER_NOTEBOOK_ID"),
        "identity_id": _uuid(parameters.get("EXPECTED_IDENTITY_ID"), "EXPECTED_IDENTITY_ID"),
        "tenant_id": _uuid(parameters.get("EXPECTED_TENANT_ID"), "EXPECTED_TENANT_ID"),
        "generator_sha256": generator_sha256,
        "producer_config": producer_config,
        "synthetic_run_id": run_id if synthetic else None,
        "destinations": sorted(targets),
        "target_bindings": targets,
        "monitor_binding_sha256": binding,
        "deadline": _utc(parameters.get("DEADLINE_UTC"), "DEADLINE_UTC"),
        "count": count,
        "timeout": _positive(parameters.get("VERIFICATION_TIMEOUT_SECONDS"), "VERIFICATION_TIMEOUT_SECONDS"),
        "poll": _positive(parameters.get("POLL_INTERVAL_SECONDS"), "POLL_INTERVAL_SECONDS"),
        "concurrency": concurrency,
    }


def batch_fingerprint(batch):
    fields = (
        "batch_id", "batch_number", "batch_class", "sha256", "row_count",
        "sales_amount_minor_units", "amount_by_date", "amount_by_product_category",
        "amount_by_customer_region",
    )
    try:
        payload = {field: batch[field] for field in fields}
    except KeyError as exc:
        raise MonitorError("A measured batch is missing immutable evidence") from exc
    data = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(data).hexdigest()


def verify_delivery_evidence(batch, record):
    """Require the producer's exact KQL reconciliation and terminal ingestion evidence."""
    deliveries = batch.get("deliveries")
    delivery = deliveries.get("demo-2") if isinstance(deliveries, dict) else None
    if not isinstance(delivery, dict) or not isinstance(record, dict):
        raise MonitorError("The measured batch lacks Demo 2 delivery evidence")
    outcome = delivery.get("ingestion_outcome")
    expected_fields = {
        "status", "timestamp_utc", "source_id", "service_operation_id", "error_code", "diagnostic"
    }
    if (
        delivery.get("status") != "verified"
        or delivery.get("submission_status") != "submitted"
        or not isinstance(outcome, dict)
        or set(outcome) != expected_fields
        or outcome.get("status") != "succeeded"
        or outcome.get("error_code") is not None
    ):
        raise MonitorError("Kusto terminal success evidence is missing or invalid")
    operation = _uuid(delivery.get("operation_id"), "delivery operation_id")
    if (
        record.get("operation_id") != operation
        or _uuid(outcome.get("source_id"), "ingestion outcome source_id") != operation
    ):
        raise MonitorError("Kusto terminal success evidence does not match the submitted source")
    if (
        outcome.get("service_operation_id") is not None
        and not isinstance(outcome["service_operation_id"], str)
    ) or (
        outcome.get("diagnostic") is not None and not isinstance(outcome["diagnostic"], str)
    ):
        raise MonitorError("Kusto terminal success evidence has invalid details")
    started_at = _utc(delivery.get("submission_started_at_utc"), "submission start timestamp")
    submitted_at = _utc(delivery.get("submission_result_at_utc"), "submission result timestamp")
    outcome_at = _utc(outcome.get("timestamp_utc"), "ingestion outcome timestamp")
    verified_at = _utc(delivery.get("verified_at_utc"), "delivery verification timestamp")
    deadline = _utc(record.get("deadline_utc"), "batch deadline")
    if not started_at <= submitted_at <= outcome_at <= verified_at < deadline:
        raise MonitorError("Kusto terminal evidence does not fit the batch verification deadline")
    if record.get("status") == "verified":
        monitor_verified_at = _utc(record.get("verified_at_utc"), "monitor verification timestamp")
        if not verified_at <= monitor_verified_at < deadline:
            raise MonitorError("Monitor verification did not complete before the batch deadline")
    expected = {
        "verified_row_count": batch["row_count"],
        "verified_total_minor_units": batch["sales_amount_minor_units"],
        "verified_distinct_sales_line_ids": batch["row_count"],
    }
    if any(type(delivery.get(key)) is not int or delivery[key] != value for key, value in expected.items()):
        raise MonitorError("Kusto reconciliation evidence does not match the measured batch")


def _measured(manifest, cfg):
    if (
        not isinstance(manifest, dict)
        or type(manifest.get("contract_version")) is not int
        or manifest["contract_version"] != 1
    ):
        raise MonitorError("Producer manifest contract version is invalid")
    config = manifest.get("config")
    destinations = manifest.get("destinations")
    if (
        manifest.get("run_id") != cfg["run_id"]
        or manifest.get("generator_sha256") != cfg["generator_sha256"]
        or not isinstance(config, dict)
        or _canonical_json(config) != _canonical_json(cfg["producer_config"])
        or not isinstance(destinations, list)
        or any(not isinstance(name, str) for name in destinations)
        or sorted(destinations) != cfg["destinations"]
        or _canonical_json(manifest.get("target_bindings")) != _canonical_json(cfg["target_bindings"])
    ):
        raise MonitorError("Producer manifest does not match the monitored run")
    batches = manifest.get("batches")
    if not isinstance(batches, dict):
        raise MonitorError("Producer manifest batches are invalid")
    seen = set()
    has_measured = False
    synthetic_run_id = cfg.get("synthetic_run_id")
    expected_class = "bootstrap" if synthetic_run_id else "measured"
    for key, batch in batches.items():
        if not isinstance(batch, dict) or batch.get("batch_class") not in (
            "measured", "bootstrap", "negative-test"
        ):
            raise MonitorError("Producer manifest contains an invalid batch class")
        batch_id = batch.get("batch_id")
        if not isinstance(batch_id, str) or batch_id in seen:
            raise MonitorError("Producer manifest contains duplicate or invalid batch IDs")
        seen.add(batch_id)
        if synthetic_run_id and batch["batch_class"] != expected_class:
            raise MonitorError("Synthetic monitor accepts bootstrap batches only")
        if batch["batch_class"] == expected_class:
            has_measured = True
            if (
                not isinstance(key, str)
                or not key.isdecimal()
                or len(key) > len(str(cfg["count"]))
                or str(int(key)) != key
                or not 1 <= int(key) <= cfg["count"]
                or type(batch.get("batch_number")) is not int
                or batch["batch_number"] != int(key)
                or batch_id != f"{cfg['run_id']}_{int(key):04d}"
            ):
                raise MonitorError("Measured batch identity does not match its run")
    if not has_measured:
        return []
    try:
        return measured_batches(manifest, synthetic_run_id=synthetic_run_id)
    except (VerificationError, TypeError, ValueError) as exc:
        raise MonitorError(f"Producer manifest batch evidence is invalid: {exc}") from exc


def _exit(job, mode, expected):
    properties = job.get("properties")
    value = properties.get("exitValue") if isinstance(properties, dict) else None
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except ValueError as exc:
            raise MonitorError("Verification notebook returned invalid exit JSON") from exc
    if not isinstance(value, dict) or value.get("mode") != mode or value.get("status") != "verified":
        raise MonitorError("Verification notebook did not return verified exit evidence")
    if mode == "verify_batch":
        if value.get("batch") != expected:
            raise MonitorError("Verification notebook batch evidence does not match the manifest")
    elif any(value.get(key) != expected[key] for key in expected):
        raise MonitorError("Verification notebook run evidence does not match the manifest")


class Controller:
    """Observe producer snapshots and schedule read-only Spark jobs without blocking production."""

    def __init__(self, cfg, client, *, clock=lambda: datetime.now(timezone.utc), sleep=time.sleep):
        self.cfg = cfg
        self.client = client
        self.clock = clock
        self.sleep = sleep
        self.state = None
        self.etag = None
        self.lock_etag = None

    def _save(self):
        self.state["heartbeat_at_utc"] = _stamp(self.clock())
        payload = json.dumps(self.state, sort_keys=True, separators=(",", ":")).encode("utf-8")
        try:
            self.etag = self.client.write(self.cfg["state_url"], payload, self.etag)
        except MonitorError as exc:
            actual, etag = self.client.read(self.cfg["state_url"], missing=True)
            if actual != payload or not etag:
                raise MonitorError("Monitor state write is uncertain; reconciliation is required") from exc
            self.etag = etag

    def _begin(self):
        now = self.clock()
        if now >= self.cfg["deadline"]:
            raise MonitorError("Monitor deadline has already expired")
        self.client.preflight(self.cfg)
        self.client.prepare_directory(self.cfg["run_id"])
        lock = json.dumps({
            "contract_version": 1,
            "run_id": self.cfg["run_id"],
            "controller_id": self.cfg["controller_id"],
        }, sort_keys=True, separators=(",", ":")).encode("utf-8")
        self.lock_etag = self.client.create_lock(self.cfg["lock_url"], lock)
        previous, self.etag = self.client.read(self.cfg["state_url"], missing=True)
        if previous is None:
            self.state = {"contract_version": 1, "batches": {}, "run_verification": None}
        else:
            try:
                previous_state = json.loads(previous)
            except (TypeError, ValueError) as exc:
                raise MonitorError("Existing monitor state is invalid") from exc
            if (
                not isinstance(previous_state, dict)
                or previous_state.get("contract_version") != 1
                or previous_state.get("run_id") != self.cfg["run_id"]
                or previous_state.get("manifest_uri") != self.cfg["manifest_url"]
                or previous_state.get("generator_sha256") != self.cfg["generator_sha256"]
                or previous_state.get("monitor_binding_sha256") != self.cfg["monitor_binding_sha256"]
                or not isinstance(previous_state.get("batches"), dict)
                or previous_state.get("state") in ("completed", "failed")
            ):
                raise MonitorError("Existing monitor state requires explicit reconciliation")
            records = list(previous_state["batches"].values())
            if previous_state.get("run_verification"):
                records.append(previous_state["run_verification"])
            if any(not isinstance(record, dict) or record.get("status") == "submitting" for record in records):
                raise MonitorError("A previous verification submission is uncertain; do not resubmit it")
            try:
                for record in previous_state["batches"].values():
                    if record.get("status") == "verified":
                        verified_at = _utc(record.get("verified_at_utc"), "monitor verification timestamp")
                        deadline = _utc(record.get("deadline_utc"), "batch deadline")
                        if verified_at > now or verified_at >= deadline:
                            raise ValueError
            except (MonitorError, ValueError) as exc:
                raise MonitorError("A previous verified batch lacks valid deadline evidence") from exc
            self.state = previous_state
        self.state.update({
            "run_id": self.cfg["run_id"],
            "controller_id": self.cfg["controller_id"],
            "manifest_uri": self.cfg["manifest_url"],
            "deadline_utc": _stamp(self.cfg["deadline"]),
            "state": "running",
            "sentinel_verified": True,
            "job_access_verified": True,
            "identity_id": self.cfg["identity_id"],
            "tenant_id": self.cfg["tenant_id"],
            "generator_sha256": self.cfg["generator_sha256"],
            "monitor_binding_sha256": self.cfg["monitor_binding_sha256"],
            "manifest_observed": self.state.get("manifest_observed", False),
        })
        self._save()

    def _record(self, batch, delivery, now):
        batch_id = batch["batch_id"]
        fingerprint = batch_fingerprint(batch)
        record = self.state["batches"].get(batch_id)
        if record and record["fingerprint"] != fingerprint:
            raise MonitorError(f"Measured batch {batch_id} changed after observation")
        status = delivery.get("submission_status")
        started = delivery.get("submission_started_at_utc")
        if record and record.get("operation_id") and status != "submitted":
            raise MonitorError(f"Measured batch {batch_id} lost its accepted submission evidence")
        if status in (None, "unknown") and started:
            accepted = _utc(started, "submission_started_at_utc")
        elif status == "submitted":
            accepted = _utc(delivery.get("submission_result_at_utc"), "submission_result_at_utc")
        elif status == "permanent_failure":
            if record is None:
                self.state["batches"][batch_id] = {"fingerprint": fingerprint, "status": "failed"}
            raise MonitorError(f"Measured batch {batch_id} was permanently rejected")
        elif status in (None, "retryable_failure"):
            return record
        else:
            raise MonitorError(f"Measured batch {batch_id} has an invalid submission status")
        if accepted > now:
            raise MonitorError(f"Measured batch {batch_id} has a future submission timestamp")
        deadline = accepted + timedelta(seconds=self.cfg["timeout"])
        if record is None:
            record = {
                "fingerprint": fingerprint,
                "status": "awaiting_submission",
                "deadline_utc": _stamp(deadline),
            }
            self.state["batches"][batch_id] = record
        else:
            prior = _utc(record["deadline_utc"], "batch deadline")
            record["deadline_utc"] = _stamp(min(prior, deadline))
        if status == "submitted":
            operation_id = _uuid(delivery.get("operation_id"), "delivery operation_id")
            if record.get("operation_id", operation_id) != operation_id:
                raise MonitorError(f"Measured batch {batch_id} changed its submission identity")
            record["operation_id"] = operation_id
            if record["status"] == "awaiting_submission":
                record["status"] = "queued"
        return record

    def _poll(self, record, mode, expected, now):
        if record.get("status") != "running":
            return
        if now < _utc(record["next_poll_utc"], "job next poll"):
            return
        try:
            job, retry_after = self.client.job(record["job_id"])
        except RetryLater as exc:
            record["next_poll_utc"] = _stamp(now + timedelta(seconds=exc.seconds))
            return
        if job.get("status") == "Completed":
            _exit(job, mode, expected)
            record["status"] = "spark_verified"
        elif job.get("status") in ("Failed", "Cancelled", "Deduped"):
            raise MonitorError(f"A {mode} notebook ended in state {job['status']}")
        elif job.get("status") in ("NotStarted", "InProgress"):
            record["next_poll_utc"] = _stamp(now + timedelta(seconds=retry_after))
        else:
            raise MonitorError("Verification notebook returned an unknown job state")

    def _launch(self, record, mode, value, deadline, now):
        remaining = math.floor((deadline - now).total_seconds())
        if remaining < 1:
            raise MonitorError("Verification deadline expired before a notebook could be submitted")
        record["status"] = "submitting"
        self._save()
        job_id, retry_after = self.client.start_job(mode, value, remaining, self.cfg["poll"])
        record.update({
            "status": "running",
            "job_id": _uuid(job_id, "verification job ID"),
            "next_poll_utc": _stamp(self.clock() + timedelta(seconds=retry_after)),
        })
        self._save()

    def _tick(self):
        now = self.clock()
        if now >= self.cfg["deadline"]:
            raise MonitorError("Monitor reached its absolute deadline")
        for batch_id, record in self.state["batches"].items():
            if record.get("status") != "verified" and now >= _utc(record["deadline_utc"], "batch deadline"):
                raise MonitorError(f"Measured batch {batch_id} exceeded its verification deadline")
        self.client.verify_sentinel(self.cfg["sentinel_url"])
        payload, etag = self.client.read(self.cfg["manifest_url"], missing=True)
        if payload is None:
            if self.state["manifest_observed"]:
                raise MonitorError("A previously observed producer manifest disappeared")
            self._save()
            return
        if not etag:
            raise MonitorError("Producer manifest snapshot has no ETag")
        try:
            manifest = json.loads(payload)
        except (TypeError, ValueError) as exc:
            raise MonitorError("Producer manifest is not valid JSON") from exc
        batches = _measured(manifest, self.cfg)
        self.state["manifest_observed"] = True
        scheduler = manifest.get("scheduler")
        if scheduler is not None and not isinstance(scheduler, dict):
            raise MonitorError("Producer scheduler state is invalid")
        if scheduler and scheduler.get("status") == "failed":
            raise MonitorError("Producer scheduler reported failure")
        if scheduler and scheduler.get("status") == "completed":
            result = scheduler.get("result")
            if (
                not scheduler.get("finished_at_utc")
                or not isinstance(result, dict)
                or result.get("verified") is not True
                or result.get("errors") != []
                or len(batches) != self.cfg["count"]
            ):
                raise MonitorError("Producer completion evidence is incomplete")
            _utc(scheduler["finished_at_utc"], "producer completion timestamp")
            _utc(result.get("deadline_utc"), "producer deadline")

        known = {batch["batch_id"]: batch for batch in batches}
        if any(batch_id not in known for batch_id in self.state["batches"]):
            raise MonitorError("A previously observed batch disappeared from the producer manifest")
        for batch in batches:
            deliveries = batch.get("deliveries")
            delivery = deliveries.get("demo-2") if isinstance(deliveries, dict) else None
            if not isinstance(delivery, dict):
                raise MonitorError("Measured batch lacks its Demo 2 delivery record")
            record = self._record(batch, delivery, now)
            if record is None:
                continue
            deadline = _utc(record["deadline_utc"], "batch deadline")
            if record.get("status") != "verified" and self.clock() >= deadline:
                raise MonitorError(f"Measured batch {batch['batch_id']} exceeded its verification deadline")
            expected = {key: batch[key] for key in ("batch_id", "row_count", "sales_amount_minor_units")}
            self._poll(record, "verify_batch", expected, self.clock())
            if record.get("status") != "verified" and self.clock() >= deadline:
                raise MonitorError(f"Measured batch {batch['batch_id']} exceeded its verification deadline")
            if record.get("status") == "verified" and delivery.get("status") != "verified":
                raise MonitorError(f"Measured batch {batch['batch_id']} lost its transport verification")
            if record.get("status") in ("spark_verified", "verified") and delivery.get("status") == "verified":
                verify_delivery_evidence(batch, record)
                if record["status"] == "spark_verified":
                    verified_at = self.clock()
                    if not _utc(delivery["verified_at_utc"], "delivery verification timestamp") \
                            <= verified_at < deadline:
                        raise MonitorError(f"Measured batch {batch['batch_id']} exceeded its verification deadline")
                    record.update(status="verified", verified_at_utc=_stamp(verified_at))

        active = sum(
            record.get("status") in ("submitting", "running")
            for record in self.state["batches"].values()
        )
        queued = [
            batch for batch in batches
            if self.state["batches"].get(batch["batch_id"], {}).get("status") == "queued"
        ]
        queued.sort(key=lambda batch: (
            _utc(self.state["batches"][batch["batch_id"]]["deadline_utc"], "batch deadline"),
            batch["batch_number"],
        ))
        for batch in queued:
            if active >= self.cfg["concurrency"]:
                break
            record = self.state["batches"][batch["batch_id"]]
            deadline = _utc(record["deadline_utc"], "batch deadline")
            self._launch(record, "verify_batch", batch, deadline, self.clock())
            active += 1

        if scheduler and scheduler.get("status") == "completed":
            if all(self.state["batches"].get(batch["batch_id"], {}).get("status") == "verified"
                   for batch in batches):
                expected = {
                    "batch_ids": sorted(batch["batch_id"] for batch in batches),
                    "row_count": sum(batch["row_count"] for batch in batches),
                    "sales_amount_minor_units": sum(batch["sales_amount_minor_units"] for batch in batches),
                }
                record = self.state.get("run_verification")
                if record is None:
                    record = {"status": "queued"}
                    self.state["run_verification"] = record
                    self._launch(record, "verify_run_keys", manifest, self.cfg["deadline"], self.clock())
                else:
                    self._poll(record, "verify_run_keys", expected, self.clock())
                if record.get("status") == "spark_verified":
                    record["status"] = "verified"
                    self.state["state"] = "completed"
        self._save()

    def run(self):
        try:
            self._begin()
            while self.state["state"] == "running":
                try:
                    self._tick()
                except RetryLater as exc:
                    deadlines = [self.cfg["deadline"]] + [
                        _utc(record["deadline_utc"], "batch deadline")
                        for record in self.state["batches"].values()
                        if record.get("status") != "verified"
                    ]
                    if self.clock() + timedelta(seconds=exc.seconds) >= min(deadlines):
                        raise MonitorError("A service retry would exceed a verification deadline") from exc
                    self.sleep(exc.seconds)
                    continue
                if self.state["state"] == "running":
                    remaining = (self.cfg["deadline"] - self.clock()).total_seconds()
                    deadlines = [
                        (_utc(record["deadline_utc"], "batch deadline") - self.clock()).total_seconds()
                        for record in self.state["batches"].values()
                        if record.get("status") != "verified"
                    ]
                    self.sleep(max(0, min([self.cfg["poll"], remaining, *deadlines])))
            self.client.delete_lock(self.cfg["lock_url"], self.lock_etag)
            return {"mode": "monitor", "status": "verified", "run_id": self.cfg["run_id"]}
        except MonitorError as exc:
            if self.state is not None and self.lock_etag:
                self.state["state"] = "failed"
                self.state["diagnostic"] = str(exc)
                persistence_error = None
                try:
                    self._save()
                except MonitorError as save_exc:
                    persistence_error = save_exc
                records = list(self.state["batches"].values())
                if self.state.get("run_verification"):
                    records.append(self.state["run_verification"])
                cancel_until = min(
                    self.clock() + timedelta(seconds=30),
                    self.cfg["deadline"] + timedelta(seconds=30),
                )
                for record in records:
                    if record.get("status") == "running" and record.get("job_id"):
                        if self.clock() >= cancel_until:
                            record["cancel_requested"] = False
                        else:
                            try:
                                self.client.cancel_job(record["job_id"])
                                record["cancel_requested"] = True
                            except MonitorError:
                                record["cancel_requested"] = False
                    if record.get("status") != "verified":
                        if record.get("status") == "submitting" and not record.get("job_id"):
                            record["submission_uncertain"] = True
                        record["status"] = "failed"
                        record["diagnostic"] = str(exc)
                try:
                    self._save()
                except MonitorError as save_exc:
                    persistence_error = save_exc
                if persistence_error:
                    raise MonitorError("Monitor failure could not be durably recorded") from persistence_error
            elif self.lock_etag:
                try:
                    self.client.delete_lock(self.cfg["lock_url"], self.lock_etag)
                except MonitorError as release_exc:
                    raise MonitorError("Monitor startup failed and its lock could not be released") from release_exc
            raise


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


class FabricClient:
    """Fabric Jobs and OneLake Blob requests using the notebook runtime identity."""

    def __init__(self, notebookutils, cfg):
        self.notebookutils = notebookutils
        self.cfg = cfg
        self.opener = build_opener(_NoRedirect())

    def _request(self, method, url, *, audience, body=None, headers=None, missing=False):
        parsed = urlsplit(url)
        allowed = parsed.scheme == "https" and parsed.username is None and parsed.password is None
        if audience == "pbi":
            allowed = allowed and parsed.hostname == "api.fabric.microsoft.com" and parsed.path.startswith("/v1/workspaces/")
        elif audience == "storage":
            allowed = allowed and parsed.hostname == "onelake.blob.fabric.microsoft.com" and not parsed.query
        else:
            allowed = False
        try:
            allowed = allowed and parsed.port in (None, 443) and not parsed.fragment
        except ValueError:
            allowed = False
        if not allowed:
            raise MonitorError("Refusing to send credentials to an unexpected service")
        request_headers = {
            "Authorization": "Bearer " + self.notebookutils.credentials.getToken(audience),
            **(headers or {}),
        }
        if audience == "pbi":
            request_headers["x-ms-fabric-skill"] = "spark-cli"
        else:
            request_headers["x-ms-version"] = "2021-12-02"
            request_headers["x-ms-date"] = format_datetime(datetime.now(timezone.utc), usegmt=True)
        request = Request(url, data=body, headers=request_headers, method=method)
        try:
            with self.opener.open(request, timeout=15) as response:
                return response.status, dict(response.headers.items()), response.read()
        except HTTPError as exc:
            if missing and exc.code == 404:
                return 404, {}, None
            if exc.code == 429 and method == "GET":
                try:
                    delay = max(1, int(exc.headers.get("Retry-After", self.cfg["poll"])))
                except ValueError:
                    delay = self.cfg["poll"]
                raise RetryLater(delay) from exc
            raise MonitorError(f"{audience} {method} returned HTTP {exc.code}") from exc
        except (URLError, TimeoutError, OSError) as exc:
            raise MonitorError(f"{audience} {method} could not be confirmed") from exc

    @staticmethod
    def _claim(token, name):
        try:
            part = token.split(".")[1]
            payload = json.loads(base64.urlsafe_b64decode(part + "=" * (-len(part) % 4)))
            return _uuid(payload[name], name)
        except (IndexError, KeyError, TypeError, ValueError) as exc:
            raise MonitorError("A runtime token does not contain the expected identity claims") from exc

    def preflight(self, cfg):
        for audience in ("storage", "pbi"):
            token = self.notebookutils.credentials.getToken(audience)
            if self._claim(token, "oid") != cfg["identity_id"] or self._claim(token, "tid") != cfg["tenant_id"]:
                raise MonitorError("The monitor runtime identity does not match the approved producer identity")
        self.verify_sentinel(cfg["sentinel_url"])
        path = f"{FABRIC}/v1/workspaces/{cfg['workspace_id']}/items/{cfg['notebook_id']}"
        status, _, body = self._request("GET", path, audience="pbi")
        try:
            item = json.loads(body)
        except (TypeError, ValueError) as exc:
            raise MonitorError("Verification notebook readback is invalid") from exc
        if status != 200 or item.get("id", "").lower() != cfg["notebook_id"] or item.get("type") != "Notebook":
            raise MonitorError("Verification notebook readback does not match the configured item")

    def verify_sentinel(self, url):
        _, _, data = self._request("GET", url, audience="storage")
        if hashlib.sha256(data).hexdigest() != SENTINEL_SHA256:
            raise MonitorError("Producer storage readiness sentinel changed")

    def prepare_directory(self, run_id):
        path = f"Files/demo2-verification/runs/{run_id}"
        if not self.notebookutils.fs.mkdirs(path) and not self.notebookutils.fs.exists(path):
            raise MonitorError("Cannot create the Demo 2 monitor state directory")

    def read(self, url, *, missing=False):
        status, headers, body = self._request("GET", url, audience="storage", missing=missing)
        if status == 404:
            return None, None
        return body, next((value for key, value in headers.items() if key.lower() == "etag"), None)

    def _put(self, url, body, condition):
        status, headers, _ = self._request(
            "PUT", url, audience="storage", body=body,
            headers={"x-ms-blob-type": "BlockBlob", "Content-Type": "application/json", **condition},
        )
        etag = next((value for key, value in headers.items() if key.lower() == "etag"), None)
        if status not in (200, 201) or not etag:
            raise MonitorError("OneLake did not confirm the conditional write")
        return etag

    def create_lock(self, url, body):
        return self._put(url, body, {"If-None-Match": "*"})

    def write(self, url, body, etag):
        return self._put(url, body, {"If-Match": etag} if etag else {"If-None-Match": "*"})

    def delete_lock(self, url, etag):
        status, _, _ = self._request("DELETE", url, audience="storage", headers={"If-Match": etag})
        if status not in (200, 202, 204):
            raise MonitorError("OneLake did not confirm release of the monitor lock")

    def start_job(self, mode, value, timeout, poll):
        parameters = [
            {"name": "MODE", "type": "Text", "value": mode},
            {"name": "BATCH_JSON" if mode == "verify_batch" else "MANIFEST_JSON", "type": "Text",
             "value": json.dumps(value, separators=(",", ":"), sort_keys=True)},
            {"name": "VERIFICATION_TIMEOUT_SECONDS", "type": "Integer", "value": timeout},
            {"name": "POLL_INTERVAL_SECONDS", "type": "Integer", "value": poll},
        ]
        url = (
            f"{FABRIC}/v1/workspaces/{self.cfg['workspace_id']}/notebooks/"
            f"{self.cfg['notebook_id']}/jobs/execute/instances?beta=false"
        )
        body = json.dumps({"parameters": parameters, "executionData": {"compute": "Spark"}}).encode("utf-8")
        status, headers, _ = self._request(
            "POST", url, audience="pbi", body=body, headers={"Content-Type": "application/json"}
        )
        location = next((value for key, value in headers.items() if key.lower() == "location"), "")
        prefix = (
            f"{FABRIC}/v1/workspaces/{self.cfg['workspace_id']}/items/"
            f"{self.cfg['notebook_id']}/jobs/instances/"
        )
        if status != 202 or not location.startswith(prefix):
            raise MonitorError("Verification job submission could not be reconciled")
        job_id = _uuid(location[len(prefix):], "verification job ID")
        return job_id, self._retry_after(headers)

    def _retry_after(self, headers):
        value = next((value for key, value in headers.items() if key.lower() == "retry-after"), self.cfg["poll"])
        try:
            return max(1, int(value))
        except (TypeError, ValueError):
            return self.cfg["poll"]

    def job(self, job_id):
        job_id = _uuid(job_id, "verification job ID")
        url = (
            f"{FABRIC}/v1/workspaces/{self.cfg['workspace_id']}/notebooks/"
            f"{self.cfg['notebook_id']}/jobs/execute/instances/{job_id}?beta=true"
        )
        _, headers, payload = self._request("GET", url, audience="pbi")
        try:
            job = json.loads(payload)
        except (TypeError, ValueError) as exc:
            raise MonitorError("Verification job readback is invalid") from exc
        if job.get("id", "").lower() != job_id or job.get("itemId", "").lower() != self.cfg["notebook_id"]:
            raise MonitorError("Verification job readback does not match its owned identity")
        return job, self._retry_after(headers)

    def cancel_job(self, job_id):
        job_id = _uuid(job_id, "verification job ID")
        url = (
            f"{FABRIC}/v1/workspaces/{self.cfg['workspace_id']}/items/"
            f"{self.cfg['notebook_id']}/jobs/instances/{job_id}/cancel"
        )
        status, _, _ = self._request("POST", url, audience="pbi")
        if status != 202:
            raise MonitorError("Verification job cancellation was not confirmed")
