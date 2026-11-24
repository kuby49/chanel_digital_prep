"""Reusable guard entry point; no new run is admitted from a manifest alone."""

import hashlib
import json
import re
import time
from dataclasses import asdict
from datetime import datetime, timedelta, timezone

from audit import AuditError, BatchAudit, LOCK_PATH, _decode, _event_id, _uuid


_SUBJECT = re.compile(
    r"/Files/incoming/sales/sales_([A-Za-z0-9][A-Za-z0-9_-]{0,63})_([0-9]{4,})\.csv\Z",
    re.ASCII,
)
_TEMPLATE = {"batch_class", "start_date", "seed", "date_count", "products", "customers", "interval_seconds"}
_SHA = re.compile(r"[0-9a-f]{64}\Z", re.ASCII)


def config_sha256(config):
    return hashlib.sha256(json.dumps(
        config, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False,
    ).encode("utf-8")).hexdigest()


def source_run(subject):
    match = _SUBJECT.fullmatch(subject) if isinstance(subject, str) else None
    if not match:
        raise ValueError("native source must be a canonical immutable sales CSV")
    run_id, text = match.groups()
    number = int(text)
    if number < 1 or text != f"{number:04d}":
        raise ValueError("native source batch number is not canonical")
    return run_id, number


def validate_policy(policy):
    if (not isinstance(policy, dict) or type(policy.get("version")) is not int or policy["version"] != 1
            or not isinstance(policy.get("run_template"), dict)
            or set(policy["run_template"]) != _TEMPLATE
            or policy["run_template"].get("batch_class") != "measured"):
        raise ValueError("native policy must pin the measured generation template")
    for key in ("max_file_count", "max_rows_per_file", "max_admission_seconds"):
        if type(policy.get(key)) is not int or policy[key] < 1:
            raise ValueError("native policy needs positive file, row and lifetime bounds")
    wait = policy.get("writer_wait_seconds", 300)
    if type(wait) is not int or not 1 <= wait <= 300:
        raise ValueError("native writer wait must be bounded to at most 300 seconds")
    return policy


def validate_run_policy(config, policy):
    validate_policy(policy)
    config.validate()
    values = asdict(config)
    if ({key: values[key] for key in _TEMPLATE} != policy["run_template"]
            or config.file_count > policy["max_file_count"]
            or config.rows_per_file > policy["max_rows_per_file"]):
        raise ValueError("native run exceeds the deployment-bound generation policy")


def admitted_config(generator, settings, subject, manifest_bytes, *, finishing=False, now=None):
    """Resolve only a destination-bound, unexpired, explicitly open producer run."""
    policy = validate_policy(settings["native_policy"])
    run_id, number = source_run(subject)
    manifest = generator.strict_json_loads(manifest_bytes.decode("utf-8"))
    if (not isinstance(manifest, dict) or type(manifest.get("contract_version")) is not int
            or manifest["contract_version"] != 1 or manifest.get("run_id") != run_id
            or manifest.get("generator_sha256") != generator.generator_source_sha256()):
        raise ValueError("native manifest identity or pinned generator differs")
    config = generator.RunConfig(**manifest["config"])
    validate_run_policy(config, policy)
    values = asdict(config)
    if config.run_id != run_id or number > config.file_count:
        raise ValueError("native source is outside the manifest run")
    admission = manifest.get("demo3_admission")
    if (not isinstance(admission, dict) or type(admission.get("version")) is not int
            or admission["version"] != 1 or admission.get("state") not in ("open", "stopped")
            or admission.get("workspace_id") != settings["workspace_id"]
            or admission.get("lakehouse_id") != settings["item_id"]
            or admission.get("config_sha256") != config_sha256(values)
            or not isinstance(admission.get("stop_config_sha256"), str)
            or not _SHA.fullmatch(admission["stop_config_sha256"])
            or not isinstance(admission.get("expires_at_utc"), str)
            or not isinstance(manifest.get("destinations"), list)
            or manifest["destinations"].count("demo-3") != 1):
        raise ValueError("native run lacks matching destination-bound admission")
    expires = datetime.fromisoformat(admission["expires_at_utc"].replace("Z", "+00:00"))
    if expires.utcoffset() != timedelta(0):
        raise ValueError("native admission deadline must be timezone-aware UTC")
    current = datetime.now(timezone.utc) if now is None else now
    if current.utcoffset() != timedelta(0):
        raise ValueError("native admission clock must be timezone-aware UTC")
    if not finishing and (admission["state"] != "open" or not current < expires
                          or expires > current + timedelta(seconds=policy["max_admission_seconds"])):
        raise ValueError("native run is stopped, expired or beyond its bounded admission window")
    return config


def validate_stop(marker, run_id, expected_hash):
    if (not isinstance(marker, dict) or set(marker) != {"version", "run_id", "nonce", "config_sha256"}
            or type(marker["version"]) is not int or marker["version"] != 1
            or marker["run_id"] != run_id or marker["config_sha256"] != expected_hash
            or marker["nonce"] != "native-" + expected_hash[:32]):
        raise ValueError("native stop marker identity differs; do not ignore or admit this run")


def resolve_config(generator, settings, subject, producer, *, finishing=False, now=None):
    run_id, _ = source_run(subject)
    body, _ = producer.read(f"Files/producer/runs/{run_id}/manifest.json")
    config = admitted_config(generator, settings, subject, body, finishing=finishing, now=now)
    if not finishing:
        try:
            marker, _ = producer.read(f"Files/producer/runs/{run_id}/stop.json")
        except FileNotFoundError:
            return config
        document = generator.strict_json_loads(body.decode("utf-8"))
        validate_stop(generator.strict_json_loads(marker.decode("utf-8")), run_id,
                      document["demo3_admission"]["stop_config_sha256"])
        raise ValueError("native run has a valid stop request; new Copy admission is prohibited")
    return config


class AdmissionAudit(BatchAudit):
    def __init__(self, store, workspace_id, item_id, authorize, *,
                 lock_conflict=(), observation=None, wait_seconds=300,
                 clock=time.monotonic, sleep=time.sleep):
        super().__init__(store, workspace_id, item_id)
        if type(wait_seconds) is not int or not 1 <= wait_seconds <= 300:
            raise ValueError("native writer wait must be bounded to at most 300 seconds")
        self.authorize = authorize
        self.lock_conflict = lock_conflict
        self.observation = observation
        self.wait_seconds = wait_seconds
        self.clock, self.sleep = clock, sleep

    def acquire(self, pipeline_run_id, batch_id):
        deadline = self.clock() + self.wait_seconds
        ticket = None
        etag = None
        path = "Files/demo3-audit/waits/" + _uuid(pipeline_run_id) + ".json"

        def persist(status, **extra):
            nonlocal etag
            ticket.update(status=status, updated_at_utc=datetime.now(timezone.utc).isoformat(), **extra)
            etag = self._checked_write(path, ticket, etag)

        while True:
            if ticket is not None and self.clock() >= deadline:
                persist("blocked", reason="writer-lock-timeout")
                raise TimeoutError("native event remains durably blocked; reconcile the writer and guarded recovery")
            try:
                self.authorize()
            except Exception:
                if ticket is not None:
                    persist("blocked", reason="admission-revoked-or-unavailable")
                raise
            try:
                token = super().acquire(pipeline_run_id, batch_id)
            except self.lock_conflict:
                if ticket is None:
                    if not isinstance(self.observation, dict):
                        raise AuditError("contended native event identity is unavailable")
                    _event_id(self.observation.get("event_id"), self.observation.get("origin"))
                    ticket = {
                        "version": 1, "target": self.target, "pipeline_run_id": pipeline_run_id,
                        "batch_id": batch_id, "subject": f"/Files/incoming/sales/sales_{batch_id}.csv",
                        "observation": self.observation,
                    }
                    persist("waiting")
                try:
                    raw, _ = self.store.read(LOCK_PATH)
                except FileNotFoundError:
                    continue
                owner = _decode(raw)
                if not isinstance(owner, dict) or owner.get("target") != self.target:
                    persist("blocked", reason="unknown-writer-identity")
                    raise AuditError("writer identity differs; no lock is stolen or retried")
                held_by = _uuid(owner.get("pipeline_run_id"))
                if held_by == pipeline_run_id:
                    persist("blocked", reason="writer-owned-by-this-job")
                    raise AuditError("this job already owns the writer; reconcile instead of reacquiring")
                print(json.dumps({"phase": "writer-lock-wait", "pipeline_run_id": pipeline_run_id,
                                  "batch_id": batch_id, "held_by": held_by}), flush=True)
                remaining = deadline - self.clock()
                if remaining > 0:
                    self.sleep(min(5, remaining))
                continue
            if ticket is not None:
                persist("acquired")
            return token

    def copy_intent(self, token, batch_id):
        self._lock(token, batch_id)
        try:
            self.authorize()
        except Exception:
            # No Copy intent exists yet; a revoked admission must not strand the validated lock.
            self.release(token, batch_id)
            raise
        return super().copy_intent(token, batch_id)


def execute(spark, notebookutils, settings, arguments):
    import generator
    import guard_spark
    from delivery_runtime import GuardedDelivery
    from fabric_storage import OneLakeStore, StorageConflict

    context = notebookutils.runtime.context
    if (context["isForPipeline"] is not True or context["currentWorkspaceId"] != settings["workspace_id"]
            or context["defaultLakehouseId"] != settings["item_id"]):
        raise RuntimeError("native guard must run in its bound pipeline and Lakehouse")
    phase = arguments["phase"]
    if phase not in ("prepare", "finish"):
        raise ValueError("unrecognized native guard phase")
    finishing = phase == "finish"
    if finishing:
        prepared = generator.strict_json_loads(arguments["prepared_json"])
        subject = "/Files/incoming/sales/sales_" + prepared["batch_id"] + ".csv"
    else:
        subject = arguments["subject"]
    source_run(subject)
    token = lambda: notebookutils.credentials.getToken("storage")
    producer = OneLakeStore(
        settings["producer_workspace_id"], settings["producer_item_id"], token)
    config = resolve_config(generator, settings, subject, producer, finishing=finishing)

    def authorize():
        if resolve_config(generator, settings, subject, producer) != config:
            raise ValueError("native run configuration changed before Copy intent")

    target = OneLakeStore(settings["workspace_id"], settings["item_id"], token)
    audit = AdmissionAudit(
        target, settings["workspace_id"], settings["item_id"], authorize,
        lock_conflict=StorageConflict,
        observation={"event_id": arguments["event_id"] or None, "origin": arguments["origin"]},
        wait_seconds=settings["native_policy"].get("writer_wait_seconds", 300))
    guard = GuardedDelivery(generator, config, producer, audit)
    app_id = spark.sparkContext.applicationId
    if not isinstance(app_id, str) or not app_id:
        raise RuntimeError("Spark application identity is unavailable")
    print(json.dumps({"phase": phase, "spark_application_id": app_id, "run_id": config.run_id}))
    if not finishing:
        return json.dumps(guard.prepare(
            arguments["job_id"], subject, arguments["event_id"] or None, arguments["origin"]))
    copy = generator.strict_json_loads(arguments["copy_json"])
    guard.finish(arguments["job_id"], prepared, {
        "status": "Succeeded", "pipeline_run_id": arguments["job_id"], "activity_run_id": None,
        "files_read": copy["filesRead"], "rows_read": copy["rowsRead"], "rows_copied": copy["rowsCopied"],
    }, lambda batch: guard_spark.query_target(spark, notebookutils, settings, config, batch))
    return json.dumps({"decision": "completed", "batch_id": prepared["batch_id"]})
