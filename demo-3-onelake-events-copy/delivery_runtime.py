"""Guard functions shared by the event pipeline and bounded reconciliation."""

import time
import json
import uuid
from dataclasses import asdict

from audit import AuditError, WriterToken
from batch_gate import _PUBLISHED_OR_UNCERTAIN, source_number, validate_batch


def manifest_path(config):
    return f"Files/producer/runs/{config.run_id}/manifest.json"


class GuardedDelivery:
    def __init__(self, generator, config, producer_store, audit):
        config.validate()
        self.generator = generator
        self.config = config
        self.producer_store = producer_store
        self.audit = audit

    def prepare(self, job_id, subject, event_id, origin):
        """Return a filename only after durable Copy intent; exceptions prohibit Copy."""
        source_number(self.config, subject, self.audit.landing_directory)
        manifest, _ = self.producer_store.read(manifest_path(self.config))
        source, _ = self.audit.store.read(subject[1:])
        batch = validate_batch(
            self.generator, self.config, manifest, subject, source, event_id=event_id,
            origin=origin, landing_directory=self.audit.landing_directory,
        )
        batch_id = batch["batch_id"]
        token = self.audit.acquire(job_id, batch_id)
        decision = self.audit.begin(token, batch)
        if decision == "already-completed":
            self.audit.release(token, batch_id)
            return {"decision": decision, "batch_id": batch_id}
        attempt = self.audit.copy_intent(token, batch_id)
        return {
            "decision": decision, "batch_id": batch_id, "filename": subject.rsplit("/", 1)[1],
            "attempt_id": attempt, "lock_body": token.lock_body.decode("utf-8"),
            "lock_etag": token.lock_etag,
        }

    def finish(self, job_id, prepared, copy_result, query_target):
        """Record a successful Copy and require independently supplied exact fact evidence."""
        batch_id = prepared["batch_id"]
        token = WriterToken(job_id, batch_id, prepared["lock_body"].encode("utf-8"),
                            prepared["lock_etag"])
        self.audit.copy_succeeded(token, batch_id, prepared["attempt_id"], copy_result)
        proof = query_target(batch_id)
        self.audit.complete(token, batch_id, proof)
        self.audit.release(token, batch_id)

    def reconcile(self, run_guarded_pipeline, *, timeout_seconds, poll_seconds, event_grace_seconds=60,
                  clock=time.monotonic, sleep=time.sleep):
        """Observe only one approved run; launch the same guard, never a raw Copy.

        The launcher must wait for terminal job success within the supplied remaining
        budget or raise (including on ambiguous acceptance). No job is resubmitted
        here on error. Use independent job and lock reconciliation before restarting.
        """
        if (type(timeout_seconds) is not int or type(poll_seconds) is not int
                or not 1 <= poll_seconds <= timeout_seconds
                or type(event_grace_seconds) is not int or not 0 <= event_grace_seconds < timeout_seconds):
            raise ValueError("invalid reconciliation deadline or polling interval")
        deadline = clock() + timeout_seconds
        pending = set(range(1, self.config.file_count + 1))
        observed_at = {}
        while pending and clock() < deadline:
            try:
                body, _ = self.producer_store.read(manifest_path(self.config))
            except FileNotFoundError:
                body = None
            if body is not None:
                manifest = self.generator.strict_json_loads(body.decode("utf-8"))
                if (not isinstance(manifest, dict)
                        or type(manifest.get("contract_version")) is not int
                        or manifest["contract_version"] != 1
                        or manifest.get("run_id") != self.config.run_id
                        or not isinstance(manifest.get("batches"), dict)):
                    raise ValueError("reconciliation manifest identity differs")
                for number in sorted(pending):
                    if clock() >= deadline:
                        break
                    batch_id = f"{self.config.run_id}_{number:04d}"
                    subject = f"/Files/{self.audit.landing_directory}/sales_{batch_id}.csv"
                    entry = manifest["batches"].get(str(number))
                    if entry is None and str(number) not in manifest["batches"]:
                        continue
                    deliveries = entry.get("deliveries") if isinstance(entry, dict) else None
                    delivery = deliveries.get("demo-3") if isinstance(deliveries, dict) else None
                    status = delivery.get("status") if isinstance(delivery, dict) else None
                    if status in ("pending", "retryable_failure"):
                        continue
                    if not isinstance(status, str) or status not in _PUBLISHED_OR_UNCERTAIN:
                        raise ValueError("reconciliation delivery state differs or is terminally failed")
                    try:
                        landed, _ = self.audit.store.read(subject[1:])
                    except FileNotFoundError:
                        continue
                    evidence = validate_batch(
                        self.generator, self.config, body, subject, landed,
                        origin="recovery", landing_directory=self.audit.landing_directory,
                    )
                    state = self.audit.batch_state(evidence)
                    if state == "completed":
                        pending.remove(number)
                        continue
                    if state not in (None, "validated"):
                        continue
                    now = clock()
                    if now < observed_at.setdefault(number, now) + event_grace_seconds:
                        continue
                    remaining = deadline - clock()
                    if remaining <= 0:
                        break
                    dispatch = f"Files/demo3-audit/recovery/{batch_id}.json"
                    intent = json.dumps({"version": 1, "batch_id": batch_id, "subject": subject,
                                         "nonce": str(uuid.uuid4())},
                                        sort_keys=True).encode("utf-8")
                    etag = self.audit.store.create(dispatch, intent)
                    if self.audit.store.read(dispatch) != (intent, etag):
                        raise AuditError("recovery submission intent did not read back")
                    remaining = deadline - clock()
                    if remaining <= 0:
                        raise AuditError("recovery submission budget expired; reconcile persisted intent")
                    run_guarded_pipeline(subject, "", "recovery", remaining)
                    if clock() >= deadline or self.audit.batch_state(evidence) != "completed":
                        raise AuditError("recovery job did not confirm the audited batch before deadline")
                    self.audit.store.delete(dispatch, etag)
                    pending.remove(number)
            if pending:
                remaining = deadline - clock()
                if remaining > 0:
                    sleep(min(poll_seconds, remaining))
        if pending:
            raise TimeoutError(f"{len(pending)} expected Demo 3 batches remain unverified")
