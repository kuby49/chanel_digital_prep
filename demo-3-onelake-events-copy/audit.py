"""Cooperative, fail-closed audit primitives; not wired into the live pipeline."""

import json
import re
import uuid
from datetime import datetime, timezone
from typing import NamedTuple


LOCK_PATH = "Files/demo3-audit/writer.lock"
RECORD_PREFIX = "Files/demo3-audit/batches/"
GROUPS = ("amount_by_date", "amount_by_product_category", "amount_by_customer_region")
_BATCH_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]{0,83}\Z")
_SEGMENT = re.compile(r"[A-Za-z0-9_-][A-Za-z0-9._-]*\Z")
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_MAX_ID = 9223372036854775807
_DELIVERY_STATES = ("submitting", "submitted", "unknown", "verified")


class AuditError(RuntimeError):
    pass


class WriterToken(NamedTuple):
    pipeline_run_id: str
    batch_id: str
    lock_body: bytes
    lock_etag: str


def _uuid(value):
    try:
        parsed = uuid.UUID(value)
    except (ValueError, TypeError, AttributeError) as exc:
        raise AuditError("invalid UUID") from exc
    if str(parsed) != value or not parsed.int:
        raise AuditError("UUID must be canonical and nonzero")
    return value


def _time():
    return datetime.now(timezone.utc).isoformat()


def _timestamp(value):
    try:
        parsed = datetime.fromisoformat(value)
    except (ValueError, TypeError) as exc:
        raise AuditError("invalid UTC timestamp") from exc
    if parsed.utcoffset() != timezone.utc.utcoffset(parsed):
        raise AuditError("timestamp must be UTC")


def _unique_pairs(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise AuditError("duplicate audit JSON key")
        result[key] = value
    return result


def _invalid_constant(_):
    raise AuditError("invalid JSON constant")


def _decode(value):
    try:
        return json.loads(value.decode("utf-8"), object_pairs_hook=_unique_pairs,
                          parse_constant=_invalid_constant)
    except (ValueError, UnicodeError) as exc:
        raise AuditError("invalid audit JSON") from exc


def _encode(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")


def _integer(value, minimum=0):
    return type(value) is int and value >= minimum


def _event_id(value, origin):
    if origin not in ("event", "recovery") or (
        value is not None and
        (not isinstance(value, str) or not value or len(value) > 512 or not value.isprintable())
    ) or (origin == "event" and value is None):
        raise AuditError("invalid event observation")


def _delivery_status(value):
    if not isinstance(value, str) or value not in _DELIVERY_STATES:
        raise AuditError("invalid producer delivery state")


def _batch(evidence, landing_directory):
    fields = ("batch_id", "source_subject", "sha256", "row_count", "sales_amount_minor_units",
              "first_sales_line_id", "last_sales_line_id", *GROUPS)
    if not isinstance(evidence, dict) or any(field not in evidence for field in fields):
        raise AuditError("batch evidence is incomplete")
    batch = {field: evidence[field] for field in fields}
    batch_id = batch["batch_id"]
    if not isinstance(batch_id, str) or not _BATCH_ID.fullmatch(batch_id):
        raise AuditError("invalid batch ID")
    if batch["source_subject"] != f"/Files/{landing_directory}/sales_{batch_id}.csv":
        raise AuditError("batch subject does not match the landing directory")
    if not isinstance(batch["sha256"], str) or not _SHA256.fullmatch(batch["sha256"]):
        raise AuditError("invalid batch checksum")
    count, total = batch["row_count"], batch["sales_amount_minor_units"]
    first, last = batch["first_sales_line_id"], batch["last_sales_line_id"]
    if (not _integer(count, 1) or not _integer(total, 1) or type(first) is not int or first != 1
            or not _integer(last, 1) or last > _MAX_ID or last != count):
        raise AuditError("invalid batch counts, amount, or key range")
    for name in GROUPS:
        groups = batch[name]
        if (not isinstance(groups, dict) or not groups or any(
            not isinstance(key, str) or not key or not _integer(value, 1)
            for key, value in groups.items()
        ) or sum(groups.values()) != total):
            raise AuditError("invalid batch grouped totals")
    return batch


def _proof_matches(batch, proof):
    required = {
        "row_count": batch["row_count"], "distinct_sales_line_ids": batch["row_count"],
        "first_sales_line_id": batch["first_sales_line_id"],
        "last_sales_line_id": batch["last_sales_line_id"],
        "sales_amount_minor_units": batch["sales_amount_minor_units"],
        "orphan_rows": 0, "row_difference_count": 0,
        **{name: batch[name] for name in GROUPS},
    }
    return isinstance(proof, dict) and _encode(proof) == _encode(required)


class BatchAudit:
    """Use shared OneLakeStore conditional operations for cooperating writers only."""

    def __init__(self, store, workspace_id, item_id, *, landing_directory="incoming/sales"):
        self.target = {"workspace_id": _uuid(workspace_id), "item_id": _uuid(item_id),
                       "fact": "dbo.factsales"}
        if (getattr(store, "workspace_id", None) != workspace_id
                or getattr(store, "lakehouse_id", None) != item_id):
            raise AuditError("audit store must be the target Lakehouse")
        if not isinstance(landing_directory, str) or not all(
            part not in (".", "..") and _SEGMENT.fullmatch(part)
            for part in landing_directory.split("/")
        ):
            raise AuditError("invalid landing directory")
        self.store = store
        self.landing_directory = landing_directory

    def _checked_write(self, path, body, etag=None):
        data = _encode(body)
        written = self.store.create(path, data) if etag is None else self.store.replace(path, data, etag)
        actual, read_etag = self.store.read(path)
        if actual != data or read_etag != written:
            raise AuditError("audit write did not read back exactly")
        return read_etag

    def acquire(self, pipeline_run_id, batch_id):
        pipeline_run_id = _uuid(pipeline_run_id)
        self._path(batch_id)
        body = {"contract_version": 1, "target": self.target, "pipeline_run_id": pipeline_run_id,
                "batch_id": batch_id, "nonce": str(uuid.uuid4()), "acquired_at_utc": _time()}
        encoded = _encode(body)
        etag = self._checked_write(LOCK_PATH, body)
        return WriterToken(pipeline_run_id, batch_id, encoded, etag)

    def _lock(self, token, batch_id):
        if not isinstance(token, WriterToken):
            raise AuditError("writer token is required")
        if token.batch_id != batch_id:
            raise AuditError("writer lock is bound to another batch")
        actual, etag = self.store.read(LOCK_PATH)
        if actual != token.lock_body or etag != token.lock_etag:
            raise AuditError("writer lock changed; reconcile before continuing")
        body = _decode(actual)
        if (not isinstance(body, dict) or set(body) != {
            "contract_version", "target", "pipeline_run_id", "batch_id", "nonce", "acquired_at_utc"
        } or type(body["contract_version"]) is not int or body["contract_version"] != 1
                or body["target"] != self.target or body["pipeline_run_id"] != token.pipeline_run_id
                or body["batch_id"] != token.batch_id):
            raise AuditError("writer lock identity differs")
        _uuid(body["nonce"])
        _uuid(token.pipeline_run_id)
        self._path(token.batch_id)
        _timestamp(body["acquired_at_utc"])

    def _path(self, batch_id):
        if not isinstance(batch_id, str) or not _BATCH_ID.fullmatch(batch_id):
            raise AuditError("invalid batch ID")
        return RECORD_PREFIX + batch_id + ".json"

    def _record(self, batch_id):
        data, etag = self.store.read(self._path(batch_id))
        record = _decode(data)
        if (not isinstance(record, dict) or set(record) != {
            "contract_version", "target", "batch", "observations", "state",
            "copy_attempt", "copy_result", "verification"
        } or type(record["contract_version"]) is not int or record["contract_version"] != 1
                or record["target"] != self.target
                or _batch(record["batch"], self.landing_directory) != record["batch"]
                or record["batch"]["batch_id"] != batch_id):
            raise AuditError("audit record identity or schema differs")
        observations = record["observations"]
        if not isinstance(observations, list) or not observations:
            raise AuditError("audit observations are missing")
        for observation in observations:
            if not isinstance(observation, dict) or set(observation) != {
                "event_id", "origin", "delivery_status", "pipeline_run_id", "observed_at_utc"
            }:
                raise AuditError("invalid audit observation")
            _event_id(observation["event_id"], observation["origin"])
            _delivery_status(observation["delivery_status"])
            _uuid(observation["pipeline_run_id"])
            _timestamp(observation["observed_at_utc"])
        attempt, result, verification = (record["copy_attempt"], record["copy_result"],
                                          record["verification"])
        state = record["state"]
        if state not in ("validated", "copying", "verifying", "completed"):
            raise AuditError("invalid audit state")
        if state == "validated":
            if attempt is not None or result is not None or verification is not None:
                raise AuditError("invalid pre-Copy audit state")
        else:
            if not isinstance(attempt, dict) or set(attempt) != {
                "attempt_id", "pipeline_run_id", "started_at_utc"
            }:
                raise AuditError("invalid Copy intent")
            _uuid(attempt["attempt_id"])
            _uuid(attempt["pipeline_run_id"])
            _timestamp(attempt["started_at_utc"])
            if not any(observation["pipeline_run_id"] == attempt["pipeline_run_id"]
                       for observation in observations):
                raise AuditError("Copy intent has no matching event observation")
            if state == "copying":
                if result is not None or verification is not None:
                    raise AuditError("invalid in-flight Copy state")
            else:
                self._copy_result(record["batch"], attempt, result)
                if state == "verifying" and verification is not None:
                    raise AuditError("invalid verification state")
                if state == "completed":
                    if not isinstance(verification, dict) or set(verification) != {"proof", "verified_at_utc"}:
                        raise AuditError("verification evidence is missing")
                    _timestamp(verification["verified_at_utc"])
                    if not _proof_matches(record["batch"], verification["proof"]):
                        raise AuditError("verification evidence differs from the batch")
        return record, etag

    @staticmethod
    def _copy_result(batch, attempt, result):
        if not isinstance(result, dict) or set(result) != {
            "status", "pipeline_run_id", "activity_run_id", "files_read",
            "rows_read", "rows_copied", "confirmed_at_utc"
        }:
            raise AuditError("invalid Copy result")
        if result["activity_run_id"] is not None:
            _uuid(result["activity_run_id"])
        _timestamp(result["confirmed_at_utc"])
        if (result["status"] != "Succeeded" or result["pipeline_run_id"] != attempt["pipeline_run_id"]
                or type(result["files_read"]) is not int or result["files_read"] != 1
                or type(result["rows_read"]) is not int or result["rows_read"] != batch["row_count"]
                or type(result["rows_copied"]) is not int or result["rows_copied"] != batch["row_count"]):
            raise AuditError("Copy result does not match the intended batch")

    def begin(self, token, evidence):
        batch = _batch(evidence, self.landing_directory)
        _event_id(evidence.get("event_id"), evidence.get("origin"))
        _delivery_status(evidence.get("delivery_status"))
        self._lock(token, batch["batch_id"])
        observation = {"event_id": evidence.get("event_id"), "origin": evidence["origin"],
                       "delivery_status": evidence["delivery_status"],
                       "pipeline_run_id": token.pipeline_run_id, "observed_at_utc": _time()}
        try:
            record, etag = self._record(batch["batch_id"])
        except FileNotFoundError:
            record = {"contract_version": 1, "target": self.target, "batch": batch,
                      "observations": [observation], "state": "validated", "copy_attempt": None,
                      "copy_result": None, "verification": None}
            self._checked_write(self._path(batch["batch_id"]), record)
            return "copy-required"
        if record["batch"] != batch:
            raise AuditError("existing batch identity or contents differ")
        if record["state"] not in ("validated", "completed"):
            raise AuditError("prior Copy requires reconciliation before replay")
        record["observations"].append(observation)
        self._checked_write(self._path(batch["batch_id"]), record, etag)
        return "already-completed" if record["state"] == "completed" else "copy-required"

    def batch_state(self, evidence):
        batch = _batch(evidence, self.landing_directory)
        try:
            record, _ = self._record(batch["batch_id"])
        except FileNotFoundError:
            return None
        if record["batch"] != batch:
            raise AuditError("existing batch identity or contents differ")
        return record["state"]

    def copy_intent(self, token, batch_id):
        self._lock(token, batch_id)
        record, etag = self._record(batch_id)
        if (record["state"] != "validated"
                or record["observations"][-1]["pipeline_run_id"] != token.pipeline_run_id):
            raise AuditError("Copy cannot start from this audit state")
        attempt_id = str(uuid.uuid4())
        record["state"] = "copying"
        record["copy_attempt"] = {"attempt_id": attempt_id, "pipeline_run_id": token.pipeline_run_id,
                                  "started_at_utc": _time()}
        self._checked_write(self._path(batch_id), record, etag)
        return attempt_id

    def copy_succeeded(self, token, batch_id, attempt_id, result):
        self._lock(token, batch_id)
        record, etag = self._record(batch_id)
        attempt = record["copy_attempt"]
        if (record["state"] != "copying" or attempt["attempt_id"] != attempt_id
                or attempt["pipeline_run_id"] != token.pipeline_run_id or not isinstance(result, dict)):
            raise AuditError("Copy intent does not belong to this writer")
        confirmed = {**result, "confirmed_at_utc": _time()}
        self._copy_result(record["batch"], attempt, confirmed)
        record["state"] = "verifying"
        record["copy_result"] = confirmed
        self._checked_write(self._path(batch_id), record, etag)

    def complete(self, token, batch_id, proof):
        self._lock(token, batch_id)
        record, etag = self._record(batch_id)
        if (record["state"] != "verifying"
                or record["copy_attempt"]["pipeline_run_id"] != token.pipeline_run_id
                or not _proof_matches(record["batch"], proof)):
            raise AuditError("independent target verification is missing or differs")
        record["state"] = "completed"
        record["verification"] = {"proof": proof, "verified_at_utc": _time()}
        self._checked_write(self._path(batch_id), record, etag)

    def release(self, token, batch_id):
        self._lock(token, batch_id)
        record, _ = self._record(batch_id)
        if record["state"] not in ("validated", "completed"):
            raise AuditError("in-flight or unverified Copy requires reconciliation")
        self.store.delete(LOCK_PATH, token.lock_etag)
        try:
            self.store.read(LOCK_PATH)
        except FileNotFoundError:
            return
        raise AuditError("writer lock remains or another writer has acquired it")
