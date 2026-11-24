#!/usr/bin/env python3
"""Deploy and freshly verify Demo 2 using the version 1 track contract.

The state directory is the ownership ledger: reuse it for later invocations.
Existing resources are never adopted for modification or cleanup by name alone.
"""

import argparse
import base64
import binascii
import contextlib
import functools
import hashlib
import importlib
import json
import os
import re
import shutil
import stat
import subprocess
import sys
import time
import uuid
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from email.utils import format_datetime
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener

if __name__ == "__main__":
    sys.modules["deploy"] = sys.modules[__name__]

from eventhouse_core import (
    CSV_COLUMNS,
    ConfigurationError,
    batch_verification_query,
    discover_mirror_location,
    identifier,
    schema_commands,
    shortcut_payload,
)
from lakehouse_validation import VerificationError, validate_batch_evidence


ROOT = Path(__file__).resolve().parent.parent
FABRIC = "https://api.fabric.microsoft.com"
KUSTO_AUDIENCE = "https://kusto.kusto.windows.net"
STORAGE_AUDIENCE = "https://storage.azure.com"
STATE_FILE = "demo-2-state.json"
LOCK_FILE = "demo-2.lock"
LOCK_LEASE = "os-lease-v1"
MONITOR_FILE = "demo-2-monitor.json"
MONITOR_START_FILE = "demo-2-monitor-start.json"
ONELAKE = "https://onelake.blob.fabric.microsoft.com"
MAX_SECONDS = 2**31 - 1
MONITOR_STARTUP_HEADROOM_SECONDS = 1050
PREFIX = re.compile(r"[a-z0-9][a-z0-9-]{0,39}\Z")
KUSTO_HOST = re.compile(r"[a-z0-9-]+(?:\.[a-z0-9-]+)*\.kusto\.fabric\.microsoft\.com\Z")


class Failure(Exception):
    """A configuration or operation failed."""


class Blocked(Exception):
    """An expected prerequisite or safe ownership proof is unavailable."""


class BatchPending(Blocked):
    """A valid batch has not yet fully arrived."""


class TargetBindingError(ValueError):
    """The selected target map is malformed."""


def _targets(value):
    if not isinstance(value, dict) or not value or any(
        name not in ("demo-1", "demo-2", "demo-3") for name in value
    ):
        raise TargetBindingError("Target bindings are invalid")
    for target in value.values():
        if not isinstance(target, dict) or set(target) != {"workspace_id", "item_id", "fact"}:
            raise TargetBindingError("Target identity is invalid")
        for field in ("workspace_id", "item_id"):
            try:
                raw = target[field]
                parsed = uuid.UUID(raw)
                if not isinstance(raw, str) or str(parsed) != raw or parsed.int == 0:
                    raise ValueError
            except (ValueError, TypeError, AttributeError) as exc:
                raise TargetBindingError("Target identity is invalid") from exc
        if (
            not isinstance(target["fact"], str)
            or not re.fullmatch(r"[A-Za-z0-9_][A-Za-z0-9_.-]{0,255}", target["fact"])
        ):
            raise TargetBindingError("Target identity is invalid")
    return value


def _uuid(value, label):
    try:
        return str(uuid.UUID(value))
    except (ValueError, TypeError, AttributeError) as exc:
        raise Failure(f"{label} must be a UUID") from exc


def _optional_uuid(value, label):
    return None if value is None else _uuid(value, label)


def _positive_seconds(value, label):
    if type(value) is not int or not 1 <= value <= MAX_SECONDS:
        raise Failure(f"{label} must be an integer from 1 to {MAX_SECONDS}")
    return value


def _optional_name(value, default, label):
    try:
        return identifier(default if value is None else value, label)
    except ConfigurationError as exc:
        raise Failure(str(exc)) from exc


def _service_uri(value, label):
    if not isinstance(value, str):
        raise Failure(f"{label} must be a Kusto HTTPS endpoint")
    try:
        parsed = urlsplit(value)
        valid = (
            parsed.scheme == "https"
            and parsed.hostname is not None
            and KUSTO_HOST.fullmatch(parsed.hostname.lower()) is not None
            and parsed.username is None
            and parsed.password is None
            and parsed.port in (None, 443)
            and parsed.path in ("", "/")
            and not parsed.query
            and not parsed.fragment
        )
    except ValueError:
        valid = False
    if not valid:
        raise Failure(f"{label} must be a Kusto HTTPS endpoint without credentials or a path")
    return f"https://{parsed.hostname.lower()}"


def _batch(value):
    if value is None:
        return None
    if not isinstance(value, dict) or value.get("batch_class") not in ("bootstrap", "measured"):
        raise Failure("demo-2.verification_batch must be a bootstrap or measured batch")
    try:
        validate_batch_evidence(value)
    except (VerificationError, TypeError, ValueError) as exc:
        raise Failure("demo-2.verification_batch is not a valid complete v1 batch") from exc
    return dict(value)


def validate_config(config, prefix):
    if not isinstance(config, dict) or type(config.get("contract_version")) is not int or config["contract_version"] != 1:
        raise Failure("configuration contract_version must be 1")
    if not isinstance(prefix, str) or not PREFIX.fullmatch(prefix):
        raise Failure("prefix must be 1-40 lowercase letters, digits or hyphens, beginning with a letter or digit")
    demo = config.get("demo-2")
    execution = config.get("execution", {})
    data = config.get("data")
    shared = config.get("shared", {})
    capacity = config.get("capacity", {})
    if not isinstance(demo, dict) or not isinstance(execution, dict):
        raise Failure("demo-2 and execution must be objects")
    if not isinstance(data, dict) or not isinstance(shared, dict) or not isinstance(capacity, dict):
        raise Failure("data, shared and capacity must be objects")
    reuse_synthetic_target = demo.get("reuse_synthetic_target", False)
    if type(reuse_synthetic_target) is not bool:
        raise Failure("demo-2.reuse_synthetic_target must be a boolean")

    if "verification_timeout_seconds" in execution:
        _positive_seconds(execution["verification_timeout_seconds"], "execution.verification_timeout_seconds")
    override = demo.get("verification_timeout_seconds")
    if override is not None:
        _positive_seconds(override, "demo-2.verification_timeout_seconds")
    timeout = override if override is not None else execution.get("verification_timeout_seconds", 900)
    poll = _positive_seconds(execution.get("poll_interval_seconds", 15), "execution.poll_interval_seconds")
    track_timeout = _positive_seconds(execution.get("track_timeout_seconds", 1800),
                                      "execution.track_timeout_seconds")
    concurrency = execution.get("max_concurrency", 4)
    if type(concurrency) is not int or not 1 <= concurrency <= 4:
        raise Failure("execution.max_concurrency must be an integer from 1 to 4")
    for field in ("file_count", "max_duration_seconds"):
        if field in data:
            _positive_seconds(data[field], f"data.{field}")
    rows_per_file = data.get("rows_per_file")
    if "rows_per_file" in data and (
        type(rows_per_file) is not int or not 1 <= rows_per_file <= 2**63 - 1
    ):
        raise Failure("data.rows_per_file must be a positive 64-bit integer")
    interval_seconds = data.get("interval_seconds")
    if "interval_seconds" in data and (
        type(interval_seconds) is not int or interval_seconds < 0
    ):
        raise Failure("data.interval_seconds must be a nonnegative integer")
    latency = demo.get("target_latency_minutes", 5)
    if type(latency) is not int or latency != 5:
        raise Failure("demo-2.target_latency_minutes must be exactly 5")

    workspace_name = demo.get("workspace_name")
    if workspace_name is None:
        workspace_name = "{prefix}-demo2-eventhouse"
    if not isinstance(workspace_name, str):
        raise Failure("demo-2.workspace_name must be a string")
    workspace_name = workspace_name.replace("{prefix}", prefix)
    if (not workspace_name.startswith(prefix + "-")
            or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9 _-]{0,255}", workspace_name)):
        raise Failure("demo-2.workspace_name must be a safe name scoped by the prefix")

    for field in ("date_count", "product_count", "customer_count"):
        if type(data.get(field)) is not int or not 1 <= data[field] <= 2**31 - 1:
            raise Failure(f"data.{field} must be a positive 32-bit integer")
    if type(data.get("seed")) is not int:
        raise Failure("data.seed must be an integer")
    try:
        start = date.fromisoformat(data.get("start_date"))
        start + timedelta(days=data["date_count"] - 1)
    except (TypeError, ValueError, OverflowError) as exc:
        raise Failure("data.start_date and date_count must form a valid date range") from exc
    if start.isoformat() != data["start_date"]:
        raise Failure("data.start_date must use YYYY-MM-DD")
    if data.get("currency", "USD") != "USD":
        raise Failure("data.currency must be USD")
    run_id = data.get("run_id")
    if run_id is not None and (
        not isinstance(run_id, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}", run_id)
    ):
        raise Failure("data.run_id is invalid")

    authentication = config.get("authentication", {})
    if not isinstance(authentication, dict):
        raise Failure("authentication must be an object")
    if authentication.get("method", "signed-in-identity") != "signed-in-identity":
        raise Failure("only signed-in-identity authentication is supported")

    expected_location = None
    if demo.get("onelake_table_path") is not None:
        try:
            expected_location = discover_mirror_location({
                "IsEnabled": True, "Format": "parquet", "TargetLatencyInMinutes": 5,
                "ConnectionStrings": [demo["onelake_table_path"]],
            }, _optional_name(demo.get("sales_table"), "FactSales", "sales table"))
        except ConfigurationError as exc:
            raise Failure("demo-2.onelake_table_path is not a valid table location") from exc

    ingestion_endpoint = demo.get("ingestion_endpoint")
    if ingestion_endpoint is not None:
        ingestion_endpoint = _service_uri(ingestion_endpoint, "demo-2.ingestion_endpoint")
    query_endpoint = demo.get("query_endpoint")
    if query_endpoint is not None:
        query_endpoint = _service_uri(query_endpoint, "demo-2.query_endpoint")
    delivery = demo.get("delivery_method")
    if delivery is not None and delivery not in ("queued", "queued-ingestion", "kusto-queued"):
        raise Failure("demo-2.delivery_method must select queued Kusto ingestion")
    capacity_name = capacity.get("name")
    if capacity_name is not None and (not isinstance(capacity_name, str) or not capacity_name.strip()):
        raise Failure("capacity.name must be a nonempty string")
    schema = demo.get("lakehouse_schema")
    if schema is not None:
        schema = _optional_name(schema, None, "lakehouse schema")
    identity_type = shared.get("identity_type")
    if identity_type is not None and identity_type not in ("User", "ServicePrincipal"):
        raise Failure("shared.identity_type must be User or ServicePrincipal")
    return {
        "prefix": prefix,
        "tenant_id": _optional_uuid(config.get("tenant_id"), "tenant_id"),
        "workspace_name": workspace_name,
        "workspace_id": _optional_uuid(demo.get("workspace_id"), "demo-2.workspace_id"),
        "eventhouse_name": _optional_name(demo.get("eventhouse_name"), "SalesEventhouse", "eventhouse name"),
        "eventhouse_id": _optional_uuid(demo.get("eventhouse_id"), "demo-2.eventhouse_id"),
        "database_name": _optional_name(demo.get("kql_database_name"), "SalesDatabase", "KQL database name"),
        "database_name_supplied": demo.get("kql_database_name") is not None,
        "database_id": _optional_uuid(demo.get("kql_database_id"), "demo-2.kql_database_id"),
        "lakehouse_name": _optional_name(demo.get("lakehouse_name"), "SalesLakehouse", "lakehouse name"),
        "lakehouse_id": _optional_uuid(demo.get("lakehouse_id"), "demo-2.lakehouse_id"),
        "notebook_name": _optional_name(demo.get("notebook_name"), "Demo2Validation", "notebook name"),
        "notebook_id": _optional_uuid(demo.get("notebook_id"), "demo-2.notebook_id"),
        "monitor_notebook_name": _optional_name(demo.get("monitor_notebook_name"), "Demo2Monitor",
                                                "monitor notebook name"),
        "monitor_notebook_id": _optional_uuid(demo.get("monitor_notebook_id"), "demo-2.monitor_notebook_id"),
        "measured_notebook_name": "Demo2MeasuredValidation",
        "measured_monitor_notebook_name": "Demo2MeasuredMonitor",
        "reuse_synthetic_target": reuse_synthetic_target,
        "table": _optional_name(demo.get("sales_table"), "FactSales", "sales table"),
        "mapping": _optional_name(demo.get("csv_mapping"), "SalesCsvV1", "CSV mapping"),
        "shortcut": _optional_name(demo.get("shortcut_name"), "EventhouseFactSales", "shortcut name"),
        "schema": schema,
        "capacity_id": _optional_uuid(capacity.get("id"), "capacity.id"),
        "capacity_name": capacity_name,
        "ingestion_endpoint": ingestion_endpoint,
        "query_endpoint": query_endpoint,
        "expected_location": expected_location,
        "producer_identity": _optional_uuid(shared.get("identity_id"), "shared.identity_id"),
        "producer_identity_type": identity_type,
        "target_bindings": shared.get("target_bindings"),
        "synthetic_proof": config.get("synthetic_proof"),
        "shared_workspace_id": _optional_uuid(shared.get("workspace_id"), "shared.workspace_id"),
        "shared_lakehouse_id": _optional_uuid(shared.get("staging_lakehouse_id"),
                                               "shared.staging_lakehouse_id"),
        "run_state_location": shared.get("run_state_location"),
        "batch": _batch(demo.get("verification_batch")),
        "timeout": timeout,
        "track_timeout": track_timeout,
        "poll": poll,
        "max_concurrency": concurrency,
        "expected_batch_count": data.get("file_count"),
        "max_duration": data.get("max_duration_seconds"),
        "rows_per_file": rows_per_file,
        "interval_seconds": interval_seconds,
        "data": {key: data.get(key) for key in
                 ("run_id", "seed", "start_date", "date_count", "product_count", "customer_count")},
    }


def _synthetic_run(settings):
    scope = re.fullmatch(r"demo2synthetic-([0-9a-f]{12})", settings.get("prefix", ""))
    return bool(scope and settings.get("data", {}).get("run_id") == f"demo2synthetic_{scope[1]}")


def _notebook_key(settings, *, monitor=False):
    role = "monitor_notebook" if monitor else "notebook"
    return f"measured_{role}" if settings.get("reuse_synthetic_target") else role


def _atomic_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.pending")
    try:
        with temporary.open("x", encoding="utf-8", newline="\n") as stream:
            json.dump(value, stream, indent=2, sort_keys=True)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        for attempt in range(6):
            try:
                os.replace(temporary, path)
                break
            except PermissionError:
                if attempt == 5:
                    raise
                time.sleep(0.05 * 2**attempt)
    finally:
        if temporary.exists():
            temporary.unlink()


def _atomic_create_json(path, value):
    """Create an immutable record without ever replacing an existing one."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.pending")
    try:
        with temporary.open("x", encoding="utf-8", newline="\n") as stream:
            json.dump(value, stream, indent=2, sort_keys=True)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.link(temporary, path)
    finally:
        if temporary.exists():
            temporary.unlink()


class Ledger:
    def __init__(self, directory, settings, create=False):
        self.directory = directory
        self.path = directory / STATE_FILE
        self.lock = directory / LOCK_FILE
        self.settings = settings
        self.create = create
        self.data = None
        self.lock_fd = None

    def _acquire(self):
        try:
            self.lock.mkdir(mode=0o700, exist_ok=True)
        except FileExistsError as exc:
            raise Blocked("A legacy or unrecognized Demo 2 lock exists; its owner must be reconciled.") from exc
        except OSError as exc:
            raise Blocked("The Demo 2 OS-backed lock directory cannot be established safely.") from exc
        try:
            if self.lock.is_symlink() or not self.lock.is_dir():
                raise Blocked("The Demo 2 lock directory is not a trusted directory.")
            if any(path.name != LOCK_LEASE for path in self.lock.iterdir()):
                raise Blocked("The Demo 2 lock directory contains unrecognized ownership data.")
            lease = self.lock / LOCK_LEASE
            if lease.is_symlink():
                raise Blocked("The Demo 2 lock file is not a trusted regular file.")
            flags = os.O_CREAT | os.O_RDWR | getattr(os, "O_BINARY", 0) | getattr(os, "O_NOFOLLOW", 0)
            fd = os.open(lease, flags, 0o600)
            try:
                os.set_inheritable(fd, False)
                if not stat.S_ISREG(os.fstat(fd).st_mode):
                    raise Blocked("The Demo 2 lock file is not a trusted regular file.")
                if os.name == "nt":
                    import msvcrt

                    os.lseek(fd, 0, os.SEEK_SET)
                    msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
                elif os.name == "posix":
                    import fcntl

                    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                else:
                    raise Blocked("This platform cannot establish an OS-backed Demo 2 lock.")
                opened, current = os.fstat(fd), os.stat(lease, follow_symlinks=False)
                if (opened.st_dev, opened.st_ino) != (current.st_dev, current.st_ino):
                    raise Blocked("The Demo 2 lock file changed while acquiring ownership.")
                self.lock_fd = fd
            except BaseException:
                os.close(fd)
                raise
        except Blocked:
            raise
        except (OSError, ImportError) as exc:
            raise Blocked("The Demo 2 state directory is locked or its OS-backed lock cannot be acquired.") from exc

    def __enter__(self):
        if not self.path.exists() and not self.create:
            raise Blocked("No Demo 2 ownership ledger exists in state-dir; no resource can be modified or cleaned up.")
        if self.create:
            self.directory.mkdir(parents=True, exist_ok=True)
        self._acquire()
        try:
            if self.path.exists():
                with self.path.open(encoding="utf-8") as stream:
                    self.data = json.load(stream)
                self._validate()
            elif self.create:
                self.data = {
                    "contract_version": 1, "track": "demo-2", "prefix": self.settings["prefix"],
                    "tenant_id": self.settings["tenant_id"],
                    "owner_marker": f"copilot-demo-2:{self.settings['prefix']}:{uuid.uuid4()}",
                    "resources": {}, "pending": None,
                }
                self.save()
            else:
                raise Blocked("No Demo 2 ownership ledger exists in state-dir.")
            return self
        except Exception:
            self.__exit__(None, None, None)
            raise

    def __exit__(self, *_):
        if self.lock_fd is not None:
            fd, self.lock_fd = self.lock_fd, None
            os.close(fd)

    def _validate(self):
        state = self.data
        expected_prefix = f"copilot-demo-2:{self.settings['prefix']}:"
        if (not isinstance(state, dict) or type(state.get("contract_version")) is not int
                or state["contract_version"] != 1 or state.get("track") != "demo-2"
                or state.get("prefix") != self.settings["prefix"]
                or state.get("tenant_id") != self.settings["tenant_id"]
                or not isinstance(state.get("resources"), dict)
                or not isinstance(state.get("owner_marker"), str)
                or not state["owner_marker"].startswith(expected_prefix)):
            raise Blocked("The ownership ledger does not match this prefix and tenant.")
        try:
            uuid.UUID(state["owner_marker"][len(expected_prefix):])
        except ValueError as exc:
            raise Blocked("The ownership marker in the ledger is invalid.") from exc
        for key in ("workspace", "eventhouse", "database", "lakehouse", "notebook", "monitor_notebook",
                    "measured_notebook", "measured_monitor_notebook"):
            entry = state["resources"].get(key)
            if entry is not None:
                try:
                    if not isinstance(entry, dict) or _uuid(entry.get("id"), "recorded resource ID") != entry["id"]:
                        raise ValueError
                except (Failure, KeyError, ValueError) as exc:
                    raise Blocked("The ownership ledger contains an invalid resource ID.") from exc
        if state.get("pending") is not None and not isinstance(state["pending"], dict):
            raise Blocked("The ownership ledger contains invalid pending operation state.")

    def save(self):
        _atomic_json(self.path, self.data)

    def intent(self, kind, **fields):
        if self.data.get("pending") is not None:
            raise Blocked("An earlier operation has an uncertain outcome; reconcile it before continuing.")
        self.data["pending"] = {"kind": kind, **fields}
        self.save()

    def finish(self):
        self.data["pending"] = None
        self.save()


class Deadline:
    def __init__(self, seconds, clock=time.monotonic, sleeper=time.sleep, label="verification"):
        self.clock = clock
        self.sleeper = sleeper
        self.label = label
        self.end = clock() + seconds

    def remaining(self):
        remaining = self.end - self.clock()
        if remaining <= 0:
            raise Blocked(f"The configured {self.label} deadline expired; the operation was not assumed successful.")
        return remaining

    def wait(self, seconds):
        remaining = self.remaining()
        if seconds >= remaining:
            self.sleeper(remaining)
            raise Blocked(f"The configured {self.label} deadline expired; the operation was not assumed successful.")
        self.sleeper(seconds)

    def child(self, seconds):
        self.remaining()
        child = object.__new__(Deadline)
        child.clock = self.clock
        child.sleeper = self.sleeper
        requested_end = self.clock() + seconds
        child.end = min(self.end, requested_end)
        child.label = self.label if self.end <= requested_end else "verification"
        return child


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def _fabric_url(path):
    url = path if path.startswith("https://") else FABRIC + path
    try:
        parsed = urlsplit(url)
        valid = (parsed.scheme == "https" and parsed.hostname == "api.fabric.microsoft.com"
                 and parsed.username is None and parsed.password is None
                 and parsed.port in (None, 443) and not parsed.fragment
                 and parsed.path.startswith("/v1/"))
    except (TypeError, ValueError):
        valid = False
    if not valid:
        raise Failure("Fabric returned an unsafe API URL")
    return url


class FabricClient:
    def __init__(self, tenant_id, deadline):
        if tenant_id is None:
            raise Blocked("tenant_id is required to verify the Azure CLI identity safely.")
        self.tenant_id = tenant_id
        self.deadline = deadline
        self.opener = build_opener(_NoRedirect())
        self.fabric_skill = "eventhouse-cli"
        self._tokens = {}

    def _token(self, audience):
        self.deadline.remaining()
        key = (audience, os.environ.get("AZURE_CONFIG_DIR"))
        cached = self._tokens.get(key)
        if cached is not None and time.time() < cached[1]:
            return cached[0]
        self._tokens.pop(key, None)
        az = shutil.which("az")
        if not az:
            raise Blocked("Azure CLI is unavailable; sign in using the intended isolated Azure configuration.")
        try:
            proc = subprocess.run(
                [az, "account", "get-access-token", "--resource", audience,
                 "--tenant", self.tenant_id, "--output", "json", "--only-show-errors"],
                capture_output=True, text=True, timeout=self.deadline.remaining(), check=False,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise Blocked("Azure CLI token acquisition failed or timed out.") from exc
        if proc.returncode:
            raise Blocked("Azure CLI has no usable token for the configured tenant and audience.")
        try:
            token = json.loads(proc.stdout)
            if (_uuid(token["tenant"], "token tenant") != self.tenant_id
                    or not isinstance(token["accessToken"], str) or not token["accessToken"]):
                raise ValueError
            expires = token.get("expires_on")
            if (type(expires) is int or isinstance(expires, str) and expires.isascii() and expires.isdecimal()):
                refresh_at = min(time.time() + 600, int(expires) - 60)
                if refresh_at > time.time():
                    self._tokens[key] = (token["accessToken"], refresh_at)
            return token["accessToken"]
        except (KeyError, TypeError, ValueError, Failure, json.JSONDecodeError) as exc:
            raise Blocked("Azure CLI returned a token for an unverifiable or different tenant.") from exc

    def _request(self, method, url, audience, body=None, allow_missing=False, extra_headers=None):
        payload = None if body is None else json.dumps(body, separators=(",", ":")).encode("utf-8")
        headers = {"Authorization": f"Bearer {self._token(audience)}", "Accept": "application/json"}
        if extra_headers:
            headers.update(extra_headers)
        if payload is not None:
            headers["Content-Type"] = "application/json"
        if audience == FABRIC:
            headers["x-ms-fabric-skill"] = self.fabric_skill
        elif audience == STORAGE_AUDIENCE:
            headers["x-ms-version"] = "2020-10-02"
            headers["x-ms-date"] = format_datetime(datetime.now(timezone.utc), usegmt=True)
            headers["Cache-Control"] = "no-cache"
        request = Request(url, data=payload, headers=headers, method=method)
        try:
            with self.opener.open(request, timeout=self.deadline.remaining()) as response:
                raw = response.read(12 * 1024 * 1024 + 1)
                status = response.status
                response_headers = {key.lower(): value for key, value in response.headers.items()}
        except HTTPError as exc:
            if exc.code == 404 and allow_missing:
                return 404, {}, None
            service = "Fabric" if audience == FABRIC else "OneLake" if audience == STORAGE_AUDIENCE else "Kusto"
            if exc.code in (401, 403):
                raise Blocked(f"{service} access denied (HTTP {exc.code}).") from exc
            raise Failure(f"{service} request failed (HTTP {exc.code}).") from exc
        except (URLError, OSError, TimeoutError) as exc:
            raise Blocked("A service request failed or timed out; its outcome may be uncertain.") from exc
        if len(raw) > 12 * 1024 * 1024:
            raise Failure("A service response exceeded the size limit")
        try:
            data = json.loads(raw) if raw else None
        except (ValueError, UnicodeError) as exc:
            raise Failure("A service returned invalid JSON") from exc
        return status, response_headers, data

    def fabric(self, method, path, body=None, allow_missing=False):
        return self._request(method, _fabric_url(path), FABRIC, body, allow_missing)

    def onelake(self, method, url, allow_missing=False, extra_headers=None):
        parsed = urlsplit(url)
        if (parsed.scheme != "https" or parsed.netloc != "onelake.blob.fabric.microsoft.com"
                or parsed.query or parsed.fragment or not parsed.path.startswith("/")):
            raise Failure("The OneLake monitor URL is invalid")
        return self._request(method, url, STORAGE_AUDIENCE, allow_missing=allow_missing,
                             extra_headers=extra_headers)

    def pages(self, path):
        next_url = _fabric_url(path)
        original_path = urlsplit(next_url).path
        seen = set()
        rows = []
        while next_url:
            if next_url in seen or urlsplit(next_url).path != original_path:
                raise Failure("Fabric returned an invalid pagination continuation")
            seen.add(next_url)
            status, _, page = self.fabric("GET", next_url)
            if status != 200 or not isinstance(page, dict) or not isinstance(page.get("value"), list):
                raise Failure("Fabric returned an invalid paginated response")
            rows.extend(page["value"])
            continuation = page.get("continuationUri")
            if continuation is not None and not isinstance(continuation, str):
                raise Failure("Fabric returned an invalid pagination continuation")
            next_url = _fabric_url(continuation) if continuation else None
        return rows

    def kusto(self, endpoint, database, command, query=False):
        endpoint = _service_uri(endpoint, "Kusto query endpoint")
        path = "/v1/rest/query" if query else "/v1/rest/mgmt"
        status, _, data = self._request("POST", endpoint + path, KUSTO_AUDIENCE,
                                        {"db": database, "csl": command})
        if status != 200:
            raise Failure("Kusto returned an unexpected response status")
        return _kusto_rows(data)


def _kusto_rows(data):
    if not isinstance(data, dict) or data.get("Exceptions"):
        raise Failure("Kusto returned a failed or invalid response")
    tables = data.get("Tables")
    if not isinstance(tables, list) or not tables:
        raise Failure("Kusto returned no result table")
    parsed = []
    for table in tables:
        if not isinstance(table, dict) or not isinstance(table.get("Columns"), list) or not isinstance(table.get("Rows"), list):
            raise Failure("Kusto returned an invalid result table")
        try:
            columns = [column["ColumnName"] for column in table["Columns"]]
            if len(set(columns)) != len(columns):
                raise ValueError
            rows = []
            for row in table["Rows"]:
                if not isinstance(row, list) or len(row) != len(columns):
                    raise ValueError
                rows.append(dict(zip(columns, row)))
        except (KeyError, TypeError, ValueError) as exc:
            raise Failure("Kusto returned an invalid result table") from exc
        if table.get("TableName") == "QueryStatus":
            for row in rows:
                severity = row.get("Severity")
                if type(severity) not in (int, float) or severity <= 2:
                    raise Failure("Kusto reported a partial query failure")
        parsed.append(rows)
    return parsed[0]


def _one(rows, label):
    if not isinstance(rows, list) or len(rows) != 1 or not isinstance(rows[0], dict):
        raise Blocked(f"{label} readback is missing or ambiguous.")
    return rows[0]


def _json_value(value, label):
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except (ValueError, TypeError) as exc:
            raise Blocked(f"{label} readback is invalid.") from exc
    return value


def verify_schema(rows, table, owner_marker=None):
    row = _one(rows, "KQL table schema")
    if row.get("TableName") != table:
        raise Blocked("KQL table schema refers to a different table.")
    if owner_marker is not None and row.get("DocString") != owner_marker:
        raise Blocked("KQL table ownership marker does not match the deployment ledger.")
    schema = _json_value(row.get("Schema"), "KQL table schema")
    if not isinstance(schema, dict) or not isinstance(schema.get("OrderedColumns"), list):
        raise Blocked("KQL table schema has no ordered columns.")
    try:
        actual = [(column["Name"], column["CslType"].lower()) for column in schema["OrderedColumns"]]
    except (KeyError, TypeError, AttributeError) as exc:
        raise Blocked("KQL table schema has invalid columns.") from exc
    if actual != list(CSV_COLUMNS):
        raise Blocked("KQL table schema does not match the v1 CSV contract.")


def verify_mapping(rows, mapping):
    matching = [row for row in rows if isinstance(row, dict) and row.get("Name") == mapping]
    row = _one(matching, "KQL CSV mapping")
    if str(row.get("Kind", "")).lower() != "csv":
        raise Blocked("The applied ingestion mapping is not CSV.")
    entries = _json_value(row.get("Mapping"), "KQL CSV mapping")
    if not isinstance(entries, list) or len(entries) != len(CSV_COLUMNS):
        raise Blocked("KQL CSV mapping has the wrong number of columns.")
    actual = []
    for item in entries:
        if not isinstance(item, dict):
            raise Blocked("KQL CSV mapping has an invalid column.")
        props = item.get("Properties") or {}
        if not isinstance(props, dict):
            raise Blocked("KQL CSV mapping has invalid properties.")
        ordinal = item.get("Ordinal", props.get("Ordinal"))
        try:
            if type(ordinal) is bool:
                raise ValueError
            ordinal = int(ordinal)
        except (ValueError, TypeError) as exc:
            raise Blocked("KQL CSV mapping has an invalid ordinal.") from exc
        name = item.get("Name", item.get("column", item.get("Column")))
        kind = item.get("DataType")
        if (not isinstance(kind, str) or item.get("ConstValue") is not None
                or item.get("Transform") is not None
                or props.get("ConstValue") is not None or props.get("Transform") is not None):
            raise Blocked("KQL CSV mapping contains an unsupported conversion.")
        actual.append((ordinal, name, kind.lower()))
    expected = [(index, name, kind) for index, (name, kind) in enumerate(CSV_COLUMNS)]
    if sorted(actual) != expected:
        raise Blocked("KQL CSV mapping does not match the v1 CSV contract.")


def _integer(value):
    try:
        number = Decimal(str(value))
        if not number.is_finite() or number != number.to_integral_value():
            raise ValueError
        return int(number)
    except (InvalidOperation, ValueError, TypeError, OverflowError) as exc:
        raise Blocked("KQL batch verification returned noninteger evidence.") from exc


def verify_kql_batch(rows, batch):
    row = _one(rows, "KQL batch")
    try:
        count = _integer(row["RowCount"])
        distinct = _integer(row["DistinctSalesLineIDs"])
        amount = _integer(row["AmountMinorUnits"] if count or row["AmountMinorUnits"] is not None else 0)
        invalid = _integer(row["InvalidRows"] if count or row["InvalidRows"] is not None else 0)
    except KeyError as exc:
        raise Blocked("KQL batch verification omitted required evidence.") from exc
    if (count < 0 or distinct != count or invalid != 0 or amount < 0
            or amount > batch["sales_amount_minor_units"] or count > batch["row_count"]):
        raise Blocked("KQL batch counts, distinct keys, validity or exact total do not match.")
    if count < batch["row_count"]:
        raise BatchPending("The KQL batch has not yet fully arrived.")
    if amount != batch["sales_amount_minor_units"]:
        raise Blocked("KQL batch counts, distinct keys, validity or exact total do not match.")


def _notebook_projection(notebook):
    if not isinstance(notebook, dict) or not isinstance(notebook.get("cells"), list):
        raise Blocked("Notebook definition is invalid.")
    try:
        metadata = notebook["metadata"]
        trident = metadata["trident"]
        if not isinstance(trident, dict) or not isinstance(trident.get("lakehouse"), dict):
            raise ValueError
        cells = []
        for cell in notebook["cells"]:
            source = cell["source"]
            if isinstance(source, list):
                source = "".join(source)
            if not isinstance(source, str):
                raise ValueError
            cell_metadata = cell.get("metadata", {})
            tags = cell_metadata.get("tags", [])
            if not isinstance(tags, list):
                raise ValueError
            cells.append((cell["cell_type"], source, sorted(tags), cell_metadata.get("trident"),
                          cell_metadata.get("language"), cell_metadata.get("disabled")))
        return trident, metadata.get("kernelspec"), metadata.get("language_info"), cells
    except (AttributeError, KeyError, TypeError, ValueError) as exc:
        raise Blocked("Notebook binding or executable definition is invalid.") from exc


def _read_notebook_definition(definition):
    if not isinstance(definition, dict):
        raise Blocked("Notebook definition readback is unavailable.")
    definition = definition.get("definition", definition)
    try:
        parts = [part for part in definition["parts"] if part["path"] == "notebook-content.ipynb"
                 and part["payloadType"] == "InlineBase64"]
        if len(parts) != 1:
            raise ValueError
        return json.loads(base64.b64decode(parts[0]["payload"], validate=True))
    except (KeyError, TypeError, ValueError) as exc:
        raise Blocked("Notebook definition readback is invalid.") from exc


def _verify_notebook_exit(job, mode, batch):
    try:
        result = json.loads(job["properties"]["exitValue"])
    except (KeyError, TypeError, ValueError) as exc:
        raise Blocked("The notebook completed without a mode-specific verification exit value.") from exc
    if not isinstance(result, dict) or result.get("status") not in ("pass", "verified") or result.get("mode") != mode:
        raise Blocked("The notebook exit value does not prove the requested verification mode and batch.")
    if batch is not None:
        nested = result.get("batch")
        if (not isinstance(nested, dict) or nested.get("batch_id") != batch["batch_id"]
                or type(nested.get("row_count")) is not int or nested["row_count"] != batch["row_count"]
                or type(nested.get("sales_amount_minor_units")) is not int
                or nested["sales_amount_minor_units"] != batch["sales_amount_minor_units"]):
            raise Blocked("The notebook exit value does not prove the requested verification mode and batch.")
    return result


def _verify_dimension_exit(result, mode, settings):
    base = ".".join(part for part in (settings["lakehouse_name"], settings["schema"]) if part)
    expected = {f"{base}.{name}": count for name, count in (
        ("DimDate", settings["data"]["date_count"]), ("DimProduct", settings["data"]["product_count"]),
        ("DimCustomer", settings["data"]["customer_count"]),
    )}
    fields = ("missing",) if mode == "dimension_gaps" else ("missing_before", "missing_after")
    for field in fields:
        counts = result.get(field)
        if (not isinstance(counts, dict) or set(counts) != set(expected)
                or any(type(counts[key]) is not int or not 0 <= counts[key] <= expected[key] for key in expected)
                or (field == "missing_after" and any(counts.values()))):
            raise Blocked("The dimension-maintenance notebook did not verify the exact expected dimensions.")


def _load_builder():
    generator = ROOT / "demo-data-generator" / "generator.py"
    if not generator.is_file():
        raise Blocked("The shared demo-data-generator/generator.py is unavailable; notebook deployment is blocked.")
    try:
        module = importlib.import_module("notebook_builder")
    except ImportError as exc:
        raise Blocked("The Demo 2 notebook_builder artifact or one of its dependencies is unavailable.") from exc
    if not callable(getattr(module, "build_notebook", None)) or not callable(getattr(module, "fabric_definition", None)):
        raise Blocked("The Demo 2 notebook_builder does not implement the required build and definition interface.")
    return module, generator


def _load_monitor_builder():
    try:
        module = importlib.import_module("monitor_notebook_builder")
    except ImportError as exc:
        raise Blocked("The Demo 2 monitor notebook builder or one of its dependencies is unavailable.") from exc
    if not callable(getattr(module, "build_monitor_notebook", None)):
        raise Blocked("The Demo 2 monitor notebook builder does not implement the required build interface.")
    return module


def _load_monitor_contract():
    try:
        module = importlib.import_module("monitor_controller")
    except ImportError as exc:
        raise Blocked("The Demo 2 monitor evidence contract is unavailable.") from exc
    error = getattr(module, "MonitorError", None)
    if (not isinstance(error, type) or not issubclass(error, Exception)
            or any(not callable(getattr(module, name, None)) for name in
                   ("_measured", "_exit", "batch_fingerprint", "verify_delivery_evidence"))):
        raise Blocked("The Demo 2 monitor evidence contract is incompatible.")
    return module


@contextlib.contextmanager
def _spark_client(client):
    previous = getattr(client, "fabric_skill", None)
    client.fabric_skill = "spark-cli"
    try:
        yield
    finally:
        if previous is None:
            del client.fabric_skill
        else:
            client.fabric_skill = previous


def _spark_operation(method):
    @functools.wraps(method)
    def wrapped(self, *args, **kwargs):
        with _spark_client(self.client):
            return method(self, *args, **kwargs)
    return wrapped


def _token_identity(client, audience, tenant, identity):
    token_source = getattr(client, "_token", None)
    if not callable(token_source):
        raise Blocked("The producer-runtime identity cannot be checked against the Azure CLI tokens.")
    try:
        token = token_source(audience)
        parts = token.split(".")
        if len(parts) != 3 or not parts[1]:
            raise ValueError
        payload = parts[1].encode("ascii")
        payload += b"=" * (-len(payload) % 4)
        claims = json.loads(base64.b64decode(payload, altchars=b"-_", validate=True))
        if (not isinstance(claims, dict) or _uuid(claims["tid"], "token tenant") != tenant
                or _uuid(claims["oid"], "token principal") != identity
                or type(claims["exp"]) is not int or claims["exp"] <= time.time()):
            raise ValueError
    except (AttributeError, KeyError, TypeError, ValueError, UnicodeError, binascii.Error, Failure) as exc:
        raise Blocked("The Azure CLI token tenant or principal does not match the configured producer identity.") from exc


def verify_producer_access(settings, client, database):
    """Verify the configured principal's live access; the producer rechecks its runtime before staging."""
    identity = settings["producer_identity"]
    tenant = settings["tenant_id"]
    workspace = settings["workspace_id"]
    if identity is None or tenant is None:
        raise Blocked("The producer-runtime identity and tenant are required to verify access.")
    try:
        target = _targets(settings["target_bindings"])["demo-2"]
    except (KeyError, TargetBindingError) as exc:
        raise Blocked("The producer's exact Demo 2 target binding is required to verify access.") from exc
    if (workspace is not None and workspace != target["workspace_id"]
            or database["id"] != target["item_id"] or settings["table"] != target["fact"]):
        raise Blocked("The producer's target binding differs from the verified Demo 2 database.")
    for audience in (FABRIC, KUSTO_AUDIENCE, STORAGE_AUDIENCE):
        _token_identity(client, audience, tenant, identity)
    roles = client.pages(f"/v1/workspaces/{target['workspace_id']}/roleAssignments")
    expected_type = settings.get("producer_identity_type")
    if not any(
        isinstance(role, dict) and role.get("role") in ("Admin", "Member", "Contributor")
        and isinstance(role.get("principal"), dict)
        and role["principal"].get("id") == identity
        and role["principal"].get("type") in ("User", "ServicePrincipal")
        and (expected_type is None or role["principal"]["type"] == expected_type)
        for role in roles
    ):
        raise Blocked("The configured producer principal lacks a direct write role on the Demo 2 workspace.")
    rows = client.kusto(database["query_uri"], database["name"], "print ProducerRuntimeProbe=1", query=True)
    try:
        if _integer(_one(rows, "Producer query probe")["ProducerRuntimeProbe"]) != 1:
            raise ValueError
    except (KeyError, ValueError) as exc:
        raise Blocked("The configured producer principal's Kusto query probe did not succeed.") from exc


def _customer_delta_snapshot(settings, client, resources):
    """Read immutable Delta log heads for the verified dimensions and shortcut."""
    if settings["schema"] is not None:
        raise Blocked("Snapshot reuse is only supported for the owned non-schema Lakehouse.")
    workspace = _uuid(resources["workspace"]["id"], "workspace ID")
    lakehouse = _uuid(resources["lakehouse"]["id"], "Lakehouse ID")
    tables = {"DimDate": "dimdate", "DimProduct": "dimproduct",
              "DimCustomer": "dimcustomer", "shortcut": settings["shortcut"]}
    snapshots = {}
    for name, table in tables.items():
        directory = f"{lakehouse}/Tables/{table}/_delta_log"
        token = None
        seen = set()
        versions = {}
        while True:
            query = {"resource": "filesystem", "directory": directory,
                     "recursive": "false", "maxResults": "5000"}
            if token is not None:
                query["continuation"] = token
            url = f"https://onelake.dfs.fabric.microsoft.com/{workspace}?{urlencode(query)}"
            status, headers, data = client._request("GET", url, STORAGE_AUDIENCE)
            if status != 200 or not isinstance(data, dict) or not isinstance(data.get("paths"), list):
                raise Blocked("OneLake did not return a usable Delta transaction-log listing.")
            for entry in data["paths"]:
                if not isinstance(entry, dict) or not isinstance(entry.get("name"), str):
                    raise Blocked("OneLake returned an invalid Delta transaction-log entry.")
                match = re.fullmatch(re.escape(directory) + r"/([0-9]{20})\.json", entry["name"])
                if match is None:
                    continue
                version = match.group(1)
                etag = entry.get("etag")
                if not isinstance(etag, str) or not etag or version in versions:
                    raise Blocked("The Delta transaction-log identity is missing or ambiguous.")
                versions[version] = etag
            token = headers.get("x-ms-continuation")
            if not token:
                break
            if not isinstance(token, str) or token in seen or len(seen) >= 100:
                raise Blocked("OneLake returned an invalid Delta transaction-log continuation.")
            seen.add(token)
        if not versions:
            raise Blocked(f"The {name} Delta transaction log has no visible commit.")
        latest = max(versions)
        history = json.dumps(sorted(versions.items()), separators=(",", ":")).encode("utf-8")
        snapshots[name] = {"version": latest, "etag": versions[latest],
                           "history_sha256": hashlib.sha256(history).hexdigest()}
    return snapshots


def _customer_verification_binding(settings, resources):
    try:
        selected = set(_targets(settings["target_bindings"]))
        monitor = _monitor_config(settings, selected)
        batch = settings["batch"]
        if batch is None:
            raise ValueError
        batch_bytes = json.dumps(batch, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                                 allow_nan=False).encode("utf-8")
        return {
            "run_id": monitor["run_id"],
            "monitor_binding_sha256": monitor["monitor_binding_sha256"],
            "batch_sha256": hashlib.sha256(batch_bytes).hexdigest(),
            "workspace_id": resources["workspace"]["id"],
            "lakehouse_id": resources["lakehouse"]["id"],
            "notebook_id": resources["measured_notebook"]["id"],
        }
    except (KeyError, TypeError, ValueError, TargetBindingError) as exc:
        raise Blocked("The customer verification cannot be bound to the exact run, batch and notebook.") from exc


def verify_monitor_readiness(deployment, *, allow_fact_drift=False):
    """Recheck live access and an exact, previously verified customer snapshot."""
    database = deployment.verify_target_ownership()
    if not deployment.s["reuse_synthetic_target"]:
        verify_producer_access(deployment.s, deployment.client, database)
        raise Blocked(
            "A fresh authenticated binding of Demo 2 artifacts, dimensions, configuration and producer "
            "permissions is unavailable; a monitor heartbeat cannot establish overall readiness."
        )
    deployment.validate(allow_spark=False, allow_fact_drift=allow_fact_drift)
    deployment.monitor_notebook(False)


def verify_synthetic_runtime_access(deployment, monitor_config):
    try:
        module = importlib.import_module("synthetic_admission")
    except ImportError as exc:
        raise Blocked("The synthetic runtime admission verifier is unavailable.") from exc
    verify = getattr(module, "verify_synthetic_runtime_access", None)
    if not callable(verify):
        raise Blocked("The synthetic runtime admission verifier is incompatible.")
    with _spark_client(deployment.client):
        return verify(deployment, monitor_config, deployment.s["synthetic_proof"])


def _require_synthetic_fresh(expires):
    if (not isinstance(expires, datetime) or expires.utcoffset() != timedelta(0)
            or _now() >= expires):
        raise Blocked("Synthetic runtime preflight expired or has no valid expiry.")


class Deployment:
    def __init__(self, settings, client, deadline, ledger, builder=None):
        self.s = settings
        self.client = client
        self.deadline = deadline
        self.ledger = ledger
        self.builder = builder
        self.resources = ledger.data["resources"]
        self.marker = ledger.data["owner_marker"]

    def _get(self, path, missing=False):
        status, _, data = self.client.fabric("GET", path, allow_missing=missing)
        if status == 404 and missing:
            return None
        if status != 200 or not isinstance(data, dict):
            raise Failure("Fabric returned an invalid resource readback")
        return data

    def _poll_operation(self, operation_id, retry_after=None, *, fetch_result=True):
        operation_id = _uuid(operation_id, "operation ID")
        if retry_after is not None:
            self.deadline.wait(_retry_delay(retry_after, self.s["poll"]))
        while True:
            status, headers, data = self.client.fabric("GET", f"/v1/operations/{operation_id}")
            if status != 200 or not isinstance(data, dict):
                raise Failure("Fabric returned an invalid operation status")
            state = data.get("status")
            if state == "Succeeded":
                if not fetch_result:
                    return None
                status, _, result = self.client.fabric("GET", f"/v1/operations/{operation_id}/result")
                if status != 200 or not isinstance(result, dict):
                    raise Blocked("The successful Fabric operation did not return a usable result.")
                return result
            if state == "Failed":
                raise Failure("Fabric long-running operation failed.")
            if state not in ("Running", "NotStarted"):
                raise Failure("Fabric returned an unknown long-running operation state.")
            self.deadline.wait(_retry_delay(headers.get("retry-after"), self.s["poll"]))

    def _complete_create(self, response):
        status, headers, data = response
        if status == 201 and isinstance(data, dict):
            return data
        if status == 202:
            operation_id = _operation_id(headers)
            self.ledger.data["pending"]["operation_id"] = operation_id
            self.ledger.save()
            return self._poll_operation(operation_id, headers.get("retry-after"))
        raise Failure("Fabric did not confirm the requested resource creation.")

    def _listed_notebook(self, name):
        ws = self.resources["workspace"]["id"]
        rows = self.client.pages(f"/v1/workspaces/{ws}/items?type=Notebook")
        matches = [row for row in rows if isinstance(row, dict) and row.get("displayName") == name]
        item = _one(matches, "Created notebook")
        if (item.get("description") != self.marker or item.get("type") != "Notebook"
                or str(item.get("workspaceId", "")).lower() != ws):
            raise Blocked("The created notebook listing does not prove its ownership and workspace.")
        return item

    def _record(self, key, item_id, name, kind, **extra):
        entry = {"id": _uuid(item_id, f"{kind} ID"), "name": name, "type": kind,
                 "confirmed": False, **extra}
        self.resources[key] = entry
        self.ledger.finish()
        return entry

    def _check_config_id(self, key, entry):
        expected = self.s.get(f"{key}_id")
        if expected is not None and entry["id"] != expected:
            raise Blocked(f"Configured {key} ID does not match the recorded Demo 2 resource.")

    def _check_owned(self, value, entry, workspace=False):
        if (not isinstance(value, dict) or value.get("id", "").lower() != entry["id"]
                or value.get("displayName") != entry["name"] or value.get("description") != self.marker):
            raise Blocked("Resource ID, name or ownership marker does not match the ledger.")
        if not workspace and (value.get("type") != entry["type"]
                              or value.get("workspaceId", "").lower() != self.resources["workspace"]["id"]):
            raise Blocked("Resource type or workspace does not match the ownership ledger.")
        if not entry.get("confirmed"):
            entry["confirmed"] = True
            self.ledger.save()
        return value

    def workspace(self, create):
        entry = self.resources.get("workspace")
        if entry:
            self._check_config_id("workspace", entry)
            if entry.get("name") != self.s["workspace_name"]:
                raise Blocked("Recorded workspace name does not match the configured prefix scope.")
            return self._check_owned(self._get(f"/v1/workspaces/{entry['id']}"), entry, True)
        if not create:
            raise Blocked("The workspace has not been recorded as demo-owned in this state-dir.")
        if self.s["workspace_id"] is not None:
            raise Blocked("A configured workspace ID without a matching ownership ledger cannot be adopted.")
        matches = [row for row in self.client.pages("/v1/workspaces")
                   if isinstance(row, dict) and row.get("displayName") == self.s["workspace_name"]]
        if matches:
            raise Blocked("A workspace with that name already exists but is not recorded as demo-owned.")
        capacity = self._capacity()
        self.ledger.intent("create-workspace", name=self.s["workspace_name"])
        status, _, value = self.client.fabric("POST", "/v1/workspaces", {
            "displayName": self.s["workspace_name"], "description": self.marker, "capacityId": capacity,
        })
        if status != 201 or not isinstance(value, dict):
            raise Failure("Fabric did not confirm workspace creation.")
        entry = self._record("workspace", value.get("id"), self.s["workspace_name"], "Workspace")
        return self._check_owned(self._get(f"/v1/workspaces/{entry['id']}"), entry, True)

    def _capacity(self):
        if self.s["capacity_id"] is None and self.s["capacity_name"] is None:
            raise Blocked("A Fabric capacity ID or name is required to create the workspace.")
        capacities = self.client.pages("/v1/capacities")
        matches = [row for row in capacities if isinstance(row, dict)
                   and (self.s["capacity_id"] is None or row.get("id", "").lower() == self.s["capacity_id"])
                   and (self.s["capacity_name"] is None or row.get("displayName") == self.s["capacity_name"])]
        return _uuid(_one(matches, "Fabric capacity").get("id"), "capacity ID")

    def item(self, key, kind, name, create, definition=None):
        ws = self.resources["workspace"]["id"]
        entry = self.resources.get(key)
        if entry:
            self._check_config_id(key, entry)
            if entry.get("name") != name or entry.get("type") != kind:
                raise Blocked(f"Recorded {kind} does not match the requested configuration.")
            return self._check_owned(self._get(f"/v1/workspaces/{ws}/items/{entry['id']}"), entry)
        if not create:
            raise Blocked(f"The {kind} is not recorded as demo-owned.")
        if self.s.get(f"{key}_id") is not None:
            raise Blocked(f"A configured {kind} ID without an ownership record cannot be adopted.")
        items = self.client.pages(f"/v1/workspaces/{ws}/items?type={kind}")
        if any(isinstance(item, dict) and item.get("displayName") == name for item in items):
            raise Blocked(f"An unrecorded {kind} with the requested name already exists.")
        body = {"displayName": name, "type": kind, "description": self.marker}
        if definition is not None:
            body["definition"] = definition
        self.ledger.intent("create-item", key=key, item_type=kind, name=name)
        response = self.client.fabric("POST", f"/v1/workspaces/{ws}/items", body)
        if kind == "Notebook" and response[0] == 202:
            operation_id = _operation_id(response[1])
            self.ledger.data["pending"]["operation_id"] = operation_id
            self.ledger.save()
            self._poll_operation(operation_id, response[1].get("retry-after"), fetch_result=False)
            result = self._listed_notebook(name)
        else:
            result = self._complete_create(response)
        entry = self._record(key, result.get("id"), name, kind)
        return self._check_owned(self._get(f"/v1/workspaces/{ws}/items/{entry['id']}"), entry)

    def database(self, create):
        ws = self.resources["workspace"]["id"]
        parent = self.resources["eventhouse"]["id"]
        entry = self.resources.get("database")
        if not entry:
            if not create or self.s["database_id"] is not None:
                raise Blocked("The KQL database is not recorded as demo-owned.")
            databases = self.client.pages(f"/v1/workspaces/{ws}/kqlDatabases")
            if any(isinstance(item, dict) and item.get("displayName") == self.s["database_name"]
                   for item in databases):
                raise Blocked("A KQL database with that name exists without an ownership record.")
            self.ledger.intent("create-item", key="database", item_type="KQLDatabase",
                               name=self.s["database_name"], parent_eventhouse_id=parent)
            result = self._complete_create(self.client.fabric("POST", f"/v1/workspaces/{ws}/kqlDatabases", {
                "displayName": self.s["database_name"], "description": self.marker,
                "creationPayload": {"databaseType": "ReadWrite", "parentEventhouseItemId": parent},
            }))
            entry = self._record("database", result.get("id"), self.s["database_name"], "KQLDatabase",
                                 parent_eventhouse_id=parent)
        self._check_config_id("database", entry)
        value = self._get(f"/v1/workspaces/{ws}/kqlDatabases/{entry['id']}")
        properties = value.get("properties")
        if (value.get("id", "").lower() != entry["id"] or value.get("workspaceId", "").lower() != ws
                or value.get("type") != "KQLDatabase" or value.get("displayName") != entry["name"]
                or value.get("description") != self.marker or entry.get("name") != self.s["database_name"]
                or entry.get("parent_eventhouse_id") != parent or not isinstance(properties, dict)
                or properties.get("parentEventhouseItemId", "").lower() != parent
                or properties.get("databaseType") != "ReadWrite"):
            raise Blocked("KQL database identity or Eventhouse parent does not match the ledger.")
        query_uri = _service_uri(properties.get("queryServiceUri"), "KQL query endpoint")
        ingestion_uri = _service_uri(properties.get("ingestionServiceUri"), "KQL ingestion endpoint")
        if self.s["query_endpoint"] is not None and self.s["query_endpoint"] != query_uri:
            raise Blocked("The configured query endpoint differs from the KQL database readback.")
        if self.s["ingestion_endpoint"] is not None and self.s["ingestion_endpoint"] != ingestion_uri:
            raise Blocked("The configured ingestion endpoint differs from the KQL database readback.")
        if not entry.get("confirmed"):
            entry["confirmed"] = True
            self.ledger.save()
        return {"id": entry["id"], "name": entry["name"], "query_uri": query_uri,
                "ingestion_uri": ingestion_uri}

    def _kql(self, database, command, query=False):
        return self.client.kusto(database["query_uri"], database["name"], command, query)

    def verify_target_ownership(self, *, expected_pending=None):
        target = _journal_target(self.s, self.ledger, expected_pending=expected_pending)
        bindings = self.s["target_bindings"]
        if bindings is not None:
            try:
                configured = _targets(bindings).get("demo-2")
            except TargetBindingError as exc:
                raise Blocked("Configured selected target bindings are invalid.") from exc
            if configured != target:
                raise Blocked("The selected Demo 2 target does not match the owned deployment.")
        self.workspace(False)
        self.item("eventhouse", "Eventhouse", self.s["eventhouse_name"], False)
        database = self.database(False)
        table = target["fact"]
        verify_schema(self._kql(database, f".show table {table} schema as json"), table, self.marker)
        return database

    def reused_synthetic_target(self):
        try:
            target = _targets(self.s["target_bindings"])["demo-2"]
            expected = {"workspace_id": self.resources["workspace"]["id"],
                        "item_id": self.resources["database"]["id"], "fact": self.s["table"]}
        except (KeyError, TargetBindingError, TypeError) as exc:
            raise Blocked("The measured reuse target is not bound to the owned database.") from exc
        if target != expected or target["fact"] != "SyntheticFactSales":
            raise Blocked("The measured reuse target does not match the owned synthetic table.")
        return target

    def configure_kql(self, database):
        table, mapping = self.s["table"], self.s["mapping"]
        commands = schema_commands(table, mapping)
        tables = self._kql(database, ".show tables")
        matching = [row for row in tables if row.get("TableName") == table]
        recorded_table = self.resources.get("table")
        if not recorded_table:
            if matching:
                raise Blocked("The requested KQL table already exists without an ownership record.")
            self.ledger.intent("create-table", database_id=database["id"], name=table)
            create_table = commands[0].replace(".create-merge table ", ".create table ", 1)
            self._kql(database, f"{create_table} with (docstring = '{self.marker}')")
            verify_schema(self._kql(database, f".show table {table} schema as json"), table, self.marker)
            self.resources["table"] = {"name": table, "database_id": database["id"], "confirmed": True}
            self.ledger.finish()
        elif (recorded_table.get("name") != table or recorded_table.get("database_id") != database["id"]
              or not recorded_table.get("confirmed") or len(matching) != 1):
            raise Blocked("The recorded KQL table is missing or does not match the deployment.")
        verify_schema(self._kql(database, f".show table {table} schema as json"), table, self.marker)

        mappings = self._kql(database, f".show table {table} ingestion csv mappings")
        existing = [row for row in mappings if row.get("Name") == mapping]
        recorded_mapping = self.resources.get("mapping")
        if not recorded_mapping:
            if existing:
                raise Blocked("The requested KQL mapping already exists without an ownership record.")
            self.ledger.intent("create-mapping", database_id=database["id"], table=table, name=mapping)
            self._kql(database, commands[1].replace(".create-or-alter table ", ".create table ", 1))
            self.resources["mapping"] = {"name": mapping, "table": table, "database_id": database["id"],
                                         "confirmed": True}
            self.ledger.finish()
        elif (recorded_mapping.get("name") != mapping or recorded_mapping.get("table") != table
              or recorded_mapping.get("database_id") != database["id"]
              or not recorded_mapping.get("confirmed")):
            raise Blocked("The recorded KQL mapping does not match the deployment.")
        verify_mapping(self._kql(database, f".show table {table} ingestion csv mappings"), mapping)

        recorded_policy = self.resources.get("policy")
        if not recorded_policy:
            self.ledger.intent("configure-mirroring", database_id=database["id"], table=table)
            self._kql(database, commands[2])
            self.resources["policy"] = {"table": table, "database_id": database["id"], "confirmed": True}
            self.ledger.finish()
        elif (recorded_policy.get("table") != table or recorded_policy.get("database_id") != database["id"]
              or not recorded_policy.get("confirmed")):
            raise Blocked("The recorded mirroring policy does not match the deployment.")

    def mirror(self, database):
        row = _one(self._kql(database, f".show table {self.s['table']} policy mirroring"), "Mirroring policy")
        policy = _json_value(row.get("Policy"), "Mirroring policy")
        try:
            location = discover_mirror_location(policy, self.s["table"])
            ws_id = self.resources["workspace"]["id"]
            payload = shortcut_payload(
                location, self.s["shortcut"],
                resolve_workspace=lambda name: self._resolve_workspace(name),
                resolve_item=lambda workspace, name: self._resolve_item(workspace, name),
            )
        except ConfigurationError as exc:
            raise Blocked(f"Actual mirroring configuration or table path is unavailable: {exc}") from exc
        target = payload["target"]["oneLake"]
        if target["workspaceId"] != ws_id or target["itemId"] != database["id"]:
            raise Blocked("The returned OneLake path does not identify this workspace and KQL database.")
        expected = self.s["expected_location"]
        if expected is not None:
            try:
                expected_payload = shortcut_payload(
                    expected, self.s["shortcut"],
                    resolve_workspace=lambda name: self._resolve_workspace(name),
                    resolve_item=lambda workspace, name: self._resolve_item(workspace, name),
                )
            except ConfigurationError as exc:
                raise Blocked("The configured OneLake path cannot be resolved.") from exc
            if expected_payload["target"] != payload["target"]:
                raise Blocked("The configured OneLake path differs from the applied policy readback.")
        return payload

    def _resolve_workspace(self, name):
        matches = [item for item in self.client.pages("/v1/workspaces")
                   if isinstance(item, dict) and item.get("displayName") == name]
        return _one(matches, "Mirrored source workspace").get("id")

    def _resolve_item(self, workspace, name):
        matches = [item for item in self.client.pages(f"/v1/workspaces/{workspace}/items")
                   if isinstance(item, dict) and item.get("displayName") == name]
        return _one(matches, "Mirrored source item").get("id")

    def shortcut(self, payload, create):
        ws = self.resources["workspace"]["id"]
        lakehouse = self.resources["lakehouse"]["id"]
        name = self.s["shortcut"]
        path = f"/v1/workspaces/{ws}/items/{lakehouse}/shortcuts/Tables/{name}"
        value = self._get(path, missing=True)
        record = self.resources.get("shortcut")
        if value is None:
            if not create or record:
                raise Blocked("The recorded Lakehouse shortcut is missing.")
            self.ledger.intent("create-shortcut", lakehouse_id=lakehouse, name=name)
            status, _, _ = self.client.fabric("POST", f"/v1/workspaces/{ws}/items/{lakehouse}/shortcuts", payload)
            if status != 201:
                raise Failure("Fabric did not confirm creation of a new shortcut.")
            record = {"lakehouse_id": lakehouse, "name": name, "path": "Tables",
                      "target": payload["target"]["oneLake"], "confirmed": False}
            self.resources["shortcut"] = record
            self.ledger.finish()
            value = self._get(path)
        elif not record and create:
            raise Blocked("An unrecorded Lakehouse shortcut already exists; it will not be overwritten.")
        if record and (record.get("lakehouse_id") != lakehouse or record.get("name") != name
                       or record.get("path") != "Tables" or record.get("target") != payload["target"]["oneLake"]):
            raise Blocked("The recorded shortcut differs from the requested target.")
        if (value.get("path") != "Tables" or value.get("name") != name
                or not isinstance(value.get("target"), dict)
                or value["target"].get("oneLake") != payload["target"]["oneLake"]):
            raise Blocked("Lakehouse shortcut readback does not match the actual KQL-table path.")
        if record and not record.get("confirmed"):
            record["confirmed"] = True
            self.ledger.save()

    @_spark_operation
    def notebook(self, create):
        if self.builder is None:
            self.builder = _load_builder()
        module, generator = self.builder
        try:
            # Dimensions are independent of run ID; keep the verifier reusable across runs.
            dimension_data = {**self.s["data"], "run_id": None}
            maintenance = {"dimension_maintenance": True} if self.s.get("reuse_synthetic_target") else {}
            notebook = module.build_notebook(
                generator, workspace_id=self.resources["workspace"]["id"],
                lakehouse_id=self.resources["lakehouse"]["id"], lakehouse_name=self.s["lakehouse_name"],
                data=dimension_data, shortcut_name=self.s["shortcut"], schema=self.s["schema"],
                verification_timeout_seconds=self.s["timeout"], poll_interval_seconds=self.s["poll"],
                synthetic_run_id=self.s["data"]["run_id"] if _synthetic_run(self.s) else None,
                **maintenance,
            )
            definition = module.fabric_definition(notebook)
        except Exception as exc:
            raise Blocked(f"Cannot build the Demo 2 notebook ({type(exc).__name__}).") from exc
        expected = _notebook_projection(notebook)
        _read_notebook_definition(definition)
        key = _notebook_key(self.s)
        self.item(key, "Notebook", self.s[f"{key}_name"], create, definition)
        ws = self.resources["workspace"]["id"]
        notebook_id = self.resources[key]["id"]
        status, headers, actual = self.client.fabric(
            "POST", f"/v1/workspaces/{ws}/items/{notebook_id}/getDefinition?format=ipynb", {}
        )
        if status == 202:
            actual = self._poll_operation(_operation_id(headers), headers.get("retry-after"))
        elif status != 200:
            raise Failure("Notebook definition readback did not complete.")
        if _notebook_projection(_read_notebook_definition(actual)) != expected:
            raise Blocked("The live notebook definition or Lakehouse binding differs from the trusted builder.")

    @_spark_operation
    def monitor_notebook(self, create):
        if self.builder is None:
            self.builder = _load_builder()
        module, _ = self.builder
        monitor_builder = _load_monitor_builder()
        try:
            reuse = ({"reused_synthetic_target": self.reused_synthetic_target()}
                     if self.s.get("reuse_synthetic_target") else {})
            notebook = monitor_builder.build_monitor_notebook(
                workspace_id=self.resources["workspace"]["id"],
                lakehouse_id=self.resources["lakehouse"]["id"],
                lakehouse_name=self.s["lakehouse_name"],
                **reuse,
            )
            definition = module.fabric_definition(notebook)
        except Exception as exc:
            raise Blocked(f"Cannot build the Demo 2 monitor notebook ({type(exc).__name__}).") from exc
        expected = _notebook_projection(notebook)
        _read_notebook_definition(definition)
        key = _notebook_key(self.s, monitor=True)
        self.item(key, "Notebook", self.s[f"{key}_name"], create, definition)
        ws = self.resources["workspace"]["id"]
        notebook_id = self.resources[key]["id"]
        status, headers, actual = self.client.fabric(
            "POST", f"/v1/workspaces/{ws}/items/{notebook_id}/getDefinition?format=ipynb", {}
        )
        if status == 202:
            actual = self._poll_operation(_operation_id(headers), headers.get("retry-after"))
        elif status != 200:
            raise Failure("Monitor notebook definition readback did not complete.")
        if _notebook_projection(_read_notebook_definition(actual)) != expected:
            raise Blocked("The live monitor notebook definition or Lakehouse binding differs from the trusted builder.")

    @_spark_operation
    def ensure_dimension_quiescent(self):
        ws = self.resources["workspace"]["id"]
        sessions = self.client.pages(f"/v1/workspaces/{ws}/spark/livySessions")
        if any(not isinstance(session, dict) or session.get("state") not in
               ("Succeeded", "Failed", "Cancelled") for session in sessions):
            raise Blocked("A workspace Spark session is active or has an unknown state; dimension maintenance is blocked.")
        for key in ("notebook", "monitor_notebook", "measured_notebook", "measured_monitor_notebook"):
            entry = self.resources.get(key)
            if entry is None:
                continue
            jobs = self.client.pages(f"/v1/workspaces/{ws}/items/{entry['id']}/jobs/instances")
            if any(not isinstance(job, dict) or job.get("status") not in
                   ("Completed", "Failed", "Cancelled", "Deduped") for job in jobs):
                raise Blocked("A recorded notebook job is active or has an unknown state; dimension maintenance is blocked.")

    @_spark_operation
    def run_notebook(self, mode, batch=None, *, synthetic_preparation=None, customer_verification=None):
        ws = self.resources["workspace"]["id"]
        notebook_id = self.resources[_notebook_key(self.s)]["id"]
        jobs_path = f"/v1/workspaces/{ws}/items/{notebook_id}/jobs/instances"
        jobs = self.client.pages(jobs_path)
        if any(isinstance(job, dict) and job.get("status") in ("NotStarted", "InProgress") for job in jobs):
            raise Blocked("A notebook job is already active; wait or reconcile before submitting another.")
        parameters = [{"name": "MODE", "type": "Text", "value": mode}]
        if batch is not None:
            parameters.append({"name": "BATCH_JSON", "type": "Text",
                               "value": json.dumps(batch, separators=(",", ":"))})
        expected = ({key: batch[key] for key in ("batch_id", "row_count", "sales_amount_minor_units")}
                    if batch is not None else None)
        fields = {"mode": mode, "notebook_id": notebook_id, "batch": expected}
        if synthetic_preparation is not None:
            fields["synthetic_preparation"] = synthetic_preparation
        if customer_verification is not None:
            fields["customer_verification"] = customer_verification
        self.ledger.intent("notebook-job", **fields)
        status, headers, _ = self.client.fabric(
            "POST", f"/v1/workspaces/{ws}/notebooks/{notebook_id}/jobs/execute/instances?beta=false",
            {"parameters": parameters, "executionData": {"compute": "Spark"}},
        )
        if status != 202:
            raise Failure("Fabric did not confirm the notebook job submission.")
        job_id = _job_id(headers.get("location"), ws, notebook_id)
        self.ledger.data["pending"]["job_id"] = job_id
        self.ledger.save()
        result = self._poll_notebook_job(job_id, mode, expected, headers.get("retry-after"),
                                         notebook_id=notebook_id)
        return job_id, result

    @_spark_operation
    def _poll_notebook_job(self, job_id, mode, batch, retry_after=None, *, notebook_id=None):
        ws = self.resources["workspace"]["id"]
        notebook_id = notebook_id or self.resources[_notebook_key(self.s)]["id"]
        job_id = _uuid(job_id, "job ID")
        jobs_path = f"/v1/workspaces/{ws}/notebooks/{notebook_id}/jobs/execute/instances"
        if retry_after is not None:
            self.deadline.wait(_retry_delay(retry_after, self.s["poll"]))
        while True:
            status, headers, job = self.client.fabric("GET", f"{jobs_path}/{job_id}?beta=true")
            if (status != 200 or not isinstance(job, dict) or job.get("id", "").lower() != job_id
                    or job.get("itemId", "").lower() != notebook_id):
                raise Failure("Notebook job status readback is invalid.")
            state = job.get("status")
            if state == "Completed":
                pending = self.ledger.data["pending"]
                preparation = pending.get("synthetic_preparation")
                customer_verification = pending.get("customer_verification")
                try:
                    result = _verify_notebook_exit(job, mode, batch)
                    if mode in ("dimension_gaps", "expand_dimensions"):
                        _verify_dimension_exit(result, mode, self.s)
                except Blocked:
                    if preparation is None and mode != "expand_dimensions":
                        self.ledger.finish()
                    raise
                if preparation is not None:
                    config = _monitor_config(self.s, {"demo-2"})
                    expected = {
                        "run_id": config["run_id"], "generator_sha256": config["generator_sha256"],
                        "workspace_id": ws, "lakehouse_id": self.resources["lakehouse"]["id"],
                        "notebook_id": notebook_id,
                    }
                    if (not re.fullmatch(r"demo2synthetic-[0-9a-f]{12}", self.s["prefix"])
                            or mode != "preload" or batch is not None
                            or preparation != expected or pending.get("job_id") != job_id
                            or self.resources.get("synthetic_preparation") is not None):
                        raise Blocked("The synthetic preload job does not match its preparation intent.")
                    self.resources["synthetic_preparation"] = {**expected, "job_id": job_id}
                if customer_verification is not None:
                    binding = _customer_verification_binding(self.s, self.resources)
                    if (mode != "verify_batch" or not self.s["reuse_synthetic_target"]
                            or pending.get("job_id") != job_id
                            or not isinstance(customer_verification, dict)
                            or any(customer_verification.get(key) != value for key, value in binding.items())
                            or set(customer_verification) != set(binding) | {"delta_snapshots"}):
                        raise Blocked("The customer verification job does not match its recorded intent.")
                    snapshot = _customer_delta_snapshot(self.s, self.client, self.resources)
                    if snapshot != customer_verification["delta_snapshots"]:
                        self.ledger.finish()
                        raise Blocked("A Delta snapshot changed during the customer verification job.")
                    self.resources["measured_verification"] = {
                        **customer_verification, "job_id": job_id, "verified_at_utc": _stamp(_now()),
                    }
                self.ledger.finish()
                return result
            if state in ("Failed", "Cancelled", "Deduped"):
                if mode == "expand_dimensions":
                    raise Blocked("Dimension maintenance did not complete; inspect the exact job and dimensions "
                                  "before reconciling its retained intent.")
                self.ledger.finish()
                raise Failure(f"The notebook job ended in state {state}.")
            if state not in ("NotStarted", "InProgress"):
                raise Failure("The notebook job returned an unknown state.")
            self.deadline.wait(_retry_delay(headers.get("retry-after"), self.s["poll"]))

    def _customer_verification_is_current(self, snapshot, *, allow_fact_drift=False):
        proof = self.resources.get("measured_verification")
        binding = _customer_verification_binding(self.s, self.resources)
        if not isinstance(proof, dict) or any(proof.get(key) != value for key, value in binding.items()):
            return False
        recorded = proof.get("delta_snapshots")
        if not isinstance(recorded, dict) or set(recorded) != set(snapshot):
            return False
        for name, value in snapshot.items():
            saved = recorded[name]
            if (not isinstance(saved, dict) or set(saved) != {"version", "etag", "history_sha256"}
                    or not isinstance(saved["version"], str) or re.fullmatch(r"[0-9]{20}", saved["version"]) is None
                    or not isinstance(saved["etag"], str) or not saved["etag"]
                    or not isinstance(saved["history_sha256"], str)
                    or re.fullmatch(r"[0-9a-f]{64}", saved["history_sha256"]) is None):
                return False
            if name == "shortcut" and allow_fact_drift:
                continue
            if saved != value:
                return False
        try:
            job_id = _uuid(proof["job_id"], "verification job ID")
            if job_id != proof["job_id"]:
                raise ValueError
        except (Failure, KeyError, TypeError, ValueError):
            return False
        ws = binding["workspace_id"]
        notebook_id = binding["notebook_id"]
        with _spark_client(self.client):
            status, _, job = self.client.fabric(
                "GET", f"/v1/workspaces/{ws}/notebooks/{notebook_id}/jobs/execute/instances/{job_id}?beta=true"
            )
        if (status != 200 or not isinstance(job, dict) or job.get("id") != job_id
                or job.get("itemId") != notebook_id or job.get("status") != "Completed"):
            raise Blocked("The recorded customer verification job is not confirmed completed.")
        _verify_notebook_exit(job, "verify_batch", self.s["batch"])
        return True

    def validate(self, *, allow_spark=True, allow_fact_drift=False):
        if self.s["reuse_synthetic_target"]:
            self.verify_target_ownership()
            self.reused_synthetic_target()
        self.workspace(False)
        self.item("eventhouse", "Eventhouse", self.s["eventhouse_name"], False)
        self.item("lakehouse", "Lakehouse", self.s["lakehouse_name"], False)
        database = self.database(False)
        table, mapping = self.s["table"], self.s["mapping"]
        verify_schema(self._kql(database, f".show table {table} schema as json"), table,
                      self.marker)
        verify_mapping(self._kql(database, f".show table {table} ingestion csv mappings"), mapping)
        self.shortcut(self.mirror(database), False)
        batch = self.s["batch"]
        if batch is None:
            raise Blocked("A complete existing bootstrap or measured demo-2.verification_batch is required "
                          "to verify KQL and shortcut contents; none was supplied.")
        with self.verification_deadline():
            reuse = self.s["reuse_synthetic_target"]
            if reuse:
                verify_producer_access(self.s, self.client, database)
                before = _customer_delta_snapshot(self.s, self.client, self.resources)
            self.verify_batch(database, batch)
            self.notebook(False)
            if reuse:
                current = self._customer_verification_is_current(before, allow_fact_drift=allow_fact_drift)
                if not current:
                    if not allow_spark:
                        raise Blocked("An exact, completed customer verification is required before monitor readiness.")
                    self.ensure_dimension_quiescent()
                    intent = {**_customer_verification_binding(self.s, self.resources),
                              "delta_snapshots": before}
                    self.run_notebook("verify_batch", batch, customer_verification=intent)
                after = _customer_delta_snapshot(self.s, self.client, self.resources)
                if before != after:
                    raise Blocked("A verified Delta snapshot changed during readiness validation.")
            else:
                self.run_notebook("verify_batch", batch)
            verify_producer_access(self.s, self.client, database)
            self.deadline.remaining()

    def verify_batch(self, database, batch):
        query = batch_verification_query(self.s["table"], batch["batch_id"])
        while True:
            try:
                verify_kql_batch(self._kql(database, query, True), batch)
                return
            except BatchPending:
                self.deadline.wait(self.s["poll"])

    @contextlib.contextmanager
    def verification_deadline(self):
        parent = self.deadline
        self.deadline = parent.child(self.s["timeout"])
        client_deadline = getattr(self.client, "deadline", None)
        if client_deadline is not None:
            self.client.deadline = self.deadline
        try:
            yield
        finally:
            self.deadline = parent
            if client_deadline is not None:
                self.client.deadline = client_deadline

    def deploy(self):
        self.workspace(True)
        self.item("eventhouse", "Eventhouse", self.s["eventhouse_name"], True)
        self.item("lakehouse", "Lakehouse", self.s["lakehouse_name"], True)
        database = self.database(True)
        self.configure_kql(database)
        self.notebook(True)
        self.run_notebook("preload")
        self.shortcut(self.mirror(database), True)
        self.validate()

    def cleanup(self):
        workspace = self.workspace(False)
        ws = self.resources["workspace"]["id"]
        items = self.client.pages(f"/v1/workspaces/{ws}/items")
        if items:
            raise Blocked(
                "Cleanup is blocked: the workspace is not empty. Automatic deletion of Eventhouse, "
                "Lakehouse, notebook and their nested data cannot yet prove every child is demo-owned; "
                "no resources were deleted."
            )
        if any(key in self.resources for key in
               ("eventhouse", "lakehouse", "database", "notebook", "monitor_notebook", "shortcut")):
            raise Blocked("Recorded items are absent from an otherwise empty workspace; reconcile ownership before cleanup.")
        if self.ledger.data.get("monitor_job") is not None:
            raise Blocked("A monitor job is recorded; reconcile its ownership before cleanup.")
        self.ledger.intent("delete-workspace", id=ws)
        status, _, _ = self.client.fabric("DELETE", f"/v1/workspaces/{ws}")
        if status not in (200, 202, 204):
            raise Failure("Fabric did not confirm workspace deletion.")
        if status == 202:
            raise Blocked("Workspace deletion is asynchronous and requires manual reconciliation.")
        if self._get(f"/v1/workspaces/{ws}", missing=True) is not None:
            raise Blocked("Workspace deletion was not confirmed by readback.")
        self.resources["workspace"]["deleted"] = True
        self.ledger.finish()


class ReadOnlyDeployment(Deployment):
    """Inspect explicit IDs without adopting or executing an unowned notebook."""

    def __init__(self, settings, client, deadline):
        required = ("workspace_id", "eventhouse_id", "database_id", "lakehouse_id")
        if any(settings[key] is None for key in required):
            raise Blocked("Fresh read-only validation without an ownership ledger requires explicit "
                          "workspace, Eventhouse, KQL database and Lakehouse IDs.")
        resources = {
            key: {"id": settings[f"{key}_id"]}
            for key in ("workspace", "eventhouse", "database", "lakehouse")
        }
        ledger = type("ReadOnlyLedger", (), {"data": {"resources": resources, "owner_marker": None}})()
        super().__init__(settings, client, deadline, ledger)

    def workspace(self, create):
        value = self._get(f"/v1/workspaces/{self.s['workspace_id']}")
        if (value.get("id", "").lower() != self.s["workspace_id"]
                or value.get("displayName") != self.s["workspace_name"]):
            raise Blocked("Workspace readback does not match the configured ID and prefix-scoped name.")
        return value

    def item(self, key, kind, name, create, definition=None):
        ws = self.s["workspace_id"]
        item_id = self.s[f"{key}_id"]
        value = self._get(f"/v1/workspaces/{ws}/items/{item_id}")
        if (value.get("id", "").lower() != item_id or value.get("workspaceId", "").lower() != ws
                or value.get("type") != kind or value.get("displayName") != name):
            raise Blocked(f"The configured {kind} ID, name or workspace does not match readback.")
        return value

    def database(self, create):
        ws = self.s["workspace_id"]
        value = self._get(f"/v1/workspaces/{ws}/kqlDatabases/{self.s['database_id']}")
        properties = value.get("properties")
        if (value.get("id", "").lower() != self.s["database_id"]
                or value.get("workspaceId", "").lower() != ws or value.get("type") != "KQLDatabase"
                or not isinstance(properties, dict)
                or properties.get("parentEventhouseItemId", "").lower() != self.s["eventhouse_id"]
                or properties.get("databaseType") != "ReadWrite"):
            raise Blocked("KQL database readback does not match the configured workspace and Eventhouse.")
        name = _optional_name(value.get("displayName"), None, "KQL database name")
        if self.s["database_name_supplied"] and name != self.s["database_name"]:
            raise Blocked("KQL database readback differs from the configured name.")
        query_uri = _service_uri(properties.get("queryServiceUri"), "KQL query endpoint")
        ingestion_uri = _service_uri(properties.get("ingestionServiceUri"), "KQL ingestion endpoint")
        if self.s["query_endpoint"] is not None and self.s["query_endpoint"] != query_uri:
            raise Blocked("The configured query endpoint differs from the KQL database readback.")
        if self.s["ingestion_endpoint"] is not None and self.s["ingestion_endpoint"] != ingestion_uri:
            raise Blocked("The configured ingestion endpoint differs from the KQL database readback.")
        return {"id": self.s["database_id"], "name": name, "query_uri": query_uri,
                "ingestion_uri": ingestion_uri}

    def notebook(self, create):
        raise Blocked("Existing resources were inspected read-only, but an owned, definition-verified notebook "
                      "and fresh dimension/shortcut-content and producer-permission probes are required for readiness.")


def _retry_delay(value, default):
    if value is None:
        return default
    try:
        delay = int(value)
    except (TypeError, ValueError) as exc:
        raise Failure("Fabric returned an invalid Retry-After value") from exc
    if delay < 0:
        raise Failure("Fabric returned an invalid Retry-After value")
    return max(default, delay)


def _operation_id(headers):
    value = headers.get("x-ms-operation-id")
    location = headers.get("location")
    if location:
        try:
            parsed = urlsplit(location)
            regional = (
                parsed.scheme == "https"
                and re.fullmatch(r"wabi-[a-z0-9-]+-primary-redirect\.analysis\.windows\.net",
                                 parsed.hostname or "") is not None
                and parsed.username is None and parsed.password is None
                and parsed.port in (None, 443) and not parsed.fragment
            )
        except (TypeError, ValueError) as exc:
            raise Failure("Fabric returned an unsafe operation location") from exc
        if regional:
            if value is None:
                raise Failure("Fabric omitted the operation ID for a regional operation location")
        else:
            parsed = urlsplit(_fabric_url(location))
        match = re.fullmatch(r"/v1/operations/([0-9a-fA-F-]+)", parsed.path)
        if not match or parsed.query:
            raise Failure("Fabric returned an unsafe operation location")
        from_location = _uuid(match.group(1), "operation ID")
        if value is not None and _uuid(value, "operation ID") != from_location:
            raise Failure("Fabric returned conflicting operation IDs")
        value = from_location
    if value is None:
        raise Failure("Fabric omitted the long-running operation ID")
    return _uuid(value, "operation ID")


def _job_id(location, workspace, notebook):
    if not isinstance(location, str):
        raise Failure("Fabric omitted the notebook job location")
    parsed = urlsplit(_fabric_url(location))
    pattern = rf"/v1/workspaces/{re.escape(workspace)}/items/{re.escape(notebook)}/jobs/instances/([0-9a-fA-F-]+)"
    match = re.fullmatch(pattern, parsed.path, re.IGNORECASE)
    if not match or parsed.query:
        raise Failure("Fabric returned an unsafe notebook job location")
    return _uuid(match.group(1), "notebook job ID")


def _recover_pending(deployment):
    pending = deployment.ledger.data.get("pending")
    if pending is None:
        return
    if not isinstance(pending, dict):
        raise Blocked("The ownership ledger contains invalid pending operation state.")
    if (re.fullmatch(r"demo2synthetic-[0-9a-f]{12}", deployment.s["prefix"])
            and pending.get("kind") == "create-item" and not pending.get("operation_id")
            and pending.get("item_type") == "Notebook"
            and pending.get("key") in ("notebook", "monitor_notebook", "measured_notebook",
                                       "measured_monitor_notebook")):
        key = pending["key"]
        monitor = key.endswith("monitor_notebook")
        expected = deployment.s[f"{key}_name"]
        if (key != _notebook_key(deployment.s, monitor=monitor)
                or pending.get("name") != expected or key in deployment.resources):
            raise Blocked("The interrupted synthetic notebook creation has an ambiguous identity.")
        with _spark_client(deployment.client):
            deployment.verify_target_ownership(expected_pending=pending)
            value = deployment._listed_notebook(expected)
            deployment.resources[key] = {"id": _uuid(value.get("id"), "Notebook ID"),
                                         "name": expected, "type": "Notebook", "confirmed": True}
            try:
                if not monitor:
                    deployment.notebook(False)
                else:
                    deployment.monitor_notebook(False)
            except Exception:
                del deployment.resources[key]
                raise
        deployment.ledger.finish()
        return
    if (re.fullmatch(r"demo2synthetic-[0-9a-f]{12}", deployment.s["prefix"])
            and pending.get("kind") == "create-item" and pending.get("key") == "database"
            and not pending.get("operation_id") and "database" not in deployment.resources
            and pending.get("item_type") == "KQLDatabase"
            and pending.get("name") == deployment.s["database_name"]):
        deployment.workspace(False)
        parent = deployment.item("eventhouse", "Eventhouse", deployment.s["eventhouse_name"], False)
        if pending.get("parent_eventhouse_id") != parent["id"]:
            raise Blocked("The interrupted synthetic database creation has an ambiguous parent.")
        ws = deployment.resources["workspace"]["id"]
        rows = deployment.client.pages(f"/v1/workspaces/{ws}/kqlDatabases")
        matching = [row for row in rows if isinstance(row, dict)
                    and row.get("displayName") == pending["name"]]
        value = _one(matching, "Interrupted synthetic KQL database")
        if value.get("description") != deployment.marker:
            raise Blocked("The interrupted synthetic database lacks the exact ownership marker.")
        deployment.resources["database"] = {
            "id": _uuid(value.get("id"), "KQLDatabase ID"), "name": pending["name"],
            "type": "KQLDatabase", "confirmed": True, "parent_eventhouse_id": parent["id"],
        }
        try:
            deployment.database(False)
        except (Blocked, Failure):
            del deployment.resources["database"]
            raise
        deployment.ledger.finish()
        return
    if pending.get("kind") == "create-item" and pending.get("operation_id"):
        key, kind, name = pending.get("key"), pending.get("item_type"), pending.get("name")
        expected = {
            "eventhouse": ("Eventhouse", deployment.s["eventhouse_name"]),
            "lakehouse": ("Lakehouse", deployment.s["lakehouse_name"]),
            "notebook": ("Notebook", deployment.s["notebook_name"]),
            "monitor_notebook": ("Notebook", deployment.s["monitor_notebook_name"]),
            "measured_notebook": ("Notebook", deployment.s["measured_notebook_name"]),
            "measured_monitor_notebook": ("Notebook", deployment.s["measured_monitor_notebook_name"]),
            "database": ("KQLDatabase", deployment.s["database_name"]),
        }
        if (key not in expected or expected[key] != (kind, name) or key in deployment.resources
                or (kind == "Notebook" and key != _notebook_key(
                    deployment.s, monitor=key.endswith("monitor_notebook")
                ))):
            raise Blocked("The interrupted item creation does not match the requested deployment.")
        scope = _spark_client(deployment.client) if kind == "Notebook" else contextlib.nullcontext()
        with scope:
            deployment.workspace(False)
            extra = {}
            if key == "database":
                parent = deployment.resources.get("eventhouse", {}).get("id")
                if not parent or pending.get("parent_eventhouse_id") != parent:
                    raise Blocked("The interrupted database creation has an ambiguous parent.")
                deployment.item("eventhouse", "Eventhouse", deployment.s["eventhouse_name"], False)
                extra["parent_eventhouse_id"] = parent
            value = deployment._poll_operation(pending["operation_id"], fetch_result=kind != "Notebook")
            if kind == "Notebook":
                value = deployment._listed_notebook(name)
            entry = deployment._record(key, value.get("id"), name, kind, **extra)
            ws = deployment.resources["workspace"]["id"]
            deployment._check_owned(deployment._get(f"/v1/workspaces/{ws}/items/{entry['id']}"), entry)
        return
    if pending.get("kind") == "notebook-job" and pending.get("job_id"):
        ws = deployment.resources.get("workspace", {}).get("id")
        notebook = pending.get("notebook_id")
        key = _notebook_key(deployment.s)
        if not ws or deployment.resources.get(key, {}).get("id") != notebook:
            raise Blocked("The interrupted notebook job does not match the ownership ledger.")
        mode, batch = pending.get("mode"), pending.get("batch")
        if (mode not in ("preload", "verify_batch", "dimension_gaps", "expand_dimensions")
                or (mode != "verify_batch" and batch is not None)
                or (mode in ("dimension_gaps", "expand_dimensions")
                    and not deployment.s.get("reuse_synthetic_target"))
                or (mode == "verify_batch" and (not isinstance(batch, dict)
                    or not isinstance(batch.get("batch_id"), str)
                    or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}", batch["batch_id"])
                    or type(batch.get("row_count")) is not int or batch["row_count"] < 1
                    or type(batch.get("sales_amount_minor_units")) is not int
                    or batch["sales_amount_minor_units"] < 0))):
            raise Blocked("The interrupted notebook job has invalid mode or batch evidence.")
        with _spark_client(deployment.client):
            deployment.workspace(False)
            deployment.item(key, "Notebook", deployment.s[f"{key}_name"], False)
            job_id = pending["job_id"]
            result = deployment._poll_notebook_job(job_id, mode, batch, notebook_id=notebook)
        if mode in ("dimension_gaps", "expand_dimensions"):
            return {"mode": mode, "job_id": job_id, "result": result}
        return
    raise Blocked("An earlier resource operation has an uncertain outcome; reconcile its ledger before retrying.")


def _selected_demos(value):
    if value is None:
        return None
    if value == "all":
        return {"demo-1", "demo-2", "demo-3"}
    parts = value.split(",")
    if (not parts or any(part not in ("demo-1", "demo-2", "demo-3") for part in parts)
            or len(parts) != len(set(parts))):
        raise Failure("--selected-demos must be all or unique comma-separated demo-1,demo-2,demo-3")
    return set(parts)


def _generator_digest():
    try:
        source = (ROOT / "demo-data-generator" / "generator.py").read_bytes().decode("utf-8")
    except (OSError, UnicodeError) as exc:
        raise Blocked("The shared generator source cannot be bound to the monitor.") from exc
    return hashlib.sha256(source.replace("\r\n", "\n").replace("\r", "\n").encode("utf-8")).hexdigest()


def _monitor_binding(targets, generator_digest, producer_config):
    value = {"targets": targets, "generator_sha256": generator_digest, "config": producer_config}
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                         allow_nan=False).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _monitor_duration(track_timeout, max_duration, verification_timeout, *, legacy=False):
    headroom = 2 * track_timeout if legacy else 4 * track_timeout + MONITOR_STARTUP_HEADROOM_SECONDS
    return headroom + max_duration + verification_timeout


def _monitor_config(settings, selected, *, bind=True, require_run=True):
    if selected is not None and "demo-2" not in selected:
        raise Failure("Demo 2 must be included in --selected-demos for monitor operations")
    required = {
        "tenant_id": settings["tenant_id"],
        "shared.workspace_id": settings["shared_workspace_id"],
        "shared.staging_lakehouse_id": settings["shared_lakehouse_id"],
        "shared.identity_id": settings["producer_identity"],
        "data.run_id": settings["data"]["run_id"],
        "data.file_count": settings["expected_batch_count"],
        "data.max_duration_seconds": settings["max_duration"],
        "data.rows_per_file": settings["rows_per_file"],
        "data.interval_seconds": settings["interval_seconds"],
    }
    if not require_run:
        required.pop("data.run_id")
    missing = [key for key, value in required.items() if value is None]
    if missing:
        raise Blocked("Monitor configuration requires " + ", ".join(missing) + ".")
    producer_root = (f"{ONELAKE}/{settings['shared_workspace_id']}/"
                     f"{settings['shared_lakehouse_id']}/Files/producer/runs")
    location = settings["run_state_location"]
    if location is not None and (not isinstance(location, str) or location.rstrip("/") != producer_root):
        raise Failure("shared.run_state_location must match the derived producer runs root")
    if settings["max_duration"] <= (settings["expected_batch_count"] - 1) * settings["interval_seconds"]:
        raise Failure("data.max_duration_seconds must exceed the final scheduled batch offset")
    data = settings["data"]
    synthetic = _synthetic_run(settings)
    if synthetic and bind and selected != {"demo-2"}:
        raise Blocked("Synthetic monitor requires --selected-demos demo-2.")
    producer_config = {
        "run_id": data["run_id"], "batch_class": "bootstrap" if synthetic else "measured",
        "start_date": data["start_date"],
        "seed": data["seed"], "date_count": data["date_count"], "products": data["product_count"],
        "customers": data["customer_count"], "file_count": settings["expected_batch_count"],
        "rows_per_file": settings["rows_per_file"], "interval_seconds": settings["interval_seconds"],
    }
    duration = _monitor_duration(settings["track_timeout"], settings["max_duration"], settings["timeout"])
    try:
        _now() + timedelta(seconds=duration + settings["track_timeout"])
    except (OverflowError, ValueError) as exc:
        raise Failure("The monitor deadline bound is too large") from exc
    targets = None
    generator_digest = None
    binding_digest = None
    if bind:
        try:
            targets = _targets(settings["target_bindings"])
        except TargetBindingError as exc:
            raise Blocked("Complete independently supplied selected target bindings are required.") from exc
        if "demo-2" not in targets or (selected is not None and set(targets) != selected):
            raise Blocked("The target binding map must match every selected demo, including Demo 2.")
        expected = targets["demo-2"]
        if (settings["workspace_id"] is not None and expected["workspace_id"] != settings["workspace_id"]
                or settings["database_id"] is not None and expected["item_id"] != settings["database_id"]
                or expected["fact"] != settings["table"]):
            raise Blocked("The Demo 2 target binding conflicts with its configured workspace, database or table.")
        generator_digest = _generator_digest()
        binding_digest = _monitor_binding(targets, generator_digest, producer_config)
    return {
        "run_id": settings["data"]["run_id"],
        "producer_root": producer_root,
        "manifest_uri": (f"{producer_root}/{data['run_id']}/manifest.json" if data["run_id"] else None),
        "sentinel_uri": (f"{ONELAKE}/{settings['shared_workspace_id']}/{settings['shared_lakehouse_id']}"
                         "/Files/preflight/producer-monitor-heartbeat-v1.txt"),
        "duration": duration,
        "producer_config": producer_config,
        "synthetic_run_id": data["run_id"] if synthetic else None,
        "target_bindings": targets,
        "generator_sha256": generator_digest,
        "monitor_binding_sha256": binding_digest,
    }


def _now():
    return datetime.now(timezone.utc)


def _stamp(value):
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _utc(value, label):
    if not isinstance(value, str) or "T" not in value:
        raise Blocked(f"The monitor {label} is not a UTC timestamp.")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0):
            raise ValueError
        return parsed.astimezone(timezone.utc)
    except (ValueError, OverflowError) as exc:
        raise Blocked(f"The monitor {label} is not a UTC timestamp.") from exc


def _state_uri(workspace, lakehouse, run_id):
    return f"{ONELAKE}/{workspace}/{lakehouse}/Files/demo2-verification/runs/{run_id}/state.json"


def _lock_uri(state_uri):
    return state_uri.rsplit("/", 1)[0] + "/writer.lock"


def _same_json(left, right):
    try:
        options = {"sort_keys": True, "separators": (",", ":"), "allow_nan": False}
        return json.dumps(left, **options) == json.dumps(right, **options)
    except (TypeError, ValueError, RecursionError):
        return False


class Monitor:
    """Manage one recorded controller job; never adopt an unknown submission."""

    TERMINAL = {"Completed", "Failed", "Cancelled", "Deduped"}
    ACTIVE = {"NotStarted", "InProgress"}
    IMMUTABLE = ("run_id", "controller_id", "workspace_id", "lakehouse_id",
                 "monitor_notebook_id", "verifier_notebook_id", "manifest_uri",
                 "state_uri", "deadline_utc", "submitted_at_utc")
    START_FIXED = IMMUTABLE + ("prefix", "tenant_id", "identity_id", "owner_marker", "sentinel_uri",
                               "duration_seconds", "expected_batch_count", "verification_timeout_seconds",
                               "poll_interval_seconds", "max_concurrent_batches", "track_timeout_seconds",
                               "max_duration_seconds", "producer_config", "target_bindings",
                               "generator_sha256", "monitor_binding_sha256")
    BINDING_FIELDS = ("target_bindings", "generator_sha256", "monitor_binding_sha256")

    def __init__(self, deployment, config):
        self.d = deployment
        self.s = deployment.s
        self.client = deployment.client
        self.ledger = deployment.ledger
        self.path = self.ledger.directory / MONITOR_FILE
        self.start_path = self.ledger.directory / MONITOR_START_FILE
        self.config = config
        self.evidence = None
        self.stopping = False

    def _save(self):
        if self.stopping:
            self.ledger.data["monitor_stop"] = {
                "run_id": self.evidence["run_id"], "controller_id": self.evidence["controller_id"],
                "job_id": self.evidence["job_id"], "state": self.evidence["state"],
                "cancel_requests": dict(self.evidence["cancel_requests"]),
            }
            self.ledger.save()
        else:
            _atomic_json(self.path, self.evidence)

    def _recorded_notebook_key(self, notebook_id, *, monitor=False):
        role = "monitor_notebook" if monitor else "notebook"
        keys = (role, f"measured_{role}")
        matches = [key for key in keys
                   if isinstance(self.d.resources.get(key), dict)
                   and self.d.resources[key].get("id") == notebook_id]
        if len(matches) != 1:
            raise Blocked("The monitor notebook identity is not uniquely recorded in the ownership ledger.")
        return matches[0]

    def _load_start(self, *, current_run=True, allow_legacy=False, check_binding=True):
        try:
            with self.start_path.open(encoding="utf-8") as stream:
                start = json.load(stream)
        except (OSError, ValueError) as exc:
            raise Blocked("The immutable monitor start record is absent or invalid; termination cannot be established.") from exc
        record = self.ledger.data.get("monitor_job")
        try:
            if not isinstance(start, dict) or not isinstance(record, dict):
                raise ValueError
            legacy = not any(key in start for key in self.BINDING_FIELDS)
            if legacy and (not allow_legacy or any(key in record for key in self.BINDING_FIELDS)):
                raise ValueError
            fixed = tuple(key for key in self.START_FIXED if not legacy or key not in self.BINDING_FIELDS)
            if not legacy:
                targets = _targets(start.get("target_bindings"))
                digest = start.get("generator_sha256")
                if ("demo-2" not in targets or not isinstance(digest, str)
                        or re.fullmatch(r"[a-f0-9]{64}", digest) is None
                        or start.get("monitor_binding_sha256")
                        != _monitor_binding(targets, digest, start.get("producer_config"))):
                    raise ValueError
            resources = self.d.resources
            self._recorded_notebook_key(start.get("monitor_notebook_id"), monitor=True)
            self._recorded_notebook_key(start.get("verifier_notebook_id"))
            previous_class = ("bootstrap" if re.fullmatch(
                r"demo2synthetic_[0-9a-f]{12}", str(start.get("run_id"))
            ) else "measured")
            valid = (
                isinstance(start, dict) and type(start.get("contract_version")) is int
                and start["contract_version"] == 1 and start.get("state") == "submitted"
                and isinstance(record, dict) and start.get("prefix") == self.s["prefix"]
                and start.get("tenant_id") == self.s["tenant_id"]
                and start.get("identity_id") == self.s["producer_identity"]
                and start.get("owner_marker") == self.d.marker
                and start.get("workspace_id") == resources["workspace"]["id"]
                and start.get("lakehouse_id") == resources["lakehouse"]["id"]
                and isinstance(start.get("run_id"), str)
                and re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}", start["run_id"]) is not None
                and start.get("manifest_uri") == f"{self.config['producer_root']}/{start['run_id']}/manifest.json"
                and start.get("state_uri")
                == _state_uri(start["workspace_id"], start["lakehouse_id"], start["run_id"])
                and start.get("sentinel_uri") == self.config["sentinel_uri"]
                and type(start.get("duration_seconds")) is int and start["duration_seconds"] > 0
                and all(type(start.get(key)) is int and start[key] > 0 for key in
                        ("expected_batch_count", "verification_timeout_seconds", "poll_interval_seconds",
                         "max_concurrent_batches", "track_timeout_seconds", "max_duration_seconds"))
                and isinstance(start.get("producer_config"), dict)
                and start["producer_config"].get("run_id") == start["run_id"]
                and start["producer_config"].get("batch_class") == previous_class
                and type(start["producer_config"].get("file_count")) is int
                and start["producer_config"]["file_count"] == start["expected_batch_count"]
                and all(key in record and key in start and _same_json(record[key], start[key]) for key in fixed)
            )
            if (not valid or _uuid(start.get("controller_id"), "controller ID") != start["controller_id"]
                    or _uuid(start.get("job_id"), "owned monitor job ID") != start["job_id"]):
                raise ValueError
            submitted = _utc(start["submitted_at_utc"], "submission time")
            deadline = _utc(start["deadline_utc"], "deadline")
            durations = {
                _monitor_duration(start["track_timeout_seconds"], start["max_duration_seconds"],
                                  start["verification_timeout_seconds"], legacy=legacy_duration)
                for legacy_duration in (False, True)
            }
            if (start["duration_seconds"] not in durations
                    or submitted + timedelta(seconds=start["duration_seconds"]) != deadline
                    or start["max_concurrent_batches"] > 4):
                raise ValueError
            ledger_job = record.get("job_id")
            if ledger_job is not None and _uuid(ledger_job, "recorded monitor job ID") != ledger_job:
                raise ValueError
            if ledger_job is not None and ledger_job != start["job_id"]:
                raise ValueError
            if current_run and (
                start["run_id"] != self.config["run_id"]
                or start.get("expected_batch_count") != self.s["expected_batch_count"]
                or start.get("verification_timeout_seconds") != self.s["timeout"]
                or start.get("poll_interval_seconds") != self.s["poll"]
                or start.get("max_concurrent_batches") != self.s["max_concurrency"]
                or start.get("track_timeout_seconds") != self.s["track_timeout"]
                or start.get("max_duration_seconds") != self.s["max_duration"]
                or not _same_json(start.get("producer_config"), self.config["producer_config"])
                or (check_binding and any(not _same_json(start.get(key), self.config[key])
                                          for key in self.BINDING_FIELDS))
            ):
                raise ValueError
        except (Blocked, Failure, KeyError, TypeError, ValueError, OverflowError) as exc:
            raise Blocked("The immutable monitor start record and ownership ledger do not match; "
                          "termination cannot be established.") from exc
        if ledger_job is None:
            record["job_id"] = start["job_id"]
            self.ledger.save()
        return start

    def _journal(self, start):
        journal = self.ledger.data.get("monitor_stop")
        if journal is None:
            return {}
        if (not isinstance(journal, dict) or any(journal.get(key) != start[key] for key in
                                                 ("run_id", "controller_id", "job_id"))
                or journal.get("state") not in ("stopping", "stopped")
                or not isinstance(journal.get("cancel_requests"), dict)):
            raise Blocked("The recorded monitor cancellation journal is invalid.")
        for key, value in journal["cancel_requests"].items():
            try:
                match = re.fullmatch(r"(monitor|child):([0-9a-f-]{36})", key)
                if (not match or _uuid(match.group(2), "cancel job ID") != match.group(2)
                        or value not in ("requested", "external")
                        or (match.group(1) == "monitor" and match.group(2) != start["job_id"])):
                    raise ValueError
            except (Failure, ValueError, TypeError) as exc:
                raise Blocked("The recorded monitor cancellation journal is invalid.") from exc
        return dict(journal["cancel_requests"])

    def _load(self, *, current_run=True):
        start = self._load_start(current_run=current_run)
        try:
            with self.path.open(encoding="utf-8") as stream:
                evidence = json.load(stream)
        except (OSError, ValueError) as exc:
            raise Blocked("The monitor status sidecar is missing or invalid.") from exc
        if (not isinstance(evidence, dict) or type(evidence.get("contract_version")) is not int
                or evidence["contract_version"] != 1
                or any(not _same_json(evidence.get(key), start[key]) for key in self.START_FIXED)
                or evidence.get("state") not in ("submitting", "starting", "checking", "running", "completed",
                                                  "stopping", "stopped", "terminal")
                or not isinstance(evidence.get("cancel_requests"), dict)):
            raise Blocked("The monitor status sidecar does not match the immutable start record.")
        self.evidence = evidence
        if evidence.get("job_id") is None and evidence["state"] == "submitting":
            evidence["job_id"] = start["job_id"]
            evidence["state"] = "starting"
            self._save()
        elif evidence.get("job_id") != start["job_id"]:
            raise Blocked("The monitor status sidecar job ID differs from the immutable start record.")
        journal = self._journal(start)
        if evidence["cancel_requests"] != journal:
            raise Blocked("The monitor status sidecar does not match the cancellation journal.")
        if self.ledger.data.get("monitor_stop") is not None:
            state = self.ledger.data["monitor_stop"]["state"]
            if state != "stopped" or evidence["state"] != "completed":
                evidence["state"] = state
        return evidence

    def _owned(self, *, definitions):
        self.d.workspace(False)
        if not self.stopping:
            self._check_target()
        self.d.item("lakehouse", "Lakehouse", self.s["lakehouse_name"], False)
        if definitions:
            if (self._recorded_notebook_key(self.evidence["verifier_notebook_id"]) != _notebook_key(self.s)
                    or self._recorded_notebook_key(self.evidence["monitor_notebook_id"], monitor=True)
                    != _notebook_key(self.s, monitor=True)):
                raise Blocked("The current monitor does not use this run's verifier and controller definitions.")
            self.d.notebook(False)
            self.d.monitor_notebook(False)
        else:
            verifier = self._recorded_notebook_key(self.evidence["verifier_notebook_id"])
            monitor = self._recorded_notebook_key(self.evidence["monitor_notebook_id"], monitor=True)
            self.d.item(verifier, "Notebook", self.s[f"{verifier}_name"], False)
            self.d.item(monitor, "Notebook", self.s[f"{monitor}_name"], False)

    def _check_target(self):
        expected = self.config["target_bindings"]["demo-2"]
        if (expected["workspace_id"] != self.d.resources["workspace"]["id"]
                or expected["item_id"] != self.d.resources["database"]["id"]
                or expected["fact"] != self.s["table"]):
            raise Blocked("The Demo 2 target binding does not match the recorded owned database and table.")

    def _object(self, uri):
        status, headers, value = self.client.onelake("GET", uri, allow_missing=True)
        if status == 404:
            return None, None
        etag = headers.get("etag")
        if (status != 200 or not isinstance(value, dict) or not isinstance(etag, str)
                or not etag or len(etag) > 1024 or "\r" in etag or "\n" in etag):
            raise Blocked("OneLake did not provide a valid, versioned JSON object readback.")
        return value, etag

    def _runtime_state(self, evidence, *, fresh):
        value, etag = self._object(evidence["state_uri"])
        if value is None:
            raise MonitorPending("The monitor has not written its per-instance state.")
        if (type(value.get("contract_version")) is not int or value["contract_version"] != 1
                or any(value.get(key) != evidence[key] for key in
                       ("run_id", "controller_id", "manifest_uri", "deadline_utc", "identity_id", "tenant_id"))
                or value.get("sentinel_verified") is not True or value.get("job_access_verified") is not True
                or value.get("state") not in ("running", "completed", "failed")
                or not isinstance(value.get("batches"), dict)
                or (value.get("run_verification") is not None
                    and not isinstance(value["run_verification"], dict))):
            raise Blocked("The monitor heartbeat does not prove this controller, run, identity and sentinel access.")
        heartbeat = _utc(value.get("heartbeat_at_utc"), "heartbeat")
        submitted = _utc(evidence["submitted_at_utc"], "submission time")
        deadline = _utc(evidence["deadline_utc"], "deadline")
        if heartbeat < submitted - timedelta(seconds=5) or heartbeat > deadline:
            raise Blocked("The monitor heartbeat lies outside this job's lifetime.")
        if fresh:
            now = _now()
            freshness = min(300, max(60, 3 * self.s["poll"]))
            if (value.get("generator_sha256") != evidence["generator_sha256"]
                    or value.get("monitor_binding_sha256") != evidence["monitor_binding_sha256"]):
                raise Blocked("The monitor heartbeat does not match the bound generator and targets.")
            if (value["state"] != "running" or deadline <= now
                    or heartbeat > now + timedelta(seconds=5)
                    or now - heartbeat > timedelta(seconds=freshness)):
                raise Blocked("The monitor heartbeat is stale, future-dated, expired or not running.")
        return value, etag

    def _lock(self, evidence, *, required):
        value, etag = self._object(_lock_uri(evidence["state_uri"]))
        if value is None:
            if required:
                raise MonitorPending("The monitor has not acquired its per-run writer lock.")
            return None
        if (type(value.get("contract_version")) is not int or value["contract_version"] != 1
                or value.get("run_id") != evidence["run_id"]
                or value.get("controller_id") != evidence["controller_id"]):
            raise Blocked("The monitor writer lock is owned by a different or unverifiable controller.")
        return etag

    def _job(self, notebook_id, job_id, *, details=False):
        ws = self.evidence["workspace_id"]
        if details:
            path = (f"/v1/workspaces/{ws}/notebooks/{notebook_id}/jobs/execute/instances/"
                    f"{job_id}?beta=true")
        else:
            path = f"/v1/workspaces/{ws}/items/{notebook_id}/jobs/instances/{job_id}"
        status, headers, job = self.client.fabric("GET", path)
        if (status != 200 or not isinstance(job, dict)
                or str(job.get("id", "")).lower() != job_id
                or str(job.get("itemId", "")).lower() != notebook_id):
            raise Blocked("The Fabric job readback does not match the recorded notebook and job IDs.")
        if job.get("status") not in self.ACTIVE | self.TERMINAL:
            raise Blocked("The Fabric job returned an unknown state.")
        return job, headers

    def _verified_child(self, contract, job_id, mode, expected):
        job, _ = self._job(self.evidence["verifier_notebook_id"], job_id, details=True)
        if job["status"] != "Completed":
            raise Blocked("A recorded verification notebook job is not confirmed completed.")
        try:
            contract._exit(job, mode, expected)
            result = job["properties"]["exitValue"]
            if isinstance(result, str):
                result = json.loads(result)
            summary = result["batch"] if mode == "verify_batch" else result
            if (not isinstance(summary, dict) or type(summary.get("row_count")) is not int
                    or type(summary.get("sales_amount_minor_units")) is not int):
                raise ValueError
        except (contract.MonitorError, KeyError, TypeError, ValueError) as exc:
            raise Blocked("A recorded verification notebook exit does not match the final producer manifest.") from exc

    def _completed(self):
        evidence = self.evidence
        contract = _load_monitor_contract()
        job, _ = self._job(evidence["monitor_notebook_id"], evidence["job_id"], details=True)
        if job["status"] != "Completed":
            raise Blocked("The notebook-specific monitor job is not confirmed completed.")
        try:
            value = json.loads(job["properties"]["exitValue"])
        except (KeyError, TypeError, ValueError) as exc:
            raise Blocked("The completed monitor lacks its notebook-specific verification exit value.") from exc
        if (not isinstance(value, dict) or value.get("mode") != "monitor"
                or value.get("status") != "verified" or value.get("run_id") != evidence["run_id"]):
            raise Blocked("The completed monitor exit value does not verify this run.")
        state, state_etag = self._runtime_state(evidence, fresh=False)
        records = state["batches"]
        if (state["state"] != "completed" or state.get("manifest_observed") is not True
                or state.get("generator_sha256") != evidence["generator_sha256"]
                or state.get("monitor_binding_sha256") != evidence["monitor_binding_sha256"]
                or len(records) != evidence["expected_batch_count"]):
            raise Blocked("The completed monitor state does not contain every expected measured batch.")
        for number in range(1, evidence["expected_batch_count"] + 1):
            record = records.get(f"{evidence['run_id']}_{number:04d}")
            try:
                if (not isinstance(record, dict) or record.get("status") != "verified"
                        or not isinstance(record.get("fingerprint"), str)
                        or re.fullmatch(r"[0-9a-f]{64}", record["fingerprint"]) is None
                        or _uuid(record.get("operation_id"), "batch operation ID") != record["operation_id"]):
                    raise ValueError
                _utc(record.get("deadline_utc"), "batch deadline")
            except (Blocked, Failure, KeyError, ValueError) as exc:
                raise Blocked("A measured batch lacks verified durable monitor evidence.") from exc
        run = state.get("run_verification")
        if not isinstance(run, dict) or run.get("status") != "verified":
            raise Blocked("The completed monitor lacks verified whole-run evidence.")
        children, uncertain = self._child_records(state)
        if uncertain or len(children) != evidence["expected_batch_count"] + 1:
            raise Blocked("The completed monitor contains ambiguous verification job evidence.")

        manifest, manifest_etag = self._object(evidence["manifest_uri"])
        if manifest is None:
            raise Blocked("The final producer manifest is missing.")
        destinations = manifest.get("destinations")
        if (not isinstance(destinations, list)
                or any(name not in ("demo-1", "demo-2", "demo-3") for name in destinations)
                or len(destinations) != len(set(destinations))
                or "demo-2" not in destinations
                or set(destinations) != set(evidence["target_bindings"])
                or not _same_json(manifest.get("target_bindings"), evidence["target_bindings"])
                or not _same_json(manifest.get("config"), evidence["producer_config"])):
            raise Blocked("The final producer manifest configuration or destinations do not match this run.")
        try:
            batches = contract._measured(
                manifest, {"run_id": evidence["run_id"], "count": evidence["expected_batch_count"],
                           "generator_sha256": evidence["generator_sha256"],
                           "producer_config": evidence["producer_config"],
                           "destinations": sorted(evidence["target_bindings"]),
                           "target_bindings": evidence["target_bindings"],
                           "synthetic_run_id": self.config["synthetic_run_id"]}
            )
        except (contract.MonitorError, KeyError, TypeError, ValueError, OverflowError) as exc:
            raise Blocked("The final producer manifest does not prove the expected measured batches.") from exc
        if (len(batches) != evidence["expected_batch_count"]
                or set(records) != {batch["batch_id"] for batch in batches}
                or any(batch["row_count"] != evidence["producer_config"]["rows_per_file"] for batch in batches)):
            raise Blocked("The final producer manifest batch IDs, count or row counts do not match the run.")

        scheduler = manifest.get("scheduler")
        result = scheduler.get("result") if isinstance(scheduler, dict) else None
        if (not isinstance(scheduler, dict) or scheduler.get("status") != "completed"
                or type(scheduler.get("max_duration_seconds")) is not int
                or scheduler["max_duration_seconds"] != evidence["max_duration_seconds"]
                or type(scheduler.get("max_attempts")) is not int or scheduler["max_attempts"] < 1
                or not isinstance(result, dict)
                or set(result) != {"verified", "errors", "deadline_utc"}
                or result.get("verified") is not True or result.get("errors") != []):
            raise Blocked("The final producer scheduler does not prove terminal success.")
        try:
            started = _utc(scheduler.get("started_at_utc"), "producer start")
            finished = _utc(scheduler.get("finished_at_utc"), "producer completion")
            deadline = _utc(result.get("deadline_utc"), "producer deadline")
            if (deadline != started + timedelta(seconds=evidence["max_duration_seconds"])
                    or not started <= finished <= deadline):
                raise ValueError
        except (Blocked, OverflowError, ValueError) as exc:
            raise Blocked("The final producer scheduler timestamps do not prove bounded completion.") from exc

        database = self.d.verify_target_ownership()
        for batch in batches:
            record = records[batch["batch_id"]]
            try:
                if record["fingerprint"] != contract.batch_fingerprint(batch):
                    raise ValueError
                contract.verify_delivery_evidence(batch, record)
                submitted_at = _utc(
                    batch["deliveries"]["demo-2"]["submission_result_at_utc"], "submission result"
                )
                if _utc(record["deadline_utc"], "batch deadline") > submitted_at + timedelta(
                        seconds=evidence["verification_timeout_seconds"]):
                    raise ValueError
            except (contract.MonitorError, KeyError, TypeError, ValueError, OverflowError) as exc:
                raise Blocked("A measured batch lacks matching fingerprint and Kusto delivery evidence.") from exc
            verify_kql_batch(
                self.d._kql(
                    database, batch_verification_query(self.s["table"], batch["batch_id"]), query=True,
                ),
                batch,
            )
            expected = {key: batch[key] for key in ("batch_id", "row_count", "sales_amount_minor_units")}
            self._verified_child(contract, record["job_id"], "verify_batch", expected)

        expected_run = {
            "batch_ids": sorted(batch["batch_id"] for batch in batches),
            "row_count": sum(batch["row_count"] for batch in batches),
            "sales_amount_minor_units": sum(batch["sales_amount_minor_units"] for batch in batches),
        }
        self._verified_child(contract, run["job_id"], "verify_run_keys", expected_run)

        final_manifest, final_manifest_etag = self._object(evidence["manifest_uri"])
        final_state, final_state_etag = self._runtime_state(evidence, fresh=False)
        if manifest_etag != final_manifest_etag or not _same_json(manifest, final_manifest):
            raise Blocked("The producer manifest changed during independent completion verification.")
        if state_etag != final_state_etag or not _same_json(state, final_state):
            raise Blocked("The monitor state changed during independent completion verification.")
        if self._lock(evidence, required=False) is not None:
            raise Blocked("The completed monitor has not released its writer lock.")

    def _observe(self, *, wait, allow_completed=False):
        evidence = self.evidence
        job_id = evidence.get("job_id")
        if job_id is None:
            raise Blocked("The monitor submission outcome is uncertain; do not submit another job.")
        if evidence["state"] in ("stopping", "stopped") \
                or (not allow_completed and evidence["state"] in ("terminal", "completed")) \
                or f"monitor:{job_id}" in evidence["cancel_requests"]:
            raise Blocked("This monitor is stopping or terminal; it cannot be claimed ready.")
        while True:
            job, headers = self._job(evidence["monitor_notebook_id"], job_id)
            state = job["status"]
            if state in self.TERMINAL:
                evidence.update(state="terminal", job_status=state)
                self._save()
                if state == "Completed" and allow_completed:
                    self._completed()
                    evidence["state"] = "completed"
                    self._save()
                    return "completed"
                raise Blocked(f"The recorded monitor job is no longer running ({state}).")
            try:
                if state != "InProgress":
                    raise MonitorPending("The recorded monitor job has not started.")
                self._runtime_state(evidence, fresh=True)
                self._lock(evidence, required=True)
                confirmed, _ = self._job(evidence["monitor_notebook_id"], job_id)
                if confirmed["status"] != "InProgress":
                    raise Blocked("The recorded monitor job stopped during its readiness check.")
                if _utc(evidence["deadline_utc"], "deadline") <= _now():
                    raise Blocked("The monitor deadline expired during its readiness check.")
                evidence.update(state="running", job_status="InProgress")
                self._save()
                return "running"
            except MonitorPending:
                if not wait:
                    raise
                self.d.deadline.wait(_retry_delay(headers.get("retry-after"), self.s["poll"]))

    def _child_records(self, state):
        records = list(state["batches"].values())
        if state.get("run_verification") is not None:
            records.append(state["run_verification"])
        jobs = {}
        uncertain = False
        allowed = {"awaiting_submission", "queued", "submitting", "running", "spark_verified", "verified", "failed"}
        for record in records:
            if not isinstance(record, dict) or record.get("status") not in allowed:
                raise Blocked("The monitor's verification job state is invalid or ambiguous.")
            status, job_id = record["status"], record.get("job_id")
            if "submission_uncertain" in record and type(record["submission_uncertain"]) is not bool:
                raise Blocked("The monitor's verification submission evidence is invalid.")
            if "cancel_requested" in record and type(record["cancel_requested"]) is not bool:
                raise Blocked("The monitor's verification cancellation state is invalid.")
            if status == "submitting" or record.get("submission_uncertain") is True:
                uncertain = True
            if job_id is None:
                if status in ("running", "spark_verified", "verified") or "cancel_requested" in record:
                    uncertain = True
                continue
            try:
                if _uuid(job_id, "verification job ID") != job_id or job_id in jobs:
                    raise ValueError
            except (Failure, ValueError) as exc:
                raise Blocked("The monitor's verification job IDs are invalid or ambiguous.") from exc
            jobs[job_id] = "cancel_requested" in record
        return jobs, uncertain

    def _cancel_and_confirm(self, notebook_id, job_id, role, *, external=False):
        job, headers = self._job(notebook_id, job_id)
        if job["status"] in self.TERMINAL:
            return
        key = f"{role}:{job_id}"
        requests = self.evidence["cancel_requests"]
        if key not in requests:
            requests[key] = "external" if external else "requested"
            self._save()
            if not external:
                ws = self.evidence["workspace_id"]
                path = f"/v1/workspaces/{ws}/items/{notebook_id}/jobs/instances/{job_id}/cancel"
                try:
                    status, headers, _ = self.client.fabric("POST", path)
                    if status != 202:
                        raise Failure("Fabric did not accept the cancellation request.")
                    if headers.get("location") and _job_id(headers["location"], ws, notebook_id) != job_id:
                        raise Failure("Fabric returned a different cancellation job location.")
                except (Failure, Blocked) as exc:
                    job, headers = self._job(notebook_id, job_id)
                    if job["status"] in self.TERMINAL:
                        return
                    raise Blocked("The exact job cancellation outcome is uncertain; it will not be retried blindly.") from exc
        while True:
            self.d.deadline.wait(_retry_delay(headers.get("retry-after"), self.s["poll"]))
            job, headers = self._job(notebook_id, job_id)
            if job["status"] in self.TERMINAL:
                return

    def _reconcile_runtime(self, *, cancel):
        evidence = self.evidence
        state, state_etag = self._runtime_state(evidence, fresh=False)
        children, uncertain = self._child_records(state)
        for job_id, external in children.items():
            if cancel:
                self._cancel_and_confirm(evidence["verifier_notebook_id"], job_id, "child", external=external)
            else:
                child, _ = self._job(evidence["verifier_notebook_id"], job_id)
                if child["status"] not in self.TERMINAL:
                    raise Blocked("A prior monitor's recorded verification job is still active; stop it first.")
        if uncertain:
            raise Blocked("A verification job submission or termination is uncertain without complete job evidence.")
        _, after_etag = self._runtime_state(evidence, fresh=False)
        if after_etag != state_etag:
            raise Blocked("The monitor state changed during reconciliation; retry after verifying ownership.")
        lock_etag = self._lock(evidence, required=False)
        if lock_etag is not None:
            uri = _lock_uri(evidence["state_uri"])
            status, _, _ = self.client.onelake("DELETE", uri, extra_headers={"If-Match": lock_etag})
            if status not in (200, 202, 204):
                raise Blocked("The reconciled monitor writer lock could not be released.")
            while True:
                value, _ = self._object(uri)
                if value is None:
                    break
                self.d.deadline.wait(self.s["poll"])

    def _archive_previous(self):
        self._owned(definitions=False)
        job_id = self.evidence.get("job_id")
        if job_id is None:
            raise Blocked("An earlier monitor submission is uncertain; do not submit another job.")
        job, _ = self._job(self.evidence["monitor_notebook_id"], job_id)
        if job["status"] not in self.TERMINAL:
            raise Blocked("A prior monitor job is still active; stop it using the original run configuration.")
        self._reconcile_runtime(cancel=False)
        start = self._load_start(current_run=False)
        for prefix, value in (("demo-2-monitor", self.evidence), ("demo-2-monitor-start", start)):
            archive = self.path.with_name(f"{prefix}.{self.evidence['run_id']}.{job_id}.json")
            if archive.exists():
                try:
                    if json.loads(archive.read_text(encoding="utf-8")) != value:
                        raise ValueError
                except (OSError, ValueError) as exc:
                    raise Blocked("The prior monitor archive is invalid or conflicts with the recorded job.") from exc
            else:
                _atomic_create_json(archive, value)

    def start(self, *, admission_expires_at=None):
        replacing_prior = False
        if self.path.exists():
            self._load(current_run=False)
            if self.evidence["run_id"] == self.config["run_id"]:
                self._load()
                self._owned(definitions=True)
                self._observe(wait=True)
                return
            self._archive_previous()
            replacing_prior = True
        elif self.ledger.data.get("monitor_job") is not None or self.start_path.exists():
            raise Blocked("A monitor submission was recorded without its sidecar; reconciliation is required.")

        self.d.workspace(False)
        self._check_target()
        self.d.item("lakehouse", "Lakehouse", self.s["lakehouse_name"], False)
        self.d.notebook(False)
        ws = self.d.resources["workspace"]["id"]
        lakehouse = self.d.resources["lakehouse"]["id"]
        uri = _state_uri(ws, lakehouse, self.config["run_id"])
        for path in (uri, _lock_uri(uri)):
            status, _, _ = self.client.onelake("HEAD", path, allow_missing=True)
            if status != 404:
                raise Blocked("A prior monitor state or writer lock already exists for this run; reconcile it first.")
        self.d.monitor_notebook(True)
        monitor = self.d.resources[_notebook_key(self.s, monitor=True)]["id"]
        verifier = self.d.resources[_notebook_key(self.s)]["id"]
        if replacing_prior:
            self.start_path.unlink()
        if self.start_path.exists():
            raise Blocked("An immutable monitor start record already exists; reconcile it before submitting.")
        if admission_expires_at is not None:
            _require_synthetic_fresh(admission_expires_at)
        submitted = _now()
        try:
            deadline = submitted + timedelta(seconds=self.config["duration"])
        except OverflowError as exc:
            raise Failure("The monitor deadline bound is too large") from exc
        evidence = {
            "contract_version": 1, "prefix": self.s["prefix"], "owner_marker": self.d.marker,
            "run_id": self.config["run_id"], "job_id": None, "state": "submitting",
            "manifest_uri": self.config["manifest_uri"], "sentinel_uri": self.config["sentinel_uri"],
            "state_uri": uri, "deadline_utc": _stamp(deadline), "submitted_at_utc": _stamp(submitted),
            "duration_seconds": self.config["duration"], "controller_id": str(uuid.uuid4()),
            "tenant_id": self.s["tenant_id"], "identity_id": self.s["producer_identity"],
            "workspace_id": ws, "lakehouse_id": lakehouse, "monitor_notebook_id": monitor,
            "verifier_notebook_id": verifier, "expected_batch_count": self.s["expected_batch_count"],
            "verification_timeout_seconds": self.s["timeout"], "poll_interval_seconds": self.s["poll"],
            "max_concurrent_batches": self.s["max_concurrency"],
            "track_timeout_seconds": self.s["track_timeout"], "max_duration_seconds": self.s["max_duration"],
            "producer_config": self.config["producer_config"],
            "target_bindings": self.config["target_bindings"],
            "generator_sha256": self.config["generator_sha256"],
            "monitor_binding_sha256": self.config["monitor_binding_sha256"],
            "cancel_requests": {},
        }
        self.evidence = evidence
        self.ledger.data["monitor_job"] = {key: evidence[key] for key in self.START_FIXED}
        self.ledger.data["monitor_job"]["job_id"] = None
        self.ledger.data.pop("monitor_stop", None)
        self.ledger.save()
        self._save()
        parameters = [
            ("RUN_ID", evidence["run_id"], "Text"),
            ("MANIFEST_URL", evidence["manifest_uri"], "Text"),
            ("SENTINEL_URL", evidence["sentinel_uri"], "Text"),
            ("CONTROLLER_ID", evidence["controller_id"], "Text"),
            ("STATE_URL", evidence["state_uri"], "Text"),
            ("DEADLINE_UTC", evidence["deadline_utc"], "Text"),
            ("VERIFIER_WORKSPACE_ID", ws, "Text"),
            ("VERIFIER_NOTEBOOK_ID", verifier, "Text"),
            ("EXPECTED_IDENTITY_ID", evidence["identity_id"], "Text"),
            ("EXPECTED_TENANT_ID", evidence["tenant_id"], "Text"),
            ("EXPECTED_GENERATOR_SHA256", evidence["generator_sha256"], "Text"),
            ("EXPECTED_PRODUCER_CONFIG_JSON", json.dumps(evidence["producer_config"], sort_keys=True), "Text"),
            ("EXPECTED_TARGET_BINDINGS_JSON", json.dumps(evidence["target_bindings"], sort_keys=True), "Text"),
            ("EXPECTED_MONITOR_BINDING_SHA256", evidence["monitor_binding_sha256"], "Text"),
            ("EXPECTED_BATCH_COUNT", evidence["expected_batch_count"], "Integer"),
            ("VERIFICATION_TIMEOUT_SECONDS", evidence["verification_timeout_seconds"], "Integer"),
            ("POLL_INTERVAL_SECONDS", evidence["poll_interval_seconds"], "Integer"),
            ("MAX_CONCURRENT_BATCHES", evidence["max_concurrent_batches"], "Integer"),
        ]
        body = {"parameters": [{"name": name, "value": value, "type": kind}
                               for name, value, kind in parameters], "executionData": {"compute": "Spark"}}
        if admission_expires_at is not None:
            try:
                _require_synthetic_fresh(admission_expires_at)
            except Blocked as exc:
                raise Blocked("Synthetic runtime preflight expired before monitor submission; "
                              "reconcile the unsubmitted monitor intent.") from exc
        try:
            status, headers, _ = self.client.fabric(
                "POST", f"/v1/workspaces/{ws}/notebooks/{monitor}/jobs/execute/instances?beta=false", body,
            )
        except (Failure, Blocked) as exc:
            raise Blocked("The monitor submission outcome is uncertain; no exact job ID can be safely cancelled or retried.") from exc
        if status != 202:
            raise Blocked("Monitor submission was not confirmed; its outcome must be reconciled before retrying.")
        try:
            job_id = _job_id(headers.get("location"), ws, monitor)
        except Failure as exc:
            raise Blocked("The monitor submission may have succeeded, but its exact job ID is unavailable; "
                          "termination cannot be established.") from exc
        start = {"contract_version": 1, "state": "submitted",
                 **{key: evidence[key] for key in self.START_FIXED}, "job_id": job_id}
        try:
            _atomic_create_json(self.start_path, start)
            self.ledger.data["monitor_job"]["job_id"] = job_id
            self.ledger.save()
            evidence.update(job_id=job_id, state="starting")
            self._save()
        except OSError as exc:
            raise Blocked("The monitor was accepted but its exact job ownership could not be fully persisted; "
                          "reconcile the immutable start record before retrying.") from exc
        self._observe(wait=True)

    def status(self):
        self._load()
        if self.evidence["state"] not in ("stopping", "stopped", "terminal", "completed", "submitting"):
            self.evidence["state"] = "checking"
            self._save()
        self._owned(definitions=True)
        return self._observe(wait=False, allow_completed=True)

    def wait(self):
        self._load()
        evidence = self.evidence
        if (evidence["state"] in ("stopping", "stopped")
                or f"monitor:{evidence['job_id']}" in evidence["cancel_requests"]):
            raise Blocked("This monitor is stopping or stopped; it cannot be claimed complete.")
        try:
            until = (_utc(evidence["deadline_utc"], "deadline")
                     + timedelta(seconds=evidence["track_timeout_seconds"]))
        except OverflowError as exc:
            raise Failure("The monitor readback deadline is too large") from exc
        remaining = (until - _now()).total_seconds()
        if remaining <= 0:
            raise Blocked("The monitor completion readback deadline has expired.")
        previous = self.d.deadline
        previous_client = getattr(self.client, "deadline", None)
        self.d.deadline = Deadline(remaining, label="monitor readback")
        if previous_client is not None:
            self.client.deadline = self.d.deadline
        try:
            self._owned(definitions=True)
            evidence["state"] = "checking"
            self._save()
            while True:
                self.d.deadline.remaining()
                if _now() >= until:
                    raise Blocked("The monitor completion readback deadline has expired.")
                job, headers = self._job(evidence["monitor_notebook_id"], evidence["job_id"])
                if job["status"] in self.TERMINAL:
                    evidence.update(state="terminal", job_status=job["status"])
                    self._save()
                    if job["status"] != "Completed":
                        raise Blocked(f"The recorded monitor job ended in state {job['status']}.")
                    self._completed()
                    evidence["state"] = "completed"
                    self._save()
                    return
                self.d.deadline.wait(_retry_delay(headers.get("retry-after"), self.s["poll"]))
        finally:
            self.d.deadline = previous
            if previous_client is not None:
                self.client.deadline = previous_client

    def stop(self):
        start = self._load_start(allow_legacy=True, check_binding=False)
        self.evidence = {**start, "state": "stopping", "cancel_requests": self._journal(start)}
        self.stopping = True
        self._owned(definitions=False)
        job_id = start["job_id"]
        self._save()
        self._cancel_and_confirm(start["monitor_notebook_id"], job_id, "monitor")
        self._reconcile_runtime(cancel=True)
        self.evidence["state"] = "stopped"
        self._save()
        try:
            with self.path.open(encoding="utf-8") as stream:
                sidecar = json.load(stream)
            if (not isinstance(sidecar, dict) or type(sidecar.get("contract_version")) is not int
                    or sidecar["contract_version"] != 1
                    or any(not _same_json(sidecar.get(key), start[key]) for key in self.START_FIXED if key in start)
                    or sidecar.get("job_id") != start["job_id"]):
                return False
        except (OSError, ValueError):
            return False
        final_state = "stopped"
        if sidecar.get("state") == "completed" and not self.evidence["cancel_requests"]:
            try:
                self._completed()
            except (Blocked, Failure):
                pass
            else:
                final_state = "completed"
        sidecar.update(state=final_state, cancel_requests=dict(self.evidence["cancel_requests"]))
        _atomic_json(self.path, sidecar)
        return True


class MonitorPending(Blocked):
    """The exact monitor instance has not produced readiness evidence yet."""


def _resolve_target_candidate(args, settings):
    try:
        with Ledger(args.state_dir, settings) as ledger:
            return _write_target_candidate(args, settings, ledger)
    except (OSError, ValueError) as exc:
        raise Blocked("The existing Demo 2 ownership ledger could not be read.") from exc


def _journal_target(settings, ledger, *, expected_pending=None):
    try:
        resources = ledger.data["resources"]
        workspace, eventhouse, database, table = (resources[key] for key in
                                                   ("workspace", "eventhouse", "database", "table"))
        if (settings["tenant_id"] is None or uuid.UUID(settings["tenant_id"]).int == 0
                or ledger.data["pending"] != expected_pending
                or any(not isinstance(item, dict) or item.get("confirmed") is not True
                       for item in (workspace, eventhouse, database, table))
                or workspace.get("type") != "Workspace" or workspace.get("name") != settings["workspace_name"]
                or eventhouse.get("type") != "Eventhouse" or eventhouse.get("name") != settings["eventhouse_name"]
                or uuid.UUID(eventhouse["id"]).int == 0
                or database.get("type") != "KQLDatabase" or database.get("name") != settings["database_name"]
                or database.get("parent_eventhouse_id") != eventhouse["id"]
                or table.get("database_id") != database["id"] or table.get("name") != settings["table"]
                or any(settings[key] is not None and settings[key] != item["id"]
                       for key, item in (("workspace_id", workspace), ("eventhouse_id", eventhouse),
                                         ("database_id", database)))):
            raise ValueError
        target = {"workspace_id": workspace["id"], "item_id": database["id"], "fact": table["name"]}
        _targets({"demo-2": target})
    except (KeyError, TypeError, ValueError) as exc:
        raise Blocked("A complete, confirmed Demo 2 ownership ledger is required for a target candidate.") from exc
    return target


def _write_target_candidate(args, settings, ledger):
    target = _journal_target(settings, ledger)
    candidate = {"contract_version": 1, "track": "demo-2", "tenant_id": settings["tenant_id"],
                 "prefix": settings["prefix"], "target": target}
    path = args.result.parent / "demo-2-target-candidate.json"
    if path.resolve() == args.result.resolve() or path.resolve() == args.config.resolve():
        raise Failure("The target candidate path conflicts with the result or configuration.")
    try:
        _atomic_create_json(path, candidate)
    except OSError as exc:
        raise Blocked("The fresh target candidate could not be created without replacing an existing file.") from exc
    return "pass", False, ["A provisional ownership-ledger target candidate was recorded; live readiness is unverified."]


def _verify_synthetic_preparation(deployment, monitor_config, *, verify_definitions=True):
    resources = deployment.resources
    preparation = resources.get("synthetic_preparation")
    try:
        if (not isinstance(preparation, dict)
                or preparation.get("run_id") != monitor_config["run_id"]
                or preparation.get("generator_sha256") != monitor_config["generator_sha256"]
                or preparation.get("workspace_id") != resources["workspace"]["id"]
                or preparation.get("lakehouse_id") != resources["lakehouse"]["id"]
                or preparation.get("notebook_id") != resources["notebook"]["id"]
                or _uuid(preparation.get("job_id"), "preload job ID") != preparation["job_id"]):
            raise ValueError
    except (Failure, KeyError, TypeError, ValueError) as exc:
        raise Blocked("A verified, run-bound synthetic preparation is required before monitor start.") from exc
    database = deployment.verify_target_ownership()
    if verify_definitions:
        deployment.notebook(False)
        deployment.monitor_notebook(False)
    deployment.shortcut(deployment.mirror(database), False)
    with _spark_client(deployment.client):
        ws = preparation["workspace_id"]
        notebook = preparation["notebook_id"]
        job_id = preparation["job_id"]
        status, _, job = deployment.client.fabric(
            "GET", f"/v1/workspaces/{ws}/notebooks/{notebook}/jobs/execute/instances/{job_id}?beta=true"
        )
    if (status != 200 or not isinstance(job, dict) or job.get("id") != job_id
            or job.get("itemId") != notebook or job.get("status") != "Completed"):
        raise Blocked("The recorded synthetic dimension-preload job is not confirmed completed.")
    _verify_notebook_exit(job, "preload", None)


def _reconcile_prior_monitor(deployment, monitor_config):
    monitor = Monitor(deployment, monitor_config)
    if monitor.path.exists():
        monitor._load(current_run=False)
        monitor._archive_previous()
    elif deployment.ledger.data.get("monitor_job") is not None or monitor.start_path.exists():
        raise Blocked("A monitor submission was recorded without its sidecar; reconciliation is required.")


def _run(args, settings):
    scope = re.fullmatch(r"demo2synthetic-([0-9a-f]{12})", settings["prefix"])
    synthetic = _synthetic_run(settings)
    reuse = settings["reuse_synthetic_target"]
    synthetic_modes = {"provision-synthetic", "prepare-synthetic-monitor", "start-synthetic-monitor", "synthetic-monitor-status",
                       "wait-synthetic-monitor"}
    target_checks = {"resolve-target-candidate", "verify-target-ownership"}
    reuse_modes = {"prepare-measured-monitor", "check-dimension-gaps", "expand-compatible-dimensions",
                   "validate-only", "start-monitor", "monitor-status", "wait-monitor", "stop-monitor"}
    if scope:
        if (settings["workspace_name"] != f"{settings['prefix']}-demo2-eventhouse"
                or settings["table"] != "SyntheticFactSales"):
            raise Failure("Synthetic work requires the reserved workspace and table names.")
        run_id = settings["data"]["run_id"]
        if reuse:
            if synthetic or (run_id or "").casefold().startswith("demo2synthetic_"):
                raise Blocked("Measured reuse requires a non-synthetic run ID.")
            if args.mode not in reuse_modes | target_checks | {"dry-run"}:
                raise Blocked("The existing synthetic target may only be reused through measured-safe modes.")
        else:
            if not synthetic and (args.mode not in target_checks
                                  or (run_id or "").casefold().startswith("demo2synthetic_")):
                raise Failure("Synthetic work requires the matching reserved run ID outside ownership checks.")
            if args.mode not in synthetic_modes | target_checks | {"dry-run", "stop-monitor"}:
                raise Blocked("Production modes cannot use the reserved synthetic deployment scope.")
    elif (reuse or args.mode in synthetic_modes or args.mode in {
              "prepare-measured-monitor", "check-dimension-gaps", "expand-compatible-dimensions"}
          or settings["prefix"].startswith("demo2synthetic-")
          or (settings["data"]["run_id"] or "").casefold().startswith("demo2synthetic_")
          or settings["table"].casefold() == "syntheticfactsales"):
        raise Blocked("Synthetic runs and resources require an isolated synthetic deployment scope.")
    if args.mode == "dry-run":
        return "pass", False, [
            f"Offline configuration check only; no deployment or readiness was verified. "
            f"Verification timeout: {settings['timeout']} seconds."
        ]
    if args.mode == "resolve-target-candidate":
        return _resolve_target_candidate(args, settings)
    if args.mode == "cleanup" and not args.yes:
        raise Failure("cleanup requires --yes; no resources were removed")
    if args.mode == "expand-compatible-dimensions" and not args.yes:
        raise Failure("expand-compatible-dimensions requires --yes; no dimensions were changed")
    monitor_mode = {"start-synthetic-monitor": "start-monitor", "synthetic-monitor-status": "monitor-status",
                    "wait-synthetic-monitor": "wait-monitor"}.get(args.mode, args.mode)
    preparation_modes = {"prepare-measured-monitor", "check-dimension-gaps", "expand-compatible-dimensions"}
    monitor_config = (_monitor_config(settings, args.selected_demos, bind=monitor_mode != "stop-monitor",
                                     require_run=monitor_mode not in preparation_modes)
                      if monitor_mode in ("prepare-synthetic-monitor", "start-monitor", "monitor-status",
                                          "wait-monitor", "stop-monitor") or monitor_mode in preparation_modes else None)
    if settings["tenant_id"] is None:
        raise Blocked("tenant_id is required to verify the Azure CLI identity safely.")
    builder = _load_builder() if monitor_mode in ("deploy", "prepare-synthetic-monitor", "start-monitor", "monitor-status", "wait-monitor") or monitor_mode in preparation_modes else None
    if monitor_mode in ("prepare-synthetic-monitor", "start-monitor", "monitor-status", "wait-monitor") or monitor_mode in preparation_modes:
        _load_monitor_builder()
    deadline = Deadline(settings["track_timeout"], label="track")
    if args.mode == "validate-only" and not (args.state_dir / STATE_FILE).exists():
        client = FabricClient(settings["tenant_id"], deadline)
        ReadOnlyDeployment(settings, client, deadline).validate()
        raise Blocked("Read-only validation did not establish all readiness evidence.")
    with Ledger(args.state_dir, settings, create=args.mode in ("deploy", "provision-synthetic")) as ledger:
        client = FabricClient(settings["tenant_id"], deadline)
        deployment = Deployment(settings, client, deadline, ledger, builder)
        if args.mode == "verify-target-ownership":
            deployment.verify_target_ownership()
            return "pass", False, ["Fresh Fabric and KQL readbacks match the recorded Demo 2 ownership; "
                                   "producer permissions and readiness are unverified."]
        recovered = _recover_pending(deployment)
        if recovered is not None:
            expected_mode = {"check-dimension-gaps": "dimension_gaps",
                             "expand-compatible-dimensions": "expand_dimensions"}.get(args.mode)
            if recovered["mode"] != expected_mode:
                raise Blocked("A previous dimension-maintenance job was reconciled; rerun the intended command.")
            return "pass", False, [f"Reconciled dimension job {recovered['job_id']}: "
                                   f"{json.dumps(recovered['result'], sort_keys=True)}"]
        if args.mode == "provision-synthetic":
            deployment.workspace(True)
            deployment.item("eventhouse", "Eventhouse", settings["eventhouse_name"], True)
            deployment.item("lakehouse", "Lakehouse", settings["lakehouse_name"], True)
            database = deployment.database(True)
            deployment.configure_kql(database)
            deployment.verify_target_ownership()
            return "pass", False, ["Synthetic target ownership and schema were verified; no data was submitted."]
        if args.mode == "prepare-synthetic-monitor":
            if (deployment.resources.get("workspace", {}).get("id") == settings["shared_workspace_id"]
                    or (ledger.directory / MONITOR_FILE).exists()):
                raise Blocked("Synthetic preparation requires a distinct target and no prior monitor job.")
            if deployment.resources.get("synthetic_preparation") is not None:
                _verify_synthetic_preparation(deployment, monitor_config)
            else:
                database = deployment.verify_target_ownership()
                deployment.item("lakehouse", "Lakehouse", settings["lakehouse_name"], False)
                deployment.notebook(True)
                deployment.shortcut(deployment.mirror(database), True)
                deployment.monitor_notebook(True)
                preparation = {
                    "run_id": monitor_config["run_id"], "generator_sha256": monitor_config["generator_sha256"],
                    "workspace_id": deployment.resources["workspace"]["id"],
                    "lakehouse_id": deployment.resources["lakehouse"]["id"],
                    "notebook_id": deployment.resources["notebook"]["id"],
                }
                deployment.run_notebook("preload", synthetic_preparation=preparation)
                _verify_synthetic_preparation(deployment, monitor_config)
            return "pass", False, ["Synthetic dimensions, shortcut and notebook definitions were prepared; "
                                   "producer runtime admission and monitor start are not established."]
        if args.mode in preparation_modes:
            database = deployment.verify_target_ownership()
            if deployment.resources["workspace"]["id"] == settings["shared_workspace_id"]:
                raise Blocked("Producer staging and the reused Eventhouse must be in distinct workspaces.")
            deployment.reused_synthetic_target()
            deployment.item("lakehouse", "Lakehouse", settings["lakehouse_name"], False)
            deployment.shortcut(deployment.mirror(database), False)
            with _spark_client(client):
                _reconcile_prior_monitor(deployment, monitor_config)
                if args.mode == "prepare-measured-monitor":
                    deployment.notebook(True)
                    deployment.monitor_notebook(True)
                    return "pass", False, ["The prior monitor was reconciled and separate measured notebook "
                                           "definitions were verified; producer readiness remains unverified."]
                deployment.notebook(False)
                if args.mode == "expand-compatible-dimensions":
                    deployment.ensure_dimension_quiescent()
                    mode = "expand_dimensions"
                else:
                    mode = "dimension_gaps"
                job_id, result = deployment.run_notebook(mode)
                return "pass", False, [f"Dimension job {job_id}: {json.dumps(result, sort_keys=True)}; "
                                       "producer readiness remains unverified."]
        if monitor_config is not None:
            monitor = Monitor(deployment, monitor_config)
            admission_expires_at = None
            if monitor_mode != "stop-monitor":
                if synthetic:
                    if deployment.resources.get("workspace", {}).get("id") == settings["shared_workspace_id"]:
                        raise Blocked("Synthetic staging and Eventhouse workspaces must be distinct.")
                    if monitor_mode == "start-monitor":
                        _verify_synthetic_preparation(deployment, monitor_config, verify_definitions=False)
                        admission_expires_at = verify_synthetic_runtime_access(deployment, monitor_config)
                        _require_synthetic_fresh(admission_expires_at)
                    else:
                        deployment.verify_target_ownership()
                else:
                    if monitor_mode == "wait-monitor":
                        verify_monitor_readiness(deployment, allow_fact_drift=True)
                    else:
                        verify_monitor_readiness(deployment)
            with _spark_client(client):
                batch_label = "synthetic bootstrap" if synthetic else "measured"
                if monitor_mode == "start-monitor":
                    if synthetic:
                        monitor.start(admission_expires_at=admission_expires_at)
                    else:
                        monitor.start()
                    return "pass", not synthetic, ["The exact monitor job is running with fresh identity and sentinel evidence."]
                if monitor_mode == "monitor-status":
                    status = monitor.status()
                    if status == "completed":
                        return "pass", not synthetic, [f"The exact monitor completed and verified every {batch_label} batch and the run."]
                    return "pass", not synthetic, ["The exact monitor is running with fresh identity and sentinel evidence; "
                                                  "batch verification is not yet complete."]
                if monitor_mode == "wait-monitor":
                    monitor.wait()
                    return "pass", not synthetic, [f"The exact monitor completed and verified every {batch_label} batch and the run."]
                sidecar_updated = monitor.stop()
                message = "The recorded monitor and verification jobs are terminal; its writer lock is released."
                if not sidecar_updated:
                    message += " The invalid status sidecar was preserved for reconciliation."
                return "pass", False, [message]
        if args.mode == "deploy":
            deployment.deploy()
            return "pass", True, ["Deployment and fresh readiness checks passed."]
        if args.mode == "validate-only":
            deployment.validate()
            return "pass", True, ["Fresh readiness checks passed."]
        deployment.cleanup()
        return "pass", False, ["The empty, recorded Demo 2 workspace was deleted and its absence confirmed."]


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", required=True, choices=("dry-run", "resolve-target-candidate",
                                                          "verify-target-ownership", "validate-only", "deploy", "cleanup",
                                                          "start-monitor", "monitor-status", "wait-monitor",
                                                          "stop-monitor", "provision-synthetic", "prepare-synthetic-monitor", "start-synthetic-monitor",
                                                          "synthetic-monitor-status", "wait-synthetic-monitor",
                                                          "prepare-measured-monitor", "check-dimension-gaps",
                                                          "expand-compatible-dimensions"))
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--prefix", required=True)
    parser.add_argument("--state-dir", required=True, type=Path)
    parser.add_argument("--result", required=True, type=Path)
    parser.add_argument("--yes", action="store_true")
    parser.add_argument("--selected-demos")
    args = parser.parse_args(argv)
    result = {"contract_version": 1, "track": "demo-2", "mode": args.mode,
              "status": "fail", "ready": False, "diagnostics": []}
    safe_result = args.result.is_absolute()
    try:
        protected = {(args.state_dir / name).resolve() for name in
                     (STATE_FILE, LOCK_FILE, MONITOR_FILE, MONITOR_START_FILE)}
        lock_path = (args.state_dir / LOCK_FILE).resolve()
        result_path = args.result.resolve()
        config_path = args.config.resolve()
        archive_name = re.fullmatch(r"demo-2-monitor(?:-start)?\.[A-Za-z0-9_-]+\.[0-9a-f-]{36}\.json",
                                    result_path.name)
        if safe_result and (result_path == config_path or result_path in protected or lock_path in result_path.parents
                            or (result_path.parent == args.state_dir.resolve() and archive_name)):
            safe_result = False
            raise Failure("result must not overwrite the configuration or ownership state")
        if not all(path.is_absolute() for path in (args.config, args.state_dir, args.result)):
            raise Failure("config, state-dir and result must be absolute paths")
        if config_path in protected or lock_path in config_path.parents:
            raise Failure("config must not be an ownership or monitor state file")
        try:
            with args.config.open(encoding="utf-8") as stream:
                config = json.load(stream)
        except (OSError, ValueError) as exc:
            raise Failure("Cannot read a valid JSON configuration") from exc
        settings = validate_config(config, args.prefix)
        args.selected_demos = _selected_demos(args.selected_demos)
        status, ready, diagnostics = _run(args, settings)
        result.update(status=status, ready=ready, diagnostics=diagnostics)
    except Blocked as exc:
        result["status"] = "blocked"
        result["diagnostics"] = [str(exc)]
    except (Failure, ConfigurationError) as exc:
        result["diagnostics"] = [str(exc)]
    except Exception as exc:
        result["diagnostics"] = [f"Unexpected local or service failure ({type(exc).__name__}); details suppressed."]
    if safe_result:
        try:
            _atomic_json(args.result, result)
        except OSError:
            result.update(status="fail", ready=False, diagnostics=["Could not persist the result JSON."])
    print(json.dumps(result, separators=(",", ":")))
    return 0 if result["status"] == "pass" else 1


if __name__ == "__main__":
    sys.exit(main())
