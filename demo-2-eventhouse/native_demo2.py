"""Run-bound Demo 2 verification hooks for the native Fabric producer."""

import base64
import hashlib
import json
import re
import uuid
from datetime import timedelta
from decimal import Decimal, InvalidOperation
from urllib.request import Request, build_opener

import monitor_controller as monitor
from eventhouse_core import batch_verification_query, identifier
from lakehouse_validation import validate_batch_evidence


_ACTIVE = {"NotStarted", "InProgress"}
_BLOB = "https://onelake.blob.fabric.microsoft.com"
_KUSTO = r"https://[a-z0-9-]+(?:\.[a-z0-9-]+)*\.kusto\.fabric\.microsoft\.com"


def _require(condition, message):
    if not condition:
        raise monitor.MonitorError("Native Demo 2: " + message)


def _id(value):
    canonical = monitor._uuid(value, "recorded identity")
    _require(value == canonical and uuid.UUID(canonical).int != 0, "noncanonical or empty identity")
    return canonical


def _digest(value):
    return hashlib.sha256(monitor._canonical_json(value)).hexdigest()


def _definition_projection(definition):
    parts = [part for part in definition.get("definition", definition)["parts"]
             if part.get("path") == "notebook-content.ipynb" and part.get("payloadType") == "InlineBase64"]
    _require(len(parts) == 1, "missing or ambiguous notebook definition")
    notebook = json.loads(base64.b64decode(parts[0]["payload"], validate=True))
    metadata = notebook["metadata"]
    cells = []
    for cell in notebook["cells"]:
        source = cell["source"]
        if isinstance(source, list):
            source = "".join(source)
        _require(isinstance(source, str), "invalid notebook source")
        meta = cell.get("metadata", {})
        cells.append([cell["cell_type"], source, sorted(meta.get("tags", [])),
                      meta.get("trident"), meta.get("language"), meta.get("disabled")])
    return [metadata["trident"], metadata.get("kernelspec"), metadata.get("language_info"), cells]


def definition_sha256(definition):
    """Hash the executable ipynb projection, excluding transient Fabric metadata."""
    return _digest(_definition_projection(definition))


def _header(headers, name):
    return next((v for k, v in headers.items() if k.lower() == name), None)


def _read(store, path):
    try:
        payload, etag = store.read(path)
    except FileNotFoundError:
        return None, None
    value = json.loads(payload)
    _require(isinstance(value, dict) and isinstance(etag, str) and bool(etag), "unversioned run evidence")
    return value, etag


class _Run:
    def __init__(self, context, phase):
        self.ctx = context
        self.phase = phase
        self.clock = context["clock"]
        def deadline(key):
            value = context[key + "_utc"]
            return monitor._utc(value if isinstance(value, str) else monitor._stamp(value), key)
        self.deadline = min(deadline(phase + "_deadline"), deadline("expires_at"))
        self.store = context["store"]
        doc = context["document"]
        self.demo = demo = doc["demo-2"]
        shared, data = doc["shared"], doc["data"]
        run = data["run_id"]
        _require(isinstance(run, str) and monitor.RUN_ID.fullmatch(run) and
                 not run.lower().startswith("demo2synthetic_"), "invalid measured run ID")
        self.manifest = f"Files/producer/runs/{run}/manifest.json"
        _require(context["manifest_path"] == self.manifest, "unexpected producer manifest path")
        self.journal = f"Files/producer/runs/{run}/demo2-native-jobs.json"
        self.state = f"Files/demo2-verification/runs/{run}/state.json"
        self.lock = self.state.rsplit("/", 1)[0] + "/writer.lock"
        ws, lakehouse = _id(demo["workspace_id"]), _id(demo["lakehouse_id"])
        self.target_store = context["store_factory"](ws, lakehouse)
        self.ws_path = f"/v1/workspaces/{ws}"
        self.items = {"bootstrap": _id(demo["verifier_notebook_id"]),
                      "monitor": _id(demo["monitor_notebook_id"])}
        _require(self.items["bootstrap"] != self.items["monitor"], "monitor and verifier must be separate")
        targets = shared["target_bindings"]
        target = targets["demo-2"]
        self.fact = identifier(demo["sales_table"], "fact")
        _require(target == {"workspace_id": ws, "item_id": _id(target["item_id"]), "fact": self.fact},
                 "Demo 2 target does not match the runtime profile")
        producer_root = f"{_BLOB}/{_id(shared['workspace_id'])}/{_id(shared['staging_lakehouse_id'])}"
        producer_config = {
            "run_id": run, "batch_class": "measured", "start_date": data["start_date"], "seed": data["seed"],
            "date_count": data["date_count"], "products": data["product_count"], "customers": data["customer_count"],
            **{k: data[k] for k in ("file_count", "rows_per_file", "interval_seconds")},
        }
        from generator import generator_source_sha256
        digest = generator_source_sha256()
        binding = _digest({"targets": targets, "generator_sha256": digest, "config": producer_config})
        self.params = {
            "RUN_ID": run, "MANIFEST_URL": f"{producer_root}/{self.manifest}",
            "SENTINEL_URL": f"{producer_root}/Files/preflight/producer-monitor-heartbeat-v1.txt",
            "CONTROLLER_ID": str(uuid.uuid4()), "STATE_URL": f"{_BLOB}/{ws}/{lakehouse}/{self.state}",
            "DEADLINE_UTC": monitor._stamp(min(deadline("verification_deadline"), deadline("expires_at"))),
            "VERIFIER_WORKSPACE_ID": ws, "VERIFIER_NOTEBOOK_ID": self.items["bootstrap"],
            "EXPECTED_IDENTITY_ID": _id(shared["identity_id"]), "EXPECTED_TENANT_ID": _id(doc["tenant_id"]),
            "EXPECTED_GENERATOR_SHA256": digest, "EXPECTED_MONITOR_BINDING_SHA256": binding,
            "EXPECTED_PRODUCER_CONFIG_JSON": monitor._canonical_json(producer_config).decode(),
            "EXPECTED_TARGET_BINDINGS_JSON": monitor._canonical_json(targets).decode(),
            "EXPECTED_BATCH_COUNT": data["file_count"], "VERIFICATION_TIMEOUT_SECONDS": demo["verification_timeout_seconds"],
            "POLL_INTERVAL_SECONDS": demo["poll_interval_seconds"], "MAX_CONCURRENT_BATCHES": demo["max_concurrent_batches"],
        }
        self.poll = demo["poll_interval_seconds"]
        self.cfg = monitor.settings(self.params, lakehouse_id=lakehouse,
                                    reused_synthetic_target=target if self.fact == "SyntheticFactSales" else None)
        self.binding = {"run_id": run, "binding": binding, "items": self.items, "workspace_id": ws,
                        "lakehouse_id": lakehouse, "runtime_profile_sha256": _digest(demo),
                        "deadline_utc": self.params["DEADLINE_UTC"]}
        self.target = target
        self.max_duration = data["max_duration_seconds"]

    def remaining(self):
        if self.phase != "cleanup":
            _require(not self.ctx["stop_requested"](), "operator requested stop")
        remaining = (self.deadline - self.clock()).total_seconds()
        _require(remaining >= 1, f"{self.phase} deadline expired")
        return remaining

    def request(self, method, path, expected, payload=None):
        code, headers, body = self.ctx["fabric"].request(
            method, path, payload=payload, timeout=min(15, int(self.remaining())))
        _require(code in expected, f"unexpected Fabric {method} response ({code})")
        return headers, body

    def wait(self, headers=None):
        try:
            delay = max(1, int(_header(headers or {}, "retry-after") or self.poll))
        except (TypeError, ValueError):
            delay = self.poll
        _require(delay < self.remaining(), f"next poll would exceed the {self.phase} deadline")
        self.ctx["sleep"](delay)

    def read_definition(self, item):
        headers, body = self.request("POST", f"{self.ws_path}/items/{item}/getDefinition?format=ipynb",
                                     (200, 202), {})
        if isinstance(body, dict) and ("parts" in body or "definition" in body):
            return body
        operation = _id(_header(headers, "x-ms-operation-id"))
        while True:
            self.wait(headers)
            headers, op = self.request("GET", f"/v1/operations/{operation}", (200,))
            state = op["status"]
            if state == "Succeeded":
                return self.request("GET", f"/v1/operations/{operation}/result", (200,))[1]
            _require(state in {"NotStarted", "Running"}, "notebook definition readback failed")

    def shortcut(self):
        name = identifier(self.demo["shortcut_name"], "shortcut")
        lakehouse = _id(self.demo["lakehouse_id"])
        _, shortcut = self.request("GET", f"{self.ws_path}/items/{lakehouse}/shortcuts/Tables/{name}", (200,))
        expected = {"workspaceId": self.target["workspace_id"], "itemId": self.target["item_id"],
                    "path": self.demo["shortcut_target_path"]}
        _require(shortcut.get("name") == name and shortcut.get("path") == "Tables"
                 and shortcut.get("target", {}).get("oneLake") == expected, "shortcut target changed")

    def kql(self, batch):
        endpoint = self.demo["query_endpoint"]
        _require(re.fullmatch(_KUSTO, endpoint), "invalid bound Kusto query endpoint")
        body = monitor._canonical_json({"db": self.demo["kql_database_name"],
                                        "csl": batch_verification_query(self.fact, batch["batch_id"])})
        request = Request(endpoint + "/v1/rest/query", data=body, headers={
            "Authorization": "Bearer " + self.ctx["token_provider"]("kusto"),
            "Content-Type": "application/json",
        })
        with build_opener(monitor._NoRedirect()).open(request, timeout=min(15, int(self.remaining()))) as response:
            value = json.load(response, parse_float=Decimal)
        _require(isinstance(value, dict) and not value.get("Exceptions"), "Kusto query failed")
        names = {"RowCount", "DistinctSalesLineIDs", "AmountMinorUnits", "InvalidRows"}
        rows = []
        for table in value["Tables"]:
            columns = [col["ColumnName"] for col in table["Columns"]]
            if len(columns) == len(names) and set(columns) == names:
                rows.extend(dict(zip(columns, row, strict=True)) for row in table["Rows"])
        _require(len(rows) == 1, "Kusto batch did not return a unique result")
        try:
            row = {key: Decimal(val) for key, val in rows[0].items() if type(val) in (int, str, Decimal)}
            _require(set(row) == names and all(v.is_finite() and v == v.to_integral_value() for v in row.values()),
                     "Kusto batch did not return exact integer evidence")
        except InvalidOperation as exc:
            raise monitor.MonitorError("Native Demo 2: Kusto batch returned nonnumeric evidence") from exc
        _require(row["RowCount"] == batch["row_count"] == row["DistinctSalesLineIDs"]
                 and row["InvalidRows"] == 0 and row["AmountMinorUnits"] == batch["sales_amount_minor_units"],
                 "Kusto batch counts or totals differ")

    def load(self):
        self.record, self.etag = _read(self.store, self.journal)
        if self.record is not None:
            _require(self.record.get("contract_version") == 1 and self.record.get("binding") == self.binding,
                     "job journal does not belong to this run")
            self.params["CONTROLLER_ID"] = self.cfg["controller_id"] = _id(self.record["controller_id"])
        return self.record

    def save(self):
        self.remaining()
        payload = monitor._canonical_json(self.record)
        if self.etag is None:
            self.etag = self.store.create(self.journal, payload)
        else:
            self.etag = self.store.replace(self.journal, payload, self.etag)
        _require(isinstance(self.etag, str) and bool(self.etag), "conditional journal write not confirmed")

    def start(self, role, params):
        item = self.items[role]
        _require(role not in self.record["jobs"], "refusing a duplicate job submission")
        entry = self.record["jobs"][role] = {"started": monitor._stamp(self.clock()), "job_id": None}
        self.save()
        parameters = [{"name": k, "type": "Integer" if type(v) is int else "Text", "value": v}
                      for k, v in params.items()]
        headers, _ = self.request("POST", f"{self.ws_path}/notebooks/{item}/jobs/execute/instances?beta=false",
                                  (202,), {"parameters": parameters, "executionData": {"compute": "Spark"}})
        prefix = f"{monitor.FABRIC}{self.ws_path}/items/{item}/jobs/instances/"
        location = _header(headers, "location")
        _require(isinstance(location, str) and location.startswith(prefix), "job submission location is uncertain")
        entry["job_id"] = _id(location[len(prefix):])
        self.save()
        return entry["job_id"]

    def job(self, item, job_id):
        path = f"{self.ws_path}/notebooks/{item}/jobs/execute/instances/{_id(job_id)}?beta=true"
        headers, body = self.request("GET", path, (200,))
        _require(isinstance(body, dict) and body.get("id") == job_id and body.get("itemId") == item
                 and body.get("status") in _ACTIVE | monitor.TERMINAL_JOBS, "owned job readback does not match")
        return headers, body

    def terminal(self, item, job_id):
        while True:
            headers, body = self.job(item, job_id)
            if body["status"] in monitor.TERMINAL_JOBS:
                return body
            self.wait(headers)

    def state_read(self):
        state, etag = _read(self.target_store, self.state)
        if state is not None:
            expected = {k: self.cfg[k] for k in ("run_id", "controller_id", "tenant_id", "identity_id",
                                                 "generator_sha256", "monitor_binding_sha256")}
            expected.update(manifest_uri=self.cfg["manifest_url"], deadline_utc=self.params["DEADLINE_UTC"])
            _require(state.get("contract_version") == 1 and all(state.get(k) == v for k, v in expected.items())
                     and state.get("state") in {"running", "completed", "failed"}
                     and isinstance(state.get("batches"), dict) and state.get("sentinel_verified") is True
                     and state.get("job_access_verified") is True, "monitor state does not prove ownership")
        return state, etag

    def lock_read(self):
        lock, etag = _read(self.target_store, self.lock)
        if lock is not None:
            _require(lock == {"contract_version": 1, "run_id": self.cfg["run_id"],
                              "controller_id": self.cfg["controller_id"]}, "foreign monitor lock")
        return etag

    def child_jobs(self, state):
        result = set()
        if state is None:
            return result
        records = list(state["batches"].values())
        if state.get("run_verification") is not None:
            records.append(state["run_verification"])
        for record in records:
            status, job_id = record["status"], record.get("job_id")
            _require(status in {"awaiting_submission", "queued", "running", "spark_verified", "verified", "failed"}
                     and not record.get("submission_uncertain"), "monitor child submission is uncertain")
            if job_id is None:
                _require(status in {"awaiting_submission", "queued", "failed"}, "monitor child ID missing")
                continue
            job_id = _id(job_id)
            _require(job_id not in result, "duplicate monitor child ID")
            result.add(job_id)
        return result

    def cancel(self, item, job_id):
        _, job = self.job(item, job_id)
        if job["status"] in monitor.TERMINAL_JOBS:
            return
        intents = self.record["cancel_intents"]
        if job_id not in intents:
            intents[job_id] = item
            self.save()
            self.request("POST", f"{self.ws_path}/items/{item}/jobs/instances/{job_id}/cancel", (202,))
        self.terminal(item, job_id)


def prepare(context):
    run = _Run(context, "preparation")
    _require(context["producer_access_verified"] is True, "producer target and queue access not established")
    _require(run.load() is None and _read(run.target_store, run.state)[0] is None
             and _read(run.target_store, run.lock)[0] is None, "prior run evidence requires reconciliation")
    run.shortcut()
    for role, name in (("bootstrap", "verifier"), ("monitor", "monitor")):
        definition = run.read_definition(run.items[role])
        projection = _definition_projection(definition)
        _require(_digest(projection) == run.demo[f"{name}_definition_sha256"],
                 f"the approved {name} notebook definition changed")
        binding = projection[0]["lakehouse"]
        _require(binding.get("default_lakehouse") == run.demo["lakehouse_id"]
                 and binding.get("default_lakehouse_workspace_id") == run.target["workspace_id"],
                 f"the approved {name} notebook is bound to another Lakehouse")
        if role == "bootstrap":
            data = context["document"]["data"]
            dimension_data = {key: data[key] for key in
                              ("seed", "start_date", "date_count", "product_count", "customer_count")}
            dimension_data["run_id"] = None
            expected_data = f"DATA_JSON = {json.dumps(dimension_data, sort_keys=True)!r}"
            expected_generator = f"GENERATOR_SHA256 = {run.cfg['generator_sha256']!r}"
            lines = {line.strip() for cell in projection[3] for line in cell[1].splitlines()}
            _require(expected_data in lines and expected_generator in lines,
                     "verifier definition does not match the producer generator and dimensions")
    batch = run.demo["verification_batch"]
    validate_batch_evidence(batch)
    run.kql(batch)
    run.record = {"contract_version": 1, "binding": run.binding, "controller_id": run.cfg["controller_id"],
                  "jobs": {}, "cancel_intents": {}}
    run.save()
    bootstrap = run.start("bootstrap", {"MODE": "verify_batch", "BATCH_JSON": monitor._canonical_json(batch).decode(),
                                      "VERIFICATION_TIMEOUT_SECONDS": min(run.cfg["timeout"], int(run.remaining())),
                                      "POLL_INTERVAL_SECONDS": run.poll})
    _require(run.terminal(run.items["bootstrap"], bootstrap)["status"] == "Completed", "bootstrap verifier failed")
    expected = {k: batch[k] for k in ("batch_id", "row_count", "sales_amount_minor_units")}
    monitor._exit(run.job(run.items["bootstrap"], bootstrap)[1], "verify_batch", expected)
    job_id = run.start("monitor", run.params)
    while True:
        headers, job = run.job(run.items["monitor"], job_id)
        _require(job["status"] in _ACTIVE, "monitor stopped before producer admission")
        state, _ = run.state_read()
        if state is not None:
            _require(state["state"] == "running", "monitor did not establish running state")
            now = run.clock()
            heartbeat = monitor._utc(state["heartbeat_at_utc"], "heartbeat")
            started = monitor._utc(run.record["jobs"]["monitor"]["started"], "monitor submission")
            if (job["status"] == "InProgress" and started - timedelta(seconds=5) <= heartbeat
                    and now - timedelta(seconds=min(300, max(60, 3 * run.poll))) <= heartbeat <= now
                    and run.lock_read() and run.job(run.items["monitor"], job_id)[1]["status"] == "InProgress"):
                _require((run.cfg["deadline"] - run.clock()).total_seconds() > run.max_duration + run.cfg["timeout"],
                         "monitor has insufficient producer and verification time")
                return {"status": "ready", "run_id": run.cfg["run_id"], "monitor_job_id": job_id}
        run.wait(headers)


def verify(context, prepared):
    run = _Run(context, "verification")
    _require(run.load() is not None, "missing monitor submission record")
    job_id = run.record["jobs"]["monitor"]["job_id"]
    _require(prepared == {"status": "ready", "run_id": run.cfg["run_id"], "monitor_job_id": job_id},
             "producer admission evidence does not match the monitor")
    job = run.terminal(run.items["monitor"], job_id)
    _require(job["status"] == "Completed", "monitor did not complete")
    monitor._exit(job, "monitor", {"run_id": run.cfg["run_id"]})
    state, state_etag = run.state_read()
    _require(state is not None and state["state"] == "completed" and state.get("manifest_observed") is True,
             "monitor lacks completed manifest evidence")
    manifest, manifest_etag = _read(run.store, run.manifest)
    batches = monitor._measured(manifest, run.cfg)
    ids = sorted(batch["batch_id"] for batch in batches)
    _require(len(ids) == run.cfg["count"] and set(ids) == set(state["batches"])
             and all(b["row_count"] == run.cfg["producer_config"]["rows_per_file"] for b in batches),
             "manifest batch set or row counts differ")
    scheduler = manifest["scheduler"]
    result = scheduler["result"]
    start = monitor._utc(scheduler["started_at_utc"], "producer start")
    finish = monitor._utc(scheduler["finished_at_utc"], "producer finish")
    deadline = monitor._utc(result["deadline_utc"], "producer deadline")
    _require(scheduler["status"] == "completed" and scheduler["max_duration_seconds"] == run.max_duration
             and result["verified"] is True and result["errors"] == [] and start <= finish <= deadline
             and deadline == start + timedelta(seconds=run.max_duration), "producer completion is not bounded")
    _require(len(run.child_jobs(state)) == len(ids) + 1, "incomplete or ambiguous verifier job set")
    run.shortcut()
    for batch in batches:
        record = state["batches"][batch["batch_id"]]
        delivery = batch["deliveries"]["demo-2"]
        _require(record["status"] == "verified" and record["fingerprint"] == monitor.batch_fingerprint(batch)
                 and monitor._utc(record["deadline_utc"], "batch deadline")
                 <= monitor._utc(delivery["submission_result_at_utc"], "batch delivery") + timedelta(seconds=run.cfg["timeout"]),
                 "batch evidence changed or exceeded its verification bound")
        monitor.verify_delivery_evidence(batch, record)
        run.kql(batch)
        job = run.job(run.items["bootstrap"], record["job_id"])[1]
        _require(job["status"] == "Completed", "batch verifier did not complete")
        monitor._exit(job, "verify_batch", {k: batch[k] for k in ("batch_id", "row_count", "sales_amount_minor_units")})
    summary = {"batch_ids": ids, "row_count": sum(b["row_count"] for b in batches),
               "sales_amount_minor_units": sum(b["sales_amount_minor_units"] for b in batches)}
    final = state["run_verification"]
    job = run.job(run.items["bootstrap"], final["job_id"])[1]
    _require(final["status"] == "verified" and job["status"] == "Completed", "run-wide verifier did not complete")
    monitor._exit(job, "verify_run_keys", summary)
    _require(run.state_read() == (state, state_etag) and _read(run.store, run.manifest) == (manifest, manifest_etag)
             and run.lock_read() is None, "completion evidence changed or monitor lock remains")
    return {"status": "verified", "run_id": run.cfg["run_id"], **summary}


def stop(context, prepared):
    run = _Run(context, "cleanup")
    if run.load() is None:
        _require(_read(run.target_store, run.state)[0] is None and _read(run.target_store, run.lock)[0] is None,
                 "monitor artifacts exist without a job journal; retain the native run lock")
    else:
        jobs = run.record["jobs"]
        _require(set(jobs) <= set(run.items) and all(job.get("job_id") for job in jobs.values()),
                 "uncertain submission; retain the native run lock and reconcile")
        if "monitor" in jobs:
            run.cancel(run.items["monitor"], jobs["monitor"]["job_id"])
            state, state_etag = run.state_read()
            lock = run.lock_read()
            _require(state is not None or lock is None, "a monitor lock exists without a child journal")
            # The monitor may already have requested child cancellation before termination.
            for job_id in sorted(run.child_jobs(state)):
                run.terminal(run.items["bootstrap"], job_id)
            _require(run.state_read() == (state, state_etag) and run.lock_read() == lock,
                     "monitor evidence changed during cleanup")
            if lock is not None:
                run.remaining()
                run.target_store.delete(run.lock, lock)
                _require(run.lock_read() is None, "monitor lock release could not be confirmed")
        if "bootstrap" in jobs:
            run.cancel(run.items["bootstrap"], jobs["bootstrap"]["job_id"])
    return {"status": "stopped", "run_id": run.cfg["run_id"]}
