"""Read-only MLV schedule admission and per-run native producer verification.

The control plane owns the shared bounded schedule. These hooks never submit,
update, disable or cancel a Fabric schedule or refresh job.
"""

from datetime import datetime, timedelta, timezone
import hashlib
import json
import re
import uuid


TERMINAL = {"Completed", "Failed", "Cancelled", "Deduped"}
STOP_SCOPE = "run-specific; shared bounded setup schedule remains enabled"
MINIMUM_COVERAGE = timedelta(minutes=19)
MAX_WINDOW = timedelta(hours=24)


def _uuid(value):
    try:
        parsed = uuid.UUID(value)
    except (ValueError, TypeError, AttributeError):
        raise ValueError("Demo 1 requires canonical nonzero UUIDs") from None
    if not parsed.int or str(parsed) != value:
        raise ValueError("Demo 1 requires canonical nonzero UUIDs")
    return value


def _time(value, *, service=False):
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if service and parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0):
            raise ValueError
        return parsed
    except (AttributeError, TypeError, ValueError):
        raise ValueError("Demo 1 requires valid UTC timestamps") from None


def _stamp(value):
    return value.isoformat().replace("+00:00", "Z")


def _bytes(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")


def _hash(value):
    return hashlib.sha256(_bytes(value)).hexdigest()


def _identifier(value):
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,255}", value):
        raise ValueError("Demo 1 requires resolved safe Fabric identifiers")
    return f"`{value}`"


def _check_live(live, receipt, *, enabled=True):
    expected = receipt["request"]["configuration"]
    actual = live.get("configuration") if isinstance(live, dict) else None
    if (not isinstance(actual, dict) or set(actual) != set(expected)
            or live.get("id") != receipt["schedule_id"] or live.get("enabled") is not enabled
            or not isinstance(live.get("owner"), dict)
            or live["owner"].get("id") != receipt["principal_id"]
            or live["owner"].get("type") != receipt["create_response"]["owner"]["type"]
            or live.get("executionData") not in (None, {})
            or any(actual.get(key) != expected[key] for key in ("type", "interval", "localTimeZoneId"))
            or type(actual.get("interval")) is not int
            or any(_time(actual.get(key), service=True) != _time(expected[key], service=True)
                   for key in ("startDateTime", "endDateTime"))):
        raise RuntimeError("Demo 1 schedule ID, owner, enabled state, cadence, scope or bounds changed")


def _context(context):
    document = context["document"]
    demo, data, shared = (document[name] for name in ("demo-1", "data", "shared"))
    run = data.get("run_id")
    if not isinstance(run, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}", run):
        raise ValueError("Demo 1 requires a safe fresh run ID")
    for key, expected in (("file_count", 10), ("rows_per_file", 1000), ("interval_seconds", 60),
                          ("date_count", 365), ("product_count", 20), ("customer_count", 100)):
        if type(data.get(key)) is not int or data[key] != expected:
            raise ValueError("Demo 1 requires 10x1000/60s and the existing 365/20/100 dimensions")
    if data.get("currency") != "USD" or data.get("start_date") != "2026-01-01":
        raise ValueError("Demo 1 requires USD and the existing 2026 dimensions")
    workspace, lakehouse = _uuid(demo.get("workspace_id")), _uuid(demo.get("lakehouse_id"))
    principal, tenant = _uuid(shared.get("identity_id")), _uuid(document.get("tenant_id"))
    if shared.get("target_bindings", {}).get("demo-1") != {
            "workspace_id": workspace, "item_id": lakehouse, "fact": "silver.sales_joined"}:
        raise ValueError("Demo 1 target binding differs from the selected MLV")
    landing = f"abfss://{workspace}@onelake.dfs.fabric.microsoft.com/{lakehouse}/Files/incoming/sales/"
    if demo.get("landing_uri") != landing or tuple(demo.get(key) for key in (
            "bronze_schema", "bronze_view", "silver_schema", "silver_view")) != (
            "bronze", "sales_files", "silver", "sales_joined"):
        raise ValueError("Demo 1 landing or lineage names differ from the deployed target")
    receipt = demo.get("native_schedule")
    if (not isinstance(receipt, dict) or type(receipt.get("contract_version")) is not int
            or receipt["contract_version"] != 1 or receipt.get("track") != "demo-1"
            or receipt.get("status") != "confirmed"
            or any(receipt.get(key) != value for key, value in (
                ("tenant_id", tenant), ("principal_id", principal), ("workspace_id", workspace),
                ("lakehouse_id", lakehouse)))
            or not isinstance(receipt.get("setup_run_id"), str)
            or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}", receipt["setup_run_id"])):
        raise ValueError("Demo 1 requires the matching confirmed control-plane schedule receipt")
    _uuid(receipt.get("schedule_id"))
    request = receipt.get("request")
    settings = request.get("configuration") if isinstance(request, dict) else None
    if (not isinstance(settings, dict) or set(request) != {"enabled", "configuration"}
            or request["enabled"] is not True
            or set(settings) != {"startDateTime", "endDateTime", "localTimeZoneId", "type", "interval"}
            or settings.get("localTimeZoneId") != "UTC" or settings.get("type") != "Cron"
            or type(settings.get("interval")) is not int or settings["interval"] != 5):
        raise ValueError("Demo 1 setup must be a bounded five-minute full-lineage UTC schedule")
    start, end = (_time(settings[key], service=True) for key in ("startDateTime", "endDateTime"))
    owner = receipt.get("create_response", {}).get("owner")
    if (not MINIMUM_COVERAGE <= end - start <= MAX_WINDOW
            or not isinstance(owner, dict) or owner.get("type") not in ("User", "ServicePrincipal", "ManagedIdentity")):
        raise ValueError("Demo 1 setup receipt has invalid operating bounds or owner type")
    _check_live(receipt["create_response"], receipt)
    base = ".".join((_identifier(demo.get("workspace_name")), _identifier(demo.get("lakehouse_name"))))
    binding = {"run_id": run, "tenant_id": tenant, "principal_id": principal, "workspace_id": workspace,
               "lakehouse_id": lakehouse, "landing_uri": landing, "base": base,
               "data_sha256": _hash(data), "receipt_sha256": _hash(receipt)}
    deadlines = {key: _time(context[key]) for key in (
        "preparation_deadline_utc", "verification_deadline_utc", "cleanup_deadline_utc", "expires_at_utc")}
    if not (deadlines["preparation_deadline_utc"] < deadlines["verification_deadline_utc"]
            < deadlines["cleanup_deadline_utc"] <= deadlines["expires_at_utc"]):
        raise ValueError("Demo 1 preparation, verification, cleanup and expiry bounds are inconsistent")
    binding["deadlines"] = {key: _stamp(value) for key, value in deadlines.items()}
    now = context["clock"]()
    if not isinstance(now, datetime) or now.tzinfo is None or now.utcoffset() != timedelta(0):
        raise ValueError("Demo 1 clock must return an aware UTC datetime")
    return binding, base, receipt, start, end


def _root(binding):
    return (f"/v1/workspaces/{binding['workspace_id']}/lakehouses/{binding['lakehouse_id']}"
            "/jobs/refreshMaterializedLakeViews")


def _path(binding):
    return f"Files/producer/runs/{binding['run_id']}/demo1/schedule.json"


def _write(context, path, value, etag=None):
    store, raw = context["store"], _bytes(value)
    written = store.create(path, raw) if etag is None else store.replace(path, raw, etag)
    actual, actual_etag = store.read(path)
    if actual != raw or written != actual_etag:
        raise RuntimeError("Demo 1 run evidence readback mismatch; retain the native writer lock")


def _record(context, binding, receipt, prepared):
    raw, etag = context["store"].read(_path(binding))
    record = json.loads(raw.decode("utf-8"))
    if (not isinstance(record, dict) or type(record.get("contract_version")) is not int
            or record["contract_version"] != 1 or record.get("binding") != binding
            or record.get("schedule_id") != receipt["schedule_id"]
            or record.get("status") not in ("prepared", "verified", "stopped")):
        raise RuntimeError("Demo 1 run evidence does not match the pinned setup receipt")
    if prepared is not None and (not isinstance(prepared, dict) or any(
            prepared.get(key) != value for key, value in (
                ("run_id", binding["run_id"]), ("schedule_id", receipt["schedule_id"]),
                ("receipt_sha256", binding["receipt_sha256"])))):
        raise RuntimeError("Demo 1 prepared handle differs from retained run evidence")
    return record, etag


def _jobs(context, binding):
    rows = context["fabric"].list_all(_root(binding) + "/instances")
    if not rows or not isinstance(rows, list) or any(
            not isinstance(row, dict) or row.get("itemId") != binding["lakehouse_id"]
            or not isinstance(row.get("status"), str) for row in rows):
        raise RuntimeError("Demo 1 lineage job history is missing or invalid")
    return rows


def _idle(rows):
    if any(row["status"] not in TERMINAL for row in rows):
        return False
    if max(rows, key=lambda row: _time(row.get("startTimeUtc"), service=True))["status"] != "Completed":
        raise RuntimeError("Demo 1 latest native lineage refresh did not complete successfully")
    return True


def _schedule(context, binding, receipt):
    fabric, root = context["fabric"], _root(binding)
    schedules = fabric.list_all(root + "/schedules")
    if not isinstance(schedules, list):
        raise RuntimeError("Demo 1 schedule listing is invalid")
    own = [row for row in schedules if isinstance(row, dict) and row.get("id") == receipt["schedule_id"]]
    if (len(own) != 1 or any(not isinstance(row, dict) or
            (row.get("id") != receipt["schedule_id"] and row.get("enabled") is not False) for row in schedules)):
        raise RuntimeError("Demo 1 setup schedule is missing or a competing schedule is enabled")
    _check_live(own[0], receipt)
    _, live = fabric.get(root + "/schedules/" + receipt["schedule_id"])
    _check_live(live, receipt)


def _admit(context, binding, base, receipt, start, end):
    while True:
        _coverage(context, start, end)
        _schedule(context, binding, receipt)
        if _idle(_jobs(context, binding)):
            _empty_manifest(context, binding)
            _no_existing_batches(context, binding, base)
            _coverage(context, start, end)
            _schedule(context, binding, receipt)
            if _idle(_jobs(context, binding)):
                _coverage(context, start, end)
                return
        now = context["clock"]()
        remaining = min(
            _time(context["preparation_deadline_utc"]) - now,
            _time(context["verification_deadline_utc"]) - now - MINIMUM_COVERAGE,
            end - now - MINIMUM_COVERAGE,
        ).total_seconds()
        if remaining <= 0:
            raise RuntimeError("Demo 1 lineage did not become idle within the fixed preparation and coverage bounds")
        context["sleep"](min(10, remaining))


def _coverage(context, start, end):
    now = context["clock"]()
    deadline, expiry = (_time(context[key]) for key in ("verification_deadline_utc", "expires_at_utc"))
    if (context["stop_requested"]() or now >= _time(context["preparation_deadline_utc"])
            or not start <= now < end or end - now < MINIMUM_COVERAGE
            or deadline - now < MINIMUM_COVERAGE or not deadline <= end or not deadline < expiry):
        raise RuntimeError(
            "Demo 1 setup coverage insufficient: require an active window, at least 19 minutes from source start, "
            f"and verification deadline {_stamp(deadline)} within schedule end {_stamp(end)}; no bounds changed"
        )


def _rows(context, query):
    return [row.asDict(recursive=True) for row in context["spark"].sql(query).collect()]


def _ids(binding):
    return ", ".join(f"'{binding['run_id']}_{number:04d}'" for number in range(1, 11))


def _no_existing_batches(context, binding, base):
    for table in ("`bronze`.`sales_files`", "`silver`.`sales_joined`"):
        rows = _rows(context, f"SELECT COUNT(*) AS n FROM {base}.{table} WHERE BatchID IN ({_ids(binding)})")
        if len(rows) != 1 or rows[0].get("n") != 0:
            raise RuntimeError("Demo 1 fresh run BatchIDs already exist or cannot be proven absent")


def _dimensions(context, base):
    for table, key, count, low, high, invalid in (
        ("DimDate", "`Date`", 365, "2026-01-01", "2026-12-31", "MonthName IS NULL"),
        ("DimProduct", "ProductID", 20, "1", "20", "ProductName IS NULL OR ProductCategory IS NULL"),
        ("DimCustomer", "CustomerID", 100, "1", "100", "CustomerName IS NULL OR CustomerRegion IS NULL"),
    ):
        rows = _rows(context, f"SELECT COUNT(*) AS n, COUNT(DISTINCT {key}) AS distinct_n, MIN({key}) AS lo, "
                     f"MAX({key}) AS hi, SUM(CASE WHEN {invalid} THEN 1 ELSE 0 END) AS bad "
                     f"FROM {base}.`silver`.`{table}`")
        if (len(rows) != 1 or (rows[0]["n"], rows[0]["distinct_n"], str(rows[0]["lo"]), str(rows[0]["hi"]),
                              rows[0]["bad"]) != (count, count, low, high, 0)):
            raise RuntimeError(f"Demo 1 {table} dimensions are not ready")
    for schema, view, columns in (
        ("bronze", "sales_files", "SalesLineID, BatchID, SaleTimestamp, CustomerID, ProductID, `Date`, "
         "Quantity, UnitPriceMinorUnits, SalesAmountMinorUnits, _corrupt_record, __filepath__"),
        ("silver", "sales_joined", "SalesLineID, BatchID, SaleTimestamp, CustomerID, ProductID, `Date`, "
         "Quantity, UnitPriceMinorUnits, SalesAmountMinorUnits, UnitPrice, SalesAmount, corrupt_record, source_file, "
         "payload_variants, max_file_rows, missing_source, CustomerName, CustomerRegion, "
         "ProductName, ProductCategory, MonthName"),
    ):
        _rows(context, f"SELECT {columns} FROM {base}.`{schema}`.`{view}` LIMIT 0")


def _empty_manifest(context, binding):
    manifest = context["read_manifest"]()
    if (not isinstance(manifest, dict) or type(manifest.get("contract_version")) is not int
            or manifest["contract_version"] != 1 or manifest.get("run_id") != binding["run_id"]
            or manifest.get("batches") != {}):
        raise RuntimeError("Demo 1 requires the fresh empty run manifest before source start")


def prepare(context):
    binding, base, receipt, start, end = _context(context)
    _coverage(context, start, end)
    _empty_manifest(context, binding)
    _dimensions(context, base)
    _no_existing_batches(context, binding, base)
    _admit(context, binding, base, receipt, start, end)
    _write(context, _path(binding), {"contract_version": 1, "binding": binding,
           "schedule_id": receipt["schedule_id"], "status": "prepared", "prepared_at_utc": _stamp(context["clock"]())})
    return {"status": "ready", "run_id": binding["run_id"], "schedule_id": receipt["schedule_id"],
            "receipt_sha256": binding["receipt_sha256"], "schedule_end_utc": _stamp(end)}


def check_ready(context, prepared):
    binding, base, receipt, start, end = _context(context)
    record, _ = _record(context, binding, receipt, prepared)
    if record["status"] != "prepared":
        raise RuntimeError("Demo 1 run was already verified or stopped")
    _empty_manifest(context, binding)
    _no_existing_batches(context, binding, base)
    _admit(context, binding, base, receipt, start, end)
    return {"status": "ready", "run_id": binding["run_id"], "schedule_id": receipt["schedule_id"],
            "verification_deadline_utc": context["verification_deadline_utc"], "schedule_end_utc": _stamp(end)}


def _manifest(context, binding):
    manifest, data = context["read_manifest"](), context["document"]["data"]
    expected = {key: data[key] for key in ("run_id", "start_date", "date_count", "file_count",
                                         "rows_per_file", "interval_seconds")}
    expected.update(products=data["product_count"], customers=data["customer_count"],
                    seed=data.get("seed", 42), batch_class="measured")
    if (not isinstance(manifest, dict) or type(manifest.get("contract_version")) is not int
            or manifest["contract_version"] != 1 or manifest.get("run_id") != binding["run_id"]
            or manifest.get("config") != expected or not isinstance(manifest.get("destinations"), list)
            or "demo-1" not in manifest["destinations"] or not isinstance(manifest.get("batches"), dict)
            or set(manifest["batches"]) != {str(number) for number in range(1, 11)}):
        raise RuntimeError("Demo 1 authoritative manifest differs from the exact fresh ten-batch run")
    batches = {}
    for number in range(1, 11):
        batch = manifest["batches"][str(number)]
        if (not isinstance(batch, dict) or batch.get("batch_id") != f"{binding['run_id']}_{number:04d}"
                or type(batch.get("batch_number")) is not int or batch["batch_number"] != number
                or batch.get("batch_class") != "measured" or type(batch.get("row_count")) is not int
                or batch["row_count"] != 1000 or type(batch.get("sales_amount_minor_units")) is not int
                or not isinstance(batch.get("sha256"), str) or not re.fullmatch(r"[a-f0-9]{64}", batch["sha256"])
                or not isinstance(batch.get("deliveries"), dict)):
            raise RuntimeError(f"Demo 1 manifest batch {number} is invalid")
        delivery = batch["deliveries"].get("demo-1")
        if (not isinstance(delivery, dict) or delivery.get("status") != "verified"
                or delivery.get("submission_status") != "submitted"):
            raise RuntimeError(f"Demo 1 manifest batch {number} delivery is not verified")
        generated = _time(batch.get("generated_at_utc"))
        submitted = _time(delivery.get("submission_result_at_utc"))
        verified = _time(delivery.get("verified_at_utc"))
        if not generated <= submitted <= verified <= context["clock"]():
            raise RuntimeError(f"Demo 1 manifest batch {number} has inconsistent delivery timestamps")
        for field in ("amount_by_date", "amount_by_product_category", "amount_by_customer_region"):
            totals = batch.get(field)
            if (not isinstance(totals, dict) or not totals or any(
                    not isinstance(key, str) or type(value) is not int for key, value in totals.items())
                    or sum(totals.values()) != batch["sales_amount_minor_units"]):
                raise RuntimeError(f"Demo 1 manifest batch {number} grouped totals are invalid")
        batches[batch["batch_id"]] = batch
    return batches


def _reconcile(context, binding, base, batches):
    ready = set(batches)
    for schema, view, quality in (
        ("bronze", "sales_files", "_corrupt_record IS NOT NULL OR __filepath__ IS NULL OR __filepath__ = ''"),
        ("silver", "sales_joined", "payload_variants IS NULL OR payload_variants <> 1 OR max_file_rows IS NULL "
         "OR max_file_rows <> 1 OR missing_source IS NULL OR missing_source <> 0 OR CustomerRegion IS NULL "
         "OR ProductCategory IS NULL OR `Date` IS NULL OR corrupt_record IS NOT NULL OR source_file IS NULL "
         "OR SaleTimestamp IS NULL OR CustomerID IS NULL OR ProductID IS NULL OR Quantity IS NULL OR Quantity <= 0 "
         "OR UnitPriceMinorUnits IS NULL OR UnitPriceMinorUnits < 0 OR SalesAmountMinorUnits IS NULL "
         "OR SalesAmountMinorUnits < 0 OR UnitPrice IS NULL OR SalesAmount IS NULL OR CustomerName IS NULL "
         "OR ProductName IS NULL OR MonthName IS NULL OR CAST(Quantity AS DECIMAL(38,0)) * "
         "CAST(UnitPriceMinorUnits AS DECIMAL(38,0)) <> CAST(SalesAmountMinorUnits AS DECIMAL(38,0))"),
    ):
        rows = _rows(context, "SELECT BatchID, COUNT(*) AS n, "
                     "COUNT(DISTINCT TRY_CAST(SalesLineID AS BIGINT)) AS distinct_n, "
                     "MIN(TRY_CAST(SalesLineID AS BIGINT)) AS lo, MAX(TRY_CAST(SalesLineID AS BIGINT)) AS hi, "
                     "SUM(TRY_CAST(SalesAmountMinorUnits AS BIGINT)) AS amount, "
                     f"SUM(CASE WHEN {quality} THEN 1 ELSE 0 END) AS bad FROM {base}.`{schema}`.`{view}` "
                     f"WHERE BatchID IN ({_ids(binding)}) GROUP BY BatchID")
        seen = {row["BatchID"]: row for row in rows}
        if len(seen) != len(rows) or set(seen) - set(batches):
            raise RuntimeError("Demo 1 batch aggregate query returned unexpected keys")
        ready &= {key for key, row in seen.items() if (
            row["n"], row["distinct_n"], row["lo"], row["hi"], row["amount"], row["bad"]
        ) == (1000, 1000, 1, 1000, batches[key]["sales_amount_minor_units"], 0)}
    for column, field in (("CAST(`Date` AS STRING)", "amount_by_date"),
                          ("ProductCategory", "amount_by_product_category"),
                          ("CustomerRegion", "amount_by_customer_region")):
        rows = _rows(context, f"SELECT BatchID, {column} AS grouping, SUM(SalesAmountMinorUnits) AS amount "
                     f"FROM {base}.`silver`.`sales_joined` WHERE BatchID IN ({_ids(binding)}) GROUP BY BatchID, {column}")
        grouped = {key: {} for key in batches}
        for row in rows:
            if (row["BatchID"] not in grouped or row["grouping"] is None
                    or str(row["grouping"]) in grouped[row["BatchID"]]):
                raise RuntimeError("Demo 1 grouped query returned invalid or duplicate keys")
            grouped[row["BatchID"]][str(row["grouping"])] = row["amount"]
        ready &= {key for key in batches if grouped[key] == batches[key][field]}
    return ready


def verify(context, prepared):
    binding, base, receipt, _, end = _context(context)
    record, etag = _record(context, binding, receipt, prepared)
    if record["status"] != "prepared":
        raise RuntimeError("Demo 1 run is not awaiting verification")
    batches = _manifest(context, binding)
    deadline, observed = _time(context["verification_deadline_utc"]), {}
    if deadline > end:
        raise RuntimeError("Demo 1 schedule no longer covers the fixed verification deadline")
    while context["clock"]() < deadline:
        if context["stop_requested"]():
            raise RuntimeError("Demo 1 stop requested during reconciliation")
        _schedule(context, binding, receipt)
        ready = _reconcile(context, binding, base, batches)
        now = context["clock"]()
        for key in ready:
            observed.setdefault(key, _stamp(now))
        jobs = _jobs(context, binding)
        if ready == set(batches) and _idle(jobs):
            last_delivery = max(_time(batch["deliveries"]["demo-1"]["submission_result_at_utc"])
                                for batch in batches.values())
            completed = [row for row in jobs if row.get("status") == "Completed"
                         and row.get("invokeType") == "Scheduled"
                         and _time(row.get("endTimeUtc"), service=True) >= last_delivery]
            if completed and now < deadline:
                _schedule(context, binding, receipt)
                if context["clock"]() >= deadline:
                    raise RuntimeError("Demo 1 verification readback exceeded the fixed deadline")
                result = {"status": "verified", "run_id": binding["run_id"], "schedule_id": receipt["schedule_id"],
                          "row_count": 10000, "batch_count": 10,
                          "sales_amount_minor_units": sum(batch["sales_amount_minor_units"] for batch in batches.values()),
                          "observations": [{"batch_id": key,
                              "delivery_ack_utc": batch["deliveries"]["demo-1"]["submission_result_at_utc"],
                              "first_observed_reconciled_utc": observed[key],
                              "delivery_to_observation_seconds": (
                                  _time(observed[key]) - _time(batch["deliveries"]["demo-1"]["submission_result_at_utc"])
                              ).total_seconds()} for key, batch in batches.items()],
                          "scheduled_jobs": [{key: row.get(key) for key in (
                              "id", "status", "invokeType", "startTimeUtc", "endTimeUtc")} for row in completed],
                          "diagnostic": "Post-delivery observations are latency upper bounds, not first actual visibility. "
                          "60s is source cadence; MLV cadence is five minutes plus processing. "
                          "Job history is not schedule-ID attribution; baseline BatchIDs are excluded."}
                record.update(status="verified", result=result)
                _write(context, _path(binding), record, etag)
                return result
        context["sleep"](min(15, max(0, (deadline - context["clock"]()).total_seconds())))
    raise RuntimeError("Demo 1 final Bronze/Silver batches did not reconcile by the fixed verification deadline")


def stop(context, prepared):
    binding, _, receipt, _, _ = _context(context)
    try:
        record, etag = _record(context, binding, receipt, prepared)
    except FileNotFoundError:
        if prepared is not None:
            raise RuntimeError("Demo 1 prepared run has no retained evidence") from None
        return {"status": "stopped", "scope": STOP_SCOPE, "run_id": binding["run_id"],
                "diagnostic": "No run evidence exists; this hook never starts schedule or refresh work."}
    deadline = min(_time(context["cleanup_deadline_utc"]), _time(context["expires_at_utc"]))
    while context["clock"]() < deadline:
        if _idle(_jobs(context, binding)):
            _schedule(context, binding, receipt)
            if context["clock"]() >= deadline:
                raise RuntimeError("Demo 1 cleanup readback exceeded its deadline; retain native writer lock")
            result = {"status": "stopped", "scope": STOP_SCOPE, "run_id": binding["run_id"],
                      "schedule_id": receipt["schedule_id"],
                      "shared_schedule_enabled": True,
                      "diagnostic": "No run-specific refresh was submitted or cancelled. "
                      "Lineage is idle at readback; control-plane setup/stop owns the shared schedule."}
            record.update(status="stopped", stop_result=result)
            _write(context, _path(binding), record, etag)
            return result
        context["sleep"](min(10, max(0, (deadline - context["clock"]()).total_seconds())))
    raise RuntimeError("Demo 1 lineage remains active or unknown at cleanup deadline; retain native writer lock")
