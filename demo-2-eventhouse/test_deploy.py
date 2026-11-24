import base64
import builtins
import contextlib
import copy
import io
import json
import os
import subprocess
import sys
import tempfile
import time
import types
import unittest
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest import mock
from urllib.parse import urlsplit

import deploy
from eventhouse_core import CSV_COLUMNS


ROOT = Path(__file__).resolve().parent.parent
TENANT = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
CAPACITY = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
WORKSPACE = "cccccccc-cccc-4ccc-8ccc-cccccccccccc"
EVENTHOUSE = "dddddddd-dddd-4ddd-8ddd-dddddddddddd"
DATABASE = "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee"
LAKEHOUSE = "ffffffff-ffff-4fff-8fff-ffffffffffff"
NOTEBOOK = "11111111-1111-4111-8111-111111111111"
OPERATION = "22222222-2222-4222-8222-222222222222"
MONITOR_NOTEBOOK = "33333333-3333-4333-8333-333333333333"
MONITOR_JOB = "44444444-4444-4444-8444-444444444444"
NEXT_MONITOR_JOB = "99999999-9999-4999-8999-999999999999"
CHILD_JOB = "55555555-5555-4555-8555-555555555555"
SHARED_WORKSPACE = "66666666-6666-4666-8666-666666666666"
STAGING_LAKEHOUSE = "77777777-7777-4777-8777-777777777777"
PRODUCER_IDENTITY = "88888888-8888-4888-8888-888888888888"
RESULT_KEYS = {"contract_version", "track", "mode", "status", "ready", "diagnostics"}
QUERY_URI = "https://cluster.region.kusto.fabric.microsoft.com"
INGEST_URI = "https://ingest-cluster.region.kusto.fabric.microsoft.com"


def configuration():
    return {
        "contract_version": 1,
        "tenant_id": TENANT,
        "capacity": {"id": CAPACITY, "name": "demo-capacity"},
        "authentication": {"method": "signed-in-identity"},
        "execution": {"verification_timeout_seconds": 30, "poll_interval_seconds": 1},
        "data": {"run_id": "sample", "seed": 42, "start_date": "2026-01-01",
                 "date_count": 2, "product_count": 2, "customer_count": 2, "currency": "USD"},
        "demo-2": {"workspace_name": "{prefix}-demo2-eventhouse", "target_latency_minutes": 5},
        "shared": {},
    }


def verification_batch():
    return {
        "batch_id": "bootstrap_0001", "batch_class": "bootstrap", "sha256": "a" * 64,
        "row_count": 1, "sales_amount_minor_units": 246,
        "amount_by_date": {"2026-01-01": 246},
        "amount_by_product_category": {"Beauty": 246},
        "amount_by_customer_region": {"Americas": 246},
    }


class FakeBuilder:
    def __init__(self):
        self.calls = []

    def build_notebook(self, generator_path, **kwargs):
        self.calls.append((generator_path, copy.deepcopy(kwargs)))
        return {
            "metadata": {"trident": {"lakehouse": {
                "default_lakehouse": kwargs["lakehouse_id"],
                "default_lakehouse_workspace_id": kwargs["workspace_id"],
                "default_lakehouse_name": kwargs["lakehouse_name"],
                "known_lakehouses": [{"id": kwargs["lakehouse_id"]}],
            }}},
            "cells": [
                {"cell_type": "code", "source": "MODE = 'preload'",
                 "metadata": {"tags": ["parameters"]}},
                {"cell_type": "code", "source": "perform_validations()", "metadata": {}},
            ],
        }

    def fabric_definition(self, notebook):
        encoded = base64.b64encode(json.dumps(notebook).encode()).decode()
        return {"format": "ipynb", "parts": [{
            "path": "notebook-content.ipynb", "payloadType": "InlineBase64", "payload": encoded,
        }]}


class FakeFabric:
    """In-memory protocol simulator: no network or credentials are used."""

    def __init__(self):
        self.workspace = None
        self.items = {}
        self.definition = None
        self.shortcut = None
        self.table_exists = False
        self.table_owner = None
        self.mapping_exists = False
        self.policy_exists = False
        self.jobs = {}
        self.calls = []
        self.flags = set()
        self.operation_results = {}

    def _item(self, item_id, name, kind, description):
        return {"id": item_id, "workspaceId": WORKSPACE, "displayName": name,
                "description": description, "type": kind}

    def pages(self, path):
        self.calls.append(("LIST", path, None))
        if path == "/v1/workspaces":
            return [] if self.workspace is None else [copy.deepcopy(self.workspace)]
        if path == "/v1/capacities":
            return [{"id": CAPACITY, "displayName": "demo-capacity"}]
        if path == f"/v1/workspaces/{WORKSPACE}/kqlDatabases":
            return [copy.deepcopy(item) for item in self.items.values() if item["type"] == "KQLDatabase"]
        if path == f"/v1/workspaces/{WORKSPACE}/items":
            return [copy.deepcopy(item) for item in self.items.values()]
        if path.startswith(f"/v1/workspaces/{WORKSPACE}/items?type="):
            kind = path.split("=", 1)[1]
            return [copy.deepcopy(item) for item in self.items.values() if item["type"] == kind]
        if path == f"/v1/workspaces/{WORKSPACE}/items/{NOTEBOOK}/jobs/instances":
            return list(copy.deepcopy(self.jobs).values())
        raise AssertionError(f"Unexpected list path: {path}")

    def fabric(self, method, path, body=None, allow_missing=False):
        self.calls.append((method, path, copy.deepcopy(body)))
        if method == "POST" and path == "/v1/workspaces":
            self.workspace = {"id": WORKSPACE, "displayName": body["displayName"],
                              "description": body["description"], "type": "Workspace"}
            return 201, {}, copy.deepcopy(self.workspace)
        if method == "GET" and path == f"/v1/workspaces/{WORKSPACE}":
            if self.workspace is None and allow_missing:
                return 404, {}, None
            return 200, {}, copy.deepcopy(self.workspace)
        if method == "DELETE" and path == f"/v1/workspaces/{WORKSPACE}":
            self.workspace = None
            return 204, {}, None
        if method == "POST" and path == f"/v1/workspaces/{WORKSPACE}/items":
            kind = body["type"]
            item_id = {"Eventhouse": EVENTHOUSE, "Lakehouse": LAKEHOUSE, "Notebook": NOTEBOOK}[kind]
            self.items[item_id] = self._item(item_id, body["displayName"], kind, body["description"])
            if kind == "Notebook":
                self.definition = copy.deepcopy(body["definition"])
            return 201, {}, copy.deepcopy(self.items[item_id])
        if method == "POST" and path == f"/v1/workspaces/{WORKSPACE}/kqlDatabases":
            item = self._item(DATABASE, body["displayName"], "KQLDatabase", body["description"])
            item["properties"] = {
                "parentEventhouseItemId": body["creationPayload"]["parentEventhouseItemId"],
                "databaseType": "ReadWrite", "queryServiceUri": QUERY_URI, "ingestionServiceUri": INGEST_URI,
            }
            self.items[DATABASE] = item
            return 201, {}, copy.deepcopy(item)
        if method == "GET" and path == f"/v1/workspaces/{WORKSPACE}/kqlDatabases/{DATABASE}":
            return 200, {}, copy.deepcopy(self.items[DATABASE])
        if method == "GET" and path.startswith(f"/v1/workspaces/{WORKSPACE}/items/"):
            item_id = path.rsplit("/", 1)[-1]
            if item_id in self.items:
                return 200, {}, copy.deepcopy(self.items[item_id])
        if method == "POST" and path == f"/v1/workspaces/{WORKSPACE}/items/{NOTEBOOK}/getDefinition?format=ipynb":
            definition = copy.deepcopy(self.definition)
            if "notebook_drift" in self.flags:
                altered = FakeBuilder().build_notebook(
                    ROOT, workspace_id=WORKSPACE, lakehouse_id=LAKEHOUSE, lakehouse_name="SalesLakehouse"
                )
                altered["cells"][1]["source"] = "pass"
                definition = FakeBuilder().fabric_definition(altered)
            if "notebook_environment_drift" in self.flags:
                altered = deploy._read_notebook_definition(definition)
                altered["metadata"]["trident"]["environment"] = {"environmentId": DATABASE}
                definition = FakeBuilder().fabric_definition(altered)
            return 200, {}, {"definition": definition}
        shortcut_path = f"/v1/workspaces/{WORKSPACE}/items/{LAKEHOUSE}/shortcuts"
        if method == "POST" and path == shortcut_path:
            self.shortcut = copy.deepcopy(body)
            return 201, {}, copy.deepcopy(body)
        if method == "GET" and path == shortcut_path + "/Tables/EventhouseFactSales":
            if self.shortcut is None and allow_missing:
                return 404, {}, None
            result = copy.deepcopy(self.shortcut)
            if "shortcut_drift" in self.flags:
                result["target"]["oneLake"]["itemId"] = EVENTHOUSE
            return 200, {}, result
        job_post = f"/v1/workspaces/{WORKSPACE}/notebooks/{NOTEBOOK}/jobs/execute/instances?beta=false"
        if method == "POST" and path == job_post:
            job_id = str(uuid.UUID(int=len(self.jobs) + 1))
            parameters = {item["name"]: item["value"] for item in body["parameters"]}
            mode = parameters["MODE"]
            result = {"status": "pass", "mode": mode}
            if mode == "verify_batch":
                batch = json.loads(parameters["BATCH_JSON"])
                result["batch"] = {key: batch[key] for key in
                                   ("batch_id", "row_count", "sales_amount_minor_units")}
            if "wrong_notebook_mode" in self.flags and mode == "verify_batch":
                result["mode"] = "preload"
            if "missing_notebook_exit" in self.flags:
                result = None
            job = {"id": job_id, "itemId": NOTEBOOK, "status": "Completed",
                   "properties": {"exitValue": json.dumps(result) if result is not None else None}}
            self.jobs[job_id] = job
            location = (f"{deploy.FABRIC}/v1/workspaces/{WORKSPACE}/items/{NOTEBOOK}"
                        f"/jobs/instances/{job_id}")
            return 202, {"location": location}, None
        jobs_path = f"/v1/workspaces/{WORKSPACE}/notebooks/{NOTEBOOK}/jobs/execute/instances/"
        if method == "GET" and path.startswith(jobs_path):
            job_id = path.rsplit("/", 1)[-1].split("?", 1)[0]
            return 200, {}, copy.deepcopy(self.jobs[job_id])
        if method == "GET" and path == f"/v1/operations/{OPERATION}":
            return 200, {}, {"status": "Succeeded"}
        if method == "GET" and path == f"/v1/operations/{OPERATION}/result":
            return 200, {}, copy.deepcopy(self.operation_results[OPERATION])
        raise AssertionError(f"Unexpected Fabric request: {method} {path}")

    def kusto(self, endpoint, database, command, query=False):
        self.calls.append(("KQL", command, query))
        if command == ".show tables":
            return [{"TableName": "FactSales"}] if self.table_exists else []
        if command.startswith(".create table ") and " ingestion csv mapping " not in command:
            self.table_exists = True
            self.table_owner = "other-owner" if "racing_table" in self.flags else self.workspace["description"]
            return []
        if command.startswith(".create table ") and " ingestion csv mapping " in command:
            self.mapping_exists = True
            if "racing_mapping" in self.flags:
                raise deploy.Failure("Kusto request failed (HTTP 400).")
            return []
        if command.startswith(".alter-merge table "):
            self.policy_exists = True
            return []
        if command == ".show table FactSales schema as json":
            columns = [{"Name": name, "CslType": kind} for name, kind in CSV_COLUMNS]
            if "schema_drift" in self.flags:
                columns[-1]["CslType"] = "real"
            return [{"TableName": "FactSales", "Schema": json.dumps({"OrderedColumns": columns}),
                     "DocString": self.table_owner}]
        if command == ".show table FactSales ingestion csv mappings":
            if not self.mapping_exists:
                return []
            entries = [{"Name": name, "DataType": kind, "Ordinal": index, "ConstValue": None}
                       for index, (name, kind) in enumerate(CSV_COLUMNS)]
            if "mapping_drift" in self.flags:
                entries[0]["Ordinal"] = 1
            return [{"Name": "SalesCsvV1", "Kind": "CSV", "Mapping": json.dumps(entries)}]
        if command == ".show table FactSales policy mirroring":
            policy = {
                "IsEnabled": self.policy_exists, "Format": "parquet",
                "TargetLatencyInMinutes": 3 if "policy_drift" in self.flags else 5,
                "ConnectionStrings": [
                    f"https://westus3-api.onelake.fabric.microsoft.com/{WORKSPACE}/"
                    f"{EVENTHOUSE if 'wrong_source' in self.flags else DATABASE}/Tables/FactSales;"
                    "credential-secret-must-not-leak"
                ],
            }
            return [{"Policy": json.dumps(policy)}]
        if query and command.startswith("FactSales"):
            amount = 245 if "batch_drift" in self.flags else 246
            return [{"RowCount": 1, "DistinctSalesLineIDs": 1,
                     "AmountMinorUnits": amount, "InvalidRows": 0}]
        raise AssertionError(f"Unexpected KQL command: {command}")


class FakeMonitorBuilder:
    def build_monitor_notebook(self, *, workspace_id, lakehouse_id, lakehouse_name):
        result = FakeBuilder().build_notebook(
            ROOT, workspace_id=workspace_id, lakehouse_id=lakehouse_id, lakehouse_name=lakehouse_name,
        )
        result["cells"][1]["source"] = "run_monitor()"
        return result


class FakeMonitorFabric(FakeFabric):
    def __init__(self):
        super().__init__()
        self.monitor_definition = None
        self.monitor_jobs = {}
        self.child_jobs = {}
        self.blobs = {}
        self.skills = []

    def fabric(self, method, path, body=None, allow_missing=False):
        self.skills.append(getattr(self, "fabric_skill", "eventhouse-cli"))
        item_path = f"/v1/workspaces/{WORKSPACE}/items"
        if (method == "POST" and path == item_path and isinstance(body, dict)
                and body.get("displayName") == "Demo2Monitor"):
            self.calls.append((method, path, copy.deepcopy(body)))
            self.items[MONITOR_NOTEBOOK] = self._item(
                MONITOR_NOTEBOOK, body["displayName"], "Notebook", body["description"]
            )
            self.monitor_definition = copy.deepcopy(body["definition"])
            if "monitor_lro" in self.flags:
                return 202, {"x-ms-operation-id": OPERATION}, None
            return 201, {}, copy.deepcopy(self.items[MONITOR_NOTEBOOK])
        definition_path = f"{item_path}/{MONITOR_NOTEBOOK}/getDefinition?format=ipynb"
        if method == "POST" and path == definition_path:
            self.calls.append((method, path, copy.deepcopy(body)))
            definition = copy.deepcopy(self.monitor_definition)
            if "monitor_drift" in self.flags:
                altered = deploy._read_notebook_definition(definition)
                altered["cells"][1]["source"] = "untrusted()"
                definition = FakeBuilder().fabric_definition(altered)
            return 200, {}, {"definition": definition}
        post_path = f"/v1/workspaces/{WORKSPACE}/notebooks/{MONITOR_NOTEBOOK}/jobs/execute/instances?beta=false"
        if method == "POST" and path == post_path:
            self.calls.append((method, path, copy.deepcopy(body)))
            if "uncertain_monitor_submit" in self.flags:
                raise deploy.Blocked("test uncertain monitor submit")
            params = {item["name"]: item["value"] for item in body["parameters"]}
            job_id = MONITOR_JOB if not self.monitor_jobs else NEXT_MONITOR_JOB
            self.monitor_jobs[job_id] = {"id": job_id, "itemId": MONITOR_NOTEBOOK, "status": "InProgress"}
            state = {
                "contract_version": 1, "run_id": params["RUN_ID"],
                "controller_id": params["CONTROLLER_ID"], "manifest_uri": params["MANIFEST_URL"],
                "deadline_utc": params["DEADLINE_UTC"], "identity_id": params["EXPECTED_IDENTITY_ID"],
                "tenant_id": params["EXPECTED_TENANT_ID"], "sentinel_verified": True,
                "job_access_verified": True, "state": "running", "batches": {},
                "run_verification": None, "heartbeat_at_utc": deploy._stamp(deploy._now()),
                "manifest_observed": False,
            }
            if "no_heartbeat" not in self.flags:
                self.blobs[params["STATE_URL"]] = (state, '"state-etag"')
            lock = {"contract_version": 1, "run_id": params["RUN_ID"],
                    "controller_id": params["CONTROLLER_ID"]}
            if "no_lock" not in self.flags:
                self.blobs[deploy._lock_uri(params["STATE_URL"])] = (lock, '"lock-etag"')
            location = (f"{deploy.FABRIC}/v1/workspaces/{WORKSPACE}/items/{MONITOR_NOTEBOOK}"
                        f"/jobs/instances/{job_id}")
            return 202, {"location": location}, None
        detail_prefix = (f"/v1/workspaces/{WORKSPACE}/notebooks/{MONITOR_NOTEBOOK}"
                         "/jobs/execute/instances/")
        if method == "GET" and path.startswith(detail_prefix) and path.endswith("?beta=true"):
            self.calls.append((method, path, None))
            job_id = path[len(detail_prefix):].split("?", 1)[0]
            return 200, {}, copy.deepcopy(self.monitor_jobs[job_id])
        verifier_prefix = (f"/v1/workspaces/{WORKSPACE}/notebooks/{NOTEBOOK}"
                           "/jobs/execute/instances/")
        if method == "GET" and path.startswith(verifier_prefix) and path.endswith("?beta=true"):
            job_id = path[len(verifier_prefix):].split("?", 1)[0]
            if job_id in self.child_jobs:
                self.calls.append((method, path, None))
                return 200, {}, copy.deepcopy(self.child_jobs[job_id])
        for notebook, jobs in ((MONITOR_NOTEBOOK, self.monitor_jobs), (NOTEBOOK, self.child_jobs)):
            prefix = f"{item_path}/{notebook}/jobs/instances/"
            if path.startswith(prefix):
                tail = path[len(prefix):]
                job_id = tail.removesuffix("/cancel")
                if job_id in jobs and method == "GET" and not tail.endswith("/cancel"):
                    self.calls.append((method, path, None))
                    return 200, {}, copy.deepcopy(jobs[job_id])
                if job_id in jobs and method == "POST" and tail.endswith("/cancel"):
                    self.calls.append((method, path, copy.deepcopy(body)))
                    if "uncertain_cancel" in self.flags:
                        raise deploy.Blocked("test uncertain cancel")
                    if "cancel_stays_active" not in self.flags:
                        jobs[job_id]["status"] = "Cancelled"
                    location = (f"{deploy.FABRIC}/v1/workspaces/{WORKSPACE}/items/{notebook}"
                                f"/jobs/instances/{job_id}")
                    return 202, {"location": location}, None
        return super().fabric(method, path, body, allow_missing)

    def onelake(self, method, url, allow_missing=False, extra_headers=None):
        self.calls.append((f"STORAGE-{method}", url, copy.deepcopy(extra_headers)))
        if method in ("HEAD", "GET") and url not in self.blobs and allow_missing:
            return 404, {}, None
        if method == "HEAD":
            return 200, {"etag": self.blobs[url][1]}, None
        if method == "GET":
            value, etag = self.blobs[url]
            return 200, {"etag": etag}, copy.deepcopy(value)
        if method == "DELETE":
            _, etag = self.blobs[url]
            if extra_headers != {"If-Match": etag}:
                raise deploy.Blocked("test conditional lock mismatch")
            del self.blobs[url]
            return 202, {}, None
        raise AssertionError(f"Unexpected storage request: {method} {url}")


class DeployTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix=".deploy-test-", dir=Path(__file__).resolve().parent)
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.config = configuration()
        self.config_path = self.root / "config.json"
        self.state = self.root / "state"
        self.result = self.root / "result.json"
        self.backend = FakeFabric()
        self.builder = FakeBuilder()

    def cli(self, mode, *, yes=False, prefix="chanel", backend=None, builder=True, selected=None):
        self.config_path.write_text(json.dumps(self.config), encoding="utf-8")
        args = ["--mode", mode, "--config", str(self.config_path), "--prefix", prefix,
                "--state-dir", str(self.state), "--result", str(self.result)]
        if yes:
            args.append("--yes")
        if selected is not None:
            args.extend(["--selected-demos", selected])
        backend = backend or self.backend
        output = io.StringIO()
        builder_patch = mock.patch.object(deploy, "_load_builder", return_value=(self.builder, ROOT / "generator.py"))
        with contextlib.redirect_stdout(output), mock.patch.object(deploy, "FabricClient", return_value=backend):
            if builder:
                with builder_patch:
                    code = deploy.main(args)
            else:
                code = deploy.main(args)
        result = json.loads(self.result.read_text(encoding="utf-8"))
        self.assertEqual(json.loads(output.getvalue()), result)
        self.assertEqual(set(result), RESULT_KEYS)
        self.assertEqual(result["contract_version"], 1)
        self.assertEqual(result["track"], "demo-2")
        self.assertEqual(result["mode"], mode)
        self.assertEqual(code == 0, result["status"] == "pass")
        self.assertIs(type(result["ready"]), bool)
        self.assertTrue(all(isinstance(message, str) for message in result["diagnostics"]))
        return code, result

    def deploy_success(self):
        self.config["demo-2"]["verification_batch"] = verification_batch()
        with mock.patch.object(deploy, "verify_producer_access", return_value=None):
            code, result = self.cli("deploy")
        self.assertEqual((code, result["status"], result["ready"]), (0, "pass", True))

    def monitor_setup(self):
        self.backend = FakeMonitorFabric()
        self.config["shared"].update({"workspace_id": SHARED_WORKSPACE,
                                      "staging_lakehouse_id": STAGING_LAKEHOUSE,
                                      "identity_id": PRODUCER_IDENTITY})
        self.config["data"].update({"file_count": 3, "max_duration_seconds": 120,
                                    "rows_per_file": 2, "interval_seconds": 30})
        self.config["execution"].update({"track_timeout_seconds": 10, "max_concurrency": 2})
        patcher = mock.patch.object(deploy, "_load_monitor_builder", return_value=FakeMonitorBuilder())
        patcher.start()
        self.addCleanup(patcher.stop)
        self.reservation_patcher = mock.patch.object(deploy, "verify_global_reservation", return_value=None)
        self.reservation_patcher.start()
        self.addCleanup(self.reservation_patcher.stop)
        self.readiness_patcher = mock.patch.object(deploy, "verify_monitor_readiness", return_value=None)
        self.readiness_patcher.start()
        self.addCleanup(self.readiness_patcher.stop)
        self.deploy_success()
        self.backend.calls.clear()
        self.backend.skills.clear()

    def start_monitor(self):
        code, result = self.cli("start-monitor", selected="demo-1,demo-2")
        self.assertEqual((code, result["status"], result["ready"]), (0, "pass", True), result)
        return json.loads((self.state / deploy.MONITOR_FILE).read_text(encoding="utf-8"))

    def complete_monitor(self, evidence):
        import monitor_controller

        state, _ = self.backend.blobs[evidence["state_uri"]]
        state.update(state="completed", manifest_observed=True)
        state["batches"] = {}
        self.backend.blobs[evidence["state_uri"]] = (state, '"completed-state-etag"')
        started = deploy._now()
        timestamp = deploy._stamp(started)
        manifest = {
            "contract_version": 1, "run_id": evidence["run_id"],
            "config": copy.deepcopy(evidence["producer_config"]),
            "destinations": ["demo-1", "demo-2"], "batches": {},
            "scheduler": {
                "status": "completed", "started_at_utc": timestamp, "finished_at_utc": timestamp,
                "max_duration_seconds": evidence["max_duration_seconds"], "max_attempts": 2,
                "result": {"verified": True, "errors": [], "deadline_utc": deploy._stamp(
                    started + timedelta(seconds=evidence["max_duration_seconds"])
                )},
            },
        }
        for number in range(1, evidence["expected_batch_count"] + 1):
            batch_id = f"{evidence['run_id']}_{number:04d}"
            operation_id = str(uuid.UUID(int=100 + number))
            job_id = str(uuid.UUID(int=200 + number))
            row_count = evidence["producer_config"]["rows_per_file"]
            total = 300 * number
            batch = {
                "batch_id": batch_id, "batch_number": number, "batch_class": "measured",
                "sha256": f"{number:064x}", "row_count": row_count, "sales_amount_minor_units": total,
                "amount_by_date": {"2026-01-01": total},
                "amount_by_product_category": {"Beauty": total},
                "amount_by_customer_region": {"Americas": total},
                "deliveries": {
                    "demo-1": {"status": "verified"},
                    "demo-2": {
                        "status": "verified", "submission_status": "submitted",
                        "submission_started_at_utc": timestamp, "submission_result_at_utc": timestamp,
                        "operation_id": operation_id, "verified_at_utc": timestamp,
                        "verified_row_count": row_count, "verified_total_minor_units": total,
                        "verified_distinct_sales_line_ids": row_count,
                        "ingestion_outcome": {
                            "status": "succeeded", "timestamp_utc": timestamp, "source_id": operation_id,
                            "service_operation_id": None, "error_code": None, "diagnostic": None,
                        },
                    },
                },
            }
            manifest["batches"][str(number)] = batch
            state["batches"][batch_id] = {
                "status": "verified", "fingerprint": monitor_controller.batch_fingerprint(batch),
                "deadline_utc": evidence["deadline_utc"],
                "operation_id": operation_id, "job_id": job_id,
            }
            exit_value = {"mode": "verify_batch", "status": "verified", "batch": {
                "batch_id": batch_id, "row_count": row_count, "sales_amount_minor_units": total,
            }}
            self.backend.child_jobs[job_id] = {
                "id": job_id, "itemId": NOTEBOOK, "status": "Completed",
                "properties": {"exitValue": json.dumps(exit_value)},
            }
        run_job = str(uuid.UUID(int=300))
        state["run_verification"] = {"status": "verified", "job_id": run_job}
        run_exit = {
            "mode": "verify_run_keys", "status": "verified",
            "batch_ids": sorted(state["batches"]),
            "row_count": sum(batch["row_count"] for batch in manifest["batches"].values()),
            "sales_amount_minor_units": sum(batch["sales_amount_minor_units"]
                                            for batch in manifest["batches"].values()),
        }
        self.backend.child_jobs[run_job] = {
            "id": run_job, "itemId": NOTEBOOK, "status": "Completed",
            "properties": {"exitValue": json.dumps(run_exit)},
        }
        self.backend.blobs[evidence["manifest_uri"]] = (manifest, '"completed-manifest-etag"')
        job = self.backend.monitor_jobs[evidence["job_id"]]
        job["status"] = "Completed"
        job["properties"] = {"exitValue": json.dumps({"mode": "monitor", "status": "verified",
                                                      "run_id": evidence["run_id"]})}
        self.backend.blobs.pop(deploy._lock_uri(evidence["state_uri"]), None)
        return state

    def test_dry_run_writes_only_result_and_never_uses_cloud_or_state(self):
        with mock.patch.object(deploy, "FabricClient", side_effect=AssertionError("network")):
            self.config_path.write_text(json.dumps(self.config), encoding="utf-8")
            output = io.StringIO()
            args = ["--mode", "dry-run", "--config", str(self.config_path), "--prefix", "chanel",
                    "--state-dir", str(self.state), "--result", str(self.result)]
            with contextlib.redirect_stdout(output):
                code = deploy.main(args)
        self.assertEqual(code, 0)
        result = json.loads(self.result.read_text())
        self.assertEqual(set(result), RESULT_KEYS)
        self.assertEqual((result["status"], result["ready"]), ("pass", False))
        self.assertFalse(self.state.exists())
        self.assertEqual({path.name for path in self.root.iterdir()}, {"config.json", "result.json"})

    def test_dry_run_entrypoint_works_as_a_real_subprocess(self):
        self.config_path.write_text(json.dumps(self.config), encoding="utf-8")
        command = [sys.executable, str(ROOT / "demo-2-eventhouse" / "deploy.py"), "--mode", "dry-run",
                   "--config", str(self.config_path), "--prefix", "chanel", "--state-dir", str(self.state),
                   "--result", str(self.result), "--selected-demos", "all"]
        completed = subprocess.run(command, cwd=ROOT, capture_output=True, text=True, timeout=30,
                                   env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"}, check=False)
        self.assertEqual(completed.returncode, 0, completed.stderr)
        result = json.loads(self.result.read_text(encoding="utf-8"))
        self.assertEqual(set(result), RESULT_KEYS)
        self.assertEqual((result["status"], result["ready"]), ("pass", False))
        self.assertEqual(json.loads(completed.stdout), result)
        self.assertFalse(self.state.exists())

    def test_atomic_state_write_retries_transient_locks_without_truncating_previous_content(self):
        self.result.write_text('{"previous":true}', encoding="utf-8")
        real_replace = os.replace
        failures = [0]

        def transient(source, destination):
            failures[0] += 1
            if failures[0] <= 2:
                raise PermissionError("test sharing violation")
            return real_replace(source, destination)

        with mock.patch.object(deploy.os, "replace", side_effect=transient), \
                mock.patch.object(deploy.time, "sleep"):
            deploy._atomic_json(self.result, {"new": True})
        self.assertEqual(failures[0], 3)
        self.assertEqual(json.loads(self.result.read_text()), {"new": True})
        with mock.patch.object(deploy.os, "replace", side_effect=PermissionError), \
                mock.patch.object(deploy.time, "sleep"), self.assertRaises(PermissionError):
            deploy._atomic_json(self.result, {"never": "written"})
        self.assertEqual(json.loads(self.result.read_text()), {"new": True})
        self.assertFalse(list(self.root.glob("*.pending")))

    def test_configuration_timeout_precedence_and_default(self):
        self.assertEqual(deploy.validate_config(self.config, "chanel")["timeout"], 30)
        self.config["demo-2"]["verification_timeout_seconds"] = 3
        self.assertEqual(deploy.validate_config(self.config, "chanel")["timeout"], 3)
        self.config["demo-2"]["verification_timeout_seconds"] = None
        self.assertEqual(deploy.validate_config(self.config, "chanel")["timeout"], 30)
        del self.config["demo-2"]["verification_timeout_seconds"]
        del self.config["execution"]["verification_timeout_seconds"]
        self.assertEqual(deploy.validate_config(self.config, "chanel")["timeout"], 900)

    def test_invalid_configuration_values_fail_without_service_calls(self):
        for field, value in [
            ("verification_timeout_seconds", 0), ("verification_timeout_seconds", True),
            ("verification_timeout_seconds", 1.5),
            ("verification_timeout_seconds", deploy.MAX_SECONDS + 1),
            ("target_latency_minutes", 3), ("target_latency_minutes", True),
            ("workspace_id", "not-a-uuid"), ("sales_table", "FactSales;drop"),
            ("verification_batch", {"batch_class": "bootstrap"}),
            ("delivery_method", "manual-upload"),
            ("query_endpoint", "https://not-kusto.example"),
        ]:
            with self.subTest(field=field, value=value):
                self.config = configuration()
                self.config["demo-2"][field] = value
                code, result = self.cli("dry-run")
                self.assertEqual(code, 1)
                self.assertEqual((result["status"], result["ready"]), ("fail", False))
                self.assertTrue(result["diagnostics"])
                self.assertEqual(self.backend.calls, [])

    def test_invalid_prefix_paths_and_result_collision_fail_safely(self):
        for prefix in ("", "Chanel", "../outside", "a" * 41):
            with self.subTest(prefix=prefix):
                _, result = self.cli("dry-run", prefix=prefix)
                self.assertEqual(result["status"], "fail")
        self.config_path.write_text(json.dumps(self.config), encoding="utf-8")
        original = self.config_path.read_text()
        args = ["--mode", "dry-run", "--config", str(self.config_path), "--prefix", "chanel",
                "--state-dir", str(self.state), "--result", str(self.config_path)]
        with contextlib.redirect_stdout(io.StringIO()):
            code = deploy.main(args)
        self.assertEqual(code, 1)
        self.assertEqual(self.config_path.read_text(), original)
        args[args.index("--config") + 1] = os.path.relpath(self.config_path, Path.cwd())
        with contextlib.redirect_stdout(io.StringIO()):
            code = deploy.main(args)
        self.assertEqual(code, 1)
        self.assertEqual(self.config_path.read_text(), original)

    def test_deploy_uses_readbacks_and_repeated_run_is_idempotent(self):
        self.deploy_success()
        first_creates = [call for call in self.backend.calls if call[0] == "POST"
                         and (call[1].endswith("/items") or call[1].endswith("/kqlDatabases")
                              or call[1] == "/v1/workspaces" or call[1].endswith("/shortcuts"))]
        self.assertEqual(len(first_creates), 6)
        self.assertEqual(self.backend.shortcut["target"]["oneLake"], {
            "workspaceId": WORKSPACE, "itemId": DATABASE, "path": "Tables/FactSales",
        })
        self.assertEqual(self.builder.calls[0][1]["verification_timeout_seconds"], 30)
        self.assertEqual(self.builder.calls[0][1]["poll_interval_seconds"], 1)
        self.assertEqual(self.builder.calls[0][1]["workspace_id"], WORKSPACE)
        self.assertEqual(self.builder.calls[0][1]["lakehouse_id"], LAKEHOUSE)
        self.assertIsNone(self.builder.calls[0][1]["data"]["run_id"])
        commands = [call[1] for call in self.backend.calls if call[0] == "KQL"]
        self.assertTrue(any(command.startswith(".create table FactSales (") and "docstring" in command
                            for command in commands))
        self.assertTrue(any(command.startswith(".create table FactSales ingestion csv mapping ")
                            for command in commands))
        self.assertFalse(any(command.startswith((".create-merge table", ".create-or-alter table"))
                             for command in commands))
        ledger = json.loads((self.state / deploy.STATE_FILE).read_text())
        self.assertEqual(ledger["resources"]["database"]["id"], DATABASE)
        self.assertEqual(ledger["resources"]["database"]["parent_eventhouse_id"], EVENTHOUSE)
        self.assertIsNone(ledger["pending"])
        self.assertNotIn("credential-secret-must-not-leak", json.dumps(ledger))
        self.assertNotIn("credential-secret-must-not-leak", self.result.read_text())
        with mock.patch.object(deploy, "verify_producer_access", return_value=None):
            code, result = self.cli("deploy")
        self.assertEqual((code, result["ready"]), (0, True))
        new_creates = [call for call in self.backend.calls if call[0] == "POST"
                       and (call[1].endswith("/items") or call[1].endswith("/kqlDatabases")
                            or call[1] == "/v1/workspaces" or call[1].endswith("/shortcuts"))]
        self.assertEqual(first_creates, new_creates)
        modes = [next(param["value"] for param in call[2]["parameters"] if param["name"] == "MODE")
                 for call in self.backend.calls if call[0] == "POST" and "jobs/execute/instances" in call[1]]
        self.assertEqual(modes, ["preload", "verify_batch", "preload", "verify_batch"])
        self.assertTrue(any(method == "GET" and
                            f"/notebooks/{NOTEBOOK}/jobs/execute/instances/" in path and path.endswith("?beta=true")
                            for method, path, _ in self.backend.calls))

    def test_actual_identity_and_permissions_are_never_inferred_from_configuration(self):
        self.config["demo-2"]["verification_batch"] = verification_batch()
        self.config["shared"]["identity_id"] = TENANT
        code, result = self.cli("deploy")
        self.assertEqual(code, 1)
        self.assertEqual((result["status"], result["ready"]), ("blocked", False), result)
        self.assertIn("producer-runtime identity", result["diagnostics"][0])
        self.assertIn("ingestion/query", result["diagnostics"][0])

    def test_validation_requires_fresh_schema_mapping_policy_path_contents_and_notebook(self):
        self.deploy_success()
        for flag, text in [
            ("schema_drift", "schema"), ("mapping_drift", "mapping"),
            ("policy_drift", "mirroring"), ("wrong_source", "OneLake path"),
            ("shortcut_drift", "shortcut"), ("batch_drift", "KQL batch"),
            ("notebook_drift", "notebook definition"),
            ("notebook_environment_drift", "notebook definition"),
            ("wrong_notebook_mode", "verification mode"),
            ("missing_notebook_exit", "exit value"),
        ]:
            with self.subTest(flag=flag):
                self.backend.flags.add(flag)
                with mock.patch.object(deploy, "_load_builder", return_value=(self.builder, ROOT / "generator.py")):
                    with mock.patch.object(deploy, "verify_producer_access", return_value=None):
                        code, result = self.cli("validate-only")
                self.backend.flags.remove(flag)
                self.assertEqual(code, 1)
                self.assertEqual((result["status"], result["ready"]), ("blocked", False))
                self.assertIn(text.lower(), " ".join(result["diagnostics"]).lower())

    def test_configured_service_endpoints_must_match_database_readback(self):
        self.deploy_success()
        for field in ("query_endpoint", "ingestion_endpoint"):
            with self.subTest(field=field):
                self.config["demo-2"][field] = "https://other.region.kusto.fabric.microsoft.com"
                _, result = self.cli("validate-only")
                self.assertEqual(result["status"], "blocked")
                self.assertIn("endpoint differs", result["diagnostics"][0])
                del self.config["demo-2"][field]

    def test_kql_ingestion_is_polled_until_the_complete_batch_arrives(self):
        self.deploy_success()
        original = self.backend.kusto
        responses = iter([
            [{"RowCount": 0, "DistinctSalesLineIDs": 0, "AmountMinorUnits": None, "InvalidRows": None}],
            [{"RowCount": 1, "DistinctSalesLineIDs": 1, "AmountMinorUnits": 246, "InvalidRows": 0}],
        ])

        def eventually_visible(endpoint, database, command, query=False):
            if query:
                return next(responses)
            return original(endpoint, database, command, query)

        with mock.patch.object(self.backend, "kusto", side_effect=eventually_visible), \
                mock.patch.object(deploy.Deadline, "wait") as wait, \
                mock.patch.object(deploy, "verify_producer_access", return_value=None):
            code, result = self.cli("validate-only")
        self.assertEqual((code, result["status"], result["ready"]), (0, "pass", True))
        wait.assert_called_once_with(1)

    def test_resource_existence_and_stale_state_do_not_establish_readiness(self):
        self.deploy_success()
        del self.config["demo-2"]["verification_batch"]
        self.backend.calls.clear()
        _, result = self.cli("validate-only")
        self.assertEqual((result["status"], result["ready"]), ("blocked", False))
        self.assertIn("verification_batch", result["diagnostics"][0])
        self.assertTrue(any(call[0] == "KQL" for call in self.backend.calls))
        self.assertFalse(any(call[0] == "POST" and "jobs/execute" in call[1] for call in self.backend.calls))

    def test_read_only_validation_does_not_adopt_or_run_unowned_notebook(self):
        self.deploy_success()
        self.config["demo-2"].update({
            "workspace_id": WORKSPACE, "eventhouse_id": EVENTHOUSE,
            "kql_database_id": DATABASE, "lakehouse_id": LAKEHOUSE,
        })
        (self.state / deploy.STATE_FILE).unlink()
        self.backend.calls.clear()
        _, result = self.cli("validate-only")
        self.assertEqual((result["status"], result["ready"]), ("blocked", False))
        self.assertIn("read-only", result["diagnostics"][0])
        self.assertFalse((self.state / deploy.STATE_FILE).exists())
        self.assertFalse(any(call[0] == "POST" for call in self.backend.calls))
        self.assertTrue(any(call[0] == "KQL" and call[2] is True for call in self.backend.calls))

    def test_existing_unowned_resources_are_never_adopted_or_deleted(self):
        self.backend.workspace = {"id": WORKSPACE, "displayName": "chanel-demo2-eventhouse",
                                  "description": "somebody else", "type": "Workspace"}
        _, result = self.cli("deploy")
        self.assertEqual(result["status"], "blocked")
        self.assertIn("not recorded", result["diagnostics"][0])
        self.assertFalse(any(call[0] == "POST" for call in self.backend.calls))
        self.backend.calls.clear()
        _, result = self.cli("cleanup", yes=True)
        self.assertEqual(result["status"], "blocked")
        self.assertFalse(any(call[0] == "DELETE" for call in self.backend.calls))

    def test_cleanup_requires_confirmation_and_fails_closed_for_changed_ownership_or_children(self):
        self.deploy_success()
        self.backend.calls.clear()
        code, result = self.cli("cleanup")
        self.assertEqual((code, result["status"]), (1, "fail"))
        self.assertFalse(self.backend.calls)
        self.backend.workspace["description"] = "unowned"
        _, result = self.cli("cleanup", yes=True)
        self.assertEqual(result["status"], "blocked")
        self.assertFalse(any(call[0] == "DELETE" for call in self.backend.calls))
        ledger = json.loads((self.state / deploy.STATE_FILE).read_text())
        self.backend.workspace["description"] = ledger["owner_marker"]
        self.backend.calls.clear()
        _, result = self.cli("cleanup", yes=True)
        self.assertEqual(result["status"], "blocked")
        self.assertIn("not empty", result["diagnostics"][0])
        self.assertFalse(any(call[0] == "DELETE" for call in self.backend.calls))

    def test_confirmed_empty_workspace_can_be_cleaned_up(self):
        settings = deploy.validate_config(self.config, "chanel")
        with deploy.Ledger(self.state, settings, create=True) as ledger:
            marker = ledger.data["owner_marker"]
            ledger.data["resources"]["workspace"] = {
                "id": WORKSPACE, "name": settings["workspace_name"], "type": "Workspace", "confirmed": True,
            }
            ledger.save()
        self.backend.workspace = {"id": WORKSPACE, "displayName": settings["workspace_name"],
                                  "description": marker, "type": "Workspace"}
        code, result = self.cli("cleanup", yes=True)
        self.assertEqual((code, result["status"], result["ready"]), (0, "pass", False))
        self.assertIsNone(self.backend.workspace)
        self.assertTrue(json.loads((self.state / deploy.STATE_FILE).read_text())["resources"]["workspace"]["deleted"])

    def test_missing_builder_blocks_before_cloud_calls(self):
        with mock.patch.object(deploy, "_load_builder", side_effect=deploy.Blocked("notebook builder missing")):
            _, result = self.cli("deploy", builder=False)
        self.assertEqual((result["status"], result["ready"]), ("blocked", False))
        self.assertEqual(self.backend.calls, [])

    def test_real_builder_outputs_a_mode_and_batch_exit_handshake(self):
        import notebook_builder

        generator = self.root / "generator.py"
        generator.write_text(
            "import hashlib\nfrom pathlib import Path\n"
            "GENERATOR_SENTINEL = True\n"
            "def generator_source_sha256():\n"
            "    return hashlib.sha256(Path(__file__).read_bytes()).hexdigest()\n",
            encoding="utf-8",
        )
        settings = deploy.validate_config(self.config, "chanel")
        notebook = notebook_builder.build_notebook(
            generator, workspace_id=WORKSPACE, lakehouse_id=LAKEHOUSE,
            lakehouse_name="SalesLakehouse", data=settings["data"], shortcut_name="EventhouseFactSales",
            verification_timeout_seconds=settings["timeout"], poll_interval_seconds=settings["poll"],
        )
        module = types.ModuleType("notebookutils")
        exit_notebook = mock.Mock()
        module.notebook = types.SimpleNamespace(exit=exit_notebook)
        module.mssparkutils = types.SimpleNamespace(notebook=module.notebook)
        spark = types.SimpleNamespace(
            conf=types.SimpleNamespace(set=mock.Mock()),
            catalog=types.SimpleNamespace(tableExists=mock.Mock(return_value=True)),
        )
        preload = mock.Mock()
        dimensions = mock.Mock()
        check_batch = mock.Mock(side_effect=lambda spark, batch, *_: {
            "batch_id": batch["batch_id"], "row_count": batch["row_count"],
            "sales_amount_minor_units": batch["sales_amount_minor_units"],
        })
        real_exec = builtins.exec

        def packaged_exec(code, namespace, *args):
            real_exec(code, namespace, *args)
            if getattr(code, "co_filename", "") == "<packaged lakehouse_validation>":
                namespace.update({
                    "preload_dimensions": preload, "verify_dimensions": dimensions,
                    "verify_lakehouse_batch": check_batch, "verify_batch_with_timeout": check_batch,
                })

        for mode, batch in (("preload", None), ("verify_batch", verification_batch())):
            exit_notebook.reset_mock()
            with self.subTest(mode=mode), mock.patch.dict(sys.modules, {"notebookutils": module}), \
                    mock.patch.object(builtins, "exec", side_effect=packaged_exec), \
                    contextlib.redirect_stdout(io.StringIO()):
                scope = {"spark": spark, "notebookutils": module}
                cells = notebook["cells"]
                real_exec(compile("".join(cells[0]["source"]), "parameters", "exec"), scope)
                scope["MODE"] = mode
                if batch is not None:
                    scope["BATCH_JSON"] = json.dumps(batch)
                for cell in cells[1:]:
                    real_exec(compile("".join(cell["source"]), "notebook", "exec"), scope)
                exit_value = exit_notebook.call_args.args[0]
                deploy._verify_notebook_exit({"properties": {"exitValue": exit_value}}, mode, batch)
        preload.assert_called_once()
        dimensions.assert_called_once()
        check_batch.assert_called_once()

    def test_uncertain_mutations_are_recorded_and_not_retried(self):
        original = self.backend.fabric

        def uncertain(method, path, body=None, allow_missing=False):
            if method == "POST" and path == "/v1/workspaces":
                self.backend.calls.append((method, path, body))
                raise deploy.Blocked("uncertain request")
            return original(method, path, body, allow_missing)

        with mock.patch.object(self.backend, "fabric", side_effect=uncertain):
            _, first = self.cli("deploy")
        self.assertEqual(first["status"], "blocked")
        pending = json.loads((self.state / deploy.STATE_FILE).read_text())["pending"]
        self.assertEqual(pending["kind"], "create-workspace")
        self.backend.calls.clear()
        _, second = self.cli("deploy")
        self.assertEqual(second["status"], "blocked")
        self.assertIn("uncertain", second["diagnostics"][0])
        self.assertFalse(any(call[0] == "POST" for call in self.backend.calls))

    def test_interrupted_item_creation_resumes_its_saved_lro_without_recreating(self):
        original = self.backend.fabric

        def interrupted(method, path, body=None, allow_missing=False):
            if method == "POST" and path == f"/v1/workspaces/{WORKSPACE}/items" and body["type"] == "Eventhouse":
                _, _, item = original(method, path, body, allow_missing)
                self.backend.operation_results[OPERATION] = item
                return 202, {"x-ms-operation-id": OPERATION}, None
            if method == "GET" and path == f"/v1/operations/{OPERATION}":
                raise deploy.Blocked("test interruption")
            return original(method, path, body, allow_missing)

        with mock.patch.object(self.backend, "fabric", side_effect=interrupted):
            _, result = self.cli("deploy")
        self.assertEqual(result["status"], "blocked")
        pending = json.loads((self.state / deploy.STATE_FILE).read_text())["pending"]
        self.assertEqual((pending["kind"], pending["operation_id"]), ("create-item", OPERATION))
        self.backend.calls.clear()
        self.deploy_success()
        self.assertTrue(any(call[0] == "GET" and call[1] == f"/v1/operations/{OPERATION}"
                            for call in self.backend.calls))
        self.assertFalse(any(call[0] == "POST" and isinstance(call[2], dict)
                             and call[2].get("type") == "Eventhouse" for call in self.backend.calls))

    def test_failed_lro_is_reported_without_recreating(self):
        original = self.backend.fabric

        def failed(method, path, body=None, allow_missing=False):
            if method == "POST" and path == f"/v1/workspaces/{WORKSPACE}/items" and body["type"] == "Eventhouse":
                original(method, path, body, allow_missing)
                return 202, {"x-ms-operation-id": OPERATION}, None
            if method == "GET" and path == f"/v1/operations/{OPERATION}":
                return 200, {}, {"status": "Failed"}
            return original(method, path, body, allow_missing)

        with mock.patch.object(self.backend, "fabric", side_effect=failed):
            code, result = self.cli("deploy")
        self.assertEqual((code, result["status"]), (1, "fail"))
        self.assertIn("long-running operation failed", result["diagnostics"][0])
        self.assertEqual(json.loads((self.state / deploy.STATE_FILE).read_text())["pending"]["operation_id"], OPERATION)

    def test_recovered_notebook_job_requires_matching_exit_evidence(self):
        self.deploy_success()
        ledger_path = self.state / deploy.STATE_FILE
        ledger = json.loads(ledger_path.read_text())
        job_id = next(iter(self.backend.jobs))
        ledger["pending"] = {"kind": "notebook-job", "mode": "verify_batch", "notebook_id": NOTEBOOK,
                             "batch": {key: verification_batch()[key] for key in
                                       ("batch_id", "row_count", "sales_amount_minor_units")},
                             "job_id": job_id}
        ledger_path.write_text(json.dumps(ledger), encoding="utf-8")
        self.backend.calls.clear()
        _, result = self.cli("validate-only")
        self.assertEqual(result["status"], "blocked")
        self.assertIn("verification mode and batch", result["diagnostics"][0])
        self.assertFalse(any(call[0] == "POST" for call in self.backend.calls))
        self.assertIsNone(json.loads(ledger_path.read_text())["pending"])

    def test_table_create_race_cannot_adopt_or_alter_an_unowned_table(self):
        self.backend.flags.add("racing_table")
        _, result = self.cli("deploy")
        self.assertEqual((result["status"], result["ready"]), ("blocked", False))
        self.assertIn("table ownership marker", result["diagnostics"][0])
        commands = [call[1] for call in self.backend.calls if call[0] == "KQL"]
        self.assertFalse(any("ingestion csv mapping" in command or "policy mirroring" in command
                             for command in commands))
        pending = json.loads((self.state / deploy.STATE_FILE).read_text())["pending"]
        self.assertEqual(pending["kind"], "create-table")

    def test_mapping_create_race_never_overwrites_or_applies_policy(self):
        self.backend.flags.add("racing_mapping")
        code, result = self.cli("deploy")
        self.assertEqual((code, result["status"], result["ready"]), (1, "fail", False))
        commands = [call[1] for call in self.backend.calls if call[0] == "KQL"]
        self.assertFalse(any(command.startswith(".create-or-alter") or "policy mirroring" in command
                             for command in commands))
        pending = json.loads((self.state / deploy.STATE_FILE).read_text())["pending"]
        self.assertEqual(pending["kind"], "create-mapping")

    def test_service_errors_and_policy_connection_secrets_are_not_logged(self):
        def fail(*args, **kwargs):
            raise RuntimeError("sensitive-token-must-not-leak")

        with mock.patch.object(self.backend, "pages", side_effect=fail):
            _, result = self.cli("deploy")
        self.assertEqual(result["status"], "fail")
        self.assertNotIn("sensitive-token-must-not-leak", json.dumps(result))

    def test_monitor_start_requires_configuration_before_cloud_calls(self):
        _, result = self.cli("start-monitor")
        self.assertEqual((result["status"], result["ready"]), ("blocked", False))
        self.assertIn("shared.workspace_id", result["diagnostics"][0])
        self.assertEqual(self.backend.calls, [])
        self.config["shared"].update({"workspace_id": SHARED_WORKSPACE,
                                      "staging_lakehouse_id": STAGING_LAKEHOUSE,
                                      "identity_id": PRODUCER_IDENTITY})
        self.config["data"].update({"file_count": 3, "max_duration_seconds": 120})
        _, result = self.cli("start-monitor", selected="demo-2")
        self.assertEqual((result["status"], result["ready"]), ("blocked", False))
        self.assertIn("data.rows_per_file", result["diagnostics"][0])
        self.assertIn("data.interval_seconds", result["diagnostics"][0])
        self.assertEqual(self.backend.calls, [])
        self.config["data"].update({"rows_per_file": 2, "interval_seconds": 30})
        for selected in ("demo-1", "demo-1,demo-1", "demo-4", ""):
            with self.subTest(selected=selected):
                _, result = self.cli("start-monitor", selected=selected)
                self.assertEqual(result["status"], "fail")
                self.assertEqual(self.backend.calls, [])
        self.config["execution"]["max_concurrency"] = True
        _, result = self.cli("start-monitor", selected="demo-2")
        self.assertEqual(result["status"], "fail")
        self.assertEqual(self.backend.calls, [])

    def test_monitor_start_blocks_before_submission_without_fresh_producer_proof(self):
        self.monitor_setup()
        self.readiness_patcher.stop()
        self.backend.calls.clear()
        code, result = self.cli("start-monitor")
        self.assertEqual((code, result["status"], result["ready"]), (1, "blocked", False), result)
        self.assertIn("producer-runtime identity", result["diagnostics"][0])
        self.assertFalse(any(method == "POST" for method, _, _ in self.backend.calls))
        self.assertFalse((self.state / deploy.MONITOR_START_FILE).exists())
        self.assertFalse((self.state / deploy.MONITOR_FILE).exists())
        self.assertNotIn(MONITOR_NOTEBOOK, self.backend.items)
        self.assertFalse(self.backend.monitor_jobs)

    def test_monitor_heartbeat_does_not_override_proof_and_stop_remains_available(self):
        self.monitor_setup()
        evidence = self.start_monitor()
        sidecar_path = self.state / deploy.MONITOR_FILE
        start_path = self.state / deploy.MONITOR_START_FILE
        sidecar, start = sidecar_path.read_bytes(), start_path.read_bytes()
        self.readiness_patcher.stop()
        self.backend.calls.clear()
        code, result = self.cli("monitor-status")
        self.assertEqual((code, result["status"], result["ready"]), (1, "blocked", False), result)
        self.assertIn("producer-runtime identity", result["diagnostics"][0])
        self.assertEqual(sidecar_path.read_bytes(), sidecar)
        self.assertFalse(any(method == "POST" for method, _, _ in self.backend.calls))
        self.assertEqual(self.backend.monitor_jobs[evidence["job_id"]]["status"], "InProgress")
        _, stopped = self.cli("stop-monitor")
        self.assertEqual((stopped["status"], stopped["ready"]), ("pass", False), stopped)
        self.assertEqual(self.backend.monitor_jobs[evidence["job_id"]]["status"], "Cancelled")
        self.assertEqual(start_path.read_bytes(), start)

    def test_completed_monitor_cannot_report_ready_without_fresh_producer_proof(self):
        self.monitor_setup()
        evidence = self.start_monitor()
        self.complete_monitor(evidence)
        self.readiness_patcher.stop()
        for mode in ("monitor-status", "wait-monitor"):
            with self.subTest(mode=mode):
                _, result = self.cli(mode)
                self.assertEqual((result["status"], result["ready"]), ("blocked", False), result)
                self.assertIn("producer-runtime identity", result["diagnostics"][0])
                self.assertNotEqual(json.loads((self.state / deploy.MONITOR_FILE).read_text())["state"],
                                    "completed")

    def test_producer_proof_alone_cannot_replace_the_overall_readiness_binding(self):
        self.monitor_setup()
        self.readiness_patcher.stop()
        self.backend.calls.clear()
        with mock.patch.object(deploy, "verify_producer_access", return_value=None) as proof:
            _, result = self.cli("start-monitor")
        self.assertEqual((result["status"], result["ready"]), ("blocked", False), result)
        self.assertIn("authenticated binding", result["diagnostics"][0])
        self.assertEqual(proof.call_count, 1)
        self.assertEqual(proof.call_args.args[2]["id"], DATABASE)
        self.assertFalse(any(method == "POST" for method, _, _ in self.backend.calls))
        self.assertFalse((self.state / deploy.MONITOR_START_FILE).exists())

    def test_monitor_deadline_overflow_is_rejected_before_cloud_calls(self):
        self.config["shared"].update({"workspace_id": SHARED_WORKSPACE,
                                      "staging_lakehouse_id": STAGING_LAKEHOUSE,
                                      "identity_id": PRODUCER_IDENTITY})
        self.config["data"].update({"file_count": 3, "max_duration_seconds": 120,
                                    "rows_per_file": 2, "interval_seconds": 30})
        with mock.patch.object(deploy, "_now", return_value=datetime.max.replace(tzinfo=timezone.utc)):
            _, result = self.cli("start-monitor")
        self.assertEqual((result["status"], result["ready"]), ("fail", False))
        self.assertIn("deadline bound", result["diagnostics"][0])
        self.assertEqual(self.backend.calls, [])

    def test_monitor_rejects_invalid_producer_counts_and_schedule(self):
        self.config["shared"].update({"workspace_id": SHARED_WORKSPACE,
                                      "staging_lakehouse_id": STAGING_LAKEHOUSE,
                                      "identity_id": PRODUCER_IDENTITY})
        self.config["data"].update({"file_count": 3, "max_duration_seconds": 120,
                                    "rows_per_file": 2, "interval_seconds": 30})
        original = copy.deepcopy(self.config["data"])
        for field, value in (("rows_per_file", True), ("rows_per_file", 0), ("rows_per_file", 2**62),
                             ("interval_seconds", True), ("interval_seconds", -1),
                             ("interval_seconds", 60)):
            with self.subTest(field=field, value=value):
                self.config["data"] = dict(original, **{field: value})
                _, result = self.cli("start-monitor", selected="demo-2")
                self.assertEqual((result["status"], result["ready"]), ("fail", False), result)
                self.assertEqual(self.backend.calls, [])
        self.config["data"] = dict(original, interval_seconds=0)
        settings = deploy.validate_config(self.config, "chanel")
        self.assertEqual(deploy._monitor_config(settings, {"demo-2"})["producer_config"]["interval_seconds"], 0)

    def test_monitor_start_records_exact_bounded_parameters_and_fresh_proof(self):
        self.monitor_setup()
        self.config["demo-2"]["verification_timeout_seconds"] = None
        evidence = self.start_monitor()
        immutable = json.loads((self.state / deploy.MONITOR_START_FILE).read_text(encoding="utf-8"))
        self.assertEqual(immutable["state"], "submitted")
        self.assertEqual(immutable["job_id"], MONITOR_JOB)
        self.assertEqual(evidence["state"], "running")
        for key in ("run_id", "job_id", "controller_id", "manifest_uri", "state_uri", "deadline_utc"):
            self.assertEqual(immutable[key], evidence[key])
        manifest = (f"{deploy.ONELAKE}/{SHARED_WORKSPACE}/{STAGING_LAKEHOUSE}"
                    "/Files/producer/runs/sample/manifest.json")
        sentinel = (f"{deploy.ONELAKE}/{SHARED_WORKSPACE}/{STAGING_LAKEHOUSE}"
                    "/Files/preflight/producer-monitor-heartbeat-v1.txt")
        state_uri = (f"{deploy.ONELAKE}/{WORKSPACE}/{LAKEHOUSE}"
                     "/Files/demo2-verification/runs/sample/state.json")
        self.assertEqual(evidence["manifest_uri"], manifest)
        self.assertEqual(evidence["state_uri"], state_uri)
        self.assertEqual((deploy._utc(evidence["deadline_utc"], "deadline")
                          - deploy._utc(evidence["submitted_at_utc"], "submitted")).total_seconds(), 170)
        submissions = [call for call in self.backend.calls
                       if call[0] == "POST" and f"/notebooks/{MONITOR_NOTEBOOK}/jobs/execute/" in call[1]]
        self.assertEqual(len(submissions), 1)
        params = {entry["name"]: (entry["value"], entry["type"]) for entry in submissions[0][2]["parameters"]}
        self.assertEqual(params, {
            "RUN_ID": ("sample", "Text"), "MANIFEST_URL": (manifest, "Text"),
            "SENTINEL_URL": (sentinel, "Text"), "CONTROLLER_ID": (evidence["controller_id"], "Text"),
            "STATE_URL": (state_uri, "Text"), "DEADLINE_UTC": (evidence["deadline_utc"], "Text"),
            "VERIFIER_WORKSPACE_ID": (WORKSPACE, "Text"), "VERIFIER_NOTEBOOK_ID": (NOTEBOOK, "Text"),
            "EXPECTED_IDENTITY_ID": (PRODUCER_IDENTITY, "Text"), "EXPECTED_TENANT_ID": (TENANT, "Text"),
            "EXPECTED_BATCH_COUNT": (3, "Integer"), "VERIFICATION_TIMEOUT_SECONDS": (30, "Integer"),
            "POLL_INTERVAL_SECONDS": (1, "Integer"), "MAX_CONCURRENT_BATCHES": (2, "Integer"),
        })
        self.assertTrue(all(skill == "spark-cli" for skill in self.backend.skills))
        reads = [(call[0], call[1]) for call in self.backend.calls]
        self.assertIn(("STORAGE-GET", state_uri), reads)
        self.assertIn(("STORAGE-GET", deploy._lock_uri(state_uri)), reads)
        self.assertNotIn(("STORAGE-GET", manifest), reads)
        self.assertFalse(any(path.name.endswith(".pending") for path in self.state.iterdir()))

    def test_monitor_parameters_match_actual_runtime_and_packaged_notebook(self):
        import monitor_controller
        import monitor_notebook_builder
        import notebook_builder

        self.monitor_setup()
        evidence = self.start_monitor()
        submission = next(call[2] for call in self.backend.calls
                          if call[0] == "POST" and f"/notebooks/{MONITOR_NOTEBOOK}/jobs/execute/" in call[1])
        parameters = {param["name"]: param["value"] for param in submission["parameters"]}
        actual = monitor_controller.settings(parameters, lakehouse_id=LAKEHOUSE)
        self.assertEqual((actual["run_id"], actual["controller_id"]), ("sample", evidence["controller_id"]))
        self.assertEqual((actual["identity_id"], actual["tenant_id"]), (PRODUCER_IDENTITY, TENANT))
        notebook = monitor_notebook_builder.build_monitor_notebook(
            workspace_id=WORKSPACE, lakehouse_id=LAKEHOUSE, lakehouse_name="SalesLakehouse",
        )
        self.assertEqual(deploy._notebook_projection(notebook),
                         deploy._notebook_projection(deploy._read_notebook_definition(
                             notebook_builder.fabric_definition(notebook))))
        for cell in notebook["cells"]:
            compile("".join(cell["source"]), "packaged-monitor", "exec")

    def test_monitor_status_rechecks_live_job_heartbeat_identity_and_lock(self):
        self.monitor_setup()
        evidence = self.start_monitor()
        status_path = self.state / deploy.MONITOR_FILE
        state_uri = evidence["state_uri"]
        lock_uri = deploy._lock_uri(state_uri)
        baseline_state = copy.deepcopy(self.backend.blobs[state_uri])
        baseline_lock = copy.deepcopy(self.backend.blobs[lock_uri])
        changes = [
            ("run_id", "another-run"), ("controller_id", str(uuid.uuid4())),
            ("manifest_uri", evidence["manifest_uri"] + ".other"),
            ("deadline_utc", deploy._stamp(deploy._now() + timedelta(seconds=2))),
            ("identity_id", TENANT), ("tenant_id", PRODUCER_IDENTITY),
            ("sentinel_verified", False), ("job_access_verified", False),
            ("heartbeat_at_utc", deploy._stamp(deploy._now() - timedelta(minutes=10))),
            ("heartbeat_at_utc", deploy._stamp(deploy._now() + timedelta(minutes=2))),
            ("state", "completed"), ("contract_version", True),
        ]
        for field, wrong in changes:
            with self.subTest(field=field, wrong=wrong):
                status_path.write_text(json.dumps(evidence), encoding="utf-8")
                changed = copy.deepcopy(baseline_state[0])
                changed[field] = wrong
                self.backend.blobs[state_uri] = (changed, baseline_state[1])
                _, result = self.cli("monitor-status")
                self.assertEqual((result["status"], result["ready"]), ("blocked", False), result)
        self.backend.blobs[state_uri] = baseline_state
        self.backend.blobs[lock_uri] = ({"contract_version": 1, "run_id": "sample",
                                         "controller_id": str(uuid.uuid4())}, baseline_lock[1])
        status_path.write_text(json.dumps(evidence), encoding="utf-8")
        _, result = self.cli("monitor-status")
        self.assertEqual((result["status"], result["ready"]), ("blocked", False))
        self.backend.blobs[lock_uri] = baseline_lock
        self.backend.monitor_jobs[MONITOR_JOB]["status"] = "Completed"
        status_path.write_text(json.dumps(evidence), encoding="utf-8")
        _, result = self.cli("monitor-status")
        self.assertEqual((result["status"], result["ready"]), ("blocked", False))
        self.assertIn("exit value", result["diagnostics"][0])

    def test_monitor_rechecks_owned_definitions_and_resource_markers(self):
        self.monitor_setup()
        self.start_monitor()
        self.backend.flags.add("monitor_drift")
        _, result = self.cli("monitor-status")
        self.assertEqual((result["status"], result["ready"]), ("blocked", False))
        self.assertIn("definition", result["diagnostics"][0])
        self.backend.flags.remove("monitor_drift")
        self.backend.items[MONITOR_NOTEBOOK]["description"] = "not-owned"
        self.backend.calls.clear()
        _, result = self.cli("stop-monitor")
        self.assertEqual(result["status"], "blocked")
        self.assertFalse(any(path.endswith("/cancel") for method, path, _ in self.backend.calls))

    def test_wait_monitor_polls_until_terminal_and_verifies_nested_exit_and_every_batch(self):
        self.monitor_setup()
        evidence = self.start_monitor()
        start_path = self.state / deploy.MONITOR_START_FILE
        start_bytes = start_path.read_bytes()
        original = self.backend.fabric
        job_path = (f"/v1/workspaces/{WORKSPACE}/items/{MONITOR_NOTEBOOK}"
                    f"/jobs/instances/{MONITOR_JOB}")
        attempts = []

        def eventually_complete(method, path, body=None, allow_missing=False):
            if method == "GET" and path == job_path:
                attempts.append(path)
                if len(attempts) == 2:
                    self.complete_monitor(evidence)
            return original(method, path, body, allow_missing)

        self.backend.calls.clear()
        with mock.patch.object(self.backend, "fabric", side_effect=eventually_complete), \
                mock.patch.object(deploy.Deadline, "wait") as wait:
            _, result = self.cli("wait-monitor", selected="all")
        self.assertEqual((result["status"], result["ready"]), ("pass", True), result)
        wait.assert_called_once_with(1)
        self.assertEqual(len(attempts), 2)
        self.assertTrue(any(method == "GET" and f"/notebooks/{MONITOR_NOTEBOOK}/jobs/execute/instances/"
                            in path and path.endswith("?beta=true") for method, path, _ in self.backend.calls))
        child_reads = {
            path for method, path, _ in self.backend.calls
            if method == "GET" and f"/notebooks/{NOTEBOOK}/jobs/execute/instances/" in path
        }
        self.assertEqual(child_reads, {
            f"/v1/workspaces/{WORKSPACE}/notebooks/{NOTEBOOK}/jobs/execute/instances/{job_id}?beta=true"
            for job_id in self.backend.child_jobs
        })
        for uri in (evidence["manifest_uri"], evidence["state_uri"]):
            self.assertEqual(sum(method == "STORAGE-GET" and path == uri
                                 for method, path, _ in self.backend.calls), 2)
        self.assertEqual(json.loads((self.state / deploy.MONITOR_FILE).read_text())["state"], "completed")
        self.assertEqual(start_path.read_bytes(), start_bytes)
        _, status = self.cli("monitor-status")
        self.assertEqual((status["status"], status["ready"]), ("pass", True))
        self.assertIn("every measured batch", status["diagnostics"][0])
        _, stopped = self.cli("stop-monitor")
        self.assertEqual(stopped["status"], "pass")
        self.assertEqual(json.loads((self.state / deploy.MONITOR_FILE).read_text())["state"], "completed")
        _, status = self.cli("monitor-status")
        self.assertEqual((status["status"], status["ready"]), ("pass", True))

    def test_completed_monitor_requires_exact_exit_and_complete_durable_evidence(self):
        self.monitor_setup()
        evidence = self.start_monitor()
        self.complete_monitor(evidence)
        state_uri = evidence["state_uri"]
        good_state = copy.deepcopy(self.backend.blobs[state_uri])
        good_job = copy.deepcopy(self.backend.monitor_jobs[MONITOR_JOB])
        baseline_sidecar = copy.deepcopy(evidence)
        cases = ("missing-exit", "wrong-run", "top-level-exit", "missing-batch", "extra-batch", "failed-batch",
                 "uncertain-batch", "missing-operation", "missing-final", "unverified-final",
                 "missing-manifest", "writer-lock")
        for case in cases:
            with self.subTest(case=case):
                (self.state / deploy.MONITOR_FILE).write_text(json.dumps(baseline_sidecar), encoding="utf-8")
                self.backend.blobs[state_uri] = copy.deepcopy(good_state)
                self.backend.blobs.pop(deploy._lock_uri(state_uri), None)
                self.backend.monitor_jobs[MONITOR_JOB] = copy.deepcopy(good_job)
                state = self.backend.blobs[state_uri][0]
                job = self.backend.monitor_jobs[MONITOR_JOB]
                if case == "missing-exit":
                    job.pop("properties")
                elif case == "wrong-run":
                    job["properties"]["exitValue"] = json.dumps(
                        {"mode": "monitor", "status": "verified", "run_id": "wrong"}
                    )
                elif case == "top-level-exit":
                    job["exitValue"] = job.pop("properties")["exitValue"]
                elif case == "missing-batch":
                    del state["batches"]["sample_0001"]
                elif case == "extra-batch":
                    state["batches"]["sample_9999"] = copy.deepcopy(state["batches"]["sample_0001"])
                elif case == "failed-batch":
                    state["batches"]["sample_0001"]["status"] = "failed"
                elif case == "uncertain-batch":
                    state["batches"]["sample_0001"]["submission_uncertain"] = True
                elif case == "missing-operation":
                    state["batches"]["sample_0001"].pop("operation_id")
                elif case == "missing-final":
                    state["run_verification"] = None
                elif case == "unverified-final":
                    state["run_verification"]["status"] = "running"
                elif case == "missing-manifest":
                    state["manifest_observed"] = False
                else:
                    self.backend.blobs[deploy._lock_uri(state_uri)] = (
                        {"contract_version": 1, "run_id": evidence["run_id"],
                         "controller_id": evidence["controller_id"]}, '"still-locked"'
                    )
                _, result = self.cli("wait-monitor")
                self.assertEqual((result["status"], result["ready"]), ("blocked", False), result)
                self.assertNotEqual(json.loads((self.state / deploy.MONITOR_FILE).read_text())["state"], "completed")

    def test_completion_requires_matching_manifest_scheduler_and_delivery_evidence(self):
        self.monitor_setup()
        evidence = self.start_monitor()
        self.complete_monitor(evidence)
        manifest_uri, state_uri = evidence["manifest_uri"], evidence["state_uri"]
        good_manifest = copy.deepcopy(self.backend.blobs[manifest_uri])
        good_state = copy.deepcopy(self.backend.blobs[state_uri])
        sidecar_path = self.state / deploy.MONITOR_FILE
        cases = (
            "missing-manifest", "missing-etag", "wrong-run", "wrong-config", "wrong-destinations",
            "duplicate-destination", "missing-measured", "wrong-rows", "scheduler-running",
            "scheduler-unverified", "scheduler-errors", "scheduler-duration", "scheduler-deadline",
            "scheduler-late", "fingerprint", "operation", "source", "outcome-failed",
            "outcome-extra", "outcome-missing", "submission", "verified-count", "verified-total",
            "verified-distinct",
        )
        for case in cases:
            with self.subTest(case=case):
                sidecar_path.write_text(json.dumps(evidence), encoding="utf-8")
                self.backend.blobs[manifest_uri] = copy.deepcopy(good_manifest)
                self.backend.blobs[state_uri] = copy.deepcopy(good_state)
                manifest = self.backend.blobs[manifest_uri][0]
                state = self.backend.blobs[state_uri][0]
                batch = manifest["batches"]["1"]
                delivery = batch["deliveries"]["demo-2"]
                scheduler = manifest["scheduler"]
                if case == "missing-manifest":
                    del self.backend.blobs[manifest_uri]
                elif case == "missing-etag":
                    self.backend.blobs[manifest_uri] = (manifest, None)
                elif case == "wrong-run":
                    manifest["run_id"] = "other"
                elif case == "wrong-config":
                    manifest["config"]["seed"] += 1
                elif case == "wrong-destinations":
                    manifest["destinations"] = ["demo-1"]
                elif case == "duplicate-destination":
                    manifest["destinations"] = ["demo-2", "demo-2"]
                elif case == "missing-measured":
                    del manifest["batches"]["3"]
                elif case == "wrong-rows":
                    batch["row_count"] += 1
                elif case == "scheduler-running":
                    scheduler["status"] = "running"
                elif case == "scheduler-unverified":
                    scheduler["result"]["verified"] = False
                elif case == "scheduler-errors":
                    scheduler["result"]["errors"] = ["failure"]
                elif case == "scheduler-duration":
                    scheduler["max_duration_seconds"] += 1
                elif case == "scheduler-deadline":
                    scheduler["result"]["deadline_utc"] = scheduler["finished_at_utc"]
                elif case == "scheduler-late":
                    scheduler["finished_at_utc"] = deploy._stamp(
                        deploy._utc(scheduler["result"]["deadline_utc"], "deadline") + timedelta(seconds=1)
                    )
                elif case == "fingerprint":
                    state["batches"]["sample_0001"]["fingerprint"] = "0" * 64
                elif case == "operation":
                    state["batches"]["sample_0001"]["operation_id"] = str(uuid.UUID(int=999))
                elif case == "source":
                    delivery["ingestion_outcome"]["source_id"] = str(uuid.UUID(int=999))
                elif case == "outcome-failed":
                    delivery["ingestion_outcome"]["status"] = "failed"
                elif case == "outcome-extra":
                    delivery["ingestion_outcome"]["unrecognized"] = "value"
                elif case == "outcome-missing":
                    del delivery["ingestion_outcome"]
                elif case == "submission":
                    delivery["submission_status"] = "unknown"
                elif case == "verified-count":
                    delivery["verified_row_count"] += 1
                elif case == "verified-total":
                    delivery["verified_total_minor_units"] += 1
                else:
                    delivery["verified_distinct_sales_line_ids"] -= 1
                _, result = self.cli("wait-monitor")
                self.assertEqual((result["status"], result["ready"]), ("blocked", False), result)
                self.assertNotEqual(json.loads(sidecar_path.read_text())["state"], "completed")

    def test_completion_is_blocked_without_independent_global_reservation_proof(self):
        self.monitor_setup()
        evidence = self.start_monitor()
        self.complete_monitor(evidence)
        self.reservation_patcher.stop()
        _, result = self.cli("wait-monitor")
        self.assertEqual((result["status"], result["ready"]), ("blocked", False), result)
        self.assertIn("SalesLineID allocation is provisional", result["diagnostics"][0])
        self.assertNotEqual(json.loads((self.state / deploy.MONITOR_FILE).read_text())["state"], "completed")

    def test_completion_rechecks_each_exact_child_job_and_exit(self):
        self.monitor_setup()
        evidence = self.start_monitor()
        state = self.complete_monitor(evidence)
        batch_job = state["batches"]["sample_0001"]["job_id"]
        run_job = state["run_verification"]["job_id"]
        good_jobs = copy.deepcopy(self.backend.child_jobs)
        sidecar_path = self.state / deploy.MONITOR_FILE
        cases = (
            "job-failed", "wrong-job-id", "wrong-notebook", "missing-properties", "top-level-exit",
            "invalid-json", "batch-mode", "batch-status", "batch-id", "batch-count", "batch-total",
            "run-mode", "run-ids", "run-count", "run-total",
        )
        for case in cases:
            with self.subTest(case=case):
                sidecar_path.write_text(json.dumps(evidence), encoding="utf-8")
                self.backend.child_jobs = copy.deepcopy(good_jobs)
                job = self.backend.child_jobs[run_job if case.startswith("run-") else batch_job]
                result = json.loads(job["properties"]["exitValue"])
                if case == "job-failed":
                    job["status"] = "Failed"
                elif case == "wrong-job-id":
                    job["id"] = str(uuid.UUID(int=999))
                elif case == "wrong-notebook":
                    job["itemId"] = MONITOR_NOTEBOOK
                elif case == "missing-properties":
                    del job["properties"]
                elif case == "top-level-exit":
                    job["exitValue"] = job.pop("properties")["exitValue"]
                elif case == "invalid-json":
                    job["properties"]["exitValue"] = "{"
                else:
                    if case == "batch-mode":
                        result["mode"] = "preload"
                    elif case == "batch-status":
                        result["status"] = "pass"
                    elif case == "batch-id":
                        result["batch"]["batch_id"] = "other"
                    elif case == "batch-count":
                        result["batch"]["row_count"] += 1
                    elif case == "batch-total":
                        result["batch"]["sales_amount_minor_units"] += 1
                    elif case == "run-mode":
                        result["mode"] = "verify_run"
                    elif case == "run-ids":
                        result["batch_ids"].reverse()
                    elif case == "run-count":
                        result["row_count"] += 1
                    else:
                        result["sales_amount_minor_units"] += 1
                    job["properties"]["exitValue"] = json.dumps(result)
                _, outcome = self.cli("wait-monitor")
                self.assertEqual((outcome["status"], outcome["ready"]), ("blocked", False), outcome)
                self.assertNotEqual(json.loads(sidecar_path.read_text())["state"], "completed")

    def test_boolean_child_counts_never_equal_numeric_verification_counts(self):
        self.monitor_setup()
        self.config["data"].update({"file_count": 1, "rows_per_file": 1})
        evidence = self.start_monitor()
        state = self.complete_monitor(evidence)
        good_jobs = copy.deepcopy(self.backend.child_jobs)
        sidecar_path = self.state / deploy.MONITOR_FILE
        for role, job_id in (
            ("batch", state["batches"]["sample_0001"]["job_id"]),
            ("run", state["run_verification"]["job_id"]),
        ):
            with self.subTest(role=role):
                sidecar_path.write_text(json.dumps(evidence), encoding="utf-8")
                self.backend.child_jobs = copy.deepcopy(good_jobs)
                job = self.backend.child_jobs[job_id]
                value = json.loads(job["properties"]["exitValue"])
                summary = value["batch"] if role == "batch" else value
                summary["row_count"] = True
                job["properties"]["exitValue"] = json.dumps(value)
                _, result = self.cli("wait-monitor")
                self.assertEqual((result["status"], result["ready"]), ("blocked", False), result)

    def test_completion_rejects_changed_manifest_and_state_snapshots(self):
        self.monitor_setup()
        evidence = self.start_monitor()
        self.complete_monitor(evidence)
        sidecar_path = self.state / deploy.MONITOR_FILE
        originals = {uri: copy.deepcopy(self.backend.blobs[uri])
                     for uri in (evidence["manifest_uri"], evidence["state_uri"])}
        for uri in originals:
            originals[uri][0]["snapshot_counter"] = 1
        original_onelake = self.backend.onelake
        for target in originals:
            for change in ("etag", "content", "missing-etag", "deleted"):
                with self.subTest(target=target, change=change):
                    sidecar_path.write_text(json.dumps(evidence), encoding="utf-8")
                    for uri, snapshot in originals.items():
                        self.backend.blobs[uri] = copy.deepcopy(snapshot)
                    reads = []

                    def changing(method, url, allow_missing=False, extra_headers=None):
                        response = original_onelake(method, url, allow_missing, extra_headers)
                        if method == "GET" and url == target:
                            reads.append(url)
                            if len(reads) == 1:
                                value, etag = self.backend.blobs[url]
                                if change == "deleted":
                                    del self.backend.blobs[url]
                                else:
                                    value = copy.deepcopy(value)
                                    if change == "content":
                                        value["snapshot_counter"] = True
                                    if change == "etag":
                                        etag = '"another-version"'
                                    elif change == "missing-etag":
                                        etag = None
                                    self.backend.blobs[url] = (value, etag)
                        return response

                    with mock.patch.object(self.backend, "onelake", side_effect=changing):
                        _, result = self.cli("wait-monitor")
                    self.assertEqual((result["status"], result["ready"]), ("blocked", False), result)
                    self.assertEqual(len(reads), 2)
                    self.assertNotEqual(json.loads(sidecar_path.read_text())["state"], "completed")

    def test_changed_producer_configuration_cannot_reuse_monitor_start(self):
        self.monitor_setup()
        evidence = self.start_monitor()
        self.complete_monitor(evidence)
        original = copy.deepcopy(self.config["data"])
        for field, value in (("seed", 43), ("product_count", 3), ("rows_per_file", 3),
                             ("interval_seconds", 29), ("file_count", 2)):
            with self.subTest(field=field):
                self.config["data"] = dict(original, **{field: value})
                self.backend.calls.clear()
                _, result = self.cli("wait-monitor")
                self.assertEqual((result["status"], result["ready"]), ("blocked", False), result)
                self.assertFalse(any(method == "STORAGE-GET" for method, _, _ in self.backend.calls))

    def test_resolved_producer_config_is_bound_to_monitor_identity_across_modes(self):
        self.monitor_setup()
        with mock.patch.object(deploy, "verify_producer_access", return_value=None):
            _, validated = self.cli("validate-only")
        self.assertEqual((validated["status"], validated["ready"]), ("pass", True))
        evidence = self.start_monitor()
        self.assertIn(f"/{SHARED_WORKSPACE}/{STAGING_LAKEHOUSE}/", evidence["manifest_uri"])
        self.assertIn(f"/{SHARED_WORKSPACE}/{STAGING_LAKEHOUSE}/", evidence["sentinel_uri"])
        _, running = self.cli("monitor-status")
        self.assertEqual((running["status"], running["ready"]), ("pass", True))
        original_config = copy.deepcopy(self.config)
        for field in ("workspace_id", "staging_lakehouse_id", "identity_id"):
            with self.subTest(field=field):
                self.config = copy.deepcopy(original_config)
                self.config["shared"][field] = str(uuid.UUID(int=999))
                self.backend.calls.clear()
                _, result = self.cli("monitor-status")
                self.assertEqual((result["status"], result["ready"]), ("blocked", False), result)
                self.assertFalse(any("jobs/instances" in path for _, path, _ in self.backend.calls))
        self.config = copy.deepcopy(original_config)
        self.config["tenant_id"] = str(uuid.UUID(int=999))
        _, wrong_tenant = self.cli("stop-monitor")
        self.assertEqual(wrong_tenant["status"], "blocked")
        self.config = copy.deepcopy(original_config)
        _, wrong_prefix = self.cli("stop-monitor", prefix="other")
        self.assertEqual(wrong_prefix["status"], "blocked")
        self.complete_monitor(evidence)
        _, finished = self.cli("wait-monitor")
        self.assertEqual((finished["status"], finished["ready"]), ("pass", True), finished)
        _, stopped = self.cli("stop-monitor")
        self.assertEqual((stopped["status"], stopped["ready"]), ("pass", False), stopped)
        self.assertFalse((self.state / "shared-producer-v1.json").exists())

    def test_wait_monitor_blocks_on_failure_or_absolute_readback_deadline(self):
        self.monitor_setup()
        evidence = self.start_monitor()
        self.backend.monitor_jobs[MONITOR_JOB]["status"] = "Failed"
        _, result = self.cli("wait-monitor")
        self.assertEqual((result["status"], result["ready"]), ("blocked", False))
        self.assertIn("Failed", result["diagnostics"][0])
        self.backend.monitor_jobs[MONITOR_JOB]["status"] = "InProgress"
        deadline = deploy._utc(evidence["deadline_utc"], "deadline") + timedelta(seconds=11)
        self.backend.calls.clear()
        with mock.patch.object(deploy, "_now", return_value=deadline):
            _, result = self.cli("wait-monitor")
        self.assertEqual((result["status"], result["ready"]), ("blocked", False))
        self.assertIn("readback deadline", result["diagnostics"][0])
        self.assertFalse(any("jobs/instances" in path for method, path, _ in self.backend.calls))

    def test_monitor_existing_writer_lock_or_state_blocks_submission(self):
        for suffix in ("state.json", "writer.lock"):
            with self.subTest(suffix=suffix):
                self.state = self.root / f"state-{suffix}"
                self.monitor_setup()
                path = (f"{deploy.ONELAKE}/{WORKSPACE}/{LAKEHOUSE}"
                        f"/Files/demo2-verification/runs/sample/{suffix}")
                self.backend.blobs[path] = ({"other": True}, '"occupied"')
                _, result = self.cli("start-monitor")
                self.assertEqual((result["status"], result["ready"]), ("blocked", False))
                self.assertIn("prior monitor", result["diagnostics"][0])
                self.assertFalse(any("jobs/execute/instances" in path and method == "POST"
                                     for method, path, _ in self.backend.calls))
                self.assertNotIn(MONITOR_NOTEBOOK, self.backend.items)

    def test_monitor_duplicate_start_is_idempotent_and_recovers_known_start_identity(self):
        self.monitor_setup()
        original = self.start_monitor()
        self.backend.calls.clear()
        again = self.start_monitor()
        self.assertEqual(again["job_id"], original["job_id"])
        self.assertFalse(any("jobs/execute/instances" in path and method == "POST"
                             for method, path, _ in self.backend.calls))
        ledger_path = self.state / deploy.STATE_FILE
        ledger = json.loads(ledger_path.read_text(encoding="utf-8"))
        ledger["monitor_job"]["job_id"] = None
        ledger_path.write_text(json.dumps(ledger), encoding="utf-8")
        sidecar = dict(original, job_id=None, state="submitting")
        (self.state / deploy.MONITOR_FILE).write_text(json.dumps(sidecar), encoding="utf-8")
        self.backend.calls.clear()
        _, result = self.cli("monitor-status")
        self.assertEqual((result["status"], result["ready"]), ("pass", True))
        self.assertEqual(json.loads(ledger_path.read_text())["monitor_job"]["job_id"], MONITOR_JOB)
        self.assertEqual(json.loads((self.state / deploy.MONITOR_FILE).read_text())["job_id"], MONITOR_JOB)
        self.assertFalse(any("jobs/execute/instances" in path and method == "POST"
                             for method, path, _ in self.backend.calls))

    def test_interrupted_monitor_notebook_creation_recovers_lro_and_lists_owned_notebook(self):
        self.monitor_setup()
        self.backend.flags.add("monitor_lro")
        original = self.backend.fabric

        def interrupted(method, path, body=None, allow_missing=False):
            if method == "GET" and path == f"/v1/operations/{OPERATION}":
                raise deploy.Blocked("test interruption")
            return original(method, path, body, allow_missing)

        with mock.patch.object(self.backend, "fabric", side_effect=interrupted):
            _, result = self.cli("start-monitor")
        self.assertEqual(result["status"], "blocked")
        pending = json.loads((self.state / deploy.STATE_FILE).read_text())["pending"]
        self.assertEqual((pending["key"], pending["operation_id"]), ("monitor_notebook", OPERATION))
        self.backend.calls.clear()
        self.backend.skills.clear()
        self.start_monitor()
        self.assertTrue(any(method == "LIST" and path.endswith("/items?type=Notebook")
                            for method, path, _ in self.backend.calls))
        self.assertFalse(any(method == "GET" and path == f"/v1/operations/{OPERATION}/result"
                             for method, path, _ in self.backend.calls))
        self.assertFalse(any(method == "POST" and isinstance(body, dict)
                             and body.get("displayName") == "Demo2Monitor"
                             for method, path, body in self.backend.calls))
        self.assertTrue(all(skill == "spark-cli" for skill in self.backend.skills))

    def test_monitor_timeout_after_confirmed_submission_retains_exact_job_identity(self):
        self.monitor_setup()
        self.backend.flags.add("no_heartbeat")
        with mock.patch.object(deploy.Deadline, "wait", side_effect=deploy.Blocked("test timeout")):
            _, result = self.cli("start-monitor")
        self.assertEqual((result["status"], result["ready"]), ("blocked", False))
        start = json.loads((self.state / deploy.MONITOR_START_FILE).read_text())
        self.assertEqual(start["job_id"], MONITOR_JOB)
        with mock.patch.object(deploy.Deadline, "wait"):
            _, result = self.cli("stop-monitor")
        self.assertEqual(result["status"], "blocked")
        self.assertEqual(self.backend.monitor_jobs[MONITOR_JOB]["status"], "Cancelled")
        self.assertIn("per-instance state", result["diagnostics"][0])

    def test_uncertain_monitor_submission_never_retries_or_cancels_by_guess(self):
        self.monitor_setup()
        self.backend.flags.add("uncertain_monitor_submit")
        _, first = self.cli("start-monitor")
        self.assertEqual((first["status"], first["ready"]), ("blocked", False))
        self.assertEqual(json.loads((self.state / deploy.MONITOR_FILE).read_text())["state"], "submitting")
        self.assertFalse((self.state / deploy.MONITOR_START_FILE).exists())
        self.backend.flags.remove("uncertain_monitor_submit")
        self.backend.calls.clear()
        _, retry = self.cli("start-monitor")
        _, stopped = self.cli("stop-monitor")
        self.assertEqual((retry["status"], stopped["status"]), ("blocked", "blocked"))
        self.assertIn("termination cannot be established", stopped["diagnostics"][0])
        self.assertFalse(any(method == "POST" and ("jobs/execute/instances" in path or path.endswith("/cancel"))
                             for method, path, _ in self.backend.calls))

    def test_accepted_job_without_persisted_start_record_is_not_guessed(self):
        self.monitor_setup()
        with mock.patch.object(deploy, "_atomic_create_json", side_effect=OSError("test disk failure")):
            _, result = self.cli("start-monitor")
        self.assertEqual((result["status"], result["ready"]), ("blocked", False))
        self.assertIn("accepted", result["diagnostics"][0])
        self.assertEqual(self.backend.monitor_jobs[MONITOR_JOB]["status"], "InProgress")
        self.assertFalse((self.state / deploy.MONITOR_START_FILE).exists())
        self.backend.calls.clear()
        _, result = self.cli("stop-monitor")
        self.assertEqual(result["status"], "blocked")
        self.assertIn("termination cannot be established", result["diagnostics"][0])
        self.assertFalse(any(path.endswith("/cancel") for method, path, _ in self.backend.calls))

    def test_stop_uses_immutable_start_record_even_when_status_sidecar_is_tampered(self):
        self.monitor_setup()
        evidence = self.start_monitor()
        start_path = self.state / deploy.MONITOR_START_FILE
        original_start = start_path.read_bytes()
        other = str(uuid.uuid4())
        self.backend.monitor_jobs[other] = {"id": other, "itemId": MONITOR_NOTEBOOK, "status": "InProgress"}
        evidence["job_id"] = other
        (self.state / deploy.MONITOR_FILE).write_text(json.dumps(evidence), encoding="utf-8")
        self.backend.calls.clear()
        _, result = self.cli("monitor-status")
        self.assertEqual(result["status"], "blocked")
        with mock.patch.object(deploy.Deadline, "wait"):
            _, result = self.cli("stop-monitor")
        self.assertEqual((result["status"], result["ready"]), ("pass", False))
        cancelled = [path for method, path, _ in self.backend.calls if method == "POST" and path.endswith("/cancel")]
        self.assertEqual(cancelled, [f"/v1/workspaces/{WORKSPACE}/items/{MONITOR_NOTEBOOK}"
                                     f"/jobs/instances/{MONITOR_JOB}/cancel"])
        self.assertEqual(self.backend.monitor_jobs[other]["status"], "InProgress")
        self.assertEqual(start_path.read_bytes(), original_start)
        self.assertIn("sidecar was preserved", result["diagnostics"][0])
        self.assertEqual(json.loads((self.state / deploy.MONITOR_FILE).read_text())["job_id"], other)

    def test_stop_blocks_without_exact_immutable_start_record(self):
        for mutation in ("missing", "corrupt", "different-job"):
            with self.subTest(mutation=mutation):
                self.state = self.root / f"state-{mutation}"
                self.monitor_setup()
                self.start_monitor()
                path = self.state / deploy.MONITOR_START_FILE
                if mutation == "missing":
                    path.unlink()
                elif mutation == "corrupt":
                    path.write_text("{", encoding="utf-8")
                else:
                    value = json.loads(path.read_text())
                    value["job_id"] = str(uuid.uuid4())
                    path.write_text(json.dumps(value), encoding="utf-8")
                self.backend.calls.clear()
                _, result = self.cli("stop-monitor")
                self.assertEqual((result["status"], result["ready"]), ("blocked", False))
                self.assertIn("termination cannot be established", result["diagnostics"][0])
                self.assertFalse(any(path.endswith("/cancel") for method, path, _ in self.backend.calls))

    def test_stop_cancels_and_confirms_exact_parent_and_recorded_children(self):
        self.monitor_setup()
        evidence = self.start_monitor()
        state, etag = self.backend.blobs[evidence["state_uri"]]
        state["batches"]["sample_0001"] = {"status": "running", "job_id": CHILD_JOB}
        self.backend.child_jobs[CHILD_JOB] = {"id": CHILD_JOB, "itemId": NOTEBOOK, "status": "InProgress"}
        unrelated = str(uuid.uuid4())
        self.backend.child_jobs[unrelated] = {"id": unrelated, "itemId": NOTEBOOK, "status": "InProgress"}
        self.backend.calls.clear()
        with mock.patch.object(deploy.Deadline, "wait"):
            _, result = self.cli("stop-monitor")
        self.assertEqual((result["status"], result["ready"]), ("pass", False), result)
        cancel_paths = [path for method, path, _ in self.backend.calls
                        if method == "POST" and path.endswith("/cancel")]
        self.assertEqual(cancel_paths, [
            f"/v1/workspaces/{WORKSPACE}/items/{MONITOR_NOTEBOOK}/jobs/instances/{MONITOR_JOB}/cancel",
            f"/v1/workspaces/{WORKSPACE}/items/{NOTEBOOK}/jobs/instances/{CHILD_JOB}/cancel",
        ])
        self.assertEqual(self.backend.child_jobs[unrelated]["status"], "InProgress")
        self.assertNotIn(deploy._lock_uri(evidence["state_uri"]), self.backend.blobs)
        self.assertEqual(json.loads((self.state / deploy.MONITOR_FILE).read_text())["state"], "stopped")
        ledger = json.loads((self.state / deploy.STATE_FILE).read_text())
        self.assertEqual(ledger["monitor_stop"]["state"], "stopped")

    def test_stop_checks_job_ids_even_when_records_say_failed_or_verified(self):
        self.monitor_setup()
        evidence = self.start_monitor()
        state, _ = self.backend.blobs[evidence["state_uri"]]
        run_job = str(uuid.uuid4())
        state["state"] = "failed"
        state["batches"]["sample_0001"] = {"status": "failed", "job_id": CHILD_JOB}
        state["run_verification"] = {"status": "verified", "job_id": run_job}
        for job_id in (CHILD_JOB, run_job):
            self.backend.child_jobs[job_id] = {"id": job_id, "itemId": NOTEBOOK, "status": "InProgress"}
        self.backend.calls.clear()
        with mock.patch.object(deploy.Deadline, "wait"):
            _, result = self.cli("stop-monitor")
        self.assertEqual((result["status"], result["ready"]), ("pass", False))
        cancelled = [path for method, path, _ in self.backend.calls if method == "POST" and path.endswith("/cancel")]
        self.assertEqual(len(cancelled), 3)
        self.assertTrue(any(f"/{CHILD_JOB}/cancel" in path for path in cancelled))
        self.assertTrue(any(f"/{run_job}/cancel" in path for path in cancelled))

    def test_stop_refuses_ambiguous_children_or_missing_final_state(self):
        for case in ("submitting", "failed-uncertain", "missing-job", "missing-state"):
            with self.subTest(case=case):
                self.state = self.root / f"state-{case}"
                self.monitor_setup()
                evidence = self.start_monitor()
                state, etag = self.backend.blobs[evidence["state_uri"]]
                if case == "missing-state":
                    del self.backend.blobs[evidence["state_uri"]]
                elif case == "failed-uncertain":
                    state["batches"]["sample_0001"] = {"status": "failed", "submission_uncertain": True}
                    state["batches"]["sample_0002"] = {"status": "running", "job_id": CHILD_JOB}
                    self.backend.child_jobs[CHILD_JOB] = {"id": CHILD_JOB, "itemId": NOTEBOOK,
                                                          "status": "InProgress"}
                else:
                    state["batches"]["sample_0001"] = {"status": "submitting" if case == "submitting" else "running"}
                with mock.patch.object(deploy.Deadline, "wait"):
                    _, result = self.cli("stop-monitor")
                self.assertEqual((result["status"], result["ready"]), ("blocked", False))
                self.assertIn(deploy._lock_uri(evidence["state_uri"]), self.backend.blobs)
                self.assertNotEqual(json.loads((self.state / deploy.MONITOR_FILE).read_text())["state"], "stopped")
                if case == "failed-uncertain":
                    self.assertEqual(self.backend.child_jobs[CHILD_JOB]["status"], "Cancelled")

    def test_uncertain_cancellation_is_never_reissued_and_can_be_reconciled(self):
        self.monitor_setup()
        self.start_monitor()
        self.backend.flags.add("uncertain_cancel")
        self.backend.calls.clear()
        _, first = self.cli("stop-monitor")
        self.assertEqual(first["status"], "blocked")
        self.backend.flags.remove("uncertain_cancel")
        with mock.patch.object(deploy.Deadline, "wait", side_effect=deploy.Blocked("test deadline")):
            _, second = self.cli("stop-monitor")
        self.assertEqual(second["status"], "blocked")
        cancellations = [path for method, path, _ in self.backend.calls if method == "POST" and path.endswith("/cancel")]
        self.assertEqual(len(cancellations), 1)
        self.backend.monitor_jobs[MONITOR_JOB]["status"] = "Cancelled"
        _, reconciled = self.cli("stop-monitor")
        self.assertEqual((reconciled["status"], reconciled["ready"]), ("pass", False))

    def test_child_cancellation_marker_is_not_a_terminal_state(self):
        self.monitor_setup()
        evidence = self.start_monitor()
        state, _ = self.backend.blobs[evidence["state_uri"]]
        state["batches"]["sample_0001"] = {"status": "failed", "job_id": CHILD_JOB,
                                            "cancel_requested": True}
        self.backend.child_jobs[CHILD_JOB] = {"id": CHILD_JOB, "itemId": NOTEBOOK, "status": "InProgress"}
        self.backend.calls.clear()
        with mock.patch.object(deploy.Deadline, "wait", side_effect=[None, deploy.Blocked("test timeout")]):
            _, result = self.cli("stop-monitor")
        self.assertEqual(result["status"], "blocked")
        self.assertEqual(self.backend.monitor_jobs[MONITOR_JOB]["status"], "Cancelled")
        self.assertEqual(self.backend.child_jobs[CHILD_JOB]["status"], "InProgress")
        self.assertFalse(any(method == "POST" and f"/{CHILD_JOB}/cancel" in path
                             for method, path, _ in self.backend.calls))
        self.assertIn(deploy._lock_uri(evidence["state_uri"]), self.backend.blobs)
        self.backend.child_jobs[CHILD_JOB]["status"] = "Cancelled"
        _, result = self.cli("stop-monitor")
        self.assertEqual((result["status"], result["ready"]), ("pass", False))

    def test_status_and_stop_reject_a_mismatched_fabric_job_readback(self):
        self.monitor_setup()
        self.start_monitor()
        self.backend.monitor_jobs[MONITOR_JOB]["itemId"] = NOTEBOOK
        self.backend.calls.clear()
        _, status = self.cli("monitor-status")
        _, stopped = self.cli("stop-monitor")
        self.assertEqual((status["status"], stopped["status"]), ("blocked", "blocked"))
        self.assertFalse(any(path.endswith("/cancel") for method, path, _ in self.backend.calls))

    def test_result_cannot_overwrite_monitor_start_record(self):
        self.monitor_setup()
        self.start_monitor()
        start = self.state / deploy.MONITOR_START_FILE
        original = start.read_bytes()
        args = ["--mode", "dry-run", "--config", str(self.config_path), "--prefix", "chanel",
                "--state-dir", str(self.state), "--result", str(start)]
        with contextlib.redirect_stdout(io.StringIO()):
            code = deploy.main(args)
        self.assertEqual(code, 1)
        self.assertEqual(start.read_bytes(), original)

    def test_new_run_requires_terminal_reconciled_prior_monitor_and_archives_start_record(self):
        self.monitor_setup()
        original = self.start_monitor()
        self.config["data"]["run_id"] = "next_run"
        self.backend.calls.clear()
        _, result = self.cli("start-monitor")
        self.assertEqual(result["status"], "blocked")
        self.assertFalse(any(method == "POST" and "jobs/execute/instances" in path
                             for method, path, _ in self.backend.calls))
        self.config["data"]["run_id"] = "sample"
        with mock.patch.object(deploy.Deadline, "wait"):
            _, result = self.cli("stop-monitor")
        self.assertEqual(result["status"], "pass")
        self.config["data"]["run_id"] = "next_run"
        _, result = self.cli("start-monitor")
        self.assertEqual((result["status"], result["ready"]), ("pass", True), result)
        archived = self.state / f"demo-2-monitor-start.sample.{MONITOR_JOB}.json"
        self.assertEqual(json.loads(archived.read_text())["job_id"], original["job_id"])
        current = json.loads((self.state / deploy.MONITOR_START_FILE).read_text())
        self.assertEqual((current["run_id"], current["job_id"]), ("next_run", NEXT_MONITOR_JOB))

    def test_os_ledger_lock_excludes_other_processes_and_remains_reusable(self):
        settings = deploy.validate_config(self.config, "chanel")
        probe = """
import json
import sys
from pathlib import Path
import deploy
settings = deploy.validate_config(json.loads(sys.argv[2]), "chanel")
try:
    with deploy.Ledger(Path(sys.argv[1]), settings):
        print("acquired")
except deploy.Blocked:
    print("blocked")
"""
        args = [sys.executable, "-c", probe, str(self.state), json.dumps(self.config)]
        with deploy.Ledger(self.state, settings, create=True):
            held = subprocess.run(args, cwd=Path(__file__).parent, capture_output=True, text=True, timeout=15)
            self.assertEqual((held.returncode, held.stdout.strip()), (0, "blocked"), held.stderr)
            self.assertTrue((self.state / deploy.LOCK_FILE / deploy.LOCK_LEASE).is_file())
        available = subprocess.run(args, cwd=Path(__file__).parent, capture_output=True, text=True, timeout=15)
        self.assertEqual((available.returncode, available.stdout.strip()), (0, "acquired"), available.stderr)
        self.assertTrue((self.state / deploy.LOCK_FILE / deploy.LOCK_LEASE).is_file())

    def hold_ledger_in_subprocess(self):
        ready = self.root / f"lock-holder-{uuid.uuid4().hex}"
        script = """
import json
import sys
import time
from pathlib import Path
import deploy
settings = deploy.validate_config(json.loads(sys.argv[2]), "chanel")
with deploy.Ledger(Path(sys.argv[1]), settings):
    Path(sys.argv[3]).write_text("held", encoding="utf-8")
    time.sleep(3600)
"""
        process = subprocess.Popen(
            [sys.executable, "-c", script, str(self.state), json.dumps(self.config), str(ready)],
            cwd=Path(__file__).parent, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        )
        until = time.monotonic() + 15
        while not ready.exists() and process.poll() is None and time.monotonic() < until:
            time.sleep(0.05)
        if not ready.exists():
            if process.poll() is None:
                process.kill()
            _, stderr = process.communicate(timeout=15)
            self.fail(f"The child process did not acquire the ledger lock: {stderr}")
        return process

    def test_killed_monitor_lock_holder_releases_ledger_for_exact_stop(self):
        self.monitor_setup()
        evidence = self.start_monitor()
        start = (self.state / deploy.MONITOR_START_FILE).read_bytes()
        process = self.hold_ledger_in_subprocess()
        try:
            self.backend.calls.clear()
            _, result = self.cli("stop-monitor")
            self.assertEqual((result["status"], result["ready"]), ("blocked", False), result)
            self.assertFalse(any(path.endswith("/cancel") for method, path, _ in self.backend.calls))
            self.assertEqual(self.backend.monitor_jobs[evidence["job_id"]]["status"], "InProgress")
        finally:
            if process.poll() is None:
                process.kill()
            process.communicate(timeout=15)
        self.assertTrue((self.state / deploy.LOCK_FILE / deploy.LOCK_LEASE).is_file())
        _, result = self.cli("stop-monitor")
        self.assertEqual((result["status"], result["ready"]), ("pass", False), result)
        self.assertEqual(self.backend.monitor_jobs[evidence["job_id"]]["status"], "Cancelled")
        self.assertEqual((self.state / deploy.MONITOR_START_FILE).read_bytes(), start)

    def test_terminated_lock_holder_does_not_resolve_uncertain_monitor_submission(self):
        self.monitor_setup()
        self.backend.flags.add("uncertain_monitor_submit")
        _, result = self.cli("start-monitor")
        self.assertEqual((result["status"], result["ready"]), ("blocked", False), result)
        self.assertFalse((self.state / deploy.MONITOR_START_FILE).exists())
        process = self.hold_ledger_in_subprocess()
        process.kill()
        process.communicate(timeout=15)
        self.backend.flags.remove("uncertain_monitor_submit")
        self.backend.calls.clear()
        _, stopped = self.cli("stop-monitor")
        _, retried = self.cli("start-monitor")
        self.assertEqual((stopped["status"], retried["status"]), ("blocked", "blocked"))
        self.assertIn("termination cannot be established", stopped["diagnostics"][0])
        self.assertFalse(any(method == "POST" and ("jobs/execute/instances" in path or path.endswith("/cancel"))
                             for method, path, _ in self.backend.calls))

    def test_unrecognized_legacy_lock_is_never_removed_or_assumed_stale(self):
        self.state.mkdir()
        lock = self.state / deploy.LOCK_FILE
        lock.write_bytes(b"")
        settings = deploy.validate_config(self.config, "chanel")
        with self.assertRaisesRegex(deploy.Blocked, "legacy or unrecognized"):
            with deploy.Ledger(self.state, settings, create=True):
                self.fail("An unowned legacy lock was adopted.")
        self.assertTrue(lock.is_file())
        self.assertEqual(lock.read_bytes(), b"")
        self.assertFalse((self.state / deploy.STATE_FILE).exists())

    def test_result_cannot_replace_the_open_os_lock_lease(self):
        self.config_path.write_text(json.dumps(self.config), encoding="utf-8")
        settings = deploy.validate_config(self.config, "chanel")
        with deploy.Ledger(self.state, settings, create=True):
            lease = self.state / deploy.LOCK_FILE / deploy.LOCK_LEASE
            before = lease.stat()
            args = ["--mode", "dry-run", "--config", str(self.config_path), "--prefix", "chanel",
                    "--state-dir", str(self.state), "--result", str(lease)]
            with contextlib.redirect_stdout(io.StringIO()):
                code = deploy.main(args)
            self.assertEqual(code, 1)
            after = lease.stat()
            self.assertEqual((before.st_dev, before.st_ino), (after.st_dev, after.st_ino))
        self.assertTrue(lease.is_file())


class ProtocolTests(unittest.TestCase):
    def test_deadline_never_extends_retry_after(self):
        clock = [0.0]
        deadline = deploy.Deadline(5, clock=lambda: clock[0],
                                   sleeper=lambda seconds: clock.__setitem__(0, clock[0] + seconds))
        with self.assertRaises(deploy.Blocked):
            deadline.wait(deploy._retry_delay("30", 1))
        self.assertEqual(clock[0], 5.0)
        with self.assertRaises(deploy.Blocked):
            deadline.remaining()

    def test_per_batch_deadline_overrides_global_and_is_capped_by_track_deadline(self):
        clock = [0.0]
        sleeper = lambda seconds: clock.__setitem__(0, clock[0] + seconds)
        overall = deploy.Deadline(100, clock=lambda: clock[0], sleeper=sleeper, label="track")
        clock[0] = 60.0
        verify = overall.child(15)
        self.assertEqual(verify.remaining(), 15)
        with self.assertRaisesRegex(deploy.Blocked, "verification deadline"):
            verify.wait(20)
        self.assertEqual(clock[0], 75.0)
        later = overall.child(30)
        self.assertEqual(later.remaining(), 25)
        with self.assertRaisesRegex(deploy.Blocked, "track deadline"):
            later.wait(30)
        self.assertEqual(clock[0], 100.0)

    def test_kql_polling_expires_at_per_batch_deadline(self):
        clock = [0.0]
        overall = deploy.Deadline(
            100, clock=lambda: clock[0], sleeper=lambda seconds: clock.__setitem__(0, clock[0] + seconds)
        )
        settings = deploy.validate_config(configuration(), "chanel")
        settings["timeout"] = 3

        class PendingClient:
            deadline = overall
            calls = 0

            def kusto(self, endpoint, database, command, query):
                self.calls += 1
                return [{"RowCount": 0, "DistinctSalesLineIDs": 0, "AmountMinorUnits": 0, "InvalidRows": 0}]

        client = PendingClient()
        ledger = types.SimpleNamespace(data={"resources": {}, "owner_marker": "test"})
        deployment = deploy.Deployment(settings, client, overall, ledger)
        with self.assertRaises(deploy.Blocked):
            with deployment.verification_deadline():
                deployment.verify_batch({"query_uri": QUERY_URI, "name": "SalesDatabase"}, verification_batch())
        self.assertEqual(clock[0], 3)
        self.assertEqual(client.calls, 3)
        self.assertIs(client.deadline, overall)
        self.assertIs(deployment.deadline, overall)

    def test_notebook_nested_batch_evidence_must_match_exactly(self):
        batch = verification_batch()
        summary = {key: batch[key] for key in ("batch_id", "row_count", "sales_amount_minor_units")}
        result = {"status": "verified", "mode": "verify_batch", "batch": summary}
        deploy._verify_notebook_exit({"properties": {"exitValue": json.dumps(result)}}, "verify_batch", batch)
        for field, wrong in (("batch_id", "other"), ("row_count", True),
                             ("sales_amount_minor_units", batch["sales_amount_minor_units"] + 1)):
            with self.subTest(field=field):
                altered = copy.deepcopy(result)
                altered["batch"][field] = wrong
                with self.assertRaises(deploy.Blocked):
                    deploy._verify_notebook_exit({"properties": {"exitValue": json.dumps(altered)}}, "verify_batch", batch)
        with self.assertRaises(deploy.Blocked):
            deploy._verify_notebook_exit({"exitValue": json.dumps(result)}, "verify_batch", batch)
        with self.assertRaises(deploy.Blocked):
            deploy._verify_notebook_exit(
                {"properties": {"exitValue": json.dumps({"status": "verified", "mode": "verify_batch",
                                                          "batch_id": batch["batch_id"]})}},
                "verify_batch", batch,
            )

    def test_kusto_partial_failure_is_not_success(self):
        response = {"Tables": [
            {"TableName": "PrimaryResult", "Columns": [{"ColumnName": "Count"}], "Rows": [[1]]},
            {"TableName": "QueryStatus", "Columns": [{"ColumnName": "Severity"}], "Rows": [[2]]},
        ]}
        with self.assertRaises(deploy.Failure):
            deploy._kusto_rows(response)

    def test_untrusted_poll_and_job_urls_are_rejected(self):
        with self.assertRaises(deploy.Failure):
            deploy._operation_id({"location": f"https://attacker.example/v1/operations/{OPERATION}"})
        with self.assertRaises(deploy.Failure):
            deploy._operation_id({"location": f"{deploy.FABRIC}/v1/operations/{OPERATION}",
                                  "x-ms-operation-id": DATABASE})
        with self.assertRaises(deploy.Failure):
            deploy._job_id(f"{deploy.FABRIC}/v1/workspaces/{WORKSPACE}/items/{EVENTHOUSE}"
                           f"/jobs/instances/{OPERATION}", WORKSPACE, NOTEBOOK)
        with self.assertRaises(deploy.Failure):
            deploy._service_uri("https://cluster.kusto.fabric.microsoft.com.evil.example", "endpoint")

    def test_azure_cli_respects_existing_config_directory(self):
        env_value = str(ROOT / "isolated-azure-config")
        response = types.SimpleNamespace(returncode=0, stdout=json.dumps({
            "tenant": TENANT, "accessToken": "token-for-test-only",
        }))
        with mock.patch.dict(os.environ, {"AZURE_CONFIG_DIR": env_value}), \
                mock.patch.object(deploy.shutil, "which", return_value="az"), \
                mock.patch.object(deploy.subprocess, "run", return_value=response) as run:
            client = deploy.FabricClient(TENANT, deploy.Deadline(30))
            self.assertEqual(client._token(deploy.FABRIC), "token-for-test-only")
            self.assertEqual(os.environ["AZURE_CONFIG_DIR"], env_value)
        args, kwargs = run.call_args
        self.assertIn("--tenant", args[0])
        self.assertNotIn("env", kwargs)
        self.assertNotIn("shell", kwargs)
        self.assertNotIn("login", args[0])

    def test_wrong_tenant_token_is_blocked_without_leaking_it(self):
        response = types.SimpleNamespace(returncode=0, stdout=json.dumps({
            "tenant": DATABASE, "accessToken": "sensitive-token",
        }))
        with mock.patch.object(deploy.shutil, "which", return_value="az"), \
                mock.patch.object(deploy.subprocess, "run", return_value=response):
            client = deploy.FabricClient(TENANT, deploy.Deadline(30))
            with self.assertRaises(deploy.Blocked) as error:
                client._token(deploy.FABRIC)
        self.assertNotIn("sensitive-token", str(error.exception))

    def test_monitor_storage_uses_storage_audience_and_spark_telemetry(self):
        class Response:
            status = 200
            headers = {}

            def __enter__(self):
                return self

            def __exit__(self, *_):
                return False

            def read(self, _):
                return b"{}"

        client = deploy.FabricClient(TENANT, deploy.Deadline(30))
        url = f"{deploy.ONELAKE}/{WORKSPACE}/{LAKEHOUSE}/Files/demo2-verification/runs/sample/state.json"
        with mock.patch.object(client, "_token", return_value="not-a-real-token") as token, \
                mock.patch.object(client.opener, "open", return_value=Response()) as send:
            client.onelake("GET", url)
            token.assert_called_with(deploy.STORAGE_AUDIENCE)
            headers = {key.lower(): value for key, value in send.call_args.args[0].header_items()}
            self.assertEqual(headers["x-ms-version"], "2020-10-02")
            self.assertIn("x-ms-date", headers)
            with deploy._spark_client(client):
                client.fabric("GET", f"/v1/workspaces/{WORKSPACE}")
            headers = {key.lower(): value for key, value in send.call_args.args[0].header_items()}
            self.assertEqual(headers["x-ms-fabric-skill"], "spark-cli")
            self.assertEqual(client.fabric_skill, "eventhouse-cli")
        with self.assertRaises(deploy.Failure):
            client.onelake("GET", f"https://attacker.example/{WORKSPACE}/state.json")


if __name__ == "__main__":
    unittest.main()
