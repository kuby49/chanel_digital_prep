import contextlib
import copy
from datetime import date
import io
import json
import os
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

import deploy
import managed_refresh
from customer_validation import _sql_readback, validate_customer
from fresh_deploy import begin, confirm, record_intent, record_response
from ownership import STATE_FILE, CandidateBlocked
from test_deploy import candidate_config, INTENT, OWNER, WORKSPACE, LAKEHOUSE


class CustomerValidationTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.config = candidate_config()
        self.journal = begin(self.config, "chanel", self.root, OWNER)
        for kind, resource_id, path in (
            ("Workspace", WORKSPACE, "/v1/workspaces"),
            ("Lakehouse", LAKEHOUSE, f"/v1/workspaces/{WORKSPACE}/lakehouses"),
        ):
            expected = self.journal["expected"]
            name = expected["workspace_name" if kind == "Workspace" else "lakehouse_name"]
            request = {"displayName": name, "description": self.journal["owner"]["marker"]}
            result = {"id": resource_id, "displayName": name, "type": kind}
            live = {**result, "description": request["description"]}
            if kind == "Workspace":
                request["capacityId"] = expected["capacity_id"]
                live.update(capacityId=expected["capacity_id"], capacityAssignmentProgress="Completed")
            else:
                request["creationPayload"] = {"enableSchemas": True}
                result["workspaceId"] = WORKSPACE
                live.update(workspaceId=WORKSPACE, properties={"defaultSchema": "dbo", "sqlEndpointProperties": {}})
            intent = record_intent(self.root, self.journal, kind, request, path=path)
            record_response(self.root, self.journal, intent, 201, result, body_read=True)
            confirm(self.root, self.journal, intent, result, live)
        self.config["demo-1"].update(
            readiness_scope="customer-run", workspace_name=self.journal["expected"]["workspace_name"],
            workspace_id=WORKSPACE, lakehouse_id=LAKEHOUSE, bronze_schema="bronze", bronze_view="sales_files",
            landing_uri=f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{LAKEHOUSE}/Files/incoming/sales/",
        )
        self.config["shared"] = {"identity_id": OWNER, "target_bindings": {
            "demo-1": {"workspace_id": WORKSPACE, "item_id": LAKEHOUSE, "fact": "silver.sales_joined"},
        }}
        self.config["data"] = {
            "run_id": "customer_demo", "file_count": 1, "rows_per_file": 10, "currency": "USD",
            "start_date": "2026-01-01", "date_count": 365, "product_count": 20, "customer_count": 100,
        }
        self.enterContext(patch.dict(os.environ, {"AZURE_CONFIG_DIR": str(self.root)}))
        self.client = self.enterContext(patch("customer_validation.FabricClient"))
        self.bootstrap = self.enterContext(patch("customer_validation._bootstrap"))
        self.sql = self.enterContext(patch("customer_validation._sql_readback"))
        self.read = self.enterContext(patch("customer_validation._object_response", side_effect=self.response))
        self.overrides = {}

    def response(self, client, path):
        if path in self.overrides:
            return {}, self.overrides[path]
        if path == "/v1/capacities":
            body = {"value": [{"id": self.config["capacity"]["id"], "state": "Active"}]}
        elif path.endswith("/roleAssignments"):
            body = {"value": [{"principal": {"id": OWNER}, "role": "Admin"}]}
        elif path.endswith("/instances"):
            body = {"value": [{"status": "Completed", "itemId": LAKEHOUSE, "startTimeUtc": "2026-11-24"}]}
        elif path.endswith("/schedules"):
            body = {"value": []}
        elif path.endswith("/" + WORKSPACE):
            body = self.journal["workspace"]["service_readback"]
        elif path.endswith("/" + LAKEHOUSE):
            body = self.journal["lakehouse"]["service_readback"]
        else:
            self.fail(f"Unexpected read-only path: {path}")
        return {}, body

    def test_customer_readiness_preserves_journal_and_never_submits_jobs(self):
        before = (self.root / STATE_FILE).read_bytes()
        diagnostics = validate_customer(self.config, "chanel", self.root)
        self.assertIn("no new batch delivery", " ".join(diagnostics))
        self.assertEqual((self.root / STATE_FILE).read_bytes(), before)
        self.client.return_value.request.assert_not_called()
        self.bootstrap.assert_called_once()
        self.sql.assert_called_once()

    def test_binding_and_receipt_corruption_fail_before_service_calls(self):
        variants = []
        landing = copy.deepcopy(self.config)
        landing["demo-1"]["landing_uri"] += "other/"
        variants.append((landing, None))
        target = copy.deepcopy(self.config)
        target["shared"]["target_bindings"]["demo-1"]["item_id"] = OWNER
        variants.append((target, None))
        damaged = copy.deepcopy(self.journal)
        damaged["workspace"]["accepted_response"]["status"] = 200
        variants.append((self.config, damaged))
        for config, journal in variants:
            with self.subTest(config=config, journal=journal):
                (self.root / STATE_FILE).write_text(json.dumps(journal or self.journal))
                with self.assertRaises(CandidateBlocked):
                    validate_customer(config, "chanel", self.root)
        self.read.assert_not_called()

    def test_active_refresh_and_missing_producer_role_block(self):
        root = f"/v1/workspaces/{WORKSPACE}"
        for path, body in (
            (root + "/roleAssignments", {"value": []}),
            (root + f"/lakehouses/{LAKEHOUSE}/jobs/refreshMaterializedLakeViews/instances",
             {"value": [{"status": "InProgress", "itemId": LAKEHOUSE}]}),
        ):
            self.overrides = {path: body}
            with self.subTest(path=path), self.assertRaises(CandidateBlocked):
                validate_customer(self.config, "chanel", self.root)
        self.sql.assert_not_called()

    def test_customer_admission_checks_paginated_jobs_and_schedules(self):
        root = f"/v1/workspaces/{WORKSPACE}/lakehouses/{LAKEHOUSE}/jobs/refreshMaterializedLakeViews"
        for suffix, later in (
            ("instances", {"id": INTENT, "status": "InProgress", "itemId": LAKEHOUSE}),
            ("schedules", {"id": INTENT, "enabled": True}),
        ):
            path = root + "/" + suffix
            first = ({"id": OWNER, "status": "Completed", "itemId": LAKEHOUSE,
                      "startTimeUtc": "2026-11-24"} if suffix == "instances"
                     else {"id": OWNER, "enabled": False})
            self.overrides = {path: {
                "value": [first], "continuationToken": "token",
                "continuationUri": "https://api.fabric.microsoft.com" + path + "?continuationToken=token",
            }}
            self.client.return_value.request.return_value = (200, {}, {"value": [later]}, True)
            with self.subTest(suffix=suffix), self.assertRaises(CandidateBlocked):
                validate_customer(self.config, "chanel", self.root)
        self.sql.assert_not_called()

    def test_sql_failure_never_reports_readiness(self):
        self.sql.side_effect = CandidateBlocked("Live dimension readback failed")
        with self.assertRaisesRegex(CandidateBlocked, "dimension readback"):
            validate_customer(self.config, "chanel", self.root)

    def test_capacity_failed_refresh_and_competing_schedule_block(self):
        refresh = f"/v1/workspaces/{WORKSPACE}/lakehouses/{LAKEHOUSE}/jobs/refreshMaterializedLakeViews"
        for path, body in (
            ("/v1/capacities", {"value": [{"id": self.config["capacity"]["id"], "state": "Paused"}]}),
            (refresh + "/instances", {"value": [{"status": "Failed", "itemId": LAKEHOUSE}]}),
            (refresh + "/schedules", {"value": [{"enabled": True}]}),
        ):
            self.overrides = {path: body}
            with self.subTest(path=path), self.assertRaises(CandidateBlocked):
                validate_customer(self.config, "chanel", self.root)
        self.sql.assert_not_called()

    def test_wrong_owner_and_unresolved_intent_block(self):
        for key, value in (("pending_operations", [{"id": OWNER}]),
                           ("owner", {**self.journal["owner"], "principal_id": WORKSPACE})):
            journal = copy.deepcopy(self.journal)
            journal[key] = value
            (self.root / STATE_FILE).write_text(json.dumps(journal))
            with self.subTest(key=key), self.assertRaises(CandidateBlocked):
                validate_customer(self.config, "chanel", self.root)
        self.read.assert_not_called()

    def test_validate_only_envelope_uses_real_preflight_result(self):
        config_path = self.root / "config.json"
        result_path = self.root / "result.json"
        config_path.write_text(json.dumps(self.config))
        command = ["deploy.py", "--mode", "validate-only", "--config", str(config_path),
                   "--prefix", "chanel", "--state-dir", str(self.root), "--result", str(result_path)]
        with patch("sys.argv", command), contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(deploy.main(), 0)
        result = json.loads(result_path.read_text())
        self.assertEqual((result["status"], result["ready"]), ("pass", True))

    def recurring(self):
        self.config["data"].update(file_count=10, rows_per_file=1000, interval_seconds=60)
        self.config["demo-1"]["deployment_state_dir"] = str(self.root)

    def schedule_transport(self):
        client = Mock()
        root = f"/v1/workspaces/{WORKSPACE}/lakehouses/{LAKEHOUSE}/jobs/refreshMaterializedLakeViews"
        def mutate(method, path, body):
            state = json.loads(managed_refresh.schedule_file(self.root, self.config).read_text())
            if method == "POST":
                self.assertEqual(state["status"], "pending")
                self.assertEqual(path, root + "/schedules")
                live = {"id": INTENT, "owner": {"id": OWNER, "type": "User"}, **copy.deepcopy(body)}
                self.overrides[root + "/schedules"] = {"value": [live]}
                self.overrides[root + "/schedules/" + INTENT] = live
                return 201, {}, live, True
            self.assertEqual((method, path, state["status"]), ("PATCH", root + "/schedules/" + INTENT, "stop-pending"))
            self.overrides[path] = {**self.overrides[path], **body}
            self.overrides[root + "/schedules"] = {"value": [self.overrides[path]]}
            return 200, {}, self.overrides[path], True
        client.request.side_effect = mutate
        self.enterContext(patch("managed_refresh.FabricClient", return_value=client))
        self.enterContext(patch("managed_refresh._object_response", side_effect=self.response))
        return client, root

    def test_native_recurring_start_admission_and_safe_stop(self):
        self.recurring()
        client, root = self.schedule_transport()
        original = (self.root / STATE_FILE).read_bytes()
        with self.assertRaises(CandidateBlocked):
            validate_customer(self.config, "chanel", self.root)
        started = managed_refresh.manage(self.config, "chanel", self.root, "start", yes=True)
        self.assertTrue(started["enabled"])
        self.assertEqual(started["request"]["configuration"]["interval"], 5)
        diagnostics = validate_customer(self.config, "chanel", self.root)
        self.assertIn("five minutes", " ".join(diagnostics))
        self.assertEqual((self.root / STATE_FILE).read_bytes(), original)
        self.overrides[root + "/instances"] = {
            "value": [{"id": LAKEHOUSE, "itemId": LAKEHOUSE, "status": "InProgress", "invokeType": "Scheduled"}],
        }
        with self.assertRaisesRegex(CandidateBlocked, "already active"):
            validate_customer(self.config, "chanel", self.root)
        stopped = managed_refresh.manage(self.config, "chanel", self.root, "stop", yes=True)
        self.assertEqual((stopped["status"], stopped["enabled"]), ("blocked", False))
        self.assertEqual(stopped["active_lineage_jobs"], [LAKEHOUSE])
        self.assertEqual(stopped["lineage_jobs"][0]["invokeType"], "Scheduled")
        self.assertIn("wait", stopped["diagnostics"][0])
        del self.overrides[root + "/instances"]
        stopped = managed_refresh.manage(self.config, "chanel", self.root, "stop", yes=True)
        self.assertEqual(stopped["status"], "pass")
        self.assertEqual([call.args[0] for call in client.request.call_args_list], ["POST", "PATCH"])
        with self.assertRaises(CandidateBlocked):
            validate_customer(self.config, "chanel", self.root)

    def test_recurring_unknown_owner_competitor_and_new_run_block(self):
        self.recurring()
        client, root = self.schedule_transport()
        managed_refresh.manage(self.config, "chanel", self.root, "start", yes=True)
        live = self.overrides[root + "/schedules/" + INTENT]
        self.overrides[root + "/schedules"] = {"value": [live, {"id": WORKSPACE, "enabled": True}]}
        with self.assertRaisesRegex(CandidateBlocked, "competing"):
            validate_customer(self.config, "chanel", self.root)
        self.overrides[root + "/schedules"] = {"value": [live]}
        live["owner"] = {"id": WORKSPACE}
        with self.assertRaisesRegex(CandidateBlocked, "owner"):
            managed_refresh.manage(self.config, "chanel", self.root, "stop", yes=True)
        self.assertEqual(client.request.call_count, 1)
        live["owner"] = {"id": OWNER}
        self.config["data"]["run_id"] = "another_run"
        with self.assertRaises(CandidateBlocked):
            validate_customer(self.config, "chanel", self.root)

    def test_native_export_is_local_sanitized_and_preserves_original_journals(self):
        self.recurring()
        client, _ = self.schedule_transport()
        managed_refresh.manage(self.config, "chanel", self.root, "start", yes=True)
        paths = (self.root / STATE_FILE, managed_refresh.schedule_file(self.root, self.config))
        before = [path.read_bytes() for path in paths]
        client.request.reset_mock()
        with patch("managed_refresh.FabricClient", side_effect=AssertionError("Must not acquire a token")):
            result = managed_refresh.manage(self.config, "chanel", self.root, "export-native")
        self.assertEqual(result["status"], "pass")
        receipt = result["native_schedule"]
        self.assertEqual(set(receipt), {
            "contract_version", "track", "status", "setup_run_id", "schedule_id", "tenant_id",
            "principal_id", "workspace_id", "lakehouse_id", "request", "create_response",
        })
        self.assertEqual((receipt["schedule_id"], receipt["setup_run_id"]), (INTENT, "customer_demo"))
        self.assertEqual(receipt["create_response"]["owner"], {"id": OWNER, "type": "User"})
        self.assertEqual([path.read_bytes() for path in paths], before)
        client.request.assert_not_called()
        record = json.loads(paths[1].read_text())
        record["status"] = "stop-pending"
        paths[1].write_text(json.dumps(record))
        with self.assertRaisesRegex(CandidateBlocked, "confirmed setup"):
            managed_refresh.manage(self.config, "chanel", self.root, "export-native")

    def test_ambiguous_schedule_post_is_not_repeated(self):
        self.recurring()
        client, _ = self.schedule_transport()
        client.request.side_effect = CandidateBlocked("Lost create response; reconcile manually.")
        with self.assertRaisesRegex(CandidateBlocked, "Lost create"):
            managed_refresh.manage(self.config, "chanel", self.root, "start", yes=True)
        path = managed_refresh.schedule_file(self.root, self.config)
        self.assertEqual(json.loads(path.read_text())["status"], "pending")
        with self.assertRaisesRegex(CandidateBlocked, "never repeat"):
            managed_refresh.manage(self.config, "chanel", self.root, "start", yes=True)
        self.assertEqual(client.request.call_count, 1)

    def test_stop_persists_response_before_independent_readback(self):
        self.recurring()
        client, root = self.schedule_transport()
        managed_refresh.manage(self.config, "chanel", self.root, "start", yes=True)
        path = managed_refresh.schedule_file(self.root, self.config)
        original = json.loads(path.read_text())
        mutate = client.request.side_effect

        def patch_schedule(method, endpoint, payload):
            status, _, reply, readable = mutate(method, endpoint, payload)
            return status, {"x-ms-request-id": "request-id", "set-cookie": "not-journaled"}, reply, readable

        def readback(transport, endpoint):
            headers, reply = self.response(transport, endpoint)
            if endpoint == root + "/schedules/" + INTENT and reply["enabled"] is False:
                pending = json.loads(path.read_text())
                self.assertEqual(pending["status"], "stop-pending")
                self.assertEqual(pending["stop_intent"]["path"], endpoint)
                self.assertEqual(pending["stop_response"], {
                    "status": 200, "headers": {"x-ms-request-id": "request-id"},
                    "body": reply, "body_read": True,
                })
            return headers, reply

        client.request.side_effect = patch_schedule
        with patch("managed_refresh._object_response", side_effect=readback):
            self.assertEqual(managed_refresh.manage(self.config, "chanel", self.root, "stop", yes=True)["status"], "pass")
        stopped = json.loads(path.read_text())
        self.assertEqual(stopped["create_response"], original["create_response"])
        self.assertEqual(stopped["stop_readback"], self.overrides[root + "/schedules/" + INTENT])

    def test_unexpected_or_unreadable_stop_response_is_retained_and_never_retried(self):
        self.recurring()
        client, root = self.schedule_transport()
        managed_refresh.manage(self.config, "chanel", self.root, "start", yes=True)
        path = managed_refresh.schedule_file(self.root, self.config)
        original = path.read_bytes()
        endpoint = root + "/schedules/" + INTENT
        live = copy.deepcopy(self.overrides[endpoint])
        for status, headers, body, readable in (
            (202, {"location": "https://api.fabric.microsoft.com/v1/operations/" + OWNER,
                   "x-ms-operation-id": OWNER}, None, True),
            (200, {"x-ms-request-id": OWNER}, None, False),
        ):
            with self.subTest(status=status):
                path.write_bytes(original)
                self.overrides[endpoint] = copy.deepcopy(live)
                client.request.reset_mock(side_effect=True)
                client.request.return_value = (status, headers, body, readable)
                with self.assertRaises(CandidateBlocked):
                    managed_refresh.manage(self.config, "chanel", self.root, "stop", yes=True)
                pending = json.loads(path.read_text())
                self.assertEqual(pending["status"], "stop-pending")
                self.assertEqual(pending["stop_response"], {
                    "status": status, "headers": headers, "body": body, "body_read": readable,
                })
                with self.assertRaises(CandidateBlocked):
                    managed_refresh.manage(self.config, "chanel", self.root, "stop", yes=True)
                self.overrides[endpoint]["enabled"] = False
                with self.assertRaises(CandidateBlocked):
                    managed_refresh.manage(self.config, "chanel", self.root, "stop", yes=True)
                observed = managed_refresh.manage(self.config, "chanel", self.root, "status")
                self.assertFalse(observed["enabled"])
                self.assertEqual(json.loads(path.read_text()), pending)
                client.request.assert_called_once_with("PATCH", endpoint, pending["stop_intent"]["request"])

    def test_plan_is_local_and_enforces_exact_counts_and_window(self):
        self.recurring()
        payload = managed_refresh.plan(self.config, duration_minutes=19)
        settings = payload["configuration"]
        start = managed_refresh._time(settings["startDateTime"])
        end = managed_refresh._time(settings["endDateTime"])
        self.assertEqual((end - start).total_seconds(), 19 * 60)
        long_payload = managed_refresh.plan(self.config, duration_minutes=115)
        settings = long_payload["configuration"]
        self.assertEqual((managed_refresh._time(settings["endDateTime"])
                          - managed_refresh._time(settings["startDateTime"])).total_seconds(), 115 * 60)
        self.assertEqual(settings["interval"], 5)
        long_payload = managed_refresh.plan(self.config, duration_minutes=180)
        settings = long_payload["configuration"]
        self.assertEqual((managed_refresh._time(settings["endDateTime"])
                          - managed_refresh._time(settings["startDateTime"])).total_seconds(), 180 * 60)
        self.assertEqual(settings["interval"], 5)
        for duration in (18, 181):
            with self.subTest(duration=duration), self.assertRaises(ValueError):
                managed_refresh.plan(self.config, duration_minutes=duration)
        self.config["data"]["rows_per_file"] = 999
        with self.assertRaises(CandidateBlocked):
            managed_refresh.plan(self.config)
        self.client.assert_not_called()

    def test_long_setup_can_be_exported_and_stopped_without_changing_cadence(self):
        self.recurring()
        client, _ = self.schedule_transport()
        started = managed_refresh.manage(self.config, "chanel", self.root, "start", yes=True,
                                         duration_minutes=180)
        receipt = managed_refresh.manage(self.config, "chanel", self.root, "export-native")["native_schedule"]
        self.assertEqual(receipt["request"], started["request"])
        settings = receipt["request"]["configuration"]
        self.assertEqual((managed_refresh._time(settings["endDateTime"])
                          - managed_refresh._time(settings["startDateTime"])).total_seconds(), 180 * 60)
        self.assertEqual(settings["interval"], 5)
        self.assertEqual(managed_refresh.manage(self.config, "chanel", self.root, "stop", yes=True)["status"], "pass")
        self.assertEqual([call.args[0] for call in client.request.call_args_list], ["POST", "PATCH"])

    def test_presenter_plan_needs_no_hand_built_schedule_json(self):
        self.recurring()
        config_path = self.root / "common.json"
        config_path.write_text(json.dumps(self.config))
        output = io.StringIO()
        with patch("sys.argv", ["managed_refresh.py", "plan", "--config", str(config_path)]):
            with contextlib.redirect_stdout(output):
                self.assertEqual(managed_refresh.main(), 0)
        planned = json.loads(output.getvalue())
        self.assertEqual(planned["request"]["configuration"]["interval"], 5)
        self.client.assert_not_called()

    def test_recurring_rechecks_active_jobs_after_sql(self):
        self.recurring()
        _, root = self.schedule_transport()
        managed_refresh.manage(self.config, "chanel", self.root, "start", yes=True)
        def begin_job(*_args):
            self.overrides[root + "/instances"] = {
                "value": [{"id": LAKEHOUSE, "itemId": LAKEHOUSE, "status": "InProgress"}],
            }
        self.sql.side_effect = begin_job
        with self.assertRaisesRegex(CandidateBlocked, "already active"):
            validate_customer(self.config, "chanel", self.root)

    def test_started_or_tampered_schedule_never_admits(self):
        self.recurring()
        _, root = self.schedule_transport()
        managed_refresh.manage(self.config, "chanel", self.root, "start", yes=True)
        path = managed_refresh.schedule_file(self.root, self.config)
        original = json.loads(path.read_text())
        damaged = copy.deepcopy(original)
        damaged["request"]["configuration"]["interval"] = 1
        path.write_text(json.dumps(damaged))
        with self.assertRaises(CandidateBlocked):
            validate_customer(self.config, "chanel", self.root)
        path.write_text(json.dumps(original))
        with patch("managed_refresh.datetime", wraps=managed_refresh.datetime) as clock:
            clock.now.return_value = managed_refresh._time(original["request"]["configuration"]["startDateTime"])
            with self.assertRaisesRegex(CandidateBlocked, "future start"):
                validate_customer(self.config, "chanel", self.root)

    def test_schedule_commands_require_explicit_mutation_consent(self):
        self.recurring()
        for action in ("start", "stop"):
            with self.subTest(action=action), self.assertRaisesRegex(ValueError, "requires --yes"):
                managed_refresh.manage(self.config, "chanel", self.root, action)
        self.client.assert_not_called()


class SqlReadinessTest(unittest.TestCase):
    def test_dimension_ranges_and_lowercase_sql_endpoint_names(self):
        connection = Mock()
        cursor = connection.cursor.return_value
        cursor.execute.return_value = cursor
        cursor.fetchone.side_effect = [
            (365, 365, date(2026, 1, 1), date(2026, 12, 31), 0),
            (20, 20, 1, 20, 0), (100, 100, 1, 100, 0), (0,),
        ]
        odbc = SimpleNamespace(drivers=lambda: ["ODBC Driver 17 for SQL Server"],
                               connect=Mock(return_value=connection), Error=RuntimeError)
        with patch.dict("sys.modules", {"pyodbc": odbc}), patch("customer_validation._token", return_value="token"):
            _sql_readback(
                {"connectionString": "a.datawarehouse.fabric.microsoft.com", "provisioningStatus": "Success"},
                "Lakehouse", OWNER, OWNER,
                {"start_date": "2026-01-01", "date_count": 365, "product_count": 20, "customer_count": 100},
            )
        statements = [call.args[0] for call in cursor.execute.call_args_list]
        self.assertIn("FROM silver.dimdate", statements[0])
        self.assertIn("FROM silver.dimproduct", statements[1])
        self.assertIn("FROM silver.dimcustomer", statements[2])
        self.assertIn("payload_variants IS NULL", statements[-1])
        connection.close.assert_called_once()


class MlvPaginationTest(unittest.TestCase):
    def test_paginated_history_checks_all_pages_for_active_jobs(self):
        path = f"/v1/workspaces/{WORKSPACE}/lakehouses/{LAKEHOUSE}/jobs/refreshMaterializedLakeViews/instances"
        uri = "https://wabi-west-us3-a-primary-redirect.analysis.windows.net" + path + "?continuationToken=abc%2Bdef"
        first = {"value": [{"id": OWNER, "status": "Completed", "itemId": LAKEHOUSE}],
                 "continuationToken": "abc+def", "continuationUri": uri}
        client = Mock()
        client.request.return_value = (200, {}, {
            "value": [{"id": INTENT, "status": "InProgress", "itemId": LAKEHOUSE}]}, True)
        with patch("managed_refresh._object_response", return_value=({}, first)):
            rows = managed_refresh._page(client, path)
        client.request.assert_called_once_with("GET", path, continuation_token="abc+def")
        with self.assertRaisesRegex(CandidateBlocked, "already active"):
            managed_refresh.check_idle(rows, LAKEHOUSE)

    def test_rejects_mismatched_malicious_and_repeated_continuations(self):
        path = f"/v1/workspaces/{WORKSPACE}/lakehouses/{LAKEHOUSE}/jobs/refreshMaterializedLakeViews/schedules"
        good = "https://api.fabric.microsoft.com" + path + "?continuationToken=token"
        for uri in (good.replace("token", "other"), good.replace("api.fabric.microsoft.com", "evil.example"),
                    good.replace("/schedules", "/instances"), good + "&extra=1",
                    good.replace("https:", "http:")):
            with self.subTest(uri=uri), patch("managed_refresh._object_response", return_value=({}, {
                    "value": [], "continuationToken": "token", "continuationUri": uri})):
                client = Mock()
                with self.assertRaises(CandidateBlocked):
                    managed_refresh._page(client, path)
                client.request.assert_not_called()
        page = {"value": [], "continuationToken": "token", "continuationUri": good}
        client = Mock()
        client.request.return_value = (200, {}, page, True)
        with patch("managed_refresh._object_response", return_value=({}, page)):
            with self.assertRaises(CandidateBlocked):
                managed_refresh._page(client, path)
        self.assertEqual(client.request.call_count, 1)

    def test_rejects_failed_page_and_duplicate_ids(self):
        path = f"/v1/workspaces/{WORKSPACE}/lakehouses/{LAKEHOUSE}/jobs/refreshMaterializedLakeViews/instances"
        page = {"value": [{"id": OWNER}], "continuationToken": "t",
                "continuationUri": "https://api.fabric.microsoft.com" + path + "?continuationToken=t"}
        for response in ((500, {}, None, False), (200, {}, {"value": [{"id": OWNER}]}, True)):
            with self.subTest(response=response), patch("managed_refresh._object_response", return_value=({}, page)):
                client = Mock()
                client.request.return_value = response
                with self.assertRaises(CandidateBlocked):
                    managed_refresh._page(client, path)


if __name__ == "__main__":
    unittest.main()
