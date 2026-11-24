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
from customer_validation import _sql_readback, validate_customer
from fresh_deploy import begin, confirm, record_intent, record_response
from ownership import STATE_FILE, CandidateBlocked
from test_deploy import candidate_config, OWNER, WORKSPACE, LAKEHOUSE


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


if __name__ == "__main__":
    unittest.main()
