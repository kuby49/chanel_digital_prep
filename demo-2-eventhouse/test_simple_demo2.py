import json
import sys
import unittest
from datetime import date
from types import SimpleNamespace
from unittest.mock import MagicMock, patch
from urllib.error import HTTPError

import simple_demo2 as demo


WORKSPACE = "0a0499fe-51b5-42b1-bc99-31c121cc74a6"
DATABASE_ID = "25159007-a6dd-4020-8afc-01d393378727"
LAKEHOUSE = "f565aeac-f321-46f2-8262-076e06490707"


def context():
    return {
        "config": {
            "data": {"start_date": "2026-01-01"},
            "demo-2": {
                "workspace_id": WORKSPACE,
                "kql_database_id": DATABASE_ID,
                "kql_database_name": "SalesDatabase",
                "lakehouse_id": LAKEHOUSE,
                "query_endpoint": "https://example.kusto.fabric.microsoft.com",
                "sales_table": "SyntheticFactSales",
                "csv_mapping": "SalesCsvV1",
                "shortcut_name": "EventhouseFactSales",
                "target_latency_minutes": 5,
            },
        },
        "get_token": lambda audience: "token",
        "spark": MagicMock(),
    }


def result(rows):
    return SimpleNamespace(primary_results=[rows], errors_count=0)


def policy_result():
    return result([{"Policy": json.dumps({
        "IsEnabled": True,
        "Format": "parquet",
        "TargetLatencyInMinutes": 5,
        "ConnectionStrings": [
            f"https://westus3-api.onelake.fabric.microsoft.com/{WORKSPACE}/"
            f"{DATABASE_ID}/Tables/SyntheticFactSales"
        ],
    })}])


class SimpleDemo2Tests(unittest.TestCase):
    def setUp(self):
        self.ctx = context()
        self.client = MagicMock()
        self.client.__enter__.return_value = self.client
        self.client.execute_mgmt.return_value = policy_result()
        self.client_patch = patch.object(demo, "_client", return_value=self.client)
        self.client_patch.start()
        self.addCleanup(self.client_patch.stop)

    def test_reset_preserves_and_repairs_shortcut_after_ordered_clear(self):
        missing = HTTPError("https://example.invalid", 404, "missing", {}, None)
        with patch.object(demo, "_fabric", side_effect=[missing, {}]) as fabric, \
             patch.object(demo, "_restore_dimensions") as dimensions:
            self.assertIsNone(demo.reset(self.ctx))
        commands = [call.args[1] for call in self.client.execute_mgmt.call_args_list]
        self.assertIn(".create-merge table SyntheticFactSales", commands[0])
        self.assertIn("ingestion csv mapping 'SalesCsvV1'", commands[1])
        self.assertIn("IsEnabled=false", commands[2])
        self.assertEqual(commands[3], ".clear table SyntheticFactSales data")
        self.assertIn("IsEnabled=true, TargetLatencyInMinutes=5", commands[4])
        self.assertEqual(commands[5], ".show table SyntheticFactSales policy mirroring")
        self.assertEqual(fabric.call_args.args[1], "POST")
        self.assertEqual(fabric.call_args.args[3]["target"]["oneLake"]["itemId"], DATABASE_ID)
        dimensions.assert_called_once_with(self.ctx)

    def test_reset_reuses_matching_shortcut_and_propagates_failures(self):
        existing = {"target": {"oneLake": {
            "workspaceId": WORKSPACE, "itemId": DATABASE_ID, "path": "Tables/SyntheticFactSales"
        }}}
        with patch.object(demo, "_fabric", return_value=existing) as fabric, \
             patch.object(demo, "_restore_dimensions"):
            demo.reset(self.ctx)
        fabric.assert_called_once()
        self.client.execute_mgmt.side_effect = RuntimeError("clear failed")
        with self.assertRaisesRegex(RuntimeError, "clear failed"):
            demo.reset(self.ctx)

    def test_submit_preserves_csv_text_and_surfaces_failure(self):
        csv_bytes = b"SalesLineID,BatchID\r\n1,batch-1\r\n"
        self.assertIsNone(demo.submit_batch(self.ctx, csv_bytes))
        command = self.client.execute_mgmt.call_args.args[1]
        prefix, content = command.split("<|\n", 1)
        self.assertIn("ingestionMappingReference='SalesCsvV1'", prefix)
        self.assertIn("ignoreFirstRecord=true", prefix)
        self.assertEqual(content.encode("utf-8"), csv_bytes)
        self.client.execute_mgmt.side_effect = RuntimeError("ingestion failed")
        with self.assertRaisesRegex(RuntimeError, "ingestion failed"):
            demo.submit_batch(self.ctx, csv_bytes)
        self.client.execute_mgmt.side_effect = None
        self.client.execute_mgmt.return_value = SimpleNamespace(
            errors_count=1, get_exceptions=lambda: ["partial failure"]
        )
        with self.assertRaisesRegex(RuntimeError, "partial failure"):
            demo.submit_batch(self.ctx, csv_bytes)

    def test_reset_restores_shared_dimensions_in_fixed_lakehouse(self):
        samples = {
            "DimDate": [{"Date": "2026-01-01", "Year": 2026, "MonthNumber": 1, "MonthName": "January"}],
            "DimProduct": [{"ProductID": 1, "ProductName": "Product 001", "ProductCategory": "Beauty"}],
            "DimCustomer": [{"CustomerID": 1, "CustomerName": "Customer 001", "CustomerRegion": "Americas"}],
        }
        source = SimpleNamespace(dimensions=MagicMock(return_value=samples))
        with patch.dict(sys.modules, {"simple_data": source}):
            demo._restore_dimensions(self.ctx)
        source.dimensions.assert_called_once_with(self.ctx["config"]["data"])
        calls = self.ctx["spark"].createDataFrame.call_args_list
        self.assertEqual(calls[0].args[0][0]["Date"], date(2026, 1, 1))
        self.assertIn("DimDate", self.ctx["spark"].createDataFrame.return_value.write.format.return_value
                      .mode.return_value.save.call_args_list[0].args[0])
        self.assertEqual(len(calls), 3)

    def test_stop_waits_for_mirroring_and_failed_stop_does_nothing(self):
        ready = {
            "IsEnabled": True, "IsExportRunning": False, "LastExportResult": "Completed",
            "CompletionPercentage": 100, "PendingDataSize": 0,
        }
        self.client.execute_mgmt.side_effect = [
            result([{**ready, "PendingDataSize": 10}]), result([ready])
        ]
        with patch.object(demo.time, "sleep") as sleep, patch.object(demo, "_show_results") as show:
            self.assertIsNone(demo.stop(self.ctx))
        sleep.assert_called_once()
        show.assert_called_once_with(self.ctx, self.client)
        self.client.reset_mock()
        self.assertIsNone(demo.stop(self.ctx, failed=True))
        self.client.execute_mgmt.assert_not_called()


if __name__ == "__main__":
    unittest.main()
