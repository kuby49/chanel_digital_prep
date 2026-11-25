import sys
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

import simple_demo1 as demo


class SimpleDemo1Test(unittest.TestCase):
    def setUp(self):
        self.settings = {
            "workspace_id": "workspace", "lakehouse_id": "lakehouse",
            "lakehouse_name": "lakehouse", "landing_path": "Files/incoming/sales",
            "bronze_schema": "bronze", "bronze_view": "sales_files",
            "silver_schema": "silver", "silver_view": "sales_joined",
            "refresh_schedule_id": "schedule", "refresh_interval_minutes": 5,
        }
        self.spark = Mock()
        self.context = {"config": {"demo-1": self.settings, "data": {}},
                        "spark": self.spark, "get_token": Mock(return_value="token")}

    def test_reset_clears_and_rebuilds_same_views(self):
        self.spark.sql.side_effect = lambda sql: SimpleNamespace(collect=lambda: [
            {"tableName": "sales_joined"} if sql.endswith(" s") else {"tableName": "sales_files"}
        ])
        fs = Mock()
        fs.exists.return_value = True
        fs.rm.return_value = fs.mkdirs.return_value = fs.put.return_value = True
        notebookutils = SimpleNamespace(mssparkutils=SimpleNamespace(fs=fs))
        with patch.dict(sys.modules, {"notebookutils": notebookutils}), \
                patch.object(demo, "stop") as stop, \
                patch.object(demo, "_names", return_value=("b", "s", "b.view", "s.view")), \
                patch.object(demo, "_dimensions") as dimensions:
            demo.reset(self.context)
        stop.assert_called_once_with(self.context, failed=True)
        dimensions.assert_called_once_with(self.context, "s")
        calls = [call.args[0] for call in self.spark.sql.call_args_list]
        self.assertLess(calls.index("DROP MATERIALIZED LAKE VIEW s.view"),
                        calls.index("DROP MATERIALIZED LAKE VIEW b.view"))
        self.assertLess(next(i for i, sql in enumerate(calls) if "CREATE MATERIALIZED LAKE VIEW b.view" in sql),
                        next(i for i, sql in enumerate(calls) if "CREATE MATERIALIZED LAKE VIEW s.view" in sql))
        self.assertIn("USING OneLake_Files", "\n".join(calls))
        self.assertEqual(fs.put.call_args.args[1], demo.HEADER)
        self.assertTrue(fs.rm.call_args.args[0].endswith("/Files/incoming/sales"))

    def test_start_updates_only_the_configured_native_schedule(self):
        calls = []

        def request(context, method, path, *, body=None, params=None):
            calls.append((method, path, body))
            return {} if method == "GET" else None

        with patch.object(demo, "_request", side_effect=request):
            demo.start(self.context)
        self.assertEqual([call[0] for call in calls], ["GET", "PATCH"])
        self.assertTrue(calls[1][1].endswith("/schedules/schedule"))
        self.assertEqual(calls[1][2]["configuration"]["interval"], 5)
        self.assertTrue(calls[1][2]["enabled"])

    def test_failed_stop_disables_schedule_and_cancels_active_refresh(self):
        calls = []
        job = {"id": "job", "status": "InProgress"}
        pages = iter([{"value": [job]}, {"value": []}])

        def request(context, method, path, *, body=None, params=None):
            calls.append((method, path, body))
            if path.endswith("/schedules/schedule") and method == "GET":
                return {"enabled": True, "configuration": {"type": "Cron"}}
            if path.endswith("/instances") and method == "GET":
                return next(pages)
            return None

        with patch.object(demo, "_request", side_effect=request), patch.object(demo.time, "sleep"):
            demo.stop(self.context, failed=True)
        self.assertEqual([call[0] for call in calls], ["GET", "PATCH", "GET", "POST", "GET"])
        self.assertFalse(calls[1][2]["enabled"])
        self.assertTrue(calls[3][1].endswith("/items/lakehouse/jobs/instances/job/cancel"))

    def test_job_listing_uses_continuation_token(self):
        pages = iter([{"value": [], "continuationToken": "abc%3D"},
                      {"value": [{"id": "job", "status": "InProgress"}]}])
        with patch.object(demo, "_request", side_effect=lambda *args, **kwargs: next(pages)) as request:
            self.assertEqual(demo._jobs(self.context)[0]["id"], "job")
        self.assertEqual(request.call_args_list[1].kwargs["params"], {"continuationToken": "abc="})

    def test_normal_stop_waits_for_refresh_after_delivery(self):
        calls = []
        recent = {"id": "job", "status": "Completed", "startTimeUtc": "2999-01-01T00:00:00Z"}
        previous = {"id": "old", "status": "Completed", "startTimeUtc": "2020-01-01T00:00:00Z"}
        pages = iter([{"value": [previous]}, {"value": [recent]}, {"value": [recent]}])

        def request(context, method, path, *, body=None, params=None):
            calls.append((method, path))
            if path.endswith("/schedules/schedule") and method == "GET":
                return {"enabled": True, "configuration": {"type": "Cron"}}
            if path.endswith("/instances"):
                return next(pages)
            return None

        with patch.object(demo, "_request", side_effect=request), patch.object(demo.time, "sleep"):
            demo.stop(self.context)
        self.assertEqual([call[0] for call in calls], ["GET", "GET", "GET", "PATCH", "GET"])


if __name__ == "__main__":
    unittest.main()
