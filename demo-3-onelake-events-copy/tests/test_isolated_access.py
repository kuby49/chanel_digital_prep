import copy
import sys
import unittest
import uuid
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import customer_preflight as original
import isolated_access as access


def uid():
    return str(uuid.uuid4())


class API:
    def __init__(self, spec):
        self.spec = spec
        self.job_id, self.notebook_job_id = uid(), uid()
        self.job = {"id": self.job_id, "status": "Completed",
                    "startTimeUtc": "2026-11-25T15:50:00Z",
                    "endTimeUtc": "2026-11-25T15:52:00Z"}
        self.notebook = {"id": self.notebook_job_id, "status": "Completed",
                         "jobType": "PipelineRunNotebook",
                         "startTimeUtc": "2026-11-25T15:50:01Z",
                         "endTimeUtc": "2026-11-25T15:52:01Z"}
        self.activity = {"pipelineRunId": self.job_id, "activityType": "TridentNotebook",
                         "activityRunId": self.notebook_job_id, "status": "Succeeded",
                         "input": {"workspaceId": spec["target_workspace_id"],
                                   "notebookId": spec["notebook_id"]}}
        self.headers = {}
        self.extra = {}
        self.calls = []

    def get(self, path, *, skill):
        self.calls.append(("get", path))
        assert skill == "spark-cli"
        if f"/items/{self.spec['pipeline_id']}/jobs/instances/{self.job_id}" in path:
            return copy.deepcopy(self.job)
        if f"/items/{self.spec['notebook_id']}/jobs/instances/{self.notebook_job_id}" in path:
            return copy.deepcopy(self.notebook)
        return {}

    def request(self, method, path, body, *, skill):
        self.calls.append((method, path))
        assert method == "POST" and path.endswith("/queryactivityruns") and skill == "spark-cli"
        return 200, self.headers, {"value": [copy.deepcopy(self.activity)], **self.extra}


class IsolatedAccessTests(unittest.TestCase):
    def setUp(self):
        self.spec = {key: uid() for key in (
            "staging_workspace_id", "staging_lakehouse_id", "target_workspace_id",
            "target_lakehouse_id", "pipeline_id", "notebook_id", "principal_id")}
        self.spec["tenant_id"] = original.TENANT
        self.api = API(self.spec)
        self.now = datetime(2026, 11, 25, 16, tzinfo=timezone.utc)

    def assess(self):
        return access.assess(self.spec, self.api, pipeline_job_id=self.api.job_id, now=self.now)

    def test_absence_has_exact_blocker_and_no_calls(self):
        result = access.assess(self.spec, self.api)
        self.assertFalse(result["ready"])
        self.assertIn("executed notebook definition", result["missing_operation"])
        self.assertIn("F32", result["required_operation"])
        self.assertIn("immediately before", result["required_operation"])
        self.assertIn("only to the new target", result["required_operation"])
        self.assertEqual(self.api.calls, [])

    def test_fresh_link_never_proves_runtime_access_or_deployment(self):
        result = self.assess()
        self.assertTrue(result["job_linkage_verified"])
        for key in ("ready", "controlled_job_verified", "runtime_identity_verified",
                    "staging_read_verified", "target_read_write_verified",
                    "connection_credential_identity_verified"):
            self.assertFalse(result[key], key)
        self.assertEqual([method for method, _ in self.api.calls], ["get", "POST", "get", "get"])

    def test_original_and_equal_resource_ids_are_rejected(self):
        for key, value in (("target_lakehouse_id", original.LAKEHOUSE),
                           ("staging_lakehouse_id", original.STAGING_LAKEHOUSE),
                           ("pipeline_id", original.PIPELINE),
                           ("notebook_id", original.NOTEBOOK),
                           ("target_lakehouse_id", self.spec["staging_lakehouse_id"])):
            with self.subTest(key=key):
                bad = dict(self.spec, **{key: value})
                with self.assertRaises(ValueError):
                    access.assess(bad)

    def test_mismatched_job_or_notebook_ids_block_linkage(self):
        self.api.job["id"] = uid()
        self.assertFalse(self.assess()["job_linkage_verified"])
        self.api.job["id"] = self.api.job_id
        for key in ("workspaceId", "notebookId"):
            with self.subTest(key=key):
                old = self.api.activity["input"][key]
                self.api.activity["input"][key] = uid()
                self.assertFalse(self.assess()["job_linkage_verified"])
                self.api.activity["input"][key] = old
        self.api.notebook["id"] = uid()
        self.assertFalse(self.assess()["job_linkage_verified"])

    def test_unconfirmed_pending_or_stale_jobs_fail_closed(self):
        self.api.job["status"] = "InProgress"
        self.assertIn("not confirmed", self.assess()["job_observation"])
        self.api.job["status"] = "Completed"
        self.api.notebook["status"] = "InProgress"
        self.assertIn("not confirmed", self.assess()["job_observation"])
        self.api.notebook["status"] = "Completed"
        self.api.job["endTimeUtc"] = "2026-11-25T15:40:00Z"
        self.api.job["startTimeUtc"] = "2026-11-25T15:38:00Z"
        self.assertIn("stale", self.assess()["job_observation"])

    def test_untrusted_or_mismatched_principal_output_cannot_prove_identity(self):
        # Neither activity output nor the expected principal is authoritative runtime evidence.
        self.api.activity["output"] = {"principal_id": uid(), "success": True}
        result = self.assess()
        self.assertFalse(result["runtime_identity_verified"])
        self.assertFalse(result["target_read_write_verified"])
        self.assertFalse(result["ready"])
        self.api.activity["output"]["principal_id"] = self.spec["principal_id"]
        self.assertFalse(self.assess()["runtime_identity_verified"])

    def test_incomplete_activity_readback_is_blocked(self):
        self.api.extra["continuationToken"] = "more"
        self.assertFalse(self.assess()["job_linkage_verified"])
        self.api.extra.clear()
        self.api.headers["x-ms-continuation-token"] = "more"
        self.assertFalse(self.assess()["job_linkage_verified"])


if __name__ == "__main__":
    unittest.main()
