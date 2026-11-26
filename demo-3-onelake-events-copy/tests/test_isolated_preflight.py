import copy
import json
import sys
import tempfile
import unittest
import uuid
from datetime import datetime, timezone
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import isolated_preflight as check
from deploy import CandidateBlocked


class API:
    def __init__(self):
        self.principal_id = str(uuid.uuid4())
        self.connection_id = str(uuid.uuid4())
        self.capacity_id = str(uuid.uuid4())
        self.capacities = [{"id": self.capacity_id, "sku": "F32", "state": "Active"}]
        self.jobs = [
            {"id": check._PARENT_JOB, "itemId": check._PIPELINE, "status": "Failed",
             "startTimeUtc": "2026-11-25T13:05:44Z", "endTimeUtc": "2026-11-25T15:05:44Z"},
            {"id": check._NOTEBOOK_JOB, "itemId": check._NOTEBOOK, "status": "Failed",
             "startTimeUtc": "2026-11-25T13:05:50Z", "endTimeUtc": "2026-11-25T15:05:40Z"},
        ]
        self.activity = {"pipelineRunId": check._PARENT_JOB, "activityName": "RunProducer",
                         "activityType": "TridentNotebook", "activityRunId": check._NOTEBOOK_JOB,
                         "input": {"workspaceId": check._WORKSPACE, "notebookId": check._NOTEBOOK}}
        self.metadata = {"id": self.connection_id, "connectivityType": "ShareableCloud",
                         "connectionDetails": {"type": "Lakehouse", "path": "Lakehouse"},
                         "credentialDetails": {"credentialType": "OAuth2"}}
        self.roles = [{"principal": {"id": self.principal_id}, "role": "Owner"}]
        self.headers = {}

    def list(self, path):
        if path == "/v1/capacities":
            return self.capacities
        if "/roleAssignments" in path:
            return self.roles
        item = path.split("/items/")[1].split("/")[0]
        return [job for job in self.jobs if job.get("itemId") == item]

    def get(self, path):
        return copy.deepcopy(self.metadata)

    def request(self, method, path, body, *, skill):
        assert method == "POST" and skill == "spark-cli"
        return 200, self.headers, {"value": [self.activity]}


class IsolatedPreflightTests(unittest.TestCase):
    def setUp(self):
        self.api = API()
        self.now = datetime(2026, 11, 25, 16, tzinfo=timezone.utc)

    def test_exact_terminal_jobs_do_not_prove_writer_drain(self):
        result = check.original_parent(self.api, now=self.now)
        self.assertEqual(result["pipeline_status"], "Failed")
        self.assertFalse(result["writer_drain_verified"])
        self.assertFalse(result["ready"])

    def test_active_missing_or_unlinked_parent_is_blocked(self):
        for field in ("status", "id", "itemId", "endTimeUtc"):
            with self.subTest(field=field):
                old = self.api.jobs[0][field]
                self.api.jobs[0][field] = "InProgress" if field == "status" else "unexpected"
                with self.assertRaises(CandidateBlocked):
                    check.original_parent(self.api, now=self.now)
                self.api.jobs[0][field] = old
        self.api.activity["activityRunId"] = str(uuid.uuid4())
        with self.assertRaisesRegex(CandidateBlocked, "linkage"):
            check.original_parent(self.api, now=self.now)
        self.api.activity["activityRunId"] = check._NOTEBOOK_JOB
        self.api.headers = {"x-ms-continuation-token": "next"}
        with self.assertRaisesRegex(CandidateBlocked, "incomplete"):
            check.original_parent(self.api, now=self.now)

    def test_connection_readback_does_not_assert_runtime_target_access(self):
        result = check.connection(self.api, self.api.connection_id)
        self.assertEqual(result["caller_role"], "Owner")
        self.assertFalse(result["runtime_identity_verified"])
        self.assertFalse(result["target_access_verified"])
        self.assertFalse(result["ready"])
        self.api.metadata["connectionDetails"]["path"] = "bound-to-other-target"
        with self.assertRaisesRegex(CandidateBlocked, "metadata"):
            check.connection(self.api, self.api.connection_id)
        self.api.metadata["connectionDetails"]["path"] = "Lakehouse"
        self.api.roles = []
        with self.assertRaisesRegex(CandidateBlocked, "access"):
            check.connection(self.api, self.api.connection_id)

    def test_capacity_must_be_uniquely_active_f32(self):
        self.assertEqual(check.require_f32(self.api, self.api.capacity_id)["sku"], "F32")
        for value in ([], [{"id": self.api.capacity_id, "sku": "F16", "state": "Active"}],
                      [{"id": self.api.capacity_id, "sku": "F32", "state": "Paused"}],
                      self.api.capacities * 2):
            with self.subTest(value=value):
                self.api.capacities = value
                with self.assertRaisesRegex(CandidateBlocked, "active F32"):
                    check.require_f32(self.api, self.api.capacity_id)

    def test_staging_checks_journal_and_live_parent_without_claiming_readiness(self):
        import customer_preflight as original
        import isolated_candidate
        workspace, lakehouse = str(uuid.uuid4()), str(uuid.uuid4())
        state = {"tenant_id": original.TENANT, "capacity_id": self.api.capacity_id, "pending": None,
                 "resources": {
                     "workspace": {"id": workspace, "name": "new", "type": "Workspace",
                                   "description": "owned"},
                     "staging_lakehouse": {"id": lakehouse, "name": "stage", "type": "Lakehouse",
                                           "workspace_id": workspace, "description": "owned"},
                 }}
        workspace_read = {"id": workspace, "displayName": "new", "type": "Workspace",
                          "description": "owned", "capacityId": self.api.capacity_id}
        lakehouse_read = {"id": lakehouse, "displayName": "stage", "type": "Lakehouse",
                          "description": "owned", "workspaceId": workspace}
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "shared-producer-v1.json"
            path.write_text(json.dumps(state), encoding="utf-8")
            with mock.patch.object(isolated_candidate, "_producer_ids", return_value=(workspace, lakehouse)), \
                    mock.patch.object(self.api, "get", side_effect=[workspace_read, lakehouse_read]):
                result = check.staging(self.api, directory, "config", "new", directory)
            self.assertFalse(result["ready"])
            lakehouse_read["workspaceId"] = str(uuid.uuid4())
            with mock.patch.object(isolated_candidate, "_producer_ids", return_value=(workspace, lakehouse)), \
                    mock.patch.object(self.api, "get", side_effect=[workspace_read, lakehouse_read]), \
                    self.assertRaisesRegex(CandidateBlocked, "parent or capacity"):
                check.staging(self.api, directory, "config", "new", directory)
