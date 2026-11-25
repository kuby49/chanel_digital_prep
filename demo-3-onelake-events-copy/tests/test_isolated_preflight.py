import copy
import sys
import unittest
import uuid
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import isolated_preflight as check
from deploy import CandidateBlocked


class API:
    def __init__(self):
        self.principal_id = str(uuid.uuid4())
        self.connection_id = str(uuid.uuid4())
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
