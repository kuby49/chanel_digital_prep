import json
import tempfile
import unittest
from pathlib import Path

from fresh_deploy import begin, confirm, record_intent, record_lro_result, record_response
from ownership import CandidateBlocked, STATE_FILE
from test_deploy import candidate_config, OWNER, WORKSPACE, LAKEHOUSE, INTENT


class FreshJournalTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name)
        self.config = candidate_config()
        self.journal = begin(self.config, "chanel", self.path, OWNER)
        self.marker = self.journal["owner"]["marker"]

    def disk(self):
        return json.loads((self.path / STATE_FILE).read_text(encoding="utf-8"))

    def workspace(self):
        request = {"displayName": self.journal["expected"]["workspace_name"], "description": self.marker,
                   "capacityId": self.config["capacity"]["id"]}
        intent = record_intent(self.path, self.journal, "Workspace", request, path="/v1/workspaces")
        result = {"id": WORKSPACE, "type": "Workspace", "displayName": request["displayName"]}
        readback = {**result, "description": self.marker, "capacityId": request["capacityId"],
                    "capacityAssignmentProgress": "Completed"}
        return intent, result, readback

    def test_create_intent_and_response_precede_confirmation(self):
        intent, result, readback = self.workspace()
        self.assertEqual(self.disk()["pending_operations"][0]["id"], intent)
        record_response(self.path, self.journal, intent, 201, result)
        self.assertEqual(self.disk()["pending_operations"][0]["response"]["body"], result)
        confirm(self.path, self.journal, intent, result, readback)
        self.assertEqual(self.disk()["workspace"]["id"], WORKSPACE)
        self.assertEqual(self.disk()["pending_operations"], [])

    def test_existing_journal_and_uncertain_create_are_not_retried(self):
        intent, _, _ = self.workspace()
        with self.assertRaises(CandidateBlocked):
            begin(self.config, "chanel", self.path, OWNER)
        with self.assertRaises(CandidateBlocked):
            record_intent(self.path, self.journal, "Workspace", {}, path="/v1/workspaces")
        self.assertEqual(self.disk()["pending_operations"][0]["id"], intent)

    def test_readback_mismatch_leaves_response_pending(self):
        intent, result, readback = self.workspace()
        record_response(self.path, self.journal, intent, 201, result)
        readback["capacityId"] = OWNER
        with self.assertRaises(CandidateBlocked):
            confirm(self.path, self.journal, intent, result, readback)
        self.assertEqual(self.disk()["pending_operations"][0]["id"], intent)

    def test_lro_requires_matching_success_and_parent(self):
        intent, result, readback = self.workspace()
        record_response(self.path, self.journal, intent, 201, result)
        confirm(self.path, self.journal, intent, result, readback)
        request = {"displayName": "ChanelDemo1Lakehouse", "description": self.marker,
                   "creationPayload": {"enableSchemas": True}}
        intent = record_intent(self.path, self.journal, "Lakehouse", request,
                               path=f"/v1/workspaces/{WORKSPACE}/lakehouses")
        record_response(self.path, self.journal, intent, 202, None, INTENT)
        result = {"id": LAKEHOUSE, "type": "Lakehouse", "displayName": request["displayName"],
                  "workspaceId": WORKSPACE}
        readback = {**result, "description": self.marker, "properties": {"defaultSchema": "dbo"}}
        with self.assertRaises(CandidateBlocked):
            confirm(self.path, self.journal, intent, result, readback)
        operation = {"operation_id": INTENT, "status": "Succeeded"}
        record_lro_result(self.path, self.journal, intent, result, operation)
        with self.assertRaises(CandidateBlocked):
            confirm(self.path, self.journal, intent, result, {**readback, "workspaceId": OWNER}, operation)
        confirm(self.path, self.journal, intent, result, readback,
                {"operation_id": INTENT, "status": "Succeeded"})
        self.assertEqual(self.disk()["lakehouse"]["workspace_id"], WORKSPACE)

    def test_existing_resource_binding_and_wrong_capacity_are_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            config = candidate_config()
            config["demo-1"]["workspace_id"] = WORKSPACE
            with self.assertRaises(CandidateBlocked):
                begin(config, "chanel", Path(tmp), OWNER)
        with self.assertRaises(CandidateBlocked):
            record_intent(self.path, self.journal, "Workspace",
                          {"displayName": self.journal["expected"]["workspace_name"],
                           "description": self.marker, "capacityId": OWNER}, path="/v1/workspaces")
        self.assertEqual(self.disk()["pending_operations"], [])


if __name__ == "__main__":
    unittest.main()
