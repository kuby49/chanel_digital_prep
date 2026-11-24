import base64
from email.message import Message
from io import BytesIO
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch
from urllib.error import HTTPError

from ownership import (
    AZURE_CLI_TIMEOUT_SECONDS, FABRIC_ORIGIN, LOCK_FILE, STATE_FILE, CandidateBlocked, _az_json, _state_lock,
)
from provision_synthetic import FabricClient, provision
from test_deploy import INTENT, LAKEHOUSE, OWNER, TENANT, WORKSPACE, candidate_config


WS = "/v1/workspaces"
LH = f"{WS}/{WORKSPACE}/lakehouses"
LRO = f"/v1/operations/{INTENT}"


class ProvisionerTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.directory = Path(temporary.name)
        self.config = candidate_config()
        self.calls = []
        self.fail = None
        self.client = Mock()
        self.client.request.side_effect = self.request
        self.factory = self.enterContext(patch("provision_synthetic.FabricClient", return_value=self.client))

    def disk(self):
        return json.loads((self.directory / STATE_FILE).read_text(encoding="utf-8"))

    def run_provisioner(self, **kwargs):
        return provision(self.config, "chanel", self.directory, kwargs.pop("principal", OWNER),
                         self.directory, 60, **kwargs)

    def request(self, method, path, body=None):
        self.calls.append((method, path))
        self.assertTrue((self.directory / LOCK_FILE).is_file())
        journal = self.disk()
        marker = journal["owner"]["marker"]
        ws = {"id": WORKSPACE, "displayName": journal["expected"]["workspace_name"],
              "type": "Workspace", "description": marker,
              "capacityId": self.config["capacity"]["id"], "capacityAssignmentProgress": "Completed"}
        lh = {"id": LAKEHOUSE, "displayName": "ChanelDemo1Lakehouse", "type": "Lakehouse",
              "description": marker, "workspaceId": WORKSPACE, "properties": {"defaultSchema": "dbo"}}
        if method == "POST":
            pending = journal["pending_operations"][0]
            self.assertEqual((pending["path"], pending["request"]), (path, body))
            self.assertNotIn("response", pending)
        if path == WS:
            if self.fail == "lost":
                raise CandidateBlocked("Mock lost response")
            if self.fail == "existing-name":
                return 409, {}, None, False
            if self.fail == "wrong-location":
                return 201, {"location": FABRIC_ORIGIN + WS + "/" + OWNER}, ws, True
            if self.fail == "bad-body":
                return 201, {}, None, False
            return 201, {"location": FABRIC_ORIGIN + WS + "/" + WORKSPACE}, ws, True
        if path == f"{WS}/{WORKSPACE}":
            if self.fail == "workspace-readback":
                return 200, {}, {**ws, "description": "different"}, True
            if self.fail == "workspace-capacity":
                return 200, {}, {**ws, "capacityId": OWNER}, True
            return 200, {}, ws, True
        if path == LH:
            self.assertEqual(body["creationPayload"], {"enableSchemas": True})
            if self.fail == "lakehouse-201":
                return 201, {"location": FABRIC_ORIGIN + LH + "/" + LAKEHOUSE}, lh, True
            location = FABRIC_ORIGIN + (LRO if self.fail != "wrong-operation" else f"/v1/operations/{OWNER}")
            return 202, {"location": location, "x-ms-operation-id": INTENT, "retry-after": "2"}, None, True
        if path == LRO:
            accepted = journal["pending_operations"][0]["response"]
            self.assertEqual((accepted["status"], accepted["operation_id"]), (202, INTENT))
            if self.fail == "poll-unavailable":
                return 429, {}, None, False
            return 200, {}, {"status": "Succeeded"}, True
        if path == LRO + "/result":
            return 200, {}, lh, True
        if path == LH + "/" + LAKEHOUSE:
            if journal["pending_operations"] and journal["pending_operations"][0]["response"]["status"] == 202:
                self.assertEqual(journal["pending_operations"][0]["lro_result"]["body"]["id"], LAKEHOUSE)
            if self.fail == "lakehouse-parent":
                return 200, {}, {**lh, "workspaceId": OWNER}, True
            return 200, {}, ({**lh, "properties": {}} if self.fail == "lakehouse-readback" else lh), True
        self.failTest(f"Unexpected request: {method}, {path}")

    def test_create_and_completed_resume_never_post_again(self):
        self.assertEqual(self.run_provisioner(), {"workspace_id": WORKSPACE, "item_id": LAKEHOUSE})
        journal = self.disk()
        self.assertNotIn("fact", journal)
        self.assertIn("-synthetic-", journal["workspace"]["create_response"]["displayName"])
        self.assertEqual(journal["owner"]["principal_id"], OWNER)
        self.assertEqual(journal["lakehouse"]["accepted_response"]["status"], 202)
        self.assertEqual(journal["lakehouse"]["operation_state"]["operation_id"], INTENT)
        self.calls.clear()
        self.run_provisioner(resume=True)
        self.assertEqual(self.calls, [("GET", f"{WS}/{WORKSPACE}"), ("GET", f"{LH}/{LAKEHOUSE}")])

    def test_synchronous_lakehouse_create_and_resume(self):
        self.fail = "lakehouse-201"
        self.assertEqual(self.run_provisioner(), {"workspace_id": WORKSPACE, "item_id": LAKEHOUSE})
        self.assertEqual(self.disk()["lakehouse"]["accepted_response"]["status"], 201)
        self.calls.clear()
        self.run_provisioner(resume=True)
        self.assertEqual(self.calls, [("GET", f"{WS}/{WORKSPACE}"), ("GET", f"{LH}/{LAKEHOUSE}")])

    def test_wrong_capacity_and_lakehouse_parent_block_confirmation(self):
        for failure, expected, blocked_post in (
            ("workspace-capacity", "Workspace live readback", LH),
            ("lakehouse-parent", "Lakehouse live readback", None),
        ):
            with self.subTest(failure=failure), tempfile.TemporaryDirectory() as directory:
                self.directory = Path(directory)
                self.calls.clear()
                self.fail = failure
                with self.assertRaisesRegex(CandidateBlocked, expected):
                    self.run_provisioner()
                if blocked_post is not None:
                    self.assertNotIn(("POST", blocked_post), self.calls)
                self.assertTrue(self.disk()["pending_operations"])

    def test_resume_rejects_missing_or_inconsistent_confirmed_receipts(self):
        self.run_provisioner()
        original = self.disk()
        changes = (
            lambda journal: journal["workspace"].pop("accepted_response"),
            lambda journal: journal["workspace"]["accepted_response"].update(body={}),
            lambda journal: journal["lakehouse"]["operation_state"].update(operation_id=OWNER),
            lambda journal: journal["lakehouse"]["accepted_response"].update(body_read=False),
        )
        for change in changes:
            with self.subTest(change=change):
                journal = json.loads(json.dumps(original))
                change(journal)
                (self.directory / STATE_FILE).write_text(json.dumps(journal), encoding="utf-8")
                self.calls.clear()
                with self.assertRaises(CandidateBlocked):
                    self.run_provisioner(resume=True)
                self.assertFalse(any(method == "POST" for method, _ in self.calls))

    def test_lost_post_response_never_reposts_or_searches(self):
        self.fail = "lost"
        with self.assertRaisesRegex(CandidateBlocked, "Mock lost response"):
            self.run_provisioner()
        before = self.disk()
        with self.assertRaisesRegex(CandidateBlocked, "Existing journal"):
            self.run_provisioner()
        with self.assertRaisesRegex(CandidateBlocked, "no retained HTTP response"):
            self.run_provisioner(resume=True)
        self.assertEqual(self.calls, [("POST", WS)])
        self.assertEqual(self.disk(), before)

    def test_existing_name_rejection_is_not_adopted_or_retried(self):
        self.fail = "existing-name"
        for options in ({}, {"resume": True}):
            with self.assertRaisesRegex(CandidateBlocked, "unusable HTTP 409"):
                self.run_provisioner(**options)
        self.assertEqual(self.calls, [("POST", WS)])
        self.assertEqual(self.disk()["pending_operations"][0]["response"]["status"], 409)

    def test_intent_persistence_failure_prevents_post(self):
        with patch("fresh_deploy._persist", side_effect=OSError("mock disk failure")):
            with self.assertRaisesRegex(OSError, "mock disk failure"):
                self.run_provisioner()
        self.assertEqual(self.calls, [])

    def test_recover_workspace_receipt_before_first_child_create(self):
        self.fail = "workspace-readback"
        with self.assertRaisesRegex(CandidateBlocked, "Workspace live readback"):
            self.run_provisioner()
        self.fail = None
        self.calls.clear()
        self.run_provisioner(resume=True)
        self.assertEqual(self.calls[:2], [("GET", f"{WS}/{WORKSPACE}"), ("POST", LH)])
        self.assertNotIn(("POST", WS), self.calls)

    def test_resume_known_lro_uses_only_exact_gets(self):
        self.fail = "poll-unavailable"
        with self.assertRaisesRegex(CandidateBlocked, "HTTP 429"):
            self.run_provisioner()
        self.calls.clear()
        self.fail = None
        self.run_provisioner(resume=True)
        self.assertEqual(self.calls, [("GET", f"{WS}/{WORKSPACE}"), ("GET", LRO),
                                      ("GET", LRO + "/result"), ("GET", f"{LH}/{LAKEHOUSE}")])

    def test_resume_known_lro_result_does_not_need_to_poll_again(self):
        self.fail = "lakehouse-readback"
        with self.assertRaisesRegex(CandidateBlocked, "schema support"):
            self.run_provisioner()
        self.calls.clear()
        self.fail = None
        self.run_provisioner(resume=True)
        self.assertEqual(self.calls, [("GET", f"{WS}/{WORKSPACE}"), ("GET", f"{LH}/{LAKEHOUSE}")])

    def test_readback_mismatch_blocks_child_and_resume_checks_binding(self):
        self.fail = "workspace-readback"
        with self.assertRaisesRegex(CandidateBlocked, "Workspace live readback"):
            self.run_provisioner()
        self.calls.clear()
        with self.assertRaisesRegex(CandidateBlocked, "configuration and authenticated principal"):
            self.run_provisioner(resume=True, principal=INTENT)
        self.config["demo-1"]["lakehouse_name"] = "Changed"
        with self.assertRaisesRegex(CandidateBlocked, "configuration and authenticated principal"):
            self.run_provisioner(resume=True)
        self.assertEqual(self.calls, [])

    def test_conflicting_lro_location_is_not_followed(self):
        self.fail = "wrong-operation"
        with self.assertRaisesRegex(CandidateBlocked, "does not match its operation ID"):
            self.run_provisioner()
        self.assertEqual(self.calls[-1], ("POST", LH))

    def test_invalid_201_location_preserves_receipt_without_get(self):
        self.fail = "wrong-location"
        with self.assertRaisesRegex(CandidateBlocked, "disagrees with its Location"):
            self.run_provisioner()
        self.assertEqual(self.calls, [("POST", WS)])
        self.assertEqual(self.disk()["pending_operations"][0]["response"]["status"], 201)

    def test_unreadable_response_keeps_status_and_is_never_reposted(self):
        self.fail = "bad-body"
        for options in ({}, {"resume": True}):
            with self.assertRaisesRegex(CandidateBlocked, "unusable HTTP 201 evidence"):
                self.run_provisioner(**options)
        self.assertEqual(self.disk()["pending_operations"][0]["response"]["status"], 201)
        self.assertEqual(self.calls, [("POST", WS)])

    def test_lock_blocks_before_authentication(self):
        with _state_lock(self.directory):
            with self.assertRaisesRegex(CandidateBlocked, "state is locked"):
                self.run_provisioner()
        self.factory.assert_not_called()


class TransportTest(unittest.TestCase):
    def test_direct_process_timeout_returns_without_waiting_for_completion(self):
        started = time.monotonic()
        with patch("ownership.shutil.which", return_value=sys.executable):
            with self.assertRaisesRegex(CandidateBlocked, "timed out after 0.2s"):
                _az_json("-c", "import time; time.sleep(10)", timeout=0.2)
        self.assertLess(time.monotonic() - started, 5)

    @patch("ownership.shutil.which", return_value="az")
    @patch("ownership.subprocess.run")
    def test_fabric_token_binds_tenant_and_oid_and_headers(self, execute, _which):
        claims = base64.urlsafe_b64encode(json.dumps(
            {"tid": TENANT, "oid": OWNER, "exp": int(time.time()) + 300}
        ).encode()).decode().rstrip("=")
        token = f"header.{claims}.signature"
        execute.return_value = SimpleNamespace(
            returncode=0, stdout=json.dumps({"tenant": TENANT, "accessToken": token}))
        with self.assertRaisesRegex(CandidateBlocked, "expected tenant/principal"):
            FabricClient(TENANT, INTENT, 60, Path("isolated"))
        client = FabricClient(TENANT, OWNER, 60, Path("isolated"))
        self.assertEqual(execute.call_args.kwargs["env"]["AZURE_CONFIG_DIR"], "isolated")
        self.assertLessEqual(execute.call_args.kwargs["timeout"], 60)
        with patch.object(client.requester, "open") as send:
            reply = send.return_value.__enter__.return_value
            reply.status, reply.headers = 201, Message()
            reply.read.return_value = b'{"ok":true}'
            self.assertEqual(client.request("POST", WS, {"test": True}), (201, {}, {"ok": True}, True))
            sent = send.call_args.args[0]
            self.assertEqual(sent.full_url, FABRIC_ORIGIN + WS)
            self.assertEqual(sent.get_header("Authorization"), f"Bearer {token}")
            self.assertEqual(sent.get_header("X-ms-fabric-skill"), "spark-cli")
            reply.status = 200
            for path in (
                f"{WS}/{WORKSPACE}/roleAssignments",
                f"{LH}/{LAKEHOUSE}/jobs/refreshMaterializedLakeViews/instances",
            ):
                with self.subTest(path=path):
                    self.assertEqual(client.request("GET", path), (200, {}, {"ok": True}, True))
                    self.assertEqual(send.call_args.args[0].full_url, FABRIC_ORIGIN + path)
            with self.assertRaisesRegex(ValueError, "Invalid Fabric"):
                client.request("GET", WS + "?outside=guard")
            path = f"{LH}/{LAKEHOUSE}/jobs/refreshMaterializedLakeViews/instances"
            client.request("GET", path, continuation_token="a+b")
            sent = send.call_args.args[0]
            self.assertEqual(sent.full_url, FABRIC_ORIGIN + path + "?continuationToken=a%2Bb")
            self.assertEqual(sent.get_header("X-ms-fabric-skill"), "spark-cli")
            for method, unsafe_path in (("GET", WS), ("POST", path)):
                with self.subTest(method=method, path=unsafe_path), self.assertRaises(ValueError):
                    client.request(method, unsafe_path, continuation_token="token")
            headers = Message()
            headers["Retry-After"] = "20"
            headers["X-MS-Request-ID"] = "request-id"
            send.side_effect = HTTPError(FABRIC_ORIGIN + path, 429, "Throttled", headers,
                                         BytesIO(b'{"errorCode":"TooManyRequests"}'))
            self.assertEqual(client.request("PATCH", path), (
                429, {"retry-after": "20", "x-ms-request-id": "request-id"},
                {"errorCode": "TooManyRequests"}, True,
            ))

    @patch("ownership.shutil.which", return_value="az")
    @patch("ownership.subprocess.run")
    def test_sanitized_timeout_launch_and_exit_diagnostics(self, execute, _which):
        private = "private-token-or-command-output"
        cases = (
            (subprocess.TimeoutExpired([private], 120, output=private, stderr=private), "timed out"),
            (OSError(5, private), "failed to launch"),
            (UnicodeError(private), "unreadable UTF-8"),
        )
        for error, message in cases:
            execute.side_effect = error
            with self.subTest(message=message), self.assertRaises(CandidateBlocked) as caught:
                _az_json("account", "get-access-token")
            self.assertIn(message, str(caught.exception))
            self.assertNotIn(private, str(caught.exception))
            self.assertTrue(caught.exception.__suppress_context__)
            self.assertEqual(execute.call_args.kwargs["timeout"], AZURE_CLI_TIMEOUT_SECONDS)
        execute.side_effect = None
        execute.return_value = SimpleNamespace(returncode=17, stdout=private, stderr=private)
        with self.assertRaisesRegex(CandidateBlocked, "exited with code 17") as caught:
            _az_json("account", "get-access-token")
        self.assertNotIn(private, str(caught.exception))

    @unittest.skipUnless(os.name == "nt", "Windows MSI launcher")
    @patch("ownership.subprocess.run")
    def test_msi_runtime_avoids_cmd_and_preserves_isolated_environment(self, execute):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "python.exe").touch()
            wrapper = root / "wbin" / "az.CMD"
            environment = {"AZURE_CONFIG_DIR": "isolated", "PATH": "selected-path"}
            execute.return_value = SimpleNamespace(returncode=0, stdout='{"ok": true}')
            with patch("ownership.shutil.which", return_value=str(wrapper)) as which:
                self.assertEqual(_az_json("account", "show", timeout=13, env=environment), {"ok": True})
            which.assert_called_once_with("az", path="selected-path")
            self.assertEqual(execute.call_args.args[0][:3], [str(root / "python.exe"), "-IBm", "azure.cli"])
            self.assertEqual(execute.call_args.kwargs["timeout"], 13)
            self.assertEqual(execute.call_args.kwargs["env"],
                             {**environment, "AZ_INSTALLER": "MSI"})
            self.assertNotIn("AZ_INSTALLER", environment)
            (root / "python.exe").unlink()
            execute.reset_mock()
            with patch("ownership.shutil.which", return_value=str(wrapper)):
                with self.assertRaisesRegex(CandidateBlocked, "repair the MSI"):
                    _az_json("account", "show")
            execute.assert_not_called()


if __name__ == "__main__":
    unittest.main()
