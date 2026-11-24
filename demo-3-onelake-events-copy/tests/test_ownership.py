import base64
import json
import os
import signal
import subprocess
import sys
import tempfile
import time
import unittest
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import ownership
from deploy import CandidateBlocked, STATE_FILE


class Fabric:
    def __init__(self):
        self.principal_id = str(uuid.uuid4())
        self.capacity = str(uuid.uuid4())
        self.collections = {"/v1/workspaces": {}}
        self.calls = []
        self.before_create = None

    def list(self, path):
        if path == "/v1/capacities":
            return [{"id": self.capacity, "state": "Active"}]
        return list(self.collections[path].values())

    def get(self, path):
        collection, key = path.rsplit("/", 1)
        return self.collections[collection][key]

    def create(self, path, body):
        self.calls.append(path)
        if self.before_create:
            self.before_create()
        item = str(uuid.uuid4())
        kind = "Workspace" if path == "/v1/workspaces" else "Lakehouse"
        props = {"id": item, "type": kind, **body}
        if kind == "Workspace":
            self.collections[f"{path}/{item}/lakehouses"] = {}
        else:
            props["workspaceId"] = path.split("/")[3]
        self.collections[path][item] = props
        return {"id": item}


class OwnershipTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        self.api = Fabric()
        self.config = {"tenant_id": str(uuid.uuid4()), "capacity": {"id": self.api.capacity},
                       "demo-3": {"workspace_name": "{prefix}-fresh"}}
        self.path = self.root / STATE_FILE

    def test_create_intents_precede_posts_and_read_only_verification_rejects_drift(self):
        intents = []

        def capture():
            saved = json.loads(self.path.read_bytes())
            intents.append(saved["pending_operations"][0])

        self.api.before_create = capture
        state = ownership.provision(self.config, "demo", self.root, self.api, create=True)
        self.assertEqual([intent["type"] for intent in intents], ["Workspace", "Lakehouse"])
        self.assertEqual(state["lakehouse"]["workspace_id"], state["workspace"]["id"])
        self.assertEqual(state["pending_operations"], [])
        self.assertNotIn("fact", state)
        self.assertEqual(ownership.provision(self.config, "demo", self.root, self.api), state)
        self.assertEqual(len(self.api.calls), 2)
        self.api.get(f"/v1/workspaces/{state['workspace']['id']}")["description"] += " changed"
        with self.assertRaisesRegex(CandidateBlocked, "marker changed"):
            ownership.provision(self.config, "demo", self.root, self.api)

    def test_pending_create_is_not_replayed_or_adopted(self):
        self.api.before_create = lambda: (_ for _ in ()).throw(CandidateBlocked("response lost"))
        with self.assertRaisesRegex(CandidateBlocked, "response lost"):
            ownership.provision(self.config, "demo", self.root, self.api, create=True)
        before = self.path.read_bytes()
        with self.assertRaisesRegex(CandidateBlocked, "pending"):
            ownership.provision(self.config, "demo", self.root, self.api, create=True)
        self.assertEqual(self.path.read_bytes(), before)
        self.assertEqual(len(self.api.calls), 1)

    def test_existing_unjournaled_workspace_is_not_adopted(self):
        self.api.collections["/v1/workspaces"][str(uuid.uuid4())] = {"displayName": "DEMO-fresh"}
        with self.assertRaisesRegex(CandidateBlocked, "adoption is prohibited"):
            ownership.provision(self.config, "demo", self.root, self.api, create=True)
        self.assertEqual(self.api.calls, [])
        self.assertFalse(self.path.exists())

    def test_read_only_window_is_not_a_fence_and_blocks_enabled_schedule(self):
        ws, pipeline, activator = (str(uuid.uuid4()) for _ in range(3))
        workspace = f"/v1/workspaces/{ws}"
        names = {("DataPipeline", "Copy"): pipeline, ("Reflex", "Events"): activator}
        jobs = f"{workspace}/items/{pipeline}/jobs"
        self.api.collections.update({
            f"{workspace}/items?recursive=true": {
                name: {"displayName": name, "id": item, "type": kind}
                for (kind, name), item in names.items()
            },
            f"{workspace}/roleAssignments": {},
            f"{jobs}/Pipeline/schedules": {},
            f"{jobs}/instances?jobType=Pipeline": {},
        })
        self.api.reflex_definition = lambda *_: [{"type": "timeSeriesView-v1", "payload": {
            "definition": {"type": "Rule", "settings": {"shouldRun": False}},
        }}]
        start = datetime.now(timezone.utc)
        clock = iter((start, start + timedelta(seconds=3)))
        observed = ownership.observe_quiescence(
            self.api, ws, names, window_seconds=3, sleep=lambda _: None, now=lambda: next(clock))
        self.assertTrue(observed["observed_quiescent"])
        self.assertFalse(observed["writer_fence"])
        self.api.collections[f"{jobs}/Pipeline/schedules"]["timer"] = {"enabled": True}
        with self.assertRaisesRegex(CandidateBlocked, "schedule is enabled"):
            ownership.observe_quiescence(self.api, ws, names, window_seconds=3, sleep=lambda _: None)


class CliAuthenticationTests(unittest.TestCase):
    def test_windows_uses_direct_msi_python_and_preserves_isolated_environment(self):
        credential = {"tenant": str(uuid.uuid4()), "accessToken": "synthetic-token"}

        def finish(command, **kwargs):
            kwargs["stdout"].write(json.dumps(credential).encode("utf-8"))
            self.assertNotEqual(kwargs["stdout"], subprocess.PIPE)
            self.assertNotEqual(kwargs["stderr"], subprocess.PIPE)
            self.assertEqual(kwargs["timeout"], 120)
            self.assertEqual(kwargs["env"]["AZ_INSTALLER"], "MSI")
            self.assertEqual(kwargs["env"]["AZURE_CONFIG_DIR"], "isolated-test-config")

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            python = root / "python.exe"
            python.touch()
            launcher = str(root / "wbin" / "az.cmd")
            with mock.patch.object(ownership.sys, "platform", "win32"), \
                    mock.patch.object(ownership.shutil, "which", return_value=launcher), \
                    mock.patch.dict(os.environ, {"AZURE_CONFIG_DIR": "isolated-test-config"}), \
                    mock.patch.object(ownership.subprocess, "run", side_effect=finish) as run:
                self.assertEqual(ownership.cli_credential(ownership._API, timeout=120), credential)
            self.assertEqual(run.call_args.args[0], [
                str(python), "-IBm", "azure.cli", "account", "get-access-token",
                "--resource", ownership._API, "--output", "json",
            ])
            self.assertNotIn("shell", run.call_args.kwargs)

    def test_missing_windows_msi_python_never_falls_back_to_batch(self):
        with tempfile.TemporaryDirectory() as directory:
            with mock.patch.object(ownership.sys, "platform", "win32"), \
                    mock.patch.object(ownership.shutil, "which", return_value=str(
                        Path(directory) / "wbin" / "az.cmd")), \
                    mock.patch.object(ownership.subprocess, "run") as run:
                with self.assertRaisesRegex(OSError, "batch fallback is prohibited"):
                    ownership.cli_credential(ownership._API, timeout=120)
                run.assert_not_called()

    def test_non_windows_keeps_native_az_invocation(self):
        def finish(command, **kwargs):
            kwargs["stdout"].write(b'{"tenant":"test","accessToken":"synthetic"}')
            self.assertIsNone(kwargs["env"])

        with mock.patch.object(ownership.sys, "platform", "linux"), \
                mock.patch.object(ownership.shutil, "which", return_value="/usr/bin/az"), \
                mock.patch.object(ownership.subprocess, "run", side_effect=finish) as run:
            ownership.cli_credential(ownership._API, timeout=120)
            self.assertEqual(run.call_args.args[0][:3], ["/usr/bin/az", "account", "get-access-token"])

    def test_fabric_checks_both_tenant_sources_and_principal_after_token_acquisition(self):
        tenant, principal = str(uuid.uuid4()), str(uuid.uuid4())
        for credential_tenant, claim_tenant, claim_principal in (
            (tenant, tenant, principal), ("other", tenant, principal),
            (tenant, "other", principal), (tenant, tenant, "invalid"),
        ):
            claims = {"tid": claim_tenant, "oid": claim_principal}
            payload = base64.urlsafe_b64encode(json.dumps(claims).encode()).decode().rstrip("=")
            credential = {"tenant": credential_tenant, "accessToken": "synthetic." + payload + ".test"}
            with self.subTest(claims=claims, credential_tenant=credential_tenant), \
                    mock.patch.object(ownership, "cli_credential", return_value=credential) as token:
                if (credential_tenant, claim_tenant, claim_principal) == (tenant, tenant, principal):
                    self.assertEqual(ownership.FabricControl.from_cli(tenant, timeout=120).principal_id, principal)
                else:
                    with self.assertRaises(CandidateBlocked):
                        ownership.FabricControl.from_cli(tenant, timeout=120)
                token.assert_called_once_with(ownership._API, timeout=120)

    def test_failed_or_timed_out_acquisition_is_not_retried(self):
        for error in (subprocess.TimeoutExpired("synthetic", 120),
                      subprocess.CalledProcessError(1, "synthetic")):
            with self.subTest(error=type(error).__name__), \
                    mock.patch.object(ownership, "cli_credential", side_effect=error) as token:
                with self.assertRaisesRegex(CandidateBlocked, "identity could not be established"):
                    ownership.FabricControl.from_cli(str(uuid.uuid4()), timeout=120)
                token.assert_called_once()

    def test_timeout_does_not_wait_for_a_child_holding_inherited_output_handles(self):
        with tempfile.TemporaryDirectory() as directory:
            pid_path = Path(directory) / "child.pid"
            script = (
                "import subprocess,sys,time;from pathlib import Path;"
                "child=subprocess.Popen([sys.executable,'-c','import time;time.sleep(30)']);"
                f"Path({str(pid_path)!r}).write_text(str(child.pid));time.sleep(60)"
            )
            started = time.monotonic()
            try:
                with mock.patch.object(ownership, "_cli_command", return_value=[sys.executable, "-c", script]):
                    with self.assertRaises(subprocess.TimeoutExpired):
                        ownership.cli_credential(ownership._API, timeout=5)
                self.assertLess(time.monotonic() - started, 10)
                self.assertTrue(pid_path.is_file(), "Local stand-in must reproduce inherited child handles")
            finally:
                if pid_path.is_file():
                    os.kill(int(pid_path.read_text()), signal.SIGTERM)
