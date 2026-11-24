import json
import sys
import tempfile
import unittest
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

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
