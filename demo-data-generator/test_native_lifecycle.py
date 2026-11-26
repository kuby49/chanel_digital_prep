import copy
import hashlib
import json
import unittest
from types import SimpleNamespace
from unittest import mock

from fabric_runtime import native_parameters
from fabric_storage import StorageConflict, StorageOutcomeUnknown
from generator import canonical_bytes
from native_lifecycle import LATEST_PATH, LOCK_PATH, load_hooks, run_native_lifecycle
from test_deploy import IDENTITY, LAKEHOUSE, NOTEBOOK, TENANT, WORKSPACE, run_data
from test_fabric_ledger import MemoryStore, TEST_TARGETS
from test_fabric_runner import Adapter, EventhouseAdapter, FakeClock, run_with_scope


def profile(selected):
    data = run_data()
    data.update(run_id="native_template", file_count=1, rows_per_file=2, max_duration_seconds=60)
    producer = {"workspace_id": WORKSPACE, "staging_lakehouse_id": LAKEHOUSE, "notebook_id": NOTEBOOK}
    document = {"contract_version": 1, "tenant_id": TENANT, "data": data,
                "shared": {**producer, "producer_notebook_id": NOTEBOOK, "identity_id": IDENTITY,
                           "target_bindings": {name: TEST_TARGETS[name] for name in selected}}}
    return {"version": 2, "document": document, "selected": sorted(selected), "producer": producer,
            "activity_timeout": "0.00:31:40", "run_window_seconds": 1000,
            "preparation_timeout_seconds": 100, "cleanup_timeout_seconds": 100}


class NativeLifecycleTests(unittest.TestCase):
    def setUp(self):
        self.store, self.clock = MemoryStore(), FakeClock()
        self.events = []

    def allocate(self, selected):
        template = profile(selected)
        document, names, binding, _, expiry = native_parameters(template, now=self.clock.clock())
        return document, names, binding, expiry, template

    def hooks(self, selected):
        result = {}
        for name in selected:
            def prepare(context, name=name):
                manifest = context["read_manifest"]()
                self.assertEqual(manifest["batches"], {})
                self.assertNotIn("scheduler", manifest)
                self.events.append((name, "prepare"))
                return {"status": "ready", "run_id": context["document"]["data"]["run_id"]}

            def verify(context, prepared, name=name):
                self.assertEqual(context["read_manifest"]()["scheduler"]["status"], "completed")
                self.events.append((name, "verify"))
                return {"status": "verified"}

            def stop(context, prepared, name=name):
                run_id = context["document"]["data"]["run_id"]
                self.store.read(f"Files/producer/runs/{run_id}/stop.json")
                self.events.append((name, "stop", prepared))
                return {"status": "stopped"}

            def check_ready(context, prepared, name=name):
                self.events.append((name, "check_ready"))
                return {"status": "ready"}

            result[name] = SimpleNamespace(prepare=prepare, verify=verify, stop=stop, check_ready=check_ready)
        return result

    def execute(self, allocated, hooks):
        from fabric_runtime import run_config
        document, names, binding, expiry, template = allocated
        run, duration = run_config(document)
        adapters = {name: EventhouseAdapter(self.clock, "succeeded") if name == "demo-2" else Adapter(self.clock)
                    for name in names}

        def produce(before_source):
            return run_with_scope(self.store, run, adapters, duration, 1, clock=self.clock.clock,
                                  sleep=self.clock.sleep, independent_source=True, fresh_only=True,
                                  before_source=before_source,
                                  demo3_admission=(expiry, binding["config_sha256"]) if "demo-3" in names else None)

        return run_native_lifecycle(self.store, document, names, binding, expiry, template,
                                    {"sleep": self.clock.sleep, "stop_requested": lambda: False},
                                    produce, hooks=hooks, clock=self.clock.clock)

    def test_repeat_clicks_get_new_bound_ids_and_prepare_automatically_before_source(self):
        selected = ["demo-1", "demo-2", "demo-3"]
        first, second = self.allocate(selected), self.allocate(selected)
        self.assertNotEqual(first[0]["data"]["run_id"], second[0]["data"]["run_id"])
        self.assertNotEqual(first[2]["config_sha256"], second[2]["config_sha256"])
        self.assertEqual(first[3], second[3])
        for allocated in (first, second):
            result = self.execute(allocated, self.hooks(selected))
            self.assertEqual(result["status"], "verified")
            document, _, binding, _, _ = allocated
            self.assertEqual(binding["config_sha256"], hashlib.sha256(canonical_bytes(document)).hexdigest())
            self.assertEqual(binding["nonce"], "native-" + binding["config_sha256"][:32])
            self.assertNotIn(LOCK_PATH, self.store.files)
            journal = json.loads(self.store.read(result["lifecycle_path"])[0])
            self.assertEqual(journal["status"], "completed")
            manifest = json.loads(self.store.read(result["manifest_path"])[0])
            self.assertEqual(manifest["demo3_admission"]["state"], "stopped")
        self.assertEqual(self.events[:4], [("demo-2", "prepare"), ("demo-3", "prepare"),
                                          ("demo-1", "prepare"), ("demo-1", "check_ready")])
        self.assertEqual(json.loads(self.store.read(LATEST_PATH)[0])["run_id"], second[0]["data"]["run_id"])

    def test_partial_prepare_stops_failing_and_earlier_hooks_and_keeps_exclusion(self):
        allocated = self.allocate(["demo-1", "demo-2", "demo-3"])
        hooks = self.hooks(allocated[1])

        def partial(context):
            self.store.create("Files/track-owned-intent.json", b"accepted?")
            raise RuntimeError("mutation outcome uncertain")

        hooks["demo-3"].prepare = partial
        with self.assertRaisesRegex(RuntimeError, "mutation outcome uncertain"):
            self.execute(allocated, hooks)
        stops = [item for item in self.events if item[1] == "stop"]
        self.assertEqual([item[0] for item in stops], ["demo-3", "demo-2"])
        self.assertIsNone(stops[0][2])
        self.assertEqual(stops[1][2]["status"], "ready")
        self.assertIn(LOCK_PATH, self.store.files)
        journal_path = f"Files/producer/runs/{allocated[0]['data']['run_id']}/native-lifecycle.json"
        self.assertEqual(json.loads(self.store.read(journal_path)[0])["status"], "blocked")
        self.assertFalse(any(path.endswith(".csv") for path in self.store.files))

    def test_different_run_cannot_enter_while_global_target_gate_is_owned(self):
        first, second = self.allocate(["demo-1"]), self.allocate(["demo-1"])
        hooks = self.hooks(first[1])
        prepare = hooks["demo-1"].prepare

        def overlapping(context):
            with self.assertRaises(StorageConflict):
                self.execute(second, self.hooks(second[1]))
            return prepare(context)

        hooks["demo-1"].prepare = overlapping
        self.execute(first, hooks)
        self.assertNotIn(f"Files/producer/runs/{second[0]['data']['run_id']}/native-lifecycle.json", self.store.files)

    def test_cleanup_failure_retains_lock_and_both_errors_remain_visible(self):
        allocated = self.allocate(["demo-1"])
        hooks = self.hooks(allocated[1])
        hooks["demo-1"].verify = lambda *args: {"status": "blocked"}

        def uncertain_stop(*args):
            raise RuntimeError("owned job terminal state unknown")

        hooks["demo-1"].stop = uncertain_stop
        with self.assertRaisesRegex(RuntimeError, "did not prove verified.*terminal state unknown"):
            self.execute(allocated, hooks)
        self.assertIn(LOCK_PATH, self.store.files)

    def test_unknown_lock_create_and_run_id_collision_are_not_retried_or_stolen(self):
        allocated = self.allocate(["demo-1"])
        self.store.unknown_create_suffix = "writer.lock"
        with self.assertRaises(StorageOutcomeUnknown):
            self.execute(allocated, self.hooks(allocated[1]))
        self.assertIn(LOCK_PATH, self.store.files)
        self.assertEqual(self.events, [])
        self.store = MemoryStore()
        path = f"Files/producer/runs/{allocated[0]['data']['run_id']}/native-lifecycle.json"
        self.store.create(path, b"existing-run")
        with self.assertRaises(StorageConflict):
            self.execute(allocated, self.hooks(allocated[1]))
        self.assertEqual(self.store.read(path)[0], b"existing-run")
        self.assertIn(LOCK_PATH, self.store.files)

    def test_hook_cannot_change_bound_document_and_unproven_ready_blocks_sales(self):
        allocated = self.allocate(["demo-1"])
        hooks = self.hooks(allocated[1])

        def change(context):
            context["document"]["data"]["rows_per_file"] = 100
            return {"status": "ready"}

        original = copy.deepcopy(allocated[0])
        hooks["demo-1"].prepare = change
        with self.assertRaisesRegex(RuntimeError, "immutable"):
            self.execute(allocated, hooks)
        self.assertEqual(allocated[0], original)
        self.assertFalse(any(path.endswith(".csv") for path in self.store.files))

    def test_missing_hook_fails_before_sales_or_lock_acquisition(self):
        allocated = self.allocate(["demo-1"])
        document, names, binding, expiry, template = allocated
        with mock.patch("native_lifecycle.importlib.import_module", side_effect=ImportError):
            with self.assertRaisesRegex(RuntimeError, "unavailable"):
                run_native_lifecycle(self.store, document, names, binding, expiry, template, {}, mock.Mock())
        self.assertEqual(self.store.files, {})
        with mock.patch("native_lifecycle.importlib.import_module", return_value=SimpleNamespace()):
            with self.assertRaisesRegex(RuntimeError, "incomplete"):
                load_hooks(names)

    def test_preparation_consumes_deadline_and_cannot_shift_source_outside_window(self):
        allocated = self.allocate(["demo-1"])
        hooks = self.hooks(allocated[1])
        prepare = hooks["demo-1"].prepare

        def late(context):
            value = prepare(context)
            self.clock.sleep(100)
            return value

        hooks["demo-1"].prepare = late
        with self.assertRaisesRegex(RuntimeError, "admission window"):
            self.execute(allocated, hooks)
        self.assertFalse(any(path.endswith(".csv") for path in self.store.files))
        self.assertTrue(any(event[1] == "stop" for event in self.events))
        self.assertIn(LOCK_PATH, self.store.files)
