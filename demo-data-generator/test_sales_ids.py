import csv
import hashlib
import io
import json
import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

from delivery import begin_submission
from fabric_storage import StorageConflict, StorageError, StorageOutcomeUnknown
from generator import (MAX_SALES_LINE_ID, RunConfig, _source_sha256, generator_source_sha256,
                       sales_line_id_range, stage_batch)
from sales_ids import (AllocationCollision, AllocationError, AllocationUncertain, LOCK_PATH, REGISTRY_PATH,
                       canonical_bytes, local_reservation, registry_bytes, reserve)
from test_fabric_ledger import MemoryStore, TEST_SCOPE, TEST_TARGETS, initialized_store, ledger_for


def update_registry(store, edit):
    raw, etag = store.read(REGISTRY_PATH)
    registry = json.loads(raw)
    edit(registry)
    body = {key: value for key, value in registry.items() if key != "integrity_sha256"}
    registry["integrity_sha256"] = hashlib.sha256(canonical_bytes(body)).hexdigest()
    store.replace(REGISTRY_PATH, canonical_bytes(registry) + b"\n", etag)


class SalesIdTests(unittest.TestCase):
    def setUp(self):
        self.store = initialized_store(retained=[{"source": "demo-2 legacy bootstrap", "start": "1", "end": "5"}])
        self.config = RunConfig("allocation-a", file_count=2, rows_per_file=3)
        self.targets = {"demo-1": TEST_TARGETS["demo-1"], "demo-2": TEST_TARGETS["demo-2"]}

    def test_runs_and_bootstrap_have_disjoint_ranges_and_replay_exact_bytes(self):
        configs = (self.config, RunConfig("allocation-b", file_count=2, rows_per_file=3),
                   RunConfig("bootstrap-c", batch_class="bootstrap", file_count=2, rows_per_file=3))
        all_ids = set(range(1, 6))
        snapshots = []
        for config in configs:
            with ledger_for(self.store, config, list(self.targets)) as ledger:
                reservation = ledger.manifest["sales_line_id_reservation"]
                payloads = []
                ids = []
                for number in range(1, config.file_count + 1):
                    ledger.stage_batch(number)
                    payload = ledger.staged_bytes(number)
                    payloads.append(payload)
                    ids.extend(int(row["SalesLineID"]) for row in
                               csv.DictReader(io.StringIO(payload.decode("utf-8"))))
                start, end = int(reservation["start"]), int(reservation["end"])
                self.assertEqual(ids, list(range(start, end + 1)))
                self.assertEqual(reservation["count"], "6")
                self.assertEqual(reservation["batch_class"], config.batch_class)
                self.assertLessEqual(end, MAX_SALES_LINE_ID)
                self.assertTrue(all_ids.isdisjoint(ids))
                all_ids.update(ids)
                snapshots.append((reservation, payloads))
            with ledger_for(self.store, config, list(reversed(list(self.targets)))) as replay:
                self.assertEqual(replay.manifest["sales_line_id_reservation"], snapshots[-1][0])
                for number, payload in enumerate(snapshots[-1][1], 1):
                    replay.stage_batch(number)
                    self.assertEqual(replay.staged_bytes(number), payload)
        registry = json.loads(self.store.read(REGISTRY_PATH)[0])
        self.assertEqual(set(registry["reservations"]), {config.run_id for config in configs})
        self.assertEqual(registry["reservations"][self.config.run_id]["binding"]["targets"], self.targets)

    def test_retained_and_reserved_range_collisions_fail_before_staging(self):
        start, end = sales_line_id_range(self.config)
        store = initialized_store(retained=[{"source": "retained fact inventory",
                                             "start": str(start), "end": str(end)}])
        with self.assertRaises(AllocationCollision):
            with ledger_for(store, self.config, ["demo-1"]):
                self.fail("overlapping retained keys must block")
        self.assertNotIn(f"Files/producer/runs/{self.config.run_id}/manifest.json", store.files)
        self.assertFalse(any(path.endswith(".csv") for path in store.files))

        with patch("sales_ids.sales_line_id_range", return_value=(100, 102)):
            first = RunConfig("first", file_count=1, rows_per_file=3)
            second = RunConfig("second", file_count=1, rows_per_file=3)
            reserve(self.store, TEST_SCOPE, first, {"demo-1": TEST_TARGETS["demo-1"]})
            with self.assertRaises(AllocationCollision):
                reserve(self.store, TEST_SCOPE, second, {"demo-1": TEST_TARGETS["demo-1"]})
        registry = json.loads(self.store.read(REGISTRY_PATH)[0])
        self.assertEqual(set(registry["reservations"]), {"first"})

    def test_bigint_limits_and_noncanonical_retained_keys_fail_closed(self):
        full = RunConfig("full", file_count=1, rows_per_file=MAX_SALES_LINE_ID)
        self.assertEqual(sales_line_id_range(full), (1, MAX_SALES_LINE_ID))
        with self.assertRaisesRegex(ValueError, "BIGINT"):
            sales_line_id_range(replace(full, rows_per_file=MAX_SALES_LINE_ID + 1))
        for start, end in (("01", "2"), ("-0", "2"), ("1", str(MAX_SALES_LINE_ID + 1)),
                           ("-9223372036854775809", "2")):
            with self.subTest(start=start, end=end), self.assertRaises(AllocationError):
                registry_bytes(TEST_SCOPE, TEST_TARGETS, [{"source": "inventory", "start": start, "end": end}])

    def test_replay_rejects_configuration_targets_and_generator_changes(self):
        reserve(self.store, TEST_SCOPE, self.config, self.targets)
        with self.assertRaisesRegex(AllocationError, "differs"):
            reserve(self.store, TEST_SCOPE, replace(self.config, seed=99), self.targets, require_existing=True)
        with self.assertRaisesRegex(AllocationError, "differs"):
            reserve(self.store, TEST_SCOPE, self.config, {"demo-1": self.targets["demo-1"]}, require_existing=True)
        changed_target = dict(self.targets)
        changed_target["demo-2"] = dict(changed_target["demo-2"], fact="OtherFact")
        with self.assertRaisesRegex(AllocationError, "not registered"):
            reserve(self.store, TEST_SCOPE, self.config, changed_target, require_existing=True)
        with patch("sales_ids.generator_source_sha256", return_value="f" * 64):
            with self.assertRaisesRegex(AllocationError, "differs"):
                reserve(self.store, TEST_SCOPE, self.config, self.targets, require_existing=True)

    def test_partial_target_binding_keeps_allocator_identity_stable(self):
        store = MemoryStore()
        store.create(REGISTRY_PATH, registry_bytes(TEST_SCOPE, {"demo-1": TEST_TARGETS["demo-1"]}, []))
        first = reserve(store, TEST_SCOPE, self.config, {"demo-1": TEST_TARGETS["demo-1"]})
        second_config = RunConfig("partial-b", file_count=1, rows_per_file=2)
        with self.assertRaisesRegex(AllocationError, "not registered"):
            reserve(store, TEST_SCOPE, second_config, {"demo-2": TEST_TARGETS["demo-2"]})
        # This models a separately authorized, quiesced onboarding; it is not an onboarding API.
        update_registry(store, lambda registry: registry["target_bindings"].update({"demo-2": TEST_TARGETS["demo-2"]}))
        second = reserve(store, TEST_SCOPE, second_config, {"demo-2": TEST_TARGETS["demo-2"]})
        self.assertEqual(first["allocator_id"], second["allocator_id"])
        self.assertNotEqual(first["fingerprint"], second["fingerprint"])
        self.assertEqual(first, reserve(store, TEST_SCOPE, self.config,
                                        {"demo-1": TEST_TARGETS["demo-1"]}, require_existing=True))
        update_registry(store, lambda registry:
                        registry["target_bindings"]["demo-1"].update(fact="changed"))
        with self.assertRaisesRegex(AllocationError, "target binding differs"):
            reserve(store, TEST_SCOPE, second_config, {"demo-2": TEST_TARGETS["demo-2"]})

    def test_missing_or_corrupt_registry_cannot_start_a_run(self):
        missing = MemoryStore()
        with self.assertRaisesRegex(AllocationError, "missing"):
            with ledger_for(missing, self.config, ["demo-1"]):
                self.fail("missing registry must block")
        self.assertEqual(missing.files, {})

        raw, etag = self.store.read(REGISTRY_PATH)
        self.store.files[REGISTRY_PATH] = (raw.replace(b'"version":1', b'"version":1,"version":1', 1), etag)
        with self.assertRaisesRegex(AllocationError, "corrupt"):
            with ledger_for(self.store, self.config, ["demo-1"]):
                self.fail("duplicate registry keys must block")
        self.assertFalse(any(path.endswith(".csv") for path in self.store.files))

        self.store.files[REGISTRY_PATH] = (raw.replace(b'"integrity_sha256":"', b'"integrity_sha256":"0'), etag)
        with self.assertRaisesRegex(AllocationError, "integrity"):
            reserve(self.store, TEST_SCOPE, self.config, self.targets)

    def test_missing_manifest_or_reservation_never_restarts_a_previously_allocated_run(self):
        with ledger_for(self.store, self.config, ["demo-1"]) as ledger:
            ledger.stage_batch(1)
            manifest_path = ledger.manifest_path
            csv_path = f"{ledger.directory}/{ledger.manifest['batches']['1']['filename']}"
        del self.store.files[manifest_path]
        with self.assertRaisesRegex(AllocationError, "without a run manifest"):
            with ledger_for(self.store, self.config, ["demo-1"]):
                self.fail("missing manifest must not be reconstructed")
        self.assertIn(csv_path, self.store.files)

        store = initialized_store()
        with ledger_for(store, self.config, ["demo-1"]) as ledger:
            manifest_path = ledger.manifest_path
        update_registry(store, lambda registry: registry["reservations"].clear())
        with self.assertRaisesRegex(AllocationError, "no allocator reservation"):
            with ledger_for(store, self.config, ["demo-1"]):
                self.fail("missing reservation must block replay")
        self.assertIn(manifest_path, store.files)

    def test_duplicate_manifest_keys_and_wrong_reservation_types_are_rejected(self):
        with ledger_for(self.store, self.config, ["demo-1"]) as ledger:
            path = ledger.manifest_path
        raw, etag = self.store.read(path)
        self.store.files[path] = (b'{"contract_version":1,' + raw.lstrip()[1:], etag)
        with self.assertRaisesRegex(ValueError, "duplicate JSON key"):
            with ledger_for(self.store, self.config, ["demo-1"]):
                self.fail("duplicate manifest keys must block")
        manifest = json.loads(raw)
        manifest["sales_line_id_reservation"]["version"] = True
        self.store.files[path] = (json.dumps(manifest).encode("utf-8"), etag)
        with self.assertRaisesRegex(ValueError, "reservation differs"):
            with ledger_for(self.store, self.config, ["demo-1"]):
                self.fail("JSON boolean must not equal reservation version 1")
        self.store.files[path] = (raw, etag)
        update_registry(self.store, lambda registry:
                        registry["reservations"][self.config.run_id]["reservation"].update(version=True))
        with self.assertRaisesRegex(AllocationError, "fingerprint differs"):
            with ledger_for(self.store, self.config, ["demo-1"]):
                self.fail("invalid registry reservation must block")

    def test_registry_disappearance_after_initialization_retains_run_lock(self):
        with self.assertRaises(AllocationError):
            with ledger_for(self.store, self.config, ["demo-1"]) as ledger:
                del self.store.files[REGISTRY_PATH]
                ledger.stage_batch(1)
        self.assertIn(f"Files/producer/runs/{self.config.run_id}/writer.lock", self.store.files)
        self.assertIn(LOCK_PATH, self.store.files)
        self.assertFalse(any(path.endswith(".csv") for path in self.store.files))

    def test_allocator_lock_serializes_runs_and_rejects_replaced_lock(self):
        with ledger_for(self.store, self.config, ["demo-1"]) as ledger:
            with self.assertRaisesRegex(RuntimeError, "allocator is locked"):
                with ledger_for(self.store, RunConfig("other"), ["demo-1"]):
                    self.fail("overlapping producer runs must be serialized")
            raw, etag = self.store.read(LOCK_PATH)
            self.store.replace(LOCK_PATH, raw, etag)
            with self.assertRaisesRegex(AllocationError, "lock changed"):
                ledger.stage_batch(1)
        self.assertIn(LOCK_PATH, self.store.files)
        self.assertFalse(any(path.endswith(".csv") for path in self.store.files))

    def test_ambiguous_lock_creation_blocks_all_other_runs(self):
        for suffix in ("sales-line-id-v1.lock", "writer.lock"):
            with self.subTest(suffix=suffix):
                store = initialized_store()
                store.unknown_create_suffix = suffix
                with self.assertRaises(StorageOutcomeUnknown):
                    with ledger_for(store, self.config, ["demo-1"]):
                        self.fail("uncertain lock creation must block")
                self.assertIn(LOCK_PATH, store.files)
                self.assertFalse(any(path.endswith(".csv") for path in store.files))
                with self.assertRaisesRegex(RuntimeError, "allocator is locked"):
                    with ledger_for(store, RunConfig("other"), ["demo-1"]):
                        self.fail("uncertain lock must block other runs")

    def test_generator_source_disappearance_retains_run_lock(self):
        with tempfile.TemporaryDirectory() as root:
            with self.assertRaisesRegex(RuntimeError, "source is unavailable"):
                with ledger_for(self.store, self.config, ["demo-1"]) as ledger:
                    with patch("generator.__file__", str(Path(root) / "missing-generator.py")):
                        ledger.stage_batch(1)
        self.assertIn(f"Files/producer/runs/{self.config.run_id}/writer.lock", self.store.files)
        self.assertFalse(any(path.endswith(".csv") for path in self.store.files))

    def test_fabric_submission_rejects_tampered_staging_and_manifest(self):
        with ledger_for(self.store, self.config, ["demo-1"]) as ledger:
            batch = ledger.stage_batch(1)
            manifest_path = ledger.manifest_path
            staged_path = f"{ledger.directory}/{batch['filename']}"
        payload, file_etag = self.store.read(staged_path)
        lines = payload.decode("utf-8").splitlines()
        cells = lines[1].split(",")
        cells[0] = str(MAX_SALES_LINE_ID)
        lines[1] = ",".join(cells)
        payload = ("\n".join(lines) + "\n").encode("utf-8")
        self.store.replace(staged_path, payload, file_etag)
        raw, manifest_etag = self.store.read(manifest_path)
        manifest = json.loads(raw)
        manifest["batches"]["1"]["sha256"] = hashlib.sha256(payload).hexdigest()
        self.store.replace(manifest_path, json.dumps(manifest).encode("utf-8"), manifest_etag)
        with ledger_for(self.store, self.config, ["demo-1"]) as ledger:
            with self.assertRaisesRegex(RuntimeError, "deterministic"):
                ledger.staged_bytes(1)
            with self.assertRaisesRegex(RuntimeError, "deterministic"):
                ledger.begin_submission(1, "demo-1", 1)

    def test_etag_conflict_reloads_competing_reservation_without_losing_it(self):
        class CompetingStore(MemoryStore):
            competitor = None
            conflicts = 0

            def replace(self, path, data, etag):
                if path == REGISTRY_PATH and self.competitor is not None:
                    competitor, self.competitor = self.competitor, None
                    reserve(self, TEST_SCOPE, competitor, {"demo-1": TEST_TARGETS["demo-1"]})
                try:
                    return super().replace(path, data, etag)
                except StorageConflict:
                    self.conflicts += 1
                    raise

        store = initialized_store(CompetingStore())
        store.competitor = RunConfig("competing", file_count=2, rows_per_file=3)
        reserve(store, TEST_SCOPE, self.config, {"demo-1": TEST_TARGETS["demo-1"]})
        registry = json.loads(store.read(REGISTRY_PATH)[0])
        self.assertEqual(set(registry["reservations"]), {"competing", self.config.run_id})
        self.assertEqual(store.conflicts, 1)

    def test_uncertain_write_requires_readback_and_rejected_condition_never_succeeds(self):
        class UncertainStore(MemoryStore):
            mode = None
            writes = 0

            def replace(self, path, data, etag):
                if path == REGISTRY_PATH and self.mode:
                    mode, self.mode = self.mode, None
                    self.writes += 1
                    if mode in ("committed", "rejected"):
                        super().replace(path, data, etag)
                    if mode == "rejected":
                        raise StorageError("OneLake rejected a conditional request header")
                    raise StorageOutcomeUnknown("response lost")
                return super().replace(path, data, etag)

        for mode in ("committed", "uncommitted", "rejected"):
            with self.subTest(mode=mode):
                store = initialized_store(UncertainStore())
                store.mode = mode
                if mode == "committed":
                    with ledger_for(store, self.config, ["demo-1"]) as ledger:
                        ledger.stage_batch(1)
                    self.assertEqual(store.writes, 1)
                else:
                    with self.assertRaises(AllocationUncertain):
                        with ledger_for(store, self.config, ["demo-1"]):
                            self.fail("uncertain reservation must block")
                    self.assertEqual(store.writes, 1)
                    self.assertIn(f"Files/producer/runs/{self.config.run_id}/writer.lock", store.files)
                    self.assertIn(LOCK_PATH, store.files)
                    self.assertNotIn(f"Files/producer/runs/{self.config.run_id}/manifest.json", store.files)
                    with self.assertRaisesRegex(RuntimeError, "allocator is locked"):
                        with ledger_for(store, RunConfig("other"), ["demo-1"]):
                            self.fail("uncertain allocation must block other runs")

    def test_local_replay_rejects_missing_registry_and_missing_manifest(self):
        with tempfile.TemporaryDirectory() as root:
            stage_batch(root, self.config, 1, ["demo-1"])
            registry = Path(root) / ".sales-line-id-v1.json"
            registry.unlink()
            with self.assertRaisesRegex(AllocationError, "missing"):
                stage_batch(root, self.config, 1, ["demo-1"])
        with tempfile.TemporaryDirectory() as root:
            entry = stage_batch(root, self.config, 1, ["demo-1"])
            directory = Path(root) / self.config.run_id
            (directory / "manifest.json").unlink()
            (directory / entry["filename"]).unlink()
            with self.assertRaisesRegex(AllocationError, "without a run manifest"):
                stage_batch(root, self.config, 1, ["demo-1"])

    def test_local_delivery_rejects_tampered_bytes_even_with_a_matching_manifest_hash(self):
        with tempfile.TemporaryDirectory() as root:
            entry = stage_batch(root, self.config, 1, ["demo-1"])
            directory = Path(root) / self.config.run_id
            path = directory / entry["filename"]
            lines = path.read_text(encoding="utf-8").splitlines()
            cells = lines[1].split(",")
            cells[0] = str(MAX_SALES_LINE_ID)
            lines[1] = ",".join(cells)
            payload = ("\n".join(lines) + "\n").encode("utf-8")
            path.write_bytes(payload)
            manifest_path = directory / "manifest.json"
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            manifest["batches"]["1"]["sha256"] = hashlib.sha256(payload).hexdigest()
            manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
            with self.assertRaisesRegex(RuntimeError, "deterministic"):
                stage_batch(root, self.config, 1, ["demo-1"])
            with self.assertRaisesRegex(ValueError, "deterministic"):
                begin_submission(root, self.config.run_id, 1, "demo-1", 1)

    def test_local_ambiguous_reservation_keeps_allocator_lock(self):
        with tempfile.TemporaryDirectory() as root:
            with patch("sales_ids._LocalStore.replace", side_effect=StorageOutcomeUnknown("lost")):
                with self.assertRaises(AllocationUncertain):
                    stage_batch(root, self.config, 1, ["demo-1"])
            self.assertTrue((Path(root) / ".sales-line-id-v1.lock").exists())
            self.assertFalse((Path(root) / self.config.run_id / "manifest.json").exists())
            with self.assertRaisesRegex(AllocationError, "locked"):
                local_reservation(root, self.config, ["demo-1"])

    def test_local_concurrent_allocators_serialize_without_lost_reservations(self):
        with tempfile.TemporaryDirectory() as root:
            configs = [RunConfig(name, file_count=1, rows_per_file=3) for name in ("local-a", "local-b")]
            barrier = threading.Barrier(2)

            def allocate(config):
                barrier.wait(timeout=10)
                try:
                    return local_reservation(root, config, ["demo-1"])
                except AllocationError as error:
                    if "locked" not in str(error):
                        raise
                    return None

            with ThreadPoolExecutor(max_workers=2) as pool:
                results = list(pool.map(allocate, configs))
            for index, result in enumerate(results):
                if result is None:
                    results[index] = local_reservation(root, configs[index], ["demo-1"])
            ranges = [(int(result["start"]), int(result["end"])) for result in results]
            self.assertTrue(ranges[0][1] < ranges[1][0] or ranges[1][1] < ranges[0][0])
            registry = json.loads((Path(root) / ".sales-line-id-v1.json").read_text(encoding="utf-8"))
            self.assertEqual(set(registry["reservations"]), {"local-a", "local-b"})

    def test_source_digest_normalizes_only_utf8_line_endings_and_detects_changes(self):
        self.assertEqual(_source_sha256(b"a\r\nb\rc\n"), _source_sha256(b"a\nb\nc\n"))
        self.assertNotEqual(_source_sha256(b"a\n"), _source_sha256(b"a \n"))
        with self.assertRaises(UnicodeDecodeError):
            _source_sha256(b"\xff")
        with tempfile.TemporaryDirectory() as root:
            altered = Path(root) / "generator.py"
            altered.write_bytes(b"# different source\n")
            with patch("generator.__file__", str(altered)):
                with self.assertRaisesRegex(RuntimeError, "changed after import"):
                    generator_source_sha256()


if __name__ == "__main__":
    unittest.main()
