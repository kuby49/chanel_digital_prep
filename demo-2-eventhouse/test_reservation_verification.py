import copy
import hashlib
import importlib
import importlib.util
import json
import os
import sys
import unittest
from pathlib import Path
from unittest import mock

from reservation_verification import ReservationError, strict_json_object, verify_registry_binding


GENERATOR_SHA = "b17c89def3ea6b1cf7065894cefa448c1ac78087363e7141989a5adf085d2ed3"
FIXTURE = """{
  "allocator_id": "3c32c5dc705a822332aa7f6d2a0ebc95590ba5b8fce28ec5992f01108aa22864",
  "integrity_sha256": "185b26266d68d024c9d6bf39fa50b202335ed073caab45557d63e5c1a5d9ac35",
  "reservations": {
    "fixture-run": {
      "binding": {
        "config": {
          "batch_class": "measured", "customers": 100, "date_count": 365, "file_count": 2,
          "interval_seconds": 60, "products": 20, "rows_per_file": 3, "run_id": "fixture-run",
          "seed": 42, "start_date": "2024-01-01"
        },
        "generator_sha256": "b17c89def3ea6b1cf7065894cefa448c1ac78087363e7141989a5adf085d2ed3",
        "selected_demos": ["demo-2"],
        "targets": {"demo-2": {
          "fact": "FactSales", "item_id": "55555555-5555-4555-8555-555555555555",
          "workspace_id": "22222222-2222-4222-8222-222222222222"
        }}
      },
      "reservation": {
        "allocator_id": "3c32c5dc705a822332aa7f6d2a0ebc95590ba5b8fce28ec5992f01108aa22864",
        "batch_class": "measured", "count": "6", "end": "7706736977462811117",
        "fingerprint": "4d1aad7e04e39db93aec870fa43645e637f6820ca85966e6efe17c9ce93e4cdf",
        "generator_sha256": "b17c89def3ea6b1cf7065894cefa448c1ac78087363e7141989a5adf085d2ed3",
        "run_id": "fixture-run", "start": "7706736977462811112", "version": 1
      }
    }
  },
  "retained_ranges": [{"end": "5", "source": "example-bootstrap", "start": "1"}],
  "scope": {
    "kind": "fabric", "owner": "chanel_digital_prep/demo-data-generator",
    "producer_workspace_id": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
    "staging_lakehouse_id": "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
    "tenant_id": "cccccccc-cccc-4ccc-8ccc-cccccccccccc", "version": 1
  },
  "target_bindings": {"demo-2": {
    "fact": "FactSales", "item_id": "55555555-5555-4555-8555-555555555555",
    "workspace_id": "22222222-2222-4222-8222-222222222222"
  }},
  "version": 1
}"""


def canonical_hash(value):
    encoded = json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def resign(registry):
    registry["integrity_sha256"] = canonical_hash({
        key: value for key, value in registry.items() if key != "integrity_sha256"
    })


class RegistryVerificationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        default = Path(__file__).resolve().parent.parent / "demo-data-generator" / "generator.py"
        source = Path(os.environ.get("CHANEL_PRODUCER_GENERATOR", str(default)))
        if not source.is_file():
            raise unittest.SkipTest("The finalized shared producer is not present")
        modules = mock.patch.dict(sys.modules)
        modules.start()
        cls.addClassCleanup(modules.stop)
        paths = mock.patch.object(sys, "path", [str(source.parent), *sys.path])
        paths.start()
        cls.addClassCleanup(paths.stop)
        for name in ("generator", "sales_ids", "fabric_storage"):
            sys.modules.pop(name, None)
        spec = importlib.util.spec_from_file_location("generator", source)
        cls.generator = importlib.util.module_from_spec(spec)
        sys.modules["generator"] = cls.generator
        spec.loader.exec_module(cls.generator)
        cls.sales_ids = importlib.import_module("sales_ids")

    def setUp(self):
        self.registry = strict_json_object(FIXTURE)
        entry = self.registry["reservations"]["fixture-run"]
        binding = entry["binding"]
        self.manifest = {
            "contract_version": 1, "run_id": "fixture-run", "config": copy.deepcopy(binding["config"]),
            "destinations": copy.deepcopy(binding["selected_demos"]),
            "sales_line_id_reservation": copy.deepcopy(entry["reservation"]),
        }
        self.expected = {
            "expected_scope": copy.deepcopy(self.registry["scope"]),
            "expected_config": copy.deepcopy(binding["config"]),
            "expected_targets": copy.deepcopy(binding["targets"]),
            "expected_generator_sha256": GENERATOR_SHA,
            "generator": self.generator,
        }

    def verify(self, registry=None, manifest=None, **changes):
        return verify_registry_binding(
            self.registry if registry is None else registry,
            self.manifest if manifest is None else manifest,
            **{**self.expected, **changes},
        )

    def test_producer_fixture_verifies_against_independent_expected_values(self):
        result = self.verify()
        self.assertEqual(result["reservation"], self.manifest["sales_line_id_reservation"])
        self.assertEqual(result["registry_sha256"], self.registry["integrity_sha256"])
        self.assertEqual(result["record_sha256"], canonical_hash(self.registry["reservations"]["fixture-run"]))

    def test_rejects_wrong_expected_identity_configuration_or_generator(self):
        wrong_scope = {**self.expected["expected_scope"], "tenant_id": "dddddddd-dddd-4ddd-8ddd-dddddddddddd"}
        wrong_config = {**self.expected["expected_config"], "seed": 43}
        wrong_target = copy.deepcopy(self.expected["expected_targets"])
        wrong_target["demo-2"]["fact"] = "OtherSales"
        for changes in (
            {"expected_scope": wrong_scope}, {"expected_config": wrong_config},
            {"expected_targets": wrong_target}, {"expected_generator_sha256": "f" * 64},
        ):
            with self.subTest(changes=changes), self.assertRaises(ReservationError):
                self.verify(**changes)

    def test_rejects_manifest_claim_not_in_registry(self):
        manifest = copy.deepcopy(self.manifest)
        manifest["sales_line_id_reservation"]["start"] = "1"
        with self.assertRaises(ReservationError):
            self.verify(manifest=manifest)

    def test_rejects_resigned_range_and_retained_key_collisions(self):
        changed = copy.deepcopy(self.registry)
        changed["reservations"]["fixture-run"]["reservation"]["start"] = "7706736977462811113"
        manifest = copy.deepcopy(self.manifest)
        manifest["sales_line_id_reservation"]["start"] = "7706736977462811113"
        resign(changed)
        with self.assertRaises(ReservationError):
            self.verify(registry=changed, manifest=manifest)

        collision = copy.deepcopy(self.registry)
        collision["retained_ranges"][0]["start"] = "7706736977462811112"
        collision["retained_ranges"][0]["end"] = "7706736977462811112"
        resign(collision)
        with self.assertRaisesRegex(ReservationError, "overlapping"):
            self.verify(registry=collision)

    def test_resigned_target_substitution_cannot_replace_expected_target(self):
        registry = copy.deepcopy(self.registry)
        entry = registry["reservations"]["fixture-run"]
        registry["target_bindings"]["demo-2"]["fact"] = "OtherSales"
        entry["binding"]["targets"]["demo-2"]["fact"] = "OtherSales"
        entry["reservation"]["fingerprint"] = canonical_hash({
            "allocator_id": registry["allocator_id"], "binding": entry["binding"],
        })
        manifest = copy.deepcopy(self.manifest)
        manifest["sales_line_id_reservation"] = copy.deepcopy(entry["reservation"])
        resign(registry)
        with self.assertRaises(ReservationError):
            self.verify(registry=registry, manifest=manifest)

    def test_fresh_run_requires_its_own_distinct_registry_record(self):
        registry = copy.deepcopy(self.registry)
        config = self.generator.RunConfig(**{
            **self.expected["expected_config"], "run_id": "fresh-fixture-run",
        })
        reservation, binding = self.sales_ids._reservation(
            registry["scope"], config, self.expected["expected_targets"], GENERATOR_SHA
        )
        registry["reservations"][config.run_id] = {"reservation": reservation, "binding": binding}
        resign(registry)
        manifest = self.generator.new_manifest(config, ["demo-2"], reservation)
        result = self.verify(registry=registry, manifest=manifest, expected_config=binding["config"])
        self.assertNotEqual(result["record_sha256"], self.verify()["record_sha256"])
        self.assertNotEqual(result["reservation"]["start"], self.manifest["sales_line_id_reservation"]["start"])
        with self.assertRaises(ReservationError):
            self.verify(manifest=manifest, expected_config=binding["config"])

    def test_every_selected_target_requires_an_independent_expected_identity(self):
        registry = copy.deepcopy(self.registry)
        targets = {
            "demo-1": {
                "workspace_id": "33333333-3333-4333-8333-333333333333",
                "item_id": "44444444-4444-4444-8444-444444444444",
                "fact": "FactSales",
            },
            **copy.deepcopy(self.expected["expected_targets"]),
        }
        registry["target_bindings"].update(targets)
        config = self.generator.RunConfig(**{
            **self.expected["expected_config"], "run_id": "multi-target-fixture",
        })
        reservation, binding = self.sales_ids._reservation(registry["scope"], config, targets, GENERATOR_SHA)
        registry["reservations"][config.run_id] = {"reservation": reservation, "binding": binding}
        resign(registry)
        manifest = self.generator.new_manifest(config, sorted(targets), reservation)
        self.verify(registry=registry, manifest=manifest, expected_config=binding["config"],
                    expected_targets=targets)
        with self.assertRaises(ReservationError):
            self.verify(registry=registry, manifest=manifest, expected_config=binding["config"])
        wrong = copy.deepcopy(targets)
        wrong["demo-1"]["item_id"] = "77777777-7777-4777-8777-777777777777"
        with self.assertRaises(ReservationError):
            self.verify(registry=registry, manifest=manifest, expected_config=binding["config"],
                        expected_targets=wrong)

    def test_strict_json_rejects_ambiguous_or_non_utf8_documents(self):
        for raw in (b'{"version":1,"version":2}', b'{"value":NaN}', '{"a":1}'.encode("utf-16")):
            with self.subTest(raw=raw), self.assertRaises(ReservationError):
                strict_json_object(raw)


if __name__ == "__main__":
    unittest.main()
