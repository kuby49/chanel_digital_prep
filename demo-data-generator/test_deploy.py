import base64
import copy
import io
import json
import os
import subprocess
import sys
import tempfile
import unittest
from contextlib import nullcontext
from pathlib import Path
from types import SimpleNamespace
from unittest import mock
from urllib import error, parse

import deploy


TENANT = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
CAPACITY = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
IDENTITY = "cccccccc-cccc-4ccc-8ccc-cccccccccccc"
WORKSPACE = "11111111-1111-4111-8111-111111111111"
LAKEHOUSE = "22222222-2222-4222-8222-222222222222"
NOTEBOOK = "33333333-3333-4333-8333-333333333333"
PIPELINE = "44444444-4444-4444-8444-444444444444"
OPERATION = "55555555-5555-4555-8555-555555555555"
DEPLOYMENT = "66666666-6666-4666-8666-666666666666"
ENVIRONMENT = "77777777-7777-4777-8777-777777777777"
PUBLISHED_VERSION = "88888888-8888-4888-8888-888888888888"
RESOURCE_IDS = {"workspace": WORKSPACE, "staging_lakehouse": LAKEHOUSE,
                "environment_item": ENVIRONMENT, "notebook": NOTEBOOK, "pipeline": PIPELINE}


def config(tenant=TENANT):
    return {
        "contract_version": 1,
        "common": {"tenant_id": tenant, "capacity_id": CAPACITY, "prefix": "sample"},
        "shared": {"workspace_name": "{prefix}-producer", "identity_id": IDENTITY},
    }


def run_data():
    return {"run_id": "run-1", "seed": 42, "start_date": "2024-01-01",
            "date_count": 365, "product_count": 20, "customer_count": 100,
            "rows_per_file": 1000, "file_count": 10, "interval_seconds": 60,
            "max_duration_seconds": 541, "currency": "USD"}


def owned_state(plan, kinds=deploy.KINDS):
    state = deploy.new_state(plan)
    state["deployment_id"] = DEPLOYMENT
    for kind in kinds:
        state["resources"][kind] = deploy._record(state, plan, kind, RESOURCE_IDS[kind])
    return state


def legacy_manifest():
    records = {
        "workspace": {"id": WORKSPACE, "name": "sample-producer"},
        "staging_lakehouse": {"id": LAKEHOUSE, "name": "producer_staging", "workspace_id": WORKSPACE},
        "environment_item": {"id": ENVIRONMENT, "name": "producer_environment", "workspace_id": WORKSPACE},
        "notebook": {"id": NOTEBOOK, "name": "producer_notebook", "workspace_id": WORKSPACE},
        "pipeline": {"id": PIPELINE, "name": "producer_wrapper", "workspace_id": WORKSPACE},
    }
    for record in records.values():
        record["ownership"] = "Created for this deployment."
    return {"contract_version": 1, "environment": "MCAP", "prefix": "sample",
            "capacity": {"id": CAPACITY}, "producer_identity": {"type": "User", "object_id": IDENTITY},
            **records}


def definition(content, path="notebook-content.ipynb"):
    payload = base64.b64encode(json.dumps(content).encode("utf-8")).decode("ascii")
    return {"parts": [{"path": path, "payloadType": "InlineBase64", "payload": payload}]}


def pinned_libraries():
    return [{"name": name, "version": version, "libraryType": "External"}
            for name, version in deploy._environment_spec()["pins"].items()]


class Clock:
    def __init__(self):
        self.value = 0
        self.delays = []

    def now(self):
        return self.value

    def sleep(self, seconds):
        self.delays.append(seconds)
        self.value += seconds


class Response:
    def __init__(self, status=200, body=None, headers=None):
        self.status = status
        self.headers = headers or {}
        self.body = b"" if body is None else json.dumps(body).encode("utf-8")

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def read(self, limit):
        return self.body[:limit]


class Opener:
    def __init__(self, *responses):
        self.responses = list(responses)
        self.requests = []

    def open(self, request, timeout):
        self.requests.append((request, timeout))
        if not self.responses:
            raise AssertionError("Unexpected HTTP request")
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


class MemoryStore:
    def __init__(self, state=None):
        self.state = copy.deepcopy(state)
        self.writes = []

    def read(self):
        return copy.deepcopy(self.state)

    def write(self, state):
        self.state = copy.deepcopy(state)
        self.writes.append(copy.deepcopy(state))

    def lock(self):
        return nullcontext()


class ReadbackClient:
    def __init__(self, plan, state):
        resources = state["resources"]
        workspace = resources["workspace"]
        self.workspace = {"id": workspace["id"], "displayName": workspace["name"],
                          "description": workspace["description"], "type": "Workspace",
                          "capacityId": plan["capacity_id"], "capacityAssignmentProgress": "Completed"}
        self.roles = [{"role": "Admin", "principal": {"id": plan["identity_id"], "type": "User"}}]
        self.items = {}
        for kind in deploy.ITEM_TYPES:
            if kind not in resources:
                continue
            record = resources[kind]
            self.items[record["id"]] = {
                "id": record["id"], "displayName": record["name"], "description": record["description"],
                "type": record["type"], "workspaceId": workspace["id"],
                "defaultIdentity": {"id": plan["identity_id"], "type": "User"},
            }
        self.calls = []
        self.existing = []
        self.compute = {"instancePool": {"name": "Starter Pool"}, "runtimeVersion": "1.3"}
        self.staging_libraries = pinned_libraries()
        self.published_libraries = pinned_libraries()
        self.clock = Clock()
        self._clock = self.clock.now
        self._wait = lambda delay, deadline: self.clock.sleep(delay)
        if "environment_item" in resources:
            self.items[resources["environment_item"]["id"]]["properties"] = {
                "publishDetails": {"state": "Success", "targetVersion": PUBLISHED_VERSION}}

    def get(self, path, deadline=None):
        self.calls.append(("GET", path))
        api_path = parse.urlsplit(path).path
        if api_path.endswith("/staging/sparkcompute"):
            return {}, copy.deepcopy(self.compute)
        resource_id = api_path.rsplit("/", 1)[-1]
        if resource_id == self.workspace["id"]:
            return {}, copy.deepcopy(self.workspace)
        return {}, copy.deepcopy(self.items[resource_id])

    def list_all(self, path, collection_key="value"):
        self.calls.append(("LIST", path))
        api_path = parse.urlsplit(path).path
        if api_path.endswith("/staging/libraries"):
            return copy.deepcopy(self.staging_libraries)
        if api_path.endswith("/libraries"):
            return copy.deepcopy(self.published_libraries)
        return copy.deepcopy(self.roles if path.endswith("/roleAssignments") else self.existing)


class ContractTests(unittest.TestCase):
    def args(self, mode, extra=()):
        root = Path.cwd().resolve()
        return ["--mode", mode, "--config", str(root / "config-input.json"), "--prefix", "sample",
                "--state-dir", str(root / "owned-state"), "--result", str(root / "contract-result.json"), *extra]

    def invoke(self, mode, document, extra=()):
        with (mock.patch.object(Path, "read_text", return_value=json.dumps(document)),
              mock.patch.object(deploy, "_atomic_json") as save,
              mock.patch.object(deploy, "azure_cli_token") as token,
              mock.patch.object(deploy, "FabricClient") as client):
            exit_code = deploy.main(self.args(mode, extra))
        result = save.call_args_list[-1].args[1]
        token.assert_not_called()
        client.assert_not_called()
        self.assertEqual(set(result), {"contract_version", "track", "mode", "status", "ready", "diagnostics"})
        self.assertEqual(result["mode"], mode)
        self.assertEqual(result["track"], "shared")
        self.assertFalse(result["ready"])
        return exit_code, result

    def test_dry_run_is_a_local_plan_and_never_authenticates(self):
        code, result = self.invoke("dry-run", {"contract_version": 1})
        self.assertEqual((code, result["status"]), (0, "pass"))
        self.assertIn("Plan only", result["diagnostics"][0])

    def test_incomplete_deployment_and_validation_are_blocked(self):
        for mode in ("deploy", "validate-only"):
            with self.subTest(mode=mode):
                code, result = self.invoke(mode, {"contract_version": 1})
                self.assertEqual((code, result["status"]), (1, "blocked"))
                self.assertTrue(any("requires" in item.lower() for item in result["diagnostics"]))

    def test_cleanup_never_deletes_and_requires_yes(self):
        code, result = self.invoke("cleanup", config())
        self.assertEqual((code, result["status"]), (1, "fail"))
        self.assertIn("--yes", result["diagnostics"][0])
        code, result = self.invoke("cleanup", config(), ["--yes"])
        self.assertEqual((code, result["status"]), (1, "blocked"))
        self.assertIn("nothing was deleted", result["diagnostics"][0])

    def test_data_execution_fails_closed_even_with_a_complete_config(self):
        document = config()
        document["data"] = run_data()
        document["shared"]["ready"] = True
        document["shared"]["destination_connection_ids"] = {"demo-1": WORKSPACE, "demo-2": LAKEHOUSE}
        code, result = self.invoke("run-data", document, ["--selected-demos", "demo-1,demo-2"])
        self.assertEqual((code, result["status"]), (1, "blocked"))
        self.assertIn("no job was started", result["diagnostics"][0])
        for selected in ("demo-1,demo-1", "demo-1,demo-4", ""):
            with self.subTest(selected=selected):
                code, result = self.invoke("run-data", document, ["--selected-demos", selected])
                self.assertEqual((code, result["status"]), (1, "fail"))
        document["data"]["max_duration_seconds"] = 540
        code, result = self.invoke("run-data", document, ["--selected-demos", "demo-1,demo-2"])
        self.assertEqual((code, result["status"]), (1, "fail"))
        self.assertIn("final scheduled batch offset", result["diagnostics"][0])

    def test_invalid_configuration_does_not_disclose_values(self):
        document = config()
        document["common"]["tenant_id"] = "private-value"
        code, result = self.invoke("dry-run", document)
        self.assertEqual((code, result["status"]), (1, "fail"))
        self.assertNotIn("private-value", json.dumps(result))
        document = config()
        document["common"]["prefix"] = "other"
        code, result = self.invoke("dry-run", document)
        self.assertEqual((code, result["status"]), (1, "fail"))
        self.assertIn("prefix", result["diagnostics"][0])
        document = config()
        document["shared"]["staging_lakehouse_name"] = "{prefix}-lakehouse"
        code, result = self.invoke("dry-run", document)
        self.assertEqual((code, result["status"]), (1, "fail"))
        self.assertIn("staging_lakehouse_name", result["diagnostics"][0])

    def test_result_cannot_overwrite_config_or_state(self):
        args = self.args("dry-run")
        args[args.index("--result") + 1] = args[args.index("--config") + 1]
        with (mock.patch.object(deploy, "_atomic_json") as save,
              mock.patch("sys.stderr", new_callable=io.StringIO) as stderr):
            self.assertEqual(deploy.main(args), 1)
        save.assert_not_called()
        self.assertEqual(json.loads(stderr.getvalue())["status"], "fail")

    def test_result_cannot_overwrite_another_deployments_state(self):
        args = self.args("dry-run")
        args[args.index("--result") + 1] = str(Path.cwd() / "other-state" / deploy.STATE_NAME.upper())
        with (mock.patch.object(deploy, "_atomic_json") as save,
              mock.patch("sys.stderr", new_callable=io.StringIO) as stderr):
            self.assertEqual(deploy.main(args), 1)
        save.assert_not_called()
        self.assertEqual(json.loads(stderr.getvalue())["status"], "fail")

    def test_result_cannot_overwrite_environment_sources_or_tests(self):
        names = ("environment.yml", "fabric_storage.py", "fabric_ledger.py", "fabric_delivery.py",
                 "fabric_runner.py", "test_generator.py", "test_fabric_ledger.py",
                 "test_fabric_delivery.py", "test_deploy.py", "future_test.py")
        for name in names:
            with self.subTest(name=name):
                args = self.args("dry-run")
                args[args.index("--result") + 1] = str(deploy.HERE / name)
                with (mock.patch.object(deploy, "_atomic_json") as save,
                      mock.patch("sys.stderr", new_callable=io.StringIO) as stderr):
                    self.assertEqual(deploy.main(args), 1)
                save.assert_not_called()
                self.assertEqual(json.loads(stderr.getvalue())["status"], "fail")

    def test_bad_arguments_cannot_overwrite_environment_source(self):
        args = self.args("deploy")
        args.remove("--prefix")
        args.remove("sample")
        args[args.index("--result") + 1] = str(deploy.HERE / "environment.yml")
        with (mock.patch.object(deploy, "_atomic_json") as save,
              mock.patch("sys.stderr", new_callable=io.StringIO) as stderr):
            self.assertEqual(deploy.main(args), 1)
        save.assert_not_called()
        self.assertEqual(json.loads(stderr.getvalue())["status"], "fail")

    def test_result_write_failure_prevents_cloud_work(self):
        with (mock.patch.object(deploy, "_atomic_json", side_effect=OSError("secret")),
              mock.patch.object(deploy, "azure_cli_token") as token,
              mock.patch("sys.stderr", new_callable=io.StringIO) as stderr):
            self.assertEqual(deploy.main(self.args("deploy")), 1)
        token.assert_not_called()
        self.assertNotIn("secret", stderr.getvalue())
        self.assertEqual(json.loads(stderr.getvalue())["status"], "fail")

    def test_bad_arguments_write_the_contract_when_result_is_safe(self):
        args = self.args("deploy")
        args.remove("--prefix")
        args.remove("sample")
        with mock.patch.object(deploy, "_atomic_json") as save:
            self.assertEqual(deploy.main(args), 1)
        result = save.call_args.args[1]
        self.assertEqual((result["mode"], result["status"], result["ready"]), ("deploy", "fail", False))

    def test_ambiguous_invalid_arguments_do_not_overwrite_a_config(self):
        args = self.args("deploy") + ["--config", str(Path.cwd() / "other-config.json"), "--unknown"]
        with (mock.patch.object(deploy, "_atomic_json") as save,
              mock.patch("sys.stderr", new_callable=io.StringIO) as stderr):
            self.assertEqual(deploy.main(args), 1)
        save.assert_not_called()
        self.assertEqual(json.loads(stderr.getvalue())["status"], "fail")

    def test_relative_config_reports_failure_to_absolute_result(self):
        args = self.args("dry-run")
        args[args.index("--config") + 1] = "relative-config.json"
        with mock.patch.object(deploy, "_atomic_json") as save:
            self.assertEqual(deploy.main(args), 1)
        result = save.call_args.args[1]
        self.assertEqual(result["status"], "fail")
        self.assertIn("absolute paths", result["diagnostics"][0])


class OwnershipTests(unittest.TestCase):
    def test_candidate_cli_publishes_per_invocation_sidecar_without_overwriting(self):
        document = config()
        state = owned_state(deploy.parse_config(document, "sample"))
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            state_dir = root / "state"
            store = deploy.StateStore(state_dir)
            store.write(state)
            config_path = root / "config.json"
            config_path.write_text(json.dumps(document), encoding="utf-8")
            original_config = config_path.read_bytes()
            result_path = root / "invocation" / "result.json"
            args = [sys.executable, str(deploy.HERE / "deploy.py"), "--mode", "resolve-candidate-ids",
                    "--config", str(config_path), "--prefix", "sample", "--state-dir", str(state_dir),
                    "--result", str(result_path)]
            completed = subprocess.run(args, capture_output=True, text=True, timeout=30, check=False)
            self.assertEqual(completed.returncode, 0, completed.stderr)
            result = json.loads(result_path.read_text(encoding="utf-8"))
            self.assertEqual(set(result), {"contract_version", "track", "mode", "status", "ready", "diagnostics"})
            self.assertEqual((result["mode"], result["status"], result["ready"]),
                             ("resolve-candidate-ids", "pass", False))
            sidecar = result_path.parent / deploy.CANDIDATE_NAME
            self.assertEqual(json.loads(sidecar.read_text(encoding="utf-8")),
                             {"version": 1, "workspace_id": WORKSPACE, "staging_lakehouse_id": LAKEHOUSE})
            self.assertFalse((state_dir / deploy.CANDIDATE_NAME).exists())
            self.assertEqual(config_path.read_bytes(), original_config)

            old_sidecar = sidecar.read_bytes()
            repeated = subprocess.run(args, capture_output=True, text=True, timeout=30, check=False)
            self.assertEqual(repeated.returncode, 1)
            self.assertEqual(json.loads(result_path.read_text(encoding="utf-8"))["status"], "fail")
            self.assertEqual(sidecar.read_bytes(), old_sidecar)

            bad = copy.deepcopy(document)
            bad["shared"]["workspace_id"] = PIPELINE
            config_path.write_text(json.dumps(bad), encoding="utf-8")
            result_path = root / "another-invocation" / "result.json"
            args[-1] = str(result_path)
            blocked = subprocess.run(args, capture_output=True, text=True, timeout=30, check=False)
            self.assertEqual(blocked.returncode, 1)
            self.assertEqual(json.loads(result_path.read_text(encoding="utf-8"))["status"], "blocked")
            self.assertFalse((result_path.parent / deploy.CANDIDATE_NAME).exists())

    def test_candidate_cli_refuses_config_or_state_dir_as_output(self):
        document = config()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            state_dir = root / "state"
            store = deploy.StateStore(state_dir)
            store.write(owned_state(deploy.parse_config(document, "sample")))
            config_path = root / deploy.CANDIDATE_NAME
            config_path.write_text(json.dumps(document), encoding="utf-8")
            original = config_path.read_bytes()
            args = ["--mode", "resolve-candidate-ids", "--config", str(config_path), "--prefix", "sample",
                    "--state-dir", str(state_dir), "--result", str(root / "result.json")]
            self.assertEqual(deploy.main(args), 1)
            self.assertEqual(config_path.read_bytes(), original)
            self.assertEqual(json.loads((root / "result.json").read_text(encoding="utf-8"))["status"], "fail")
            args[-1] = str(state_dir / "result.json")
            self.assertEqual(deploy.main(args), 1)
            self.assertFalse((state_dir / deploy.CANDIDATE_NAME).exists())

    def test_candidate_ids_use_owned_journal_without_mutating_config_or_calling_fabric(self):
        document = config()
        original = copy.deepcopy(document)
        state = owned_state(deploy.parse_config(document, "sample"))
        with tempfile.TemporaryDirectory() as directory:
            store = deploy.StateStore(directory)
            store.write(state)
            before = store.path.read_bytes()
            with (mock.patch.object(deploy, "azure_cli_token") as token,
                  mock.patch.object(deploy, "FabricClient") as client):
                result = deploy.resolve_candidate_ids(document, "sample", directory)
            token.assert_not_called()
            client.assert_not_called()
            self.assertEqual(result, {"workspace_id": WORKSPACE, "staging_lakehouse_id": LAKEHOUSE})
            self.assertEqual(document, original)
            self.assertEqual(store.path.read_bytes(), before)
            self.assertFalse(store.lock_path.exists())

    def test_candidate_ids_reject_missing_pending_mismatched_and_corrupt_state(self):
        document = config()
        plan = deploy.parse_config(document, "sample")
        with tempfile.TemporaryDirectory() as directory:
            store = deploy.StateStore(directory)
            with self.assertRaisesRegex(deploy.Blocked, "journal"):
                deploy.resolve_candidate_ids(document, "sample", directory)
            missing = owned_state(plan, ("workspace",))
            store.write(missing)
            with self.assertRaisesRegex(deploy.Blocked, "both candidate IDs"):
                deploy.resolve_candidate_ids(document, "sample", directory)
            state = owned_state(plan, ("workspace", "staging_lakehouse"))
            state["pending"] = {"kind": "environment_item", "operation_id": OPERATION, "result_id": None}
            store.write(state)
            with self.assertRaisesRegex(deploy.Blocked, "pending"):
                deploy.resolve_candidate_ids(document, "sample", directory)
            state["pending"] = None
            store.write(state)
            bad = copy.deepcopy(document)
            bad["shared"]["workspace_id"] = PIPELINE
            with self.assertRaisesRegex(deploy.Blocked, "resource ID"):
                deploy.resolve_candidate_ids(bad, "sample", directory)
            bad = copy.deepcopy(document)
            bad["shared"]["staging_lakehouse_id"] = PIPELINE
            with self.assertRaisesRegex(deploy.Blocked, "resource ID"):
                deploy.resolve_candidate_ids(bad, "sample", directory)
            bad = copy.deepcopy(document)
            bad["shared"]["identity_id"] = PIPELINE
            with self.assertRaisesRegex(deploy.Blocked, "identity_id"):
                deploy.resolve_candidate_ids(bad, "sample", directory)
            state["resources"]["staging_lakehouse"]["workspace_id"] = PIPELINE
            store.write(state)
            with self.assertRaisesRegex(deploy.Blocked, "parent"):
                deploy.resolve_candidate_ids(document, "sample", directory)
            state["resources"]["staging_lakehouse"]["workspace_id"] = WORKSPACE
            store.write(state)
            raw = store.path.read_text(encoding="utf-8")
            store.path.write_text('{"pending":null,' + raw.lstrip()[1:], encoding="utf-8")
            with self.assertRaisesRegex(deploy.Blocked, "unreadable"):
                deploy.resolve_candidate_ids(document, "sample", directory)

    def test_candidate_ids_require_complete_config_and_honor_existing_lock(self):
        document = config()
        with self.assertRaisesRegex(deploy.ContractError, "absolute"):
            deploy.resolve_candidate_ids(document, "sample", "relative-state")
        with tempfile.TemporaryDirectory() as directory:
            incomplete = copy.deepcopy(document)
            del incomplete["shared"]["identity_id"]
            with self.assertRaisesRegex(deploy.ContractError, "require tenant"):
                deploy.resolve_candidate_ids(incomplete, "sample", directory)
            store = deploy.StateStore(directory)
            store.write(owned_state(deploy.parse_config(document, "sample")))
            store.lock_path.write_text("other writer", encoding="utf-8")
            with self.assertRaisesRegex(deploy.Blocked, "locked"):
                deploy.resolve_candidate_ids(document, "sample", directory)
            self.assertEqual(store.lock_path.read_text(encoding="utf-8"), "other writer")

    def test_names_expand_and_config_ids_cannot_be_adopted(self):
        document = config()
        document["shared"]["producer_notebook_name"] = "{prefix}_notebook"
        document["shared"]["workspace_id"] = WORKSPACE
        plan = deploy.parse_config(document, "sample")
        self.assertEqual(plan["names"]["workspace"], "sample-producer")
        self.assertEqual(plan["names"]["notebook"], "sample_notebook")
        with mock.patch.object(Path, "exists", return_value=False):
            with self.assertRaisesRegex(deploy.Blocked, "require matching owned"):
                deploy.load_owned_state(MemoryStore(), plan)

    def test_environment_aliases_are_checked_not_silently_ignored(self):
        document = config()
        document["shared"].update(environment_id=ENVIRONMENT, environment_item_id=ENVIRONMENT.upper(),
                                  environment_name="{prefix}_environment",
                                  environment_item_name="{prefix}_environment",
                                  environment_description="owned")
        plan = deploy.parse_config(document, "sample")
        self.assertEqual(plan["ids"]["environment_item"], ENVIRONMENT)
        self.assertEqual(plan["names"]["environment_item"], "sample_environment")
        self.assertEqual(plan["descriptions"]["environment_item"], "owned")
        for field, conflicting in (("producer_environment_id", PIPELINE),
                                   ("producer_environment_name", "different"),
                                   ("producer_environment_description", "different")):
            with self.subTest(field=field):
                bad = copy.deepcopy(document)
                bad["shared"][field] = conflicting
                with self.assertRaisesRegex(deploy.ContractError, "Conflicting"):
                    deploy.parse_config(bad, "sample")

    def test_legacy_manifest_requires_matching_identity_names_tenant_and_parents(self):
        plan = deploy.parse_config(config(deploy.MCAP_TENANT_ID), "sample")
        manifest = legacy_manifest()
        state = deploy._legacy_manifest(plan, manifest)
        self.assertEqual(state["resources"]["notebook"]["id"], NOTEBOOK)
        self.assertIsNone(state["resources"]["workspace"]["description"])

        for mutate in (
            lambda m: m["producer_identity"].update(object_id=WORKSPACE),
            lambda m: m["workspace"].update(name="other"),
            lambda m: m["notebook"].update(workspace_id=PIPELINE),
            lambda m: m["environment_item"].update(workspace_id=PIPELINE),
            lambda m: m["environment_item"].update(name="other"),
            lambda m: m["environment_item"].pop("ownership"),
            lambda m: m["capacity"].update(id=PIPELINE),
            lambda m: m.update(environment="other"),
        ):
            with self.subTest(mutation=mutate):
                bad = copy.deepcopy(manifest)
                mutate(bad)
                with self.assertRaises(deploy.Blocked):
                    deploy._legacy_manifest(plan, bad)
        wrong_config = config(deploy.MCAP_TENANT_ID)
        wrong_config["shared"]["producer_notebook_id"] = PIPELINE
        with self.assertRaises(deploy.Blocked):
            deploy._legacy_manifest(deploy.parse_config(wrong_config, "sample"), manifest)
        wrong_config = config(deploy.MCAP_TENANT_ID)
        wrong_config["shared"]["producer_environment_id"] = PIPELINE
        with self.assertRaises(deploy.Blocked):
            deploy._legacy_manifest(deploy.parse_config(wrong_config, "sample"), manifest)

    def test_legacy_descriptions_must_be_recorded_or_explicitly_configured(self):
        document = config(deploy.MCAP_TENANT_ID)
        plan = deploy.parse_config(document, "sample")
        state = deploy._legacy_manifest(plan, legacy_manifest())
        client = ReadbackClient(plan, state)
        client.workspace["description"] = "actual workspace description"
        with self.assertRaisesRegex(deploy.Blocked, "lacks the exact live description"):
            deploy._workspace_principal(client, state, plan)

        for kind, field in deploy.DESCRIPTION_FIELDS.items():
            document["shared"][field] = "actual workspace description" if kind == "workspace" else ""
        plan = deploy.parse_config(document, "sample")
        state = deploy._legacy_manifest(plan, legacy_manifest())
        client = ReadbackClient(plan, state)
        principal_type = deploy._workspace_principal(client, state, plan)
        for kind in deploy.ITEM_TYPES:
            deploy._verify_item(client, state, plan, kind, principal_type)
        client.items[NOTEBOOK]["description"] = "changed"
        with self.assertRaisesRegex(deploy.Blocked, "description"):
            deploy._verify_item(client, state, plan, "notebook", principal_type)

    def test_owned_state_rejects_stale_ids_and_changed_descriptions(self):
        plan = deploy.parse_config(config(), "sample")
        state = owned_state(plan)
        deploy.validate_state(copy.deepcopy(state), plan)
        changed = copy.deepcopy(state)
        changed["resources"]["notebook"]["description"] = "other"
        with self.assertRaises(deploy.Blocked):
            deploy.validate_state(changed, plan)
        document = config()
        document["shared"]["workspace_id"] = PIPELINE
        with self.assertRaises(deploy.Blocked):
            deploy.validate_state(state, deploy.parse_config(document, "sample"))
        changed = copy.deepcopy(state)
        changed["resources"]["pipeline"]["workspace_id"] = LAKEHOUSE
        with self.assertRaises(deploy.Blocked):
            deploy.validate_state(changed, plan)
        changed = copy.deepcopy(state)
        changed["resources"]["environment_item"]["id"] = NOTEBOOK
        with self.assertRaisesRegex(deploy.Blocked, "duplicate"):
            deploy.validate_state(changed, plan)
        document = config()
        document["shared"]["producer_notebook_description"] = "stale"
        with self.assertRaisesRegex(deploy.Blocked, "description"):
            deploy.validate_state(state, deploy.parse_config(document, "sample"))

    def test_live_name_description_parent_capacity_and_identity_are_required(self):
        plan = deploy.parse_config(config(), "sample")
        state = owned_state(plan)

        def verify(client):
            principal_type = deploy._workspace_principal(client, state, plan)
            for kind in deploy.ITEM_TYPES:
                deploy._verify_item(client, state, plan, kind, principal_type)

        verify(ReadbackClient(plan, state))
        mutations = (
            lambda c: c.workspace.update(displayName="wrong"),
            lambda c: c.workspace.update(description="wrong"),
            lambda c: c.workspace.update(capacityId=PIPELINE),
            lambda c: c.workspace.update(capacityAssignmentProgress="InProgress"),
            lambda c: c.roles[0]["principal"].update(id=PIPELINE),
            lambda c: c.roles[0].update(role="Member"),
            lambda c: c.items[LAKEHOUSE].update(workspaceId=PIPELINE),
            lambda c: c.items[ENVIRONMENT].update(description="wrong"),
            lambda c: c.items[ENVIRONMENT].update(workspaceId=PIPELINE),
            lambda c: c.items[ENVIRONMENT]["defaultIdentity"].update(id=PIPELINE),
            lambda c: c.items[NOTEBOOK].update(displayName="wrong"),
            lambda c: c.items[NOTEBOOK].update(description="wrong"),
            lambda c: c.items[PIPELINE]["defaultIdentity"].update(id=WORKSPACE),
            lambda c: c.items[PIPELINE]["defaultIdentity"].update(type="ServicePrincipal"),
        )
        for mutation in mutations:
            with self.subTest(mutation=mutation):
                client = ReadbackClient(plan, state)
                mutation(client)
                with self.assertRaises(deploy.Blocked):
                    verify(client)
        client = ReadbackClient(plan, state)
        client.items[NOTEBOOK].pop("defaultIdentity")
        client.items[PIPELINE]["defaultIdentity"] = None
        verify(client)

    def test_existing_name_is_never_adopted(self):
        plan = deploy.parse_config(config(), "sample")
        state = owned_state(plan, ())
        store = MemoryStore()
        client = mock.Mock()
        client.list_all.return_value = [{"id": WORKSPACE, "displayName": "SAMPLE-PRODUCER"}]
        with self.assertRaisesRegex(deploy.Blocked, "not adopted"):
            deploy._create_resource(client, store, state, plan, "workspace")
        client.post_operation.assert_not_called()
        self.assertEqual(store.writes, [])

    def test_uncertain_create_is_journaled_and_never_reposted(self):
        plan = deploy.parse_config(config(), "sample")
        state = owned_state(plan, ())
        store = MemoryStore()
        client = mock.Mock()
        client.list_all.return_value = []
        client.post_operation.side_effect = deploy.FabricTransportError("Fabric POST request failed or timed out.")
        with self.assertRaisesRegex(deploy.Blocked, "no POST was retried"):
            deploy._create_resource(client, store, state, plan, "workspace")
        self.assertEqual(client.post_operation.call_count, 1)
        self.assertEqual(store.state["pending"]["kind"], "workspace")
        self.assertIsNone(store.state["pending"]["operation_id"])
        with self.assertRaisesRegex(deploy.Blocked, "unknown outcome"):
            deploy.validate_state(store.state, plan)
        self.assertEqual(client.post_operation.call_count, 1)

    def test_journal_write_failure_prevents_the_post(self):
        plan = deploy.parse_config(config(), "sample")
        state = owned_state(plan, ())
        store = mock.Mock()
        store.write.side_effect = deploy.Blocked("Cannot persist deployment state.")
        client = mock.Mock()
        client.list_all.return_value = []
        with self.assertRaisesRegex(deploy.Blocked, "persist"):
            deploy._create_resource(client, store, state, plan, "workspace")
        client.post_operation.assert_not_called()

    def test_creation_persists_id_before_live_readback(self):
        plan = deploy.parse_config(config(), "sample")
        state = owned_state(plan, ())
        store = MemoryStore()
        record = deploy._record(state, plan, "workspace", WORKSPACE)
        client = mock.Mock()
        client.list_all.side_effect = [
            [],
            [{"role": "Admin", "principal": {"id": IDENTITY, "type": "User"}}],
        ]
        client.post_operation.return_value = (201, {"id": WORKSPACE})
        client.get.return_value = ({}, {"id": WORKSPACE, "displayName": record["name"],
                                        "description": record["description"], "type": "Workspace",
                                        "capacityId": CAPACITY, "capacityAssignmentProgress": "Completed"})
        deploy._create_resource(client, store, state, plan, "workspace")
        self.assertIsNone(store.writes[0]["pending"]["result_id"])
        self.assertEqual(store.writes[1]["pending"]["result_id"], WORKSPACE)
        self.assertIsNone(store.writes[-1]["pending"])
        self.assertEqual(store.state["resources"]["workspace"]["id"], WORKSPACE)
        payload = client.post_operation.call_args.args[1]
        self.assertEqual(payload["description"], record["description"])
        self.assertEqual(payload["capacityId"], CAPACITY)

    def test_async_creation_discovers_resource_without_lro_result(self):
        plan = deploy.parse_config(config(), "sample")
        state = owned_state(plan, ())
        store = MemoryStore()
        record = deploy._record(state, plan, "workspace", WORKSPACE)
        client = mock.Mock()
        client.list_all.side_effect = [
            [],
            [{"id": WORKSPACE, "displayName": record["name"], "type": "Workspace"}],
            [{"role": "Admin", "principal": {"id": IDENTITY, "type": "User"}}],
        ]
        client.get.return_value = ({}, {"id": WORKSPACE, "displayName": record["name"],
                                        "description": record["description"], "type": "Workspace",
                                        "capacityId": CAPACITY, "capacityAssignmentProgress": "Completed"})

        def accepted(path, payload, on_accepted, require_result):
            self.assertFalse(require_result)
            on_accepted(OPERATION, 1)
            return 202, None

        client.post_operation.side_effect = accepted
        deploy._create_resource(client, store, state, plan, "workspace")
        self.assertEqual(store.state["resources"]["workspace"]["id"], WORKSPACE)
        self.assertTrue(any(write["pending"] and write["pending"].get("operation_succeeded")
                            for write in store.writes))
        self.assertIsNone(store.state["pending"])
        client.poll.assert_not_called()

    def test_notebook_creation_uses_bound_asset_and_checks_default_identity(self):
        plan = deploy.parse_config(config(), "sample")
        state = owned_state(plan, ("workspace", "staging_lakehouse", "environment_item"))
        expected = definition({"cells": []})
        client = ReadbackClient(plan, state)
        notebook = deploy._record(state, plan, "notebook", NOTEBOOK)
        client.items[NOTEBOOK] = {"id": NOTEBOOK, "displayName": notebook["name"],
                                  "description": notebook["description"], "type": "Notebook",
                                  "workspaceId": WORKSPACE,
                                  "defaultIdentity": {"id": IDENTITY, "type": "User"}}
        client.post_operation = mock.Mock(return_value=(201, {"id": NOTEBOOK}))
        store = MemoryStore()
        with mock.patch.object(deploy, "_definition", return_value=expected) as build:
            deploy._create_resource(client, store, state, plan, "notebook")
        build.assert_called_once_with(state, "notebook")
        self.assertEqual(client.post_operation.call_args.args[1]["definition"], expected)
        self.assertEqual(store.state["resources"]["notebook"]["id"], NOTEBOOK)

    def test_state_file_containing_json_null_is_not_treated_as_absent(self):
        store = deploy.StateStore(Path.cwd() / "not-created")
        with (mock.patch.object(Path, "exists", return_value=True),
              mock.patch.object(Path, "read_text", return_value="null")):
            with self.assertRaisesRegex(deploy.Blocked, "invalid"):
                store.read()

    def test_existing_lock_blocks_concurrent_or_interrupted_deployment(self):
        store = deploy.StateStore(Path.cwd() / "not-created")
        with (mock.patch.object(Path, "mkdir"),
              mock.patch.object(deploy.os, "open", side_effect=FileExistsError)):
            with self.assertRaisesRegex(deploy.Blocked, "locked"):
                with store.lock():
                    self.fail("A locked deployment must not run")


class EnvironmentTests(unittest.TestCase):
    def setUp(self):
        self.plan = deploy.parse_config(config(), "sample")
        self.spec = deploy._environment_spec()

    def fresh_environment(self):
        state = owned_state(self.plan, ("workspace", "staging_lakehouse", "environment_item"))
        client = ReadbackClient(self.plan, state)
        client.items[ENVIRONMENT].pop("properties")
        state["resources"]["environment_item"]["staging_compute_sha256"] = deploy._compute_fingerprint(client, state)
        client.post_operation = mock.Mock()
        return state, MemoryStore(state), client

    def test_environment_definition_uses_the_pinned_file_and_rejects_unpinned_input(self):
        part = deploy._environment_definition(self.spec)["parts"][0]
        self.assertEqual(part["path"], "Libraries/PublicLibraries/environment.yml")
        self.assertEqual(base64.b64decode(part["payload"]), deploy.ENVIRONMENT_YML.read_bytes())
        self.assertEqual(self.spec["pins"], {"azure-identity": "1.27.0", "azure-kusto-data": "6.0.4",
                                             "azure-kusto-ingest": "6.0.4"})
        invalid = (
            b"dependencies:\n  - pip:\n      - azure-identity>=1.27.0\n",
            b"dependencies:\n  - pip:\n      - azure-identity==1.27.0\n",
            self.spec["content"] + b"\n      - azure_identity==1.27.0\n",
            self.spec["content"] + b"\n      - --index-url=https://example.invalid\n",
        )
        for content in invalid:
            with self.subTest(content=content):
                source = mock.Mock()
                source.read_bytes.return_value = content
                with mock.patch.object(deploy, "ENVIRONMENT_YML", source):
                    with self.assertRaisesRegex(deploy.Blocked, "pinned"):
                        deploy._environment_spec()

    def test_notebook_definition_binds_owned_environment(self):
        import fabric_assets

        state = owned_state(self.plan)
        cell = {"cell_type": "code", "execution_count": None, "metadata": {}, "outputs": [], "source": []}
        with mock.patch.object(fabric_assets, "_bundle_cell", return_value=cell):
            notebook = deploy._parts(deploy._definition(state, "notebook"))["notebook-content.ipynb"]
        self.assertEqual(notebook["metadata"]["dependencies"]["environment"],
                         {"environmentId": ENVIRONMENT, "workspaceId": WORKSPACE})
        self.assertEqual(notebook["metadata"]["dependencies"]["lakehouse"]["default_lakehouse"], LAKEHOUSE)

    def test_legacy_environment_requires_owned_identity_and_exact_description(self):
        plan = deploy.parse_config(config(deploy.MCAP_TENANT_ID), "sample")
        manifest = legacy_manifest()
        for kind in deploy.KINDS:
            manifest[kind]["description"] = "owned " + kind
        manifest["environment_item"]["published_version"] = PUBLISHED_VERSION
        state = deploy._legacy_manifest(plan, manifest)
        self.assertEqual(state["resources"]["environment_item"]["id"], ENVIRONMENT)
        client = ReadbackClient(plan, state)
        principal = deploy._workspace_principal(client, state, plan)
        deploy._verify_item(client, state, plan, "environment_item", principal)
        self.assertEqual(deploy._verify_published_environment(client, state, self.spec), PUBLISHED_VERSION)
        client.items[ENVIRONMENT]["description"] = "not the recorded description"
        with self.assertRaisesRegex(deploy.Blocked, "metadata"):
            deploy._verify_published_environment(client, state, self.spec)
        wrong = config(deploy.MCAP_TENANT_ID)
        wrong["shared"]["producer_environment_description"] = "different"
        with self.assertRaisesRegex(deploy.Blocked, "description"):
            deploy._legacy_manifest(deploy.parse_config(wrong, "sample"), manifest)

    def test_published_libraries_must_match_staging_and_successful_stable_version(self):
        state = owned_state(self.plan)
        state["resources"]["environment_item"]["published_version"] = PUBLISHED_VERSION
        client = ReadbackClient(self.plan, state)
        self.assertEqual(deploy._verify_published_environment(client, state, self.spec), PUBLISHED_VERSION)
        mutations = (
            lambda c: c.staging_libraries[0].update(version="0.0.0"),
            lambda c: c.published_libraries[0].update(version="0.0.0"),
            lambda c: c.published_libraries.pop(),
            lambda c: c.published_libraries.append({"name": "other", "version": "1.0", "libraryType": "External"}),
            lambda c: c.published_libraries.append({"name": "extra.whl", "libraryType": "Custom"}),
            lambda c: c.published_libraries.append(copy.deepcopy(c.published_libraries[0])),
            lambda c: c.items[ENVIRONMENT]["properties"]["publishDetails"].update(targetVersion=PIPELINE),
            lambda c: c.items[ENVIRONMENT]["properties"]["publishDetails"].update(state="Running"),
            lambda c: c.items[ENVIRONMENT]["properties"]["publishDetails"].update(
                componentPublishInfo={"sparkLibraries": {"state": "Failed"}}),
        )
        for mutation in mutations:
            with self.subTest(mutation=mutation):
                client = ReadbackClient(self.plan, state)
                mutation(client)
                with self.assertRaises(deploy.Blocked):
                    deploy._verify_published_environment(client, state, self.spec)

    def test_changed_publication_during_readback_is_blocked(self):
        state = owned_state(self.plan)
        client = ReadbackClient(self.plan, state)
        original = client.list_all

        def lists(path, collection_key="value"):
            result = original(path, collection_key)
            if parse.urlsplit(path).path.endswith("/libraries") and "/staging/" not in path:
                client.items[ENVIRONMENT]["properties"]["publishDetails"]["targetVersion"] = PIPELINE
            return result

        client.list_all = lists
        with self.assertRaisesRegex(deploy.Blocked, "changed during"):
            deploy._verify_published_environment(client, state, self.spec)

    def test_publish_200_is_journaled_and_versions_are_read_back(self):
        state, store, client = self.fresh_environment()

        def publish(path, on_accepted, require_result):
            self.assertTrue(path.endswith("/staging/publish?beta=false"))
            self.assertFalse(require_result)
            self.assertEqual(store.state["pending"]["action"], "publish")
            client.items[ENVIRONMENT]["properties"] = {
                "publishDetails": {"state": "Success", "targetVersion": PUBLISHED_VERSION}}
            return 200, {"publishDetails": {"state": "Running", "targetVersion": PUBLISHED_VERSION}}

        client.post_operation.side_effect = publish
        deploy._ensure_environment(client, store, state, self.spec)
        self.assertEqual(client.post_operation.call_count, 1)
        self.assertTrue(any(write["pending"] and write["pending"].get("target_version") == PUBLISHED_VERSION
                            for write in store.writes))
        self.assertIsNone(store.state["pending"])
        self.assertEqual(store.state["resources"]["environment_item"]["published_version"], PUBLISHED_VERSION)

    def test_unconfirmed_publish_never_reposts(self):
        state, store, client = self.fresh_environment()
        client.post_operation.side_effect = deploy.FabricTransportError("transport failed")
        with self.assertRaisesRegex(deploy.Blocked, "no POST was retried"):
            deploy._ensure_environment(client, store, state, self.spec)
        self.assertIsNone(store.state["pending"]["operation_id"])
        with self.assertRaisesRegex(deploy.Blocked, "unknown outcome"):
            deploy.validate_state(store.state, self.plan)
        self.assertEqual(client.post_operation.call_count, 1)

    def test_pending_202_publish_resumes_with_gets_and_not_a_new_post(self):
        state, store, client = self.fresh_environment()

        def interrupted(path, on_accepted, require_result):
            on_accepted(OPERATION, 3)
            raise deploy.Blocked("deadline")

        client.post_operation.side_effect = interrupted
        with self.assertRaisesRegex(deploy.Blocked, "no POST was retried"):
            deploy._ensure_environment(client, store, state, self.spec)
        deploy.validate_state(store.state, self.plan)
        document = config()
        document["shared"]["producer_environment_id"] = ENVIRONMENT
        deploy.validate_state(store.state, deploy.parse_config(document, "sample"))
        self.assertEqual(store.state["pending"]["operation_id"], OPERATION)
        resumed = copy.deepcopy(store.state)
        readback = ReadbackClient(self.plan, resumed)
        readback.poll = mock.Mock(return_value=None)
        readback.post_operation = mock.Mock()
        deploy._finish_environment_publish(readback, store, resumed, self.spec)
        readback.poll.assert_called_once_with(OPERATION, 3, require_result=False)
        readback.post_operation.assert_not_called()
        self.assertIsNone(store.state["pending"])
        self.assertEqual(store.state["resources"]["environment_item"]["published_version"], PUBLISHED_VERSION)

    def test_interrupted_200_publish_resumes_from_recorded_target_version(self):
        state, store, client = self.fresh_environment()
        original_get = client.get

        def disconnected(path, deadline=None):
            if state.get("pending") and state["pending"].get("target_version"):
                raise deploy.FabricTransportError("read interrupted")
            return original_get(path, deadline)

        client.get = disconnected
        client.post_operation.return_value = (
            200, {"publishDetails": {"state": "Running", "targetVersion": PUBLISHED_VERSION}})
        with self.assertRaises(deploy.FabricTransportError):
            deploy._ensure_environment(client, store, state, self.spec)
        self.assertEqual(store.state["pending"]["target_version"], PUBLISHED_VERSION)
        deploy.validate_state(store.state, self.plan)
        resumed = copy.deepcopy(store.state)
        readback = ReadbackClient(self.plan, resumed)
        readback.post_operation = mock.Mock()
        deploy._finish_environment_publish(readback, store, resumed, self.spec)
        readback.post_operation.assert_not_called()
        self.assertIsNone(store.state["pending"])

    def test_failed_publish_preserves_journal_and_never_reposts(self):
        state, store, client = self.fresh_environment()

        def publish(path, on_accepted, require_result):
            client.items[ENVIRONMENT]["properties"] = {
                "publishDetails": {"state": "Failed", "targetVersion": PUBLISHED_VERSION}}
            return 200, {"publishDetails": {"state": "Running", "targetVersion": PUBLISHED_VERSION}}

        client.post_operation.side_effect = publish
        with self.assertRaisesRegex(deploy.Blocked, "did not succeed"):
            deploy._ensure_environment(client, store, state, self.spec)
        self.assertEqual(store.state["pending"]["target_version"], PUBLISHED_VERSION)
        with self.assertRaisesRegex(deploy.Blocked, "did not succeed"):
            deploy._finish_environment_publish(client, store, copy.deepcopy(store.state), self.spec)
        self.assertEqual(client.post_operation.call_count, 1)

    def test_pending_200_publish_rejects_a_different_live_target(self):
        state, store, client = self.fresh_environment()
        state["pending"] = {"kind": "environment_item", "action": "publish", "spec_sha256": self.spec["sha256"],
                            "operation_id": None, "target_version": PUBLISHED_VERSION,
                            "operation_succeeded": False}
        client.items[ENVIRONMENT]["properties"] = {
            "publishDetails": {"state": "Success", "targetVersion": PIPELINE}}
        with self.assertRaisesRegex(deploy.Blocked, "version changed"):
            deploy._finish_environment_publish(client, store, state, self.spec)
        client.post_operation.assert_not_called()

    def test_completed_operation_stays_journaled_when_library_readback_fails(self):
        state, store, client = self.fresh_environment()
        state["pending"] = {"kind": "environment_item", "action": "publish", "spec_sha256": self.spec["sha256"],
                            "operation_id": OPERATION, "retry_after": 1, "target_version": None,
                            "operation_succeeded": False}
        store.write(state)
        client.items[ENVIRONMENT]["properties"] = {
            "publishDetails": {"state": "Success", "targetVersion": PUBLISHED_VERSION}}
        client.published_libraries.pop()
        client.poll = mock.Mock()
        with self.assertRaisesRegex(deploy.Blocked, "Published Environment libraries"):
            deploy._finish_environment_publish(client, store, state, self.spec)
        self.assertTrue(store.state["pending"]["operation_succeeded"])
        resumed = copy.deepcopy(store.state)
        readback = ReadbackClient(self.plan, resumed)
        readback.poll = mock.Mock()
        deploy._finish_environment_publish(readback, store, resumed, self.spec)
        readback.poll.assert_not_called()
        self.assertIsNone(store.state["pending"])

    def test_publication_is_blocked_on_drift_missing_baseline_or_legacy(self):
        for mutation in (
            lambda s, c: c.staging_libraries[0].update(version="0.0.0"),
            lambda s, c: c.compute.update(runtimeVersion="2.0"),
            lambda s, c: s["resources"]["environment_item"].pop("staging_compute_sha256"),
            lambda s, c: s.update(legacy=True),
            lambda s, c: c.items[ENVIRONMENT].update(properties={
                "publishDetails": {"state": "Running", "targetVersion": PUBLISHED_VERSION}}),
        ):
            with self.subTest(mutation=mutation):
                state, store, client = self.fresh_environment()
                mutation(state, client)
                with self.assertRaises(deploy.Blocked):
                    deploy._ensure_environment(client, store, state, self.spec)
                client.post_operation.assert_not_called()
                self.assertEqual(store.writes, [])

    def test_changed_pins_block_resuming_a_pending_publish(self):
        state, store, client = self.fresh_environment()
        state["pending"] = {"kind": "environment_item", "action": "publish", "spec_sha256": "0" * 64,
                            "operation_id": OPERATION, "operation_succeeded": False}
        client.poll = mock.Mock()
        with self.assertRaisesRegex(deploy.Blocked, "changed during"):
            deploy._finish_environment_publish(client, store, state, self.spec)
        client.poll.assert_not_called()
        client.post_operation.assert_not_called()


class FabricClientTests(unittest.TestCase):
    def client(self, opener, clock=None):
        clock = clock or Clock()
        return deploy.FabricClient("fake-sensitive-token", opener=opener, clock=clock.now, sleep=clock.sleep)

    def test_pagination_follows_canonical_continuation_and_token_only(self):
        path = "/v1/workspaces"
        opener = Opener(
            Response(body={"value": [1], "continuationUri": deploy.API_ROOT + path + "?continuationToken=a%3D"}),
            Response(body={"value": [2], "continuationToken": "b%3D"}),
            Response(body={"value": [3]}),
        )
        self.assertEqual(self.client(opener).list_all(path), [1, 2, 3])
        urls = [request.full_url for request, _ in opener.requests]
        self.assertEqual(urls[1], deploy.API_ROOT + path + "?continuationToken=a%3D")
        self.assertEqual(urls[2], deploy.API_ROOT + path + "?continuationToken=b%3D")

    def test_pagination_rejects_cross_origin_changed_path_and_cycles(self):
        for continuation in ("https://attacker.invalid/v1/workspaces?continuationToken=x",
                             deploy.API_ROOT + "/v1/operations?continuationToken=x",
                             deploy.API_ROOT + "/v1/workspaces"):
            with self.subTest(continuation=continuation):
                opener = Opener(Response(body={"value": [], "continuationUri": continuation}))
                with self.assertRaises(deploy.Blocked):
                    self.client(opener).list_all("/v1/workspaces")
                self.assertEqual(len(opener.requests), 1)

    def test_environment_library_pagination_preserves_release_query(self):
        path = f"/v1/workspaces/{WORKSPACE}/environments/{ENVIRONMENT}/libraries?beta=false"
        continuation = (f"{deploy.API_ROOT}/v1/workspaces/{WORKSPACE}/environments/{ENVIRONMENT}"
                        "/libraries?continuationToken=a%3D")
        opener = Opener(Response(body={"libraries": [1], "continuationUri": continuation}),
                        Response(body={"libraries": [2], "continuationUri": "null", "continuationToken": "null"}))
        self.assertEqual(self.client(opener).list_all(path, collection_key="libraries"), [1, 2])
        self.assertIn("beta=false", opener.requests[1][0].full_url)
        self.assertIn("continuationToken=a%3D", opener.requests[1][0].full_url)
        bad = Opener(Response(body={"libraries": [], "continuationUri": continuation + "&beta=true"}))
        with self.assertRaisesRegex(deploy.Blocked, "parameters"):
            self.client(bad).list_all(path, collection_key="libraries")
        self.assertEqual(len(bad.requests), 1)

    def test_lro_uses_operation_id_and_honors_each_retry_after(self):
        clock = Clock()
        opener = Opener(
            Response(202, headers={"x-ms-operation-id": OPERATION, "Retry-After": "3",
                                   "Location": "https://attacker.invalid/operations"}),
            Response(body={"status": "Running"}, headers={"Retry-After": "2",
                                                          "Location": "https://attacker.invalid/result"}),
            Response(body={"status": "Succeeded"}),
            Response(body={"id": NOTEBOOK}),
        )
        client = self.client(opener, clock)
        accepted = mock.Mock()
        status, result = client.post_operation("/v1/workspaces", {"displayName": "safe"}, accepted)
        self.assertEqual((status, result), (202, {"id": NOTEBOOK}))
        accepted.assert_called_once_with(OPERATION, 3)
        self.assertEqual(clock.delays, [3, 2])
        urls = [item[0].full_url for item in opener.requests]
        self.assertEqual(urls[1:3], [f"{deploy.API_ROOT}/v1/operations/{OPERATION}"] * 2)
        self.assertEqual(urls[3], f"{deploy.API_ROOT}/v1/operations/{OPERATION}/result")
        self.assertEqual([item[0].method for item in opener.requests], ["POST", "GET", "GET", "GET"])

    def test_lro_requires_operation_id_and_does_not_retry_post(self):
        for headers in ({}, {"x-ms-operation-id": "not-a-uuid"}, {"x-ms-operation-id": OPERATION,
                                                                   "Retry-After": "bad"}):
            with self.subTest(headers=headers):
                opener = Opener(Response(202, headers=headers))
                with self.assertRaises(deploy.Blocked):
                    self.client(opener).post_operation("/v1/workspaces", {})
                self.assertEqual(len(opener.requests), 1)

    def test_publish_lro_does_not_require_a_result_endpoint(self):
        clock = Clock()
        opener = Opener(Response(202, headers={"x-ms-operation-id": OPERATION, "Retry-After": "2"}),
                        Response(body={"status": "Succeeded"}))
        accepted = mock.Mock()
        status, response = self.client(opener, clock).post_operation(
            f"/v1/workspaces/{WORKSPACE}/environments/{ENVIRONMENT}/staging/publish?beta=false",
            on_accepted=accepted, require_result=False)
        self.assertEqual((status, response), (202, None))
        accepted.assert_called_once_with(OPERATION, 2)
        self.assertEqual(clock.delays, [2])
        self.assertEqual([item[0].method for item in opener.requests], ["POST", "GET"])
        self.assertTrue(opener.requests[-1][0].full_url.endswith(f"/v1/operations/{OPERATION}"))

    def test_skill_header_is_present_on_posts_polls_results_and_retries(self):
        clock = Clock()
        throttled = error.HTTPError(deploy.API_ROOT + "/v1/workspaces", 429, "throttle",
                                    {"Retry-After": "2"}, io.BytesIO())
        opener = Opener(throttled, Response(body={"value": []}),
                        Response(202, headers={"x-ms-operation-id": OPERATION, "Retry-After": "1"}),
                        Response(body={"status": "Succeeded"}), Response(body={"id": NOTEBOOK}))
        client = self.client(opener, clock)
        self.assertEqual(client.list_all("/v1/workspaces"), [])
        client.post_operation("/v1/workspaces", {"displayName": "safe"})
        self.assertGreaterEqual(len(opener.requests), 5)
        for req, _ in opener.requests:
            headers = {key.lower(): value for key, value in req.header_items()}
            self.assertEqual(headers["x-ms-fabric-skill"], "spark-cli")

    def test_lro_failure_and_deadline_do_not_fetch_result(self):
        for state, headers in (({"status": "Failed"}, {}), ({"status": "Running"}, {"Retry-After": "600"})):
            with self.subTest(state=state):
                clock = Clock()
                opener = Opener(Response(body=state, headers=headers))
                with self.assertRaises(deploy.Blocked):
                    self.client(opener, clock).poll(OPERATION, 1, timeout=10)
                self.assertEqual(len(opener.requests), 1)

    def test_get_429_retries_only_after_server_delay(self):
        throttled = error.HTTPError(deploy.API_ROOT + "/v1/workspaces", 429, "secret body",
                                    {"Retry-After": "4"}, io.BytesIO(b"secret"))
        clock = Clock()
        opener = Opener(throttled, Response(body={"value": []}))
        self.assertEqual(self.client(opener, clock).list_all("/v1/workspaces"), [])
        self.assertEqual(clock.delays, [4])
        self.assertEqual(len(opener.requests), 2)

    def test_http_and_transport_errors_never_expose_response_or_credentials(self):
        for failure in (error.HTTPError(deploy.API_ROOT + "/v1/workspaces", 403, "secret",
                                        {}, io.BytesIO(b"secret")),
                        error.HTTPError(deploy.API_ROOT + "/v1/workspaces", 429, "secret",
                                        {"Retry-After": "1"}, io.BytesIO(b"secret")),
                        error.URLError("secret bearer token")):
            with self.subTest(failure=type(failure).__name__):
                opener = Opener(failure)
                with self.assertRaises((deploy.FabricHttpError, deploy.FabricTransportError)) as caught:
                    self.client(opener).post_operation("/v1/workspaces", {})
                self.assertNotIn("secret", str(caught.exception))
                self.assertNotIn("fake-sensitive-token", str(caught.exception))
                self.assertEqual(len(opener.requests), 1)

    def test_post_redirect_is_not_followed(self):
        redirect = error.HTTPError(deploy.API_ROOT + "/v1/workspaces", 302, "redirect",
                                   {"Location": "https://attacker.invalid/"}, io.BytesIO())
        opener = Opener(redirect)
        with self.assertRaises(deploy.FabricHttpError):
            self.client(opener).post_operation("/v1/workspaces", {})
        self.assertEqual(len(opener.requests), 1)
        self.assertIsNone(deploy._NoRedirect().redirect_request(None, None, 302, "", {}, "https://attacker.invalid"))

    def test_interrupted_lro_resumes_only_with_gets(self):
        plan = deploy.parse_config(config(), "sample")
        state = owned_state(plan, ())
        store = MemoryStore()
        clock = Clock()
        opener = Opener(
            Response(body={"value": []}),
            Response(202, headers={"x-ms-operation-id": OPERATION, "Retry-After": "2"}),
            Response(body={"status": "Running"}, headers={"Retry-After": "999"}),
        )
        with self.assertRaises(deploy.Blocked):
            deploy._create_resource(self.client(opener, clock), store, state, plan, "workspace")
        self.assertEqual(store.state["pending"]["operation_id"], OPERATION)
        deploy.validate_state(store.state, plan)
        record = deploy._record(store.state, plan, "workspace", WORKSPACE)
        resumed = Opener(
            Response(body={"status": "Succeeded"}),
            Response(body={"value": [{"id": WORKSPACE, "displayName": record["name"], "type": "Workspace"}]}),
            Response(body={"id": WORKSPACE, "displayName": record["name"], "description": record["description"],
                           "type": "Workspace", "capacityId": CAPACITY, "capacityAssignmentProgress": "Completed"}),
            Response(body={"value": [{"role": "Admin", "principal": {"id": IDENTITY, "type": "User"}}]}),
        )
        loaded = copy.deepcopy(store.state)
        deploy._finish_pending(self.client(resumed, Clock()), store, loaded, plan)
        self.assertTrue(all(item[0].method == "GET" for item in resumed.requests))
        self.assertIsNone(store.state["pending"])
        self.assertEqual(store.state["resources"]["workspace"]["id"], WORKSPACE)

    def test_completed_notebook_lro_discovers_id_without_result_body(self):
        plan = deploy.parse_config(config(), "sample")
        state = owned_state(plan, ("workspace", "staging_lakehouse", "environment_item"))
        store = MemoryStore(state)
        client = ReadbackClient(plan, state)
        record = deploy._record(state, plan, "notebook", NOTEBOOK)
        client.items[NOTEBOOK] = {"id": NOTEBOOK, "displayName": record["name"],
                                  "description": record["description"], "type": "Notebook",
                                  "workspaceId": WORKSPACE,
                                  "defaultIdentity": {"id": IDENTITY, "type": "User"}}
        client.existing = [{"id": NOTEBOOK, "displayName": record["name"], "type": "Notebook"}]
        state["pending"] = {"kind": "notebook", "operation_id": OPERATION, "result_id": None}
        client.poll = mock.Mock(return_value=None)
        deploy._finish_pending(client, store, state, plan)
        client.poll.assert_called_once_with(OPERATION, 1, require_result=False)
        self.assertEqual(store.state["resources"]["notebook"]["id"], NOTEBOOK)
        self.assertIsNone(store.state["pending"])

    def test_completed_lro_with_duplicate_names_fails_closed(self):
        plan = deploy.parse_config(config(), "sample")
        state = owned_state(plan, ("workspace",))
        state["pending"] = {"kind": "staging_lakehouse", "operation_id": OPERATION,
                            "operation_succeeded": True, "result_id": None}
        client = ReadbackClient(plan, state)
        client.existing = [{"id": LAKEHOUSE, "displayName": "producer_staging", "type": "Lakehouse"},
                           {"id": NOTEBOOK, "displayName": "producer_staging", "type": "Lakehouse"}]
        with self.assertRaisesRegex(deploy.Blocked, "not uniquely discoverable"):
            deploy._finish_pending(client, MemoryStore(), state, plan)

    def test_definition_mismatch_blocks_without_updating(self):
        plan = deploy.parse_config(config(), "sample")
        state = owned_state(plan)
        local = definition({"cells": [{"source": "expected"}]})
        local["format"] = "ipynb"
        server = definition({"cells": [{"source": "different"}]})
        client = mock.Mock()
        client.post_operation.return_value = (200, {"definition": server})
        with mock.patch.object(deploy, "_definition", return_value=local):
            with self.assertRaisesRegex(deploy.Blocked, "no overwrite"):
                deploy._verify_definition(client, state, "notebook")
        self.assertEqual(client.post_operation.call_count, 1)
        self.assertTrue(client.post_operation.call_args.args[0].endswith("/getDefinition?format=ipynb"))


class MockFabricService:
    def __init__(self):
        self.workspace = None
        self.items = {}
        self.definitions = {}
        self.calls = []
        self.payloads = []
        self.compute = {"instancePool": {"name": "Starter Pool"}, "runtimeVersion": "1.3"}
        self.staging_libraries = pinned_libraries()
        self.published_libraries = []
        self.publish_details = None
        self.clock = Clock()
        self._clock = self.clock.now
        self._wait = lambda delay, deadline: self.clock.sleep(delay)

    def list_all(self, path, collection_key="value"):
        self.calls.append(("GET", path))
        if path == "/v1/workspaces":
            return [self.workspace] if self.workspace else []
        if path.endswith("/roleAssignments"):
            return [{"role": "Admin", "principal": {"id": IDENTITY, "type": "User"}}]
        api_path = parse.urlsplit(path).path
        if api_path.endswith("/staging/libraries"):
            return copy.deepcopy(self.staging_libraries)
        if api_path.endswith("/libraries"):
            return copy.deepcopy(self.published_libraries)
        return list(self.items.values())

    def get(self, path, deadline=None):
        self.calls.append(("GET", path))
        api_path = parse.urlsplit(path).path
        if api_path.endswith("/staging/sparkcompute"):
            return {}, copy.deepcopy(self.compute)
        resource_id = api_path.rsplit("/", 1)[-1]
        if resource_id == WORKSPACE:
            return {}, copy.deepcopy(self.workspace)
        item = copy.deepcopy(self.items[resource_id])
        if f"/environments/{ENVIRONMENT}" in api_path and self.publish_details is not None:
            item["properties"] = {"publishDetails": copy.deepcopy(self.publish_details)}
        return {}, item

    def post_operation(self, path, payload=None, on_accepted=None, require_result=True):
        self.calls.append(("POST", path))
        self.payloads.append((path, copy.deepcopy(payload)))
        api_path = parse.urlsplit(path).path
        if api_path.endswith("/getDefinition"):
            item_id = api_path.split("/")[-2]
            return 200, {"definition": copy.deepcopy(self.definitions[item_id])}
        if api_path.endswith("/staging/publish"):
            self.published_libraries = copy.deepcopy(self.staging_libraries)
            self.publish_details = {"state": "Success", "targetVersion": PUBLISHED_VERSION}
            return 200, {"publishDetails": {"state": "Running", "targetVersion": PUBLISHED_VERSION}}
        if path == "/v1/workspaces":
            self.workspace = {"id": WORKSPACE, "type": "Workspace",
                              "displayName": payload["displayName"], "description": payload["description"],
                              "capacityId": payload["capacityId"], "capacityAssignmentProgress": "Completed"}
            return 201, {"id": WORKSPACE}
        item_types = {"Lakehouse": LAKEHOUSE, "Environment": ENVIRONMENT,
                      "Notebook": NOTEBOOK, "DataPipeline": PIPELINE}
        kind = "Environment" if api_path.endswith("/environments") else payload["type"]
        item_id = item_types[kind]
        self.items[item_id] = {"id": item_id, "type": kind, "workspaceId": WORKSPACE,
                               "displayName": payload["displayName"], "description": payload["description"],
                               "defaultIdentity": {"id": IDENTITY, "type": "User"}}
        if "definition" in payload:
            self.definitions[item_id] = copy.deepcopy(payload["definition"])
        return 201, {"id": item_id}


def bound_definition(state, kind):
    resources = state["resources"]
    if kind == "notebook":
        notebook = definition({"workspace": resources["workspace"]["id"],
                               "lakehouse": resources["staging_lakehouse"]["id"],
                               "lakehouse_name": resources["staging_lakehouse"]["name"],
                               "environment": resources["environment_item"]["id"]})
        notebook["format"] = "ipynb"
        return notebook
    return definition({"workspace": resources["workspace"]["id"], "notebook": resources["notebook"]["id"]},
                      "pipeline-content.json")


class DeploymentFlowTests(unittest.TestCase):
    def test_deploy_then_reuse_and_validate_never_start_jobs_or_overwrite(self):
        store = MemoryStore()
        service = MockFabricService()
        with (mock.patch.object(deploy, "StateStore", return_value=store),
              mock.patch.object(deploy, "HERE", Path.cwd() / "nonexistent-owner-manifest"),
              mock.patch.object(deploy, "azure_cli_token", return_value="token") as auth,
              mock.patch.object(deploy, "FabricClient", return_value=service),
              mock.patch.object(deploy, "_definition", side_effect=bound_definition)):
            diagnostics = deploy.perform("deploy", config(), "sample", str(Path.cwd()))
            self.assertTrue(any("runtime readiness remains unproven" in text for text in diagnostics))
            self.assertEqual(set(store.state["resources"]), set(deploy.KINDS))
            self.assertIsNone(store.state["pending"])
            creations = [path for method, path in service.calls
                         if method == "POST" and not parse.urlsplit(path).path.endswith("/getDefinition")]
            self.assertEqual(len(creations), 6)
            publish = next(i for i, path in enumerate(creations) if "/staging/publish" in path)
            notebook = next(i for i, (path, payload) in enumerate(service.payloads)
                            if payload and payload.get("type") == "Notebook")
            publish_call = next(i for i, (path, _) in enumerate(service.payloads) if "/staging/publish" in path)
            self.assertLess(publish, len(creations) - 1)
            self.assertLess(publish_call, notebook)
            environment_payload = next(payload for path, payload in service.payloads
                                       if parse.urlsplit(path).path.endswith("/environments"))
            self.assertEqual(base64.b64decode(environment_payload["definition"]["parts"][0]["payload"]),
                             deploy.ENVIRONMENT_YML.read_bytes())
            self.assertEqual(store.state["resources"]["environment_item"]["published_version"], PUBLISHED_VERSION)

            service.calls.clear()
            deploy.perform("deploy", config(), "sample", str(Path.cwd()))
            with self.assertRaisesRegex(deploy.Blocked, "Selected destinations"):
                deploy.perform("validate-only", config(), "sample", str(Path.cwd()))
            self.assertTrue(all(parse.urlsplit(path).path.endswith("/getDefinition")
                                for method, path in service.calls if method == "POST"))
            self.assertFalse(any("/jobs" in path.lower() or "/instances" in path.lower()
                                 for _, path in service.calls))
            self.assertEqual(auth.call_count, 2)

    def test_deploy_resumes_owned_environment_publication_before_notebook_checks(self):
        plan = deploy.parse_config(config(), "sample")
        state = owned_state(plan)
        state["pending"] = {"kind": "environment_item", "action": "publish",
                            "spec_sha256": deploy._environment_spec()["sha256"], "operation_id": OPERATION,
                            "operation_succeeded": False, "retry_after": 2}
        store = MemoryStore(state)
        client = ReadbackClient(plan, state)
        client.poll = mock.Mock()

        def definitions(path):
            item_id = parse.urlsplit(path).path.split("/")[-2]
            kind = "notebook" if item_id == NOTEBOOK else "pipeline"
            return 200, {"definition": bound_definition(state, kind)}

        client.post_operation = mock.Mock(side_effect=definitions)
        with (mock.patch.object(deploy, "StateStore", return_value=store),
              mock.patch.object(deploy, "azure_cli_token", return_value="token"),
              mock.patch.object(deploy, "FabricClient", return_value=client),
              mock.patch.object(deploy, "_definition", side_effect=bound_definition)):
            diagnostics = deploy.perform("deploy", config(), "sample", str(Path.cwd()))
        client.poll.assert_called_once_with(OPERATION, 2, require_result=False)
        self.assertTrue(all("/getDefinition" in call.args[0] for call in client.post_operation.call_args_list))
        self.assertIsNone(store.state["pending"])
        self.assertEqual(store.state["resources"]["environment_item"]["published_version"], PUBLISHED_VERSION)
        self.assertTrue(any("runtime readiness remains unproven" in text for text in diagnostics))

    def test_pending_publish_is_not_resumed_without_live_environment_ownership(self):
        plan = deploy.parse_config(config(), "sample")
        state = owned_state(plan)
        state["pending"] = {"kind": "environment_item", "action": "publish",
                            "spec_sha256": deploy._environment_spec()["sha256"], "operation_id": OPERATION,
                            "operation_succeeded": False}
        store = MemoryStore(state)
        client = ReadbackClient(plan, state)
        client.items[ENVIRONMENT]["description"] = "different owner"
        client.poll = mock.Mock()
        client.post_operation = mock.Mock()
        with (mock.patch.object(deploy, "StateStore", return_value=store),
              mock.patch.object(deploy, "azure_cli_token", return_value="token"),
              mock.patch.object(deploy, "FabricClient", return_value=client),
              mock.patch.object(deploy, "_definition", side_effect=bound_definition)):
            with self.assertRaisesRegex(deploy.Blocked, "description"):
                deploy.perform("deploy", config(), "sample", str(Path.cwd()))
        client.poll.assert_not_called()
        client.post_operation.assert_not_called()
        self.assertEqual(store.writes, [])

    def test_stale_owned_state_blocks_before_authentication(self):
        plan = deploy.parse_config(config(), "sample")
        state = owned_state(plan)
        store = MemoryStore(state)
        document = config()
        document["shared"]["workspace_id"] = PIPELINE
        with (mock.patch.object(deploy, "StateStore", return_value=store),
              mock.patch.object(deploy, "azure_cli_token") as auth,
              mock.patch.object(deploy, "FabricClient") as client):
            with self.assertRaises(deploy.Blocked):
                deploy.perform("deploy", document, "sample", str(Path.cwd()))
        auth.assert_not_called()
        client.assert_not_called()
        self.assertEqual(store.writes, [])


class TokenTests(unittest.TestCase):
    def setUp(self):
        platform = mock.patch.object(deploy.sys, "platform", "linux")
        executable = mock.patch.object(deploy.shutil, "which", return_value="/usr/bin/az")
        platform.start()
        executable.start()
        self.addCleanup(platform.stop)
        self.addCleanup(executable.stop)

    def jwt(self, tenant=TENANT, identity=IDENTITY):
        payload = base64.urlsafe_b64encode(json.dumps({"tid": tenant, "oid": identity}).encode()).decode().rstrip("=")
        return "header." + payload + ".signature"

    def test_token_uses_cli_tenant_and_inherits_isolated_config_without_mutating_it(self):
        token = self.jwt()
        completed = SimpleNamespace(returncode=0, stdout=json.dumps({"accessToken": token}), stderr="secret")
        with (mock.patch.object(deploy.subprocess, "run", return_value=completed) as run,
              mock.patch.dict(os.environ, {"AZURE_CONFIG_DIR": r"C:\isolated-azure"})):
            before = dict(os.environ)
            self.assertEqual(deploy.azure_cli_token(TENANT, IDENTITY), token)
            self.assertEqual(dict(os.environ), before)
        command = run.call_args.args[0]
        self.assertEqual(command[0], "/usr/bin/az")
        self.assertEqual(command[1:3], ["account", "get-access-token"])
        self.assertIn(TENANT, command)
        self.assertIn(deploy.API_ROOT, command)
        self.assertIsNone(run.call_args.kwargs["env"])
        self.assertEqual(run.call_args.kwargs["timeout"], 120)
        run.assert_called_once()

    def test_windows_msi_uses_direct_python_and_preserves_isolated_environment(self):
        token = self.jwt()
        completed = SimpleNamespace(returncode=0, stdout=json.dumps({"accessToken": token}), stderr="secret")
        with tempfile.TemporaryDirectory() as directory:
            runtime = Path(directory) / "python.exe"
            runtime.touch()
            launcher = runtime.parent / "wbin" / "az.CMD"
            with (mock.patch.object(deploy.sys, "platform", "win32"),
                  mock.patch.object(deploy.shutil, "which", return_value=str(launcher)),
                  mock.patch.object(deploy.subprocess, "run", return_value=completed) as run,
                  mock.patch.dict(os.environ, {"AZURE_CONFIG_DIR": "isolated", "AZ_INSTALLER": "original"})):
                before = dict(os.environ)
                self.assertEqual(deploy.azure_cli_token(TENANT, IDENTITY), token)
                self.assertEqual(dict(os.environ), before)
            self.assertEqual(run.call_args.args[0][:3], [str(runtime), "-IBm", "azure.cli"])
            self.assertEqual(run.call_args.args[0][3:5], ["account", "get-access-token"])
            self.assertEqual(run.call_args.kwargs["env"], {**before, "AZ_INSTALLER": "MSI"})
            self.assertEqual(run.call_args.kwargs["timeout"], 120)
            run.assert_called_once()

            runtime.unlink()
            with (mock.patch.object(deploy.sys, "platform", "win32"),
                  mock.patch.object(deploy.shutil, "which", return_value=str(launcher)),
                  mock.patch.object(deploy.subprocess, "run") as run):
                with self.assertRaisesRegex(deploy.Blocked, "repair the MSI"):
                    deploy.azure_cli_token(TENANT, IDENTITY)
            run.assert_not_called()

    def test_windows_msi_timeout_does_not_retry_or_expose_command_output(self):
        with tempfile.TemporaryDirectory() as directory:
            runtime = Path(directory) / "python.exe"
            runtime.touch()
            launcher = runtime.parent / "wbin" / "az.cmd"
            timeout = subprocess.TimeoutExpired(cmd=["sensitive"], timeout=120,
                                                output="sensitive", stderr="sensitive")
            with (mock.patch.object(deploy.sys, "platform", "win32"),
                  mock.patch.object(deploy.shutil, "which", return_value=str(launcher)),
                  mock.patch.object(deploy.subprocess, "run", side_effect=timeout) as run):
                with self.assertRaisesRegex(deploy.Blocked, "120s") as caught:
                    deploy.azure_cli_token(TENANT, IDENTITY)
            self.assertNotIn("sensitive", str(caught.exception))
            self.assertTrue(caught.exception.__suppress_context__)
            self.assertEqual(run.call_args.args[0][:3], [str(runtime), "-IBm", "azure.cli"])
            run.assert_called_once()

    def test_wrong_tenant_or_identity_is_blocked_without_leaking_token(self):
        for token in (self.jwt(tenant=WORKSPACE), self.jwt(identity=WORKSPACE), "secret.invalid.token"):
            with self.subTest(token_type=token.split(".")[0]):
                completed = SimpleNamespace(returncode=0, stdout=json.dumps({"accessToken": token}), stderr="")
                with mock.patch.object(deploy.subprocess, "run", return_value=completed):
                    with self.assertRaises(deploy.Blocked) as caught:
                        deploy.azure_cli_token(TENANT, IDENTITY)
                self.assertNotIn(token, str(caught.exception))

    def test_storage_token_uses_the_same_tenant_and_principal_with_a_fixed_audience(self):
        token = self.jwt()
        completed = SimpleNamespace(returncode=0, stdout=json.dumps({"accessToken": token}), stderr="")
        with mock.patch.object(deploy.subprocess, "run", return_value=completed) as run:
            self.assertEqual(deploy.azure_cli_token(TENANT, IDENTITY, resource=deploy.STORAGE_RESOURCE), token)
        command = run.call_args.args[0]
        self.assertIn(deploy.STORAGE_RESOURCE, command)
        self.assertIn(TENANT, command)
        wrong_identity = self.jwt(identity=WORKSPACE)
        completed.stdout = json.dumps({"accessToken": wrong_identity})
        with mock.patch.object(deploy.subprocess, "run", return_value=completed):
            with self.assertRaises(deploy.Blocked):
                deploy.azure_cli_token(TENANT, IDENTITY, resource=deploy.STORAGE_RESOURCE)
        with mock.patch.object(deploy.subprocess, "run") as run:
            with self.assertRaises(deploy.ContractError):
                deploy.azure_cli_token(TENANT, IDENTITY, resource="https://attacker.invalid")
        run.assert_not_called()

    def test_cli_errors_and_timeouts_are_sanitized(self):
        failures = (
            SimpleNamespace(returncode=1, stdout="sensitive", stderr="sensitive"),
            subprocess.TimeoutExpired(cmd=["az"], timeout=120, output="sensitive"),
        )
        for failure in failures:
            with self.subTest(failure=type(failure).__name__):
                with mock.patch.object(deploy.subprocess, "run") as run:
                    if isinstance(failure, Exception):
                        run.side_effect = failure
                    else:
                        run.return_value = failure
                    with self.assertRaises(deploy.Blocked) as caught:
                        deploy.azure_cli_token(TENANT, IDENTITY)
                self.assertNotIn("sensitive", str(caught.exception))
                run.assert_called_once()


if __name__ == "__main__":
    unittest.main()
