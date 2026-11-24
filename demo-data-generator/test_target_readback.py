import copy
import hashlib
import io
import json
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock
from urllib import error, parse, request

import deploy
from fabric_runtime import RuntimeFabricClient, prepare_measured, stop_requested
from generator import canonical_bytes
from target_readback import OneLakeTableClient, TargetReadbackBlocked, read_selected_targets, selected_candidates
from test_deploy import IDENTITY, NOTEBOOK, TENANT, Opener, Response, config, owned_state, run_data


WORKSPACE = "11111111-1111-4111-8111-111111111111"
LAKEHOUSE = "22222222-2222-4222-8222-222222222222"
DATABASE = "33333333-3333-4333-8333-333333333333"
EVENTHOUSE = "44444444-4444-4444-8444-444444444444"
LANDING_WORKSPACE = "55555555-5555-4555-8555-555555555555"
LANDING_LAKEHOUSE = "66666666-6666-4666-8666-666666666666"
QUERY = "https://query.kusto.fabric.microsoft.com"
INGESTION = "https://ingest.kusto.fabric.microsoft.com"


def item(workspace, item_id, item_type, name):
    return {"id": item_id, "workspaceId": workspace, "type": item_type, "displayName": name}


def document():
    value = config()
    value["shared"]["target_bindings"] = {
        "demo-1": {"workspace_id": WORKSPACE, "item_id": LAKEHOUSE, "fact": "silver.sales_joined"},
        "demo-2": {"workspace_id": WORKSPACE, "item_id": DATABASE, "fact": "FactSales"},
        "demo-3": {"workspace_id": WORKSPACE, "item_id": LAKEHOUSE, "fact": "dbo.factsales"},
    }
    value["demo-1"] = {"landing_uri": f"abfss://{LANDING_WORKSPACE}@onelake.dfs.fabric.microsoft.com/"
                                      f"{LANDING_LAKEHOUSE}/Files/incoming/sales/"}
    value["demo-3"] = {"landing_uri": f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/"
                                      f"{LAKEHOUSE}/Files/incoming/sales/"}
    value["demo-2"] = {"query_endpoint": QUERY, "ingestion_endpoint": INGESTION,
                       "kql_database_name": "Sales", "sales_table": "FactSales", "csv_mapping": "SalesCsvV1"}
    return value


def delta_page():
    return {"tables": [{"name": "factsales", "schema_name": "dbo", "data_source_format": "DELTA",
                        "storage_location": f"https://onelake.dfs.fabric.microsoft.com/{WORKSPACE}/"
                                            f"{LAKEHOUSE}/Tables/dbo/factsales"}],
            "next_page_token": None}


class TableClient:
    def __init__(self):
        self.page = delta_page()
        self.calls = []

    def list_tables(self, candidate, deadline):
        self.calls.append((copy.deepcopy(candidate), deadline))
        return copy.deepcopy(self.page)


class ReadClient:
    def __init__(self):
        lh = item(WORKSPACE, LAKEHOUSE, "Lakehouse", "fact-lakehouse")
        landing = item(LANDING_WORKSPACE, LANDING_LAKEHOUSE, "Lakehouse", "landing-lakehouse")
        db = item(WORKSPACE, DATABASE, "KQLDatabase", "Sales")
        typed_db = dict(db, properties={"databaseType": "ReadWrite", "parentEventhouseItemId": EVENTHOUSE,
                                        "queryServiceUri": QUERY, "ingestionServiceUri": INGESTION})
        self.responses = {
            f"/v1/workspaces/{WORKSPACE}": {"id": WORKSPACE, "type": "Workspace"},
            f"/v1/workspaces/{LANDING_WORKSPACE}": {"id": LANDING_WORKSPACE, "type": "Workspace"},
            f"/v1/workspaces/{WORKSPACE}/items/{LAKEHOUSE}": lh,
            f"/v1/workspaces/{WORKSPACE}/lakehouses/{LAKEHOUSE}": copy.deepcopy(lh),
            f"/v1/workspaces/{LANDING_WORKSPACE}/items/{LANDING_LAKEHOUSE}": landing,
            f"/v1/workspaces/{LANDING_WORKSPACE}/lakehouses/{LANDING_LAKEHOUSE}": copy.deepcopy(landing),
            f"/v1/workspaces/{WORKSPACE}/items/{DATABASE}": db,
            f"/v1/workspaces/{WORKSPACE}/kqlDatabases/{DATABASE}": typed_db,
            f"/v1/workspaces/{WORKSPACE}/items/{EVENTHOUSE}": item(WORKSPACE, EVENTHOUSE, "Eventhouse", "eh"),
        }
        self.calls = []
        self.roles = {}
        self._clock = lambda: 5

    def get(self, path, deadline):
        self.calls.append((path, deadline))
        return {}, copy.deepcopy(self.responses[path])

    def list_all(self, path):
        self.calls.append((path, None))
        return copy.deepcopy(self.roles.get(path, []))


class TargetReadbackTests(unittest.TestCase):
    def setUp(self):
        self.config = document()
        self.selected = ["demo-3", "demo-2", "demo-1"]
        self.client = ReadClient()
        self.clients = {name: self.client for name in self.selected}
        self.table_client = TableClient()

    def prepare_validation(self):
        self.config["data"] = run_data()
        self.config["shared"].update(workspace_id=WORKSPACE, staging_lakehouse_id=LAKEHOUSE)
        role = {"role": "Contributor", "principal": {"id": IDENTITY, "type": "User"}}
        self.client.roles = {f"/v1/workspaces/{workspace}/roleAssignments": [role]
                             for workspace in (WORKSPACE, LANDING_WORKSPACE)}

    def test_reads_each_target_and_separate_demo1_landing_without_claiming_ownership(self):
        observations = read_selected_targets(self.config, self.selected, self.clients, table_client=self.table_client)
        self.assertEqual(len(observations), 3)
        self.assertTrue(all("ownership" in text and "remain unverified" in text for text in observations))
        self.assertIn("Delta fact listing and storage location matched", observations[2])
        self.assertEqual(self.table_client.calls, [(self.config["shared"]["target_bindings"]["demo-3"], 65)])
        self.assertIn((f"/v1/workspaces/{LANDING_WORKSPACE}/lakehouses/{LANDING_LAKEHOUSE}", 65),
                      self.client.calls)
        self.assertIn((f"/v1/workspaces/{WORKSPACE}/items/{EVENTHOUSE}", 65), self.client.calls)
        self.assertEqual(self.config["shared"]["target_bindings"]["demo-3"]["fact"], "dbo.factsales")

    def test_candidate_set_and_shape_rejected_before_any_read(self):
        for change in (
            lambda v: v["shared"]["target_bindings"].pop("demo-3"),
            lambda v: v["shared"]["target_bindings"]["demo-1"].update(extra="untrusted"),
            lambda v: v["shared"]["target_bindings"]["demo-1"].update(item_id="not-a-uuid"),
            lambda v: v["shared"]["target_bindings"]["demo-2"].update(fact="Fact Sales"),
            lambda v: v["shared"]["target_bindings"]["demo-3"].update(fact="dbo.sales.extra"),
        ):
            with self.subTest(change=change):
                value = copy.deepcopy(self.config)
                change(value)
                with self.assertRaises(TargetReadbackBlocked):
                    read_selected_targets(value, self.selected, self.clients, table_client=self.table_client)
        self.assertEqual(self.client.calls, [])
        with self.assertRaises(TargetReadbackBlocked):
            selected_candidates(self.config, ["demo-1", "demo-1"])

    def test_wrong_live_item_parent_or_type_blocks(self):
        path = f"/v1/workspaces/{WORKSPACE}/kqlDatabases/{DATABASE}"
        baseline = copy.deepcopy(self.client.responses[path])
        for field, incorrect in (("id", LAKEHOUSE), ("workspaceId", LANDING_WORKSPACE),
                                 ("type", "Lakehouse"), ("displayName", "Other")):
            with self.subTest(field=field):
                self.client.responses[path] = dict(baseline, **{field: incorrect})
                with self.assertRaises(TargetReadbackBlocked):
                    read_selected_targets(self.config, self.selected, self.clients, table_client=self.table_client)
        self.client.responses[path] = baseline
        self.client.responses[f"/v1/workspaces/{WORKSPACE}/items/{EVENTHOUSE}"]["type"] = "Lakehouse"
        with self.assertRaisesRegex(TargetReadbackBlocked, "Eventhouse parent"):
            read_selected_targets(self.config, self.selected, self.clients, table_client=self.table_client)

    def test_demo3_other_landing_is_blocked_but_demo1_can_be_separate(self):
        self.config["demo-3"]["landing_uri"] = self.config["demo-1"]["landing_uri"]
        with self.assertRaisesRegex(TargetReadbackBlocked, "landing identity differs"):
            read_selected_targets(self.config, self.selected, self.clients, table_client=self.table_client)

    def test_demo2_adapter_must_match_live_readback_not_just_candidate(self):
        for field, incorrect in (("query_endpoint", INGESTION), ("ingestion_endpoint", None),
                                 ("kql_database_name", "Different"), ("sales_table", "factsales"),
                                 ("csv_mapping", "Other")):
            with self.subTest(field=field):
                value = copy.deepcopy(self.config)
                value["demo-2"][field] = incorrect
                with self.assertRaises(TargetReadbackBlocked):
                    read_selected_targets(value, self.selected, self.clients, table_client=self.table_client)
        path = f"/v1/workspaces/{WORKSPACE}/kqlDatabases/{DATABASE}"
        self.client.responses[path]["properties"]["queryServiceUri"] = "https://attacker.example"
        with self.assertRaisesRegex(TargetReadbackBlocked, "valid Fabric Kusto"):
            read_selected_targets(self.config, self.selected, self.clients, table_client=self.table_client)

    def test_demo3_fact_requires_exact_delta_metadata_and_an_unpaginated_response(self):
        cases = (
            lambda page: page["tables"][0].update(name="FactSales"),
            lambda page: page["tables"][0].update(schema_name="other"),
            lambda page: page["tables"][0].update(data_source_format="PARQUET"),
            lambda page: page["tables"][0].update(storage_location="https://attacker.invalid/table"),
            lambda page: page["tables"].append(copy.deepcopy(page["tables"][0])),
            lambda page: page.update(next_page_token="another-page"),
            lambda page: page.pop("next_page_token"),
            lambda page: page.update(tables=[None]),
        )
        for change in cases:
            with self.subTest(change=change):
                self.table_client.page = delta_page()
                change(self.table_client.page)
                with self.assertRaises(TargetReadbackBlocked):
                    read_selected_targets(self.config, self.selected, self.clients, table_client=self.table_client)
        with self.assertRaisesRegex(TargetReadbackBlocked, "requires a OneLake"):
            read_selected_targets(self.config, self.selected, self.clients)

    def test_validate_only_checks_selected_targets_and_direct_write_roles(self):
        self.prepare_validation()
        plan = deploy.parse_config(self.config, "sample")
        state = owned_state(plan)
        with (mock.patch.object(deploy, "load_owned_state", return_value=state),
              mock.patch.object(deploy, "_verify_all") as verify_definitions,
              mock.patch.object(deploy, "azure_cli_token", side_effect=["fabric-token", "storage-token"]) as auth,
              mock.patch.object(deploy, "FabricClient", return_value=self.client) as factory,
              mock.patch.object(deploy, "OneLakeTableClient", return_value=self.table_client) as tables):
            diagnostics = deploy.perform("validate-only", self.config, "sample", ".", ",".join(self.selected))
        self.assertEqual(factory.call_args_list[1], mock.call("fabric-token", skill="eventhouse-cli"))
        verify_definitions.assert_called_once_with(self.client, state, plan, config=self.config)
        auth.assert_has_calls([mock.call(plan["tenant_id"], plan["identity_id"]),
                               mock.call(plan["tenant_id"], plan["identity_id"], resource=deploy.STORAGE_RESOURCE)])
        tables.assert_called_once_with("storage-token")
        self.assertTrue(any("demo-3:" in entry and "unverified" in entry for entry in diagnostics))
        self.assertTrue(any("direct write roles verified" in entry for entry in diagnostics))
        self.assertEqual(deploy._envelope("validate-only", "pass")["ready"], True)

    def test_validate_only_blocks_missing_direct_write_role(self):
        self.prepare_validation()
        self.client.roles[f"/v1/workspaces/{LANDING_WORKSPACE}/roleAssignments"] = []
        plan = deploy.parse_config(self.config, "sample")
        with (mock.patch.object(deploy, "load_owned_state", return_value=owned_state(plan)),
              mock.patch.object(deploy, "_verify_all"),
              mock.patch.object(deploy, "azure_cli_token", return_value="token"),
              mock.patch.object(deploy, "FabricClient", return_value=self.client),
              mock.patch.object(deploy, "OneLakeTableClient", return_value=self.table_client)):
            with self.assertRaisesRegex(deploy.Blocked, "write-role preflight failed"):
                deploy.perform("validate-only", self.config, "sample", ".", ",".join(self.selected))

    def test_validate_only_main_emits_ready_only_after_full_preflight(self):
        self.prepare_validation()
        plan = deploy.parse_config(self.config, "sample")
        root = Path.cwd()
        args = ["--mode", "validate-only", "--config", str(root / "config.json"),
                "--prefix", "sample", "--state-dir", str(root / "state"),
                "--result", str(root / "result.json"), "--selected-demos", ",".join(self.selected)]
        with (mock.patch.object(Path, "read_text", return_value=json.dumps(self.config)),
              mock.patch.object(deploy, "_atomic_json") as save,
              mock.patch.object(deploy, "load_owned_state", return_value=owned_state(plan)),
              mock.patch.object(deploy, "_verify_all"),
              mock.patch.object(deploy, "azure_cli_token", return_value="token"),
              mock.patch.object(deploy, "FabricClient", return_value=self.client),
              mock.patch.object(deploy, "OneLakeTableClient", return_value=self.table_client)):
            self.assertEqual(deploy.main(args), 0)
            self.assertEqual((save.call_args.args[1]["status"], save.call_args.args[1]["ready"]),
                             ("pass", True))
            self.client.roles[f"/v1/workspaces/{LANDING_WORKSPACE}/roleAssignments"] = []
            self.assertEqual(deploy.main(args), 1)
            self.assertEqual((save.call_args.args[1]["status"], save.call_args.args[1]["ready"]),
                             ("blocked", False))

    def test_validate_only_requires_owned_state_and_selected_destinations(self):
        self.prepare_validation()
        with mock.patch.object(deploy, "load_owned_state", return_value=None):
            with self.assertRaisesRegex(deploy.Blocked, "No owned producer"):
                deploy.perform("validate-only", self.config, "sample", ".", ",".join(self.selected))
        with self.assertRaisesRegex(deploy.Blocked, "Selected destinations"):
            deploy.perform("validate-only", self.config, "sample", ".", None)

    def test_validate_only_blocks_incomplete_or_mismatched_owned_state(self):
        self.prepare_validation()
        plan = deploy.parse_config(self.config, "sample")
        incomplete = owned_state(plan)
        incomplete["resources"].pop("notebook")
        mismatch = owned_state(plan)
        mismatch["resources"]["staging_lakehouse"]["id"] = LANDING_LAKEHOUSE
        pending = owned_state(plan)
        pending["pending"] = {"kind": "notebook"}
        for state, reason in ((incomplete, "all producer resources"),
                              (mismatch, "differ from owned state"),
                              (pending, "pending operation")):
            with self.subTest(reason=reason), mock.patch.object(deploy, "load_owned_state", return_value=state):
                with self.assertRaisesRegex(deploy.Blocked, reason):
                    deploy.perform("validate-only", self.config, "sample", ".", ",".join(self.selected))

    def test_validation_without_demo3_does_not_acquire_a_storage_token(self):
        self.prepare_validation()
        self.config["shared"]["target_bindings"].pop("demo-3")
        plan = deploy.parse_config(self.config, "sample")
        with (mock.patch.object(deploy, "load_owned_state", return_value=owned_state(plan)),
              mock.patch.object(deploy, "_verify_all"),
              mock.patch.object(deploy, "azure_cli_token", return_value="fabric-token") as auth,
              mock.patch.object(deploy, "FabricClient", return_value=self.client),
              mock.patch.object(deploy, "OneLakeTableClient") as tables):
            deploy.perform("validate-only", self.config, "sample", ".", "demo-1,demo-2")
        auth.assert_called_once_with(plan["tenant_id"], plan["identity_id"])
        tables.assert_not_called()

    def test_missing_candidate_blocks_before_authentication(self):
        self.prepare_validation()
        self.config["shared"].pop("target_bindings")
        with mock.patch.object(deploy, "azure_cli_token") as token:
            with self.assertRaisesRegex(TargetReadbackBlocked, "missing or invalid"):
                deploy.perform("validate-only", self.config, "sample", ".", "demo-1,demo-2")
        token.assert_not_called()

    def test_eventhouse_client_uses_its_own_telemetry_header(self):
        opener = Opener(Response(body={"id": DATABASE}))
        client = deploy.FabricClient("token", opener=opener, skill="eventhouse-cli")
        client.get(f"/v1/workspaces/{WORKSPACE}/kqlDatabases/{DATABASE}")
        self.assertEqual(opener.requests[0][0].get_header("X-ms-fabric-skill"), "eventhouse-cli")


class MeasuredRuntimeTests(unittest.TestCase):
    def setUp(self):
        self.document = document()
        self.document["data"] = run_data()
        self.document["shared"].update(workspace_id=WORKSPACE, staging_lakehouse_id=LAKEHOUSE,
                                       producer_notebook_id=NOTEBOOK)
        self.selected = ["demo-1", "demo-2", "demo-3"]
        self.nonce = "measured-0123456789abcdef"
        self.binding = {
            "nonce": self.nonce, "run_id": "run-1", "tenant_id": TENANT, "identity_id": IDENTITY,
            "config_sha256": hashlib.sha256(canonical_bytes(self.document)).hexdigest(),
            "producer": {"workspace_id": WORKSPACE, "staging_lakehouse_id": LAKEHOUSE,
                         "notebook_id": NOTEBOOK},
        }
        self.context = {"isForPipeline": True, "currentWorkspaceId": WORKSPACE,
                        "defaultLakehouseId": LAKEHOUSE, "currentNotebookId": NOTEBOOK}
        self.identities = {audience: (TENANT, IDENTITY) for audience in ("pbi", "storage", "kusto")}
        self.client = ReadClient()
        role = {"role": "Contributor", "principal": {"id": IDENTITY, "type": "User"}}
        self.client.roles = {f"/v1/workspaces/{workspace}/roleAssignments": [copy.deepcopy(role)]
                             for workspace in (WORKSPACE, LANDING_WORKSPACE)}
        self.clients = {name: self.client for name in self.selected}
        self.table_client = TableClient()

    def prepare(self):
        return prepare_measured(self.document, self.selected, self.nonce, self.binding, self.context,
                                self.identities, self.clients, self.table_client)

    def test_requires_matching_live_targets_identity_and_destination_write_roles(self):
        run, duration, targets, producer = self.prepare()
        self.assertEqual((run.run_id, duration), ("run-1", 541))
        self.assertEqual(targets, self.document["shared"]["target_bindings"])
        self.assertEqual(producer, self.binding["producer"])
        self.assertIn((f"/v1/workspaces/{LANDING_WORKSPACE}/roleAssignments", None), self.client.calls)

    def test_runtime_identity_and_context_mismatch_block_before_metadata(self):
        for field in ("currentWorkspaceId", "defaultLakehouseId", "currentNotebookId"):
            with self.subTest(field=field):
                original = self.context[field]
                self.context[field] = "not-a-matching-id"
                with self.assertRaises(ValueError):
                    self.prepare()
                self.context[field] = original
        self.identities["storage"] = (TENANT, WORKSPACE)
        with self.assertRaisesRegex(ValueError, "token identities"):
            self.prepare()
        self.assertEqual(self.client.calls, [])

    def test_changed_config_and_missing_direct_write_role_are_blocked(self):
        self.document["data"]["seed"] += 1
        with self.assertRaisesRegex(ValueError, "run binding"):
            self.prepare()
        self.assertEqual(self.client.calls, [])
        self.document["data"]["seed"] -= 1
        self.client.roles[f"/v1/workspaces/{LANDING_WORKSPACE}/roleAssignments"] = [
            {"role": "Viewer", "principal": {"id": IDENTITY, "type": "User"}},
            {"role": "Admin", "principal": {"id": IDENTITY, "type": "Group"}},
        ]
        with self.assertRaisesRegex(RuntimeError, "directly assigned write role"):
            self.prepare()

    def test_runtime_readback_rejects_a_different_eventhouse_endpoint(self):
        self.client.responses[f"/v1/workspaces/{WORKSPACE}/kqlDatabases/{DATABASE}"]["properties"][
            "ingestionServiceUri"] = QUERY
        with self.assertRaisesRegex(TargetReadbackBlocked, "differ"):
            self.prepare()
        self.assertFalse(any(path.endswith("/roleAssignments") for path, _ in self.client.calls))

    def test_stop_marker_must_match_the_exact_run_binding(self):
        class Store:
            data = None

            def read(self, path):
                self.path = path
                if self.data is None:
                    raise FileNotFoundError(path)
                return self.data, "etag"

        store = Store()
        digest = self.binding["config_sha256"]
        self.assertFalse(stop_requested(store, "run-1", self.nonce, digest))
        marker = {"version": 1, "run_id": "run-1", "nonce": self.nonce, "config_sha256": digest}
        store.data = json.dumps(marker).encode()
        self.assertTrue(stop_requested(store, "run-1", self.nonce, digest))
        self.assertEqual(store.path, "Files/producer/runs/run-1/stop.json")
        for value in (b"not json", json.dumps(dict(marker, nonce="other")).encode(),
                      json.dumps(dict(marker, version=True)).encode()):
            store.data = value
            with self.assertRaisesRegex(RuntimeError, "Stop request"):
                stop_requested(store, "run-1", self.nonce, digest)

    def test_runtime_role_pagination_uses_the_runtime_token_and_fixed_origin(self):
        opener = Opener(Response(body={"value": [{"role": "Viewer"}], "continuationToken": "next/token"}),
                        Response(body={"value": [{"role": "Contributor"}]}))
        client = RuntimeFabricClient(lambda: "runtime-token", opener=opener, clock=lambda: 5)
        roles = client.list_all(f"/v1/workspaces/{WORKSPACE}/roleAssignments")
        self.assertEqual([role["role"] for role in roles], ["Viewer", "Contributor"])
        req, timeout = opener.requests[1]
        self.assertEqual(parse.urlsplit(req.full_url).netloc, "api.fabric.microsoft.com")
        self.assertEqual(parse.parse_qs(parse.urlsplit(req.full_url).query), {"continuationToken": ["next/token"]})
        self.assertEqual((req.get_header("Authorization"), req.get_header("X-ms-fabric-skill"), timeout),
                         ("Bearer runtime-token", "spark-cli", 30))

    def test_runtime_mlv_token_decodes_once_and_stays_on_original_origin(self):
        path = (f"/v1/workspaces/{WORKSPACE}/lakehouses/{LAKEHOUSE}"
                "/jobs/refreshMaterializedLakeViews/instances?status=Completed")
        uri = ("https://wabi-west-us3-a-primary-redirect.analysis.windows.net" + path
               + "&continuationToken=a%3D")
        opener = Opener(Response(body={"value": [1], "continuationToken": "a%3D", "continuationUri": uri}),
                        Response(body={"value": [2]}))
        client = RuntimeFabricClient(lambda: "runtime-token", opener=opener, clock=lambda: 5)
        self.assertEqual(client.list_all(path), [1, 2])
        self.assertEqual(opener.requests[1][0].full_url,
                         "https://api.fabric.microsoft.com" + path + "&continuationToken=a%3D")

    def test_runtime_workspace_role_token_decodes_once(self):
        path = f"/v1/workspaces/{WORKSPACE}/roleAssignments"
        opener = Opener(Response(body={"value": [1], "continuationToken": "a%2B"}),
                        Response(body={"value": [2]}))
        client = RuntimeFabricClient(lambda: "runtime-token", opener=opener, clock=lambda: 5)
        self.assertEqual(client.list_all(path), [1, 2])
        self.assertEqual(opener.requests[1][0].full_url,
                         "https://api.fabric.microsoft.com" + path + "?continuationToken=a%2B")

    def test_runtime_mlv_rejects_unsafe_or_inconsistent_continuations(self):
        path = f"/v1/workspaces/{WORKSPACE}/lakehouses/{LAKEHOUSE}/jobs/refreshMaterializedLakeViews/instances"
        origin = "https://wabi-west-us3-a-primary-redirect.analysis.windows.net"
        for uri, token in (
            (origin + path + "?continuationToken=a", "b"),
            (origin + path + "?continuationToken=a&continuationToken=a", "a"),
            (origin + path + "?continuationToken=a&other=x", "a"),
            (origin + path + "/other?continuationToken=a", "a"),
            (origin + path + "?continuationToken=a#fragment", "a"),
            (origin.replace("https://", "https://user@") + path + "?continuationToken=a", "a"),
            (origin + ":443" + path + "?continuationToken=a", "a"),
            ("https://attacker.invalid" + path + "?continuationToken=a", "a"),
            (origin.replace("https://", "http://") + path + "?continuationToken=a", "a"),
            ("", "a"),
            ([], "a"),
            (None, "a%invalid"),
            (None, "a%00"),
        ):
            with self.subTest(uri=uri, token=token):
                opener = Opener(Response(body={"value": [], "continuationToken": token, "continuationUri": uri}))
                client = RuntimeFabricClient(lambda: "runtime-token", opener=opener, clock=lambda: 5)
                with self.assertRaises(RuntimeError):
                    client.list_all(path)
                self.assertEqual(len(opener.requests), 1)

    def test_runtime_rejects_repeated_decoded_token(self):
        path = f"/v1/workspaces/{WORKSPACE}/roleAssignments"
        opener = Opener(Response(body={"value": [], "continuationToken": "a%3D"}),
                        Response(body={"value": [], "continuationToken": "a="}))
        client = RuntimeFabricClient(lambda: "runtime-token", opener=opener, clock=lambda: 5)
        with self.assertRaisesRegex(RuntimeError, "continuation is invalid"):
            client.list_all(path)
        self.assertEqual(len(opener.requests), 2)

    def test_runtime_bodyless_post_and_patch_have_no_form_content_type(self):
        for method in ("POST", "PATCH"):
            with self.subTest(method=method):
                opener = Opener(Response(202), Response(202))
                client = RuntimeFabricClient(lambda: "runtime-token", opener=opener, clock=lambda: 5)
                client.request(method, f"/v1/workspaces/{WORKSPACE}")
                client.request(method, f"/v1/workspaces/{WORKSPACE}", {"example": 1})
                handler = request.HTTPHandler()
                handler.add_parent(SimpleNamespace(addheaders=[]))
                bodyless = handler.do_request_(opener.requests[0][0])
                with_json = handler.do_request_(opener.requests[1][0])
                self.assertIsNone(bodyless.data)
                self.assertIsNone(bodyless.get_header("Content-type"))
                self.assertEqual(json.loads(with_json.data), {"example": 1})
                self.assertEqual(with_json.get_header("Content-type"), "application/json")

    def test_runtime_mutations_are_bounded_authenticated_and_never_automatically_retried(self):
        from urllib.error import URLError

        calls = []

        class Unknown:
            def open(self, request, timeout):
                calls.append((request, timeout))
                raise URLError("unconfirmed")

        client = RuntimeFabricClient(lambda: "checked-token", opener=Unknown())
        with self.assertRaisesRegex(RuntimeError, "outcome may be unconfirmed"):
            client.request("POST", f"/v1/workspaces/{WORKSPACE}/notebooks/{NOTEBOOK}/jobs/instances",
                           {"executionData": {"parameters": {}}}, timeout=15)
        self.assertEqual(len(calls), 1)
        req, timeout = calls[0]
        self.assertEqual((req.method, timeout, req.get_header("X-ms-fabric-skill")), ("POST", 15, "spark-cli"))
        self.assertEqual(req.get_header("Authorization"), "Bearer checked-token")
        self.assertEqual(json.loads(req.data), {"executionData": {"parameters": {}}})
        with self.assertRaises(ValueError):
            client.request("POST", "https://untrusted.invalid/run")
        self.assertEqual(len(calls), 1)
        for skill in ("activator-cli", "eventhouse-cli"):
            client = RuntimeFabricClient(lambda: "checked-token", opener=Unknown(), skill=skill)
            with self.assertRaises(RuntimeError):
                client.request("GET", f"/v1/operations/{NOTEBOOK}")
            self.assertEqual(calls[-1][0].get_header("X-ms-fabric-skill"), skill)
        with self.assertRaises(RuntimeError):
            client.request("GET", f"/v1/connections/{NOTEBOOK}")
        count = len(calls)
        with self.assertRaises(ValueError):
            client.request("PATCH", f"/v1/connections/{NOTEBOOK}", {})
        self.assertEqual(len(calls), count)


class OneLakeTableClientTests(unittest.TestCase):
    def test_uses_fixed_endpoint_storage_token_and_exact_candidate(self):
        opener = Opener(Response(body=delta_page()))
        client = OneLakeTableClient("storage-token", opener=opener, clock=lambda: 5)
        candidate = document()["shared"]["target_bindings"]["demo-3"]
        self.assertEqual(client.list_tables(candidate, 65), delta_page())
        req, timeout = opener.requests[0]
        url = parse.urlsplit(req.full_url)
        self.assertEqual((url.scheme, url.netloc), ("https", "onelake.table.fabric.microsoft.com"))
        self.assertEqual(url.path, f"/delta/{WORKSPACE}/{LAKEHOUSE}/api/2.1/unity-catalog/tables")
        self.assertEqual(parse.parse_qs(url.query), {"catalog_name": [LAKEHOUSE], "schema_name": ["dbo"]})
        self.assertEqual((req.method, req.get_header("Authorization"), timeout), ("GET", "Bearer storage-token", 30))

    def test_invalid_or_unexpected_responses_are_blocked_without_leaking_credentials(self):
        invalid_json = Response()
        invalid_json.body = b'{"tables":[],"tables":[]}'
        responses = (invalid_json, Response(status=302), Response(status=200, body=None),
                     error.HTTPError("https://onelake.table.fabric.microsoft.com", 403, "secret", {}, io.BytesIO()))
        candidate = document()["shared"]["target_bindings"]["demo-3"]
        for response in responses:
            with self.subTest(response=response):
                client = OneLakeTableClient("private-token", opener=Opener(response), clock=lambda: 5)
                with self.assertRaises(TargetReadbackBlocked) as caught:
                    client.list_tables(candidate, 65)
                self.assertNotIn("private-token", str(caught.exception))
                self.assertNotIn("secret", str(caught.exception))

    def test_invalid_candidate_or_expired_deadline_never_calls_the_service(self):
        opener = Opener()
        client = OneLakeTableClient("token", opener=opener, clock=lambda: 5)
        candidate = document()["shared"]["target_bindings"]["demo-3"]
        for bad in (dict(candidate, workspace_id="attacker.invalid"), dict(candidate, fact="dbo.bad.name")):
            with self.subTest(candidate=bad), self.assertRaises(TargetReadbackBlocked):
                client.list_tables(bad, 65)
        with self.assertRaisesRegex(TargetReadbackBlocked, "deadline"):
            client.list_tables(candidate, 5)
        self.assertEqual(opener.requests, [])


if __name__ == "__main__":
    unittest.main()
