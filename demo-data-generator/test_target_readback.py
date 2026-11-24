import copy
import io
import json
import unittest
from unittest import mock
from urllib import error, parse

import deploy
from target_readback import OneLakeTableClient, TargetReadbackBlocked, read_selected_targets, selected_candidates
from test_deploy import Opener, Response, config, owned_state


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
        self._clock = lambda: 5

    def get(self, path, deadline):
        self.calls.append((path, deadline))
        return {}, copy.deepcopy(self.responses[path])


class TargetReadbackTests(unittest.TestCase):
    def setUp(self):
        self.config = document()
        self.selected = ["demo-3", "demo-2", "demo-1"]
        self.client = ReadClient()
        self.clients = {name: self.client for name in self.selected}
        self.table_client = TableClient()

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

    def test_validate_only_uses_selected_readback_but_never_reports_ready(self):
        plan = deploy.parse_config(self.config, "sample")
        state = owned_state(plan)
        with (mock.patch.object(deploy, "load_owned_state", return_value=state),
              mock.patch.object(deploy, "_verify_all"),
              mock.patch.object(deploy, "azure_cli_token", side_effect=["fabric-token", "storage-token"]) as auth,
              mock.patch.object(deploy, "FabricClient", return_value=self.client) as factory,
              mock.patch.object(deploy, "OneLakeTableClient", return_value=self.table_client) as tables):
            diagnostics = deploy.perform("validate-only", self.config, "sample", ".", ",".join(self.selected))
        self.assertEqual(factory.call_args_list[1], mock.call("fabric-token", skill="eventhouse-cli"))
        auth.assert_has_calls([mock.call(plan["tenant_id"], plan["identity_id"]),
                               mock.call(plan["tenant_id"], plan["identity_id"], resource=deploy.STORAGE_RESOURCE)])
        tables.assert_called_once_with("storage-token")
        self.assertTrue(any("demo-3:" in entry and "unverified" in entry for entry in diagnostics))
        self.assertTrue(any("runtime readiness remains unproven" in entry for entry in diagnostics))

    def test_validation_without_demo3_does_not_acquire_a_storage_token(self):
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
