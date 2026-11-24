"""Read-only admission for the single explicitly bound customer demonstration."""

import base64
import copy
import hashlib
import importlib.util
import json
import shutil
import subprocess
import sys
from pathlib import Path

import guarded_notebook
import guarded_pipeline
from audit import LOCK_PATH, RECORD_PREFIX
from deploy import CandidateBlocked, _load_json
from ownership import FabricControl


TENANT = "4526cee1-4399-4e4b-9134-46fabd583527"
WORKSPACE = "744223de-ceab-4c4f-82ba-f811bcb5c7d7"
LAKEHOUSE = "6bba6989-5cfd-4192-8e75-00274afb66c1"
NOTEBOOK = "54b9c653-9644-4525-8998-40b3375a4cd2"
PIPELINE = "bab19f65-b5ab-4edd-8066-89b4fc00f120"
REFLEX = "195d1691-08f6-4140-a6d5-18c332696fef"
CONNECTION = "c906689d-a8de-48fb-b505-1ac3b4c8a9c5"
STAGING_WORKSPACE = "48593f2f-d3ac-474b-ba96-08282d6f69df"
STAGING_LAKEHOUSE = "fca649c9-0297-4997-8e2d-8ac5338aaa1a"
MARKER = "chanel-demo3-a9bbae81-c4f6-4d80-97af-1302af68f910"
HOUSE_NAME = "Demo3EventE2eA9bbae81"
JOURNAL = "demo3-customer-binding-state.json"
GENERATOR_SHA = "3801662e916ba5d41ecab272a628a30b332ba39e1ab00633a7879783c7f2c81e"
STORAGE_SHA = "532f553fc02baa3e4ebba79e3b343aa1f55aed7817be82b8c74cc7155a3cf2e7"
RUN = {
    "run_id": "customer_demo_20261124_e0c409cc05ef", "batch_class": "measured",
    "start_date": "2026-01-01", "seed": 42, "date_count": 365, "products": 20,
    "customers": 100, "file_count": 1, "rows_per_file": 10, "interval_seconds": 60,
}
TERMINAL = {"Completed", "Failed", "Cancelled", "Canceled", "Deduped"}
LANDING = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{LAKEHOUSE}/Files/incoming/sales"


def _require(condition, message):
    if not condition:
        raise CandidateBlocked(message)


def _inventory(api, path, skill="spark-cli"):
    visited = set()
    while path:
        _require(path not in visited and len(visited) < 100, "Incomplete Fabric inventory.")
        visited.add(path)
        page = api.get(path, skill=skill)
        _require(isinstance(page.get("value"), list), "Invalid Fabric inventory.")
        yield from page["value"]
        path = page.get("continuationUri")
        _require(not page.get("continuationToken") or path, "Incomplete Fabric pagination.")


def _owned(api, path, name, item_id, skill="spark-cli"):
    matches = [item for item in _inventory(api, path, skill) if item.get("displayName") == name]
    _require(len(matches) == 1 and matches[0].get("id") == item_id
             and MARKER in matches[0].get("description", ""), "Owned resource identity differs.")
    return matches[0]


def _definition(api, base, resource, part, skill="spark-cli", suffix=""):
    response = api.request("POST", base + resource + "/getDefinition" + suffix, {}, skill=skill)
    document = api._operation(*response, 200, skill=skill)
    parts = [item for item in document["definition"]["parts"]
             if item.get("path") == part and item.get("payloadType") == "InlineBase64"]
    _require(len(parts) == 1, "Required definition part is missing or ambiguous.")
    return json.loads(base64.b64decode(parts[0]["payload"], validate=True))


def _canonical(graph):
    graph = copy.deepcopy(graph)
    for entity in graph:
        definition = entity.get("payload", {}).get("definition", {})
        if isinstance(definition.get("instance"), str):
            definition["instance"] = json.loads(definition["instance"])
    return sorted(graph, key=lambda entity: entity["uniqueIdentifier"])


def _rule_state(graph, state):
    rules = [item for item in graph if item.get("type") == "timeSeriesView-v1"
             and item["payload"].get("definition", {}).get("type") == "Rule"]
    _require(len(rules) == 1, "Expected exactly one customer rule.")
    running = rules[0]["payload"]["definition"]["settings"].get("shouldRun")
    _require(type(running) is bool, "Customer rule state is unavailable.")
    expected = copy.deepcopy(state["definition_disabled"])
    expected_rules = [item for item in expected if item.get("type") == "timeSeriesView-v1"
                      and item["payload"].get("definition", {}).get("type") == "Rule"]
    _require(len(expected_rules) == 1, "Journal rule definition is invalid.")
    expected_rules[0]["payload"]["definition"]["settings"]["shouldRun"] = running
    _require(_canonical(graph) == _canonical(expected), "Customer event/action binding changed.")
    actions = state["actions"]
    _require("disable" not in actions and not any(
        key in state for key in ("runtime_failure", "runtime_stop", "runtime_stop_failure")
    ), "Customer runtime was stopped or failed; do not relaunch.")
    if running:
        _require(actions.get("enable", {}).get("status") == "confirmed-readback",
                 "Enabled rule lacks confirmed coordinated enable.")
    else:
        _require("enable" not in actions, "Rule stopped after an enable attempt; reconcile first.")
    return running


def _producer(state_dir):
    folder = state_dir / "demo3-current-producer" / "demo-data-generator"
    modules = []
    for name, fingerprint in (("generator", GENERATOR_SHA), ("fabric_storage", STORAGE_SHA)):
        source = folder / (name + ".py")
        normalized = source.read_text(encoding="utf-8").encode("utf-8")
        _require(hashlib.sha256(normalized).hexdigest() == fingerprint,
                 "Pinned producer dependency changed.")
        spec = importlib.util.spec_from_file_location("_demo3_customer_" + name, source)
        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module
        spec.loader.exec_module(module)
        modules.append(module)
    return folder, *modules


def _storage_token():
    result = subprocess.run(
        [shutil.which("az"), "account", "get-access-token", "--resource",
         "https://storage.azure.com/", "-o", "json"],
        capture_output=True, check=True, text=True, timeout=30,
    )
    credential = json.loads(result.stdout)
    _require(credential["tenant"] == TENANT, "Storage credential tenant differs.")
    return credential["accessToken"]


def _absent(store, path):
    try:
        store.read(path)
    except FileNotFoundError:
        return
    raise CandidateBlocked("Customer source, audit or writer lock already exists.")


def _validate(config, prefix, state_dir, *, api=None):
    demo = config["demo-3"]
    _require(demo.get("customer_preflight") is True and prefix == "chanel"
             and config.get("tenant_id") == TENANT,
             "Only the explicitly selected MCAP customer preflight is supported.")
    for field, value in (("workspace_id", WORKSPACE), ("lakehouse_id", LAKEHOUSE),
                         ("lakehouse_name", HOUSE_NAME), ("destination_table", "dbo.factsales")):
        _require(demo.get(field) == value, "Configured customer target differs.")
    _require(demo.get("landing_path") in ("Files/incoming/sales", LANDING),
             "Configured landing path differs.")
    shared = config.get("shared", {})
    _require(shared.get("workspace_id") == STAGING_WORKSPACE
             and shared.get("staging_lakehouse_id") == STAGING_LAKEHOUSE
             and shared.get("target_bindings", {}).get("demo-3") == {
                 "workspace_id": WORKSPACE, "item_id": LAKEHOUSE, "fact": "dbo.factsales"},
             "Shared producer staging or target binding differs.")
    data = config.get("data", {})
    _require(type(data.get("max_duration_seconds")) is int
             and 0 < data["max_duration_seconds"] <= 1800 and data.get("currency") == "USD",
             "Customer run needs a bounded duration of at most 1800 seconds and USD.")
    state = _load_json(state_dir / JOURNAL)
    expected = {
        "workspace_id": WORKSPACE, "lakehouse_id": LAKEHOUSE, "guard_notebook_id": NOTEBOOK,
        "pipeline_id": PIPELINE, "reflex_id": REFLEX, "producer_workspace_id": STAGING_WORKSPACE,
        "producer_lakehouse_id": STAGING_LAKEHOUSE, "generator_sha256": GENERATOR_SHA,
        "run_config": RUN,
    }
    _require(all(state.get(key) == value for key, value in expected.items()),
             "Original customer binding journal differs.")
    _require(all(state.get("actions", {}).get(key, {}).get("status") == "confirmed-readback"
                 for key in ("guard_update", "activator_update")), "Customer binding is unconfirmed.")
    producer, generator, storage = _producer(state_dir)
    _require(guarded_notebook.run_config_from_shared(generator, config) == RUN,
             "Only the bound one-file ten-row customer run is supported.")
    api = api or FabricControl.from_cli(TENANT)
    base = "/v1/workspaces/" + WORKSPACE
    workspace = _owned(api, "/v1/workspaces", "chanel-demo3-copy-events", WORKSPACE)
    _require(workspace.get("capacityId") == "de12195c-40d2-4bee-af60-f0d6dedfbb0f",
             "Owned workspace capacity changed.")
    _owned(api, base + "/lakehouses", HOUSE_NAME, LAKEHOUSE)
    _owned(api, base + "/items?type=Notebook", "Demo3EventE2eGuardA9bbae81", NOTEBOOK)
    _owned(api, base + "/items?type=DataPipeline", "Demo3EventE2ePipelineA9bbae81", PIPELINE)
    _owned(api, base + "/reflexes", "Demo3EventE2eActivatorA9bbae81", REFLEX, "activator-cli")
    notebook = _definition(api, base, "/items/" + NOTEBOOK,
                           "notebook-content.ipynb", suffix="?format=ipynb")
    expected_notebook = guarded_notebook.build_notebook(
        producer, RUN, WORKSPACE, LAKEHOUSE, HOUSE_NAME, STAGING_WORKSPACE, STAGING_LAKEHOUSE)
    _require([cell["source"] for cell in notebook["cells"]]
             == [cell["source"] for cell in expected_notebook["cells"]]
             and notebook["metadata"]["trident"]["lakehouse"]
             == expected_notebook["metadata"]["trident"]["lakehouse"],
             "Packaged customer guard changed.")
    pipeline = _definition(api, base, "/items/" + PIPELINE, "pipeline-content.json")
    activities = pipeline["properties"]["activities"]
    _require(len(activities) == 2 and activities[1].get("name") == "CopyOnlyWhenAdmitted",
             "Guarded pipeline graph differs.")
    copy_activity = activities[1]["typeProperties"]["ifTrueActivities"][0]
    _require(pipeline["properties"] == guarded_pipeline.build_pipeline(
        WORKSPACE, LAKEHOUSE, CONNECTION, NOTEBOOK, copy_activity)["properties"],
        "Exact-file Copy, serialization or target binding changed.")
    connection = api.get("/v1/connections/" + CONNECTION)
    _require(connection.get("id") == CONNECTION
             and connection.get("connectionDetails", {}).get("type") == "Lakehouse",
             "Existing Lakehouse connection is unavailable.")
    graph = _definition(api, base, "/reflexes/" + REFLEX, "ReflexEntities.json", "activator-cli")
    running = _rule_state(graph, state)
    for item in _inventory(api, base + "/items"):
        if item.get("type") not in ("Notebook", "DataPipeline") or MARKER not in item.get("description", ""):
            continue
        job_type = "RunNotebook" if item["type"] == "Notebook" else "Pipeline"
        _require(all(job.get("status") in TERMINAL for job in _inventory(
            api, base + "/items/" + item["id"] + "/jobs/instances?jobType=" + job_type)),
            "An owned notebook or pipeline job is not terminal.")
    store = storage.OneLakeStore(WORKSPACE, LAKEHOUSE, _storage_token)
    for path in (LOCK_PATH, RECORD_PREFIX + RUN["run_id"] + "_0001.json",
                 "Files/incoming/sales/sales_" + RUN["run_id"] + "_0001.csv"):
        _absent(store, path)
    proof = json.loads(store.read("Files/demo3-audit/setup/common-dimensions-a9bbae81.json")[0])
    _require(proof.get("workspace_id") == WORKSPACE and proof.get("lakehouse_id") == LAKEHOUSE
             and proof.get("generator_sha256") == GENERATOR_SHA
             and proof["result"].get("dimensions") == {"DimDate": 369, "DimProduct": 20, "DimCustomer": 100}
             and proof["result"].get("fact_unchanged") is True, "Dimension preparation proof differs.")
    access = _load_json(state_dir / "demo3-staging-access-state.json")
    _require(access.get("workspace_id") == WORKSPACE and access.get("lakehouse_id") == LAKEHOUSE
             and access.get("producer_workspace_id") == STAGING_WORKSPACE
             and access.get("producer_lakehouse_id") == STAGING_LAKEHOUSE
             and access.get("result", {}).get("notebook_job_status") == "Completed",
             "Original pipeline-context staging access proof is missing.")
    _require(_load_json(state_dir / JOURNAL) == state, "Customer journal changed during admission.")
    return [
        "Bound customer-run preflight passed; rule is " + ("enabled." if running else "stopped."),
        "Existing guard, exact-file Copy, target, connection, dimensions, terminal jobs and absent "
        "writer lock verified. No data/job/rule mutation or SQL sync required.",
        "Ready means admission for this one-file ten-row demonstration only, not delivered data, "
        "future object access, general deployment readiness or the ten-file acceptance run.",
    ]


def validate(config, prefix, state_dir, *, api=None):
    try:
        return _validate(config, prefix, state_dir, api=api)
    except (KeyError, TypeError, IndexError, RuntimeError, subprocess.SubprocessError) as exc:
        raise CandidateBlocked(f"Customer preflight could not verify its evidence: {type(exc).__name__}.") from exc
