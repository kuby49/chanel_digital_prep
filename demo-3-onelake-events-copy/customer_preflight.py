"""Read-only admission for the single explicitly bound customer demonstration."""

import base64
import copy
import hashlib
import importlib.util
import json
import subprocess
import sys
from pathlib import Path

import guarded_notebook
import guarded_pipeline
from audit import LOCK_PATH, RECORD_PREFIX
from deploy import CandidateBlocked, _load_json
from ownership import FabricControl, cli_credential


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
FRESH_RUN_ID = "customer_demo_20261124_dc2792654b5b"
NATIVE_JOURNAL = "demo3-native-binding-state.json"
TERMINAL = {"Completed", "Failed", "Cancelled", "Canceled", "Deduped"}
LANDING = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{LAKEHOUSE}/Files/incoming/sales"


def _require(condition, message):
    if not condition:
        raise CandidateBlocked(message)


def config_sha256(config):
    return hashlib.sha256(json.dumps(
        config, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False,
    ).encode("utf-8")).hexdigest()


def customer_run(config):
    run_id = config.get("data", {}).get("run_id")
    _require(run_id in (RUN["run_id"], FRESH_RUN_ID), "Customer run is not explicitly approved.")
    return {**RUN, "run_id": run_id}


def binding_journal(config):
    run = customer_run(config)
    expected = JOURNAL if run["run_id"] == RUN["run_id"] else (
        "demo3-customer-" + run["run_id"] + "-binding-state.json")
    supplied = config["demo-3"].get("customer_binding_journal", JOURNAL)
    _require(supplied == expected, "Customer journal must be the exact run-specific basename.")
    return expected


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


def _rule_state(graph, state, *, reusable=False):
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
    if reusable:
        return running
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
    credential = cli_credential("https://storage.azure.com/", timeout=120)
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
    native = demo.get("native_preflight") is True
    _require((native != (demo.get("customer_preflight") is True)) and prefix == "chanel"
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
    if native:
        import native_guard

        _require(demo.get("native_binding_journal") == NATIVE_JOURNAL,
                 "Native binding journal must use its exact separate basename.")
        policy = native_guard.validate_policy(demo["native_policy"])
        _require(policy["run_template"] == {key: RUN[key] for key in native_guard._TEMPLATE}
                 and policy["max_file_count"] <= 10 and policy["max_rows_per_file"] <= 1000,
                 "Native generation policy exceeds prepared dimensions or ten-file/1000-row bounds.")
        journal_path = state_dir / NATIVE_JOURNAL
        run = None
    else:
        run = customer_run(config)
        journal_path = state_dir / binding_journal(config)
    state = _load_json(journal_path)
    expected = {
        "workspace_id": WORKSPACE, "lakehouse_id": LAKEHOUSE, "guard_notebook_id": NOTEBOOK,
        "pipeline_id": PIPELINE, "reflex_id": REFLEX, "producer_workspace_id": STAGING_WORKSPACE,
        "producer_lakehouse_id": STAGING_LAKEHOUSE, "generator_sha256": GENERATOR_SHA,
    }
    if native:
        expected.update(native_policy=policy, native_policy_sha256=config_sha256(policy))
    else:
        expected["run_config"] = run
    _require(all(state.get(key) == value for key, value in expected.items()),
             "Original customer binding journal differs.")
    _require(all(state.get("actions", {}).get(key, {}).get("status") == "confirmed-readback"
                 for key in ("guard_update", "activator_update")), "Customer binding is unconfirmed.")
    if native:
        previous_name = "demo3-customer-" + FRESH_RUN_ID + "-binding-state.json"
        previous = _load_json(state_dir / previous_name)
        original = _load_json(state_dir / JOURNAL)
        _require(state.get("previous_journal") == previous_name
                 and state.get("previous_journal_sha256") == config_sha256(previous)
                 and set(previous.get("actions", {})) == {"guard_update", "activator_update"}
                 and all(item.get("status") == "confirmed-readback"
                         for item in previous["actions"].values())
                 and previous.get("previous_journal") == JOURNAL
                 and previous.get("previous_journal_sha256") == config_sha256(original)
                 and original.get("runtime_stop") == "rule-disabled-jobs-terminal-lock-absent"
                 and original.get("actions", {}).get("disable", {}).get("status") == "confirmed-readback",
                 "Native binding must preserve the previously stopped, never-enabled fresh attempt.")
    elif run["run_id"] == FRESH_RUN_ID:
        _require(state.get("config_sha256") == config_sha256(config),
                 "Fresh customer binding does not match the final agreed config hash.")
        previous = _load_json(state_dir / JOURNAL)
        _require(state.get("previous_journal") == JOURNAL
                 and state.get("previous_journal_sha256") == config_sha256(previous)
                 and previous.get("runtime_stop") == "rule-disabled-jobs-terminal-lock-absent"
                 and previous.get("actions", {}).get("disable", {}).get("status") == "confirmed-readback",
                 "Previous customer attempt is not preserved and safely stopped.")
    producer, generator, storage = _producer(state_dir)
    if native:
        projected = {**config, "data": {**data, "run_id": "native-validation"}}
        candidate = guarded_notebook.run_config_from_shared(generator, projected)
        native_guard.validate_run_policy(generator.RunConfig(**candidate), policy)
    else:
        _require(guarded_notebook.run_config_from_shared(generator, config) == run,
                 "Only the bound one-file ten-row customer run is supported.")
    api = api or FabricControl.from_cli(TENANT, timeout=120)
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
    if native:
        expected_notebook = guarded_notebook.build_native_notebook(
            producer, policy, WORKSPACE, LAKEHOUSE, HOUSE_NAME, STAGING_WORKSPACE, STAGING_LAKEHOUSE)
    else:
        expected_notebook = guarded_notebook.build_notebook(
            producer, run, WORKSPACE, LAKEHOUSE, HOUSE_NAME, STAGING_WORKSPACE, STAGING_LAKEHOUSE)
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
    running = _rule_state(graph, state, reusable=native)
    if native:
        sources = [item for item in graph if item.get("type") == "realTimeHubSource-v1"]
        _require(len(sources) == 1 and sources[0]["payload"]["filterSettings"] == {
            "eventTypes": [{"name": "Microsoft.Fabric.OneLake.FileCreated"}],
            "filters": [
                {"operatorType": "StringBeginsWith", "key": "subject",
                 "values": ["/Files/incoming/sales/sales_"]},
                {"operatorType": "StringEndsWith", "key": "subject", "values": [".csv"]},
            ]}, "Native event source is not the exact reusable sales-file filter.")
    for item in _inventory(api, base + "/items"):
        if item.get("type") not in ("Notebook", "DataPipeline") or MARKER not in item.get("description", ""):
            continue
        job_type = "RunNotebook" if item["type"] == "Notebook" else "Pipeline"
        _require(all(job.get("status") in TERMINAL for job in _inventory(
            api, base + "/items/" + item["id"] + "/jobs/instances?jobType=" + job_type)),
            "An owned notebook or pipeline job is not terminal.")
    token = _storage_token()
    store = storage.OneLakeStore(WORKSPACE, LAKEHOUSE, lambda: token)
    paths = (LOCK_PATH,) if native else (
        LOCK_PATH, RECORD_PREFIX + run["run_id"] + "_0001.json",
        "Files/incoming/sales/sales_" + run["run_id"] + "_0001.csv")
    for path in paths:
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
    _require(_load_json(journal_path) == state, "Customer journal changed during admission.")
    return [
        ("Bound reusable native preflight passed; rule is " if native else
         "Bound customer-run preflight passed; rule is ") + ("enabled." if running else "stopped."),
        "Existing guard, exact-file Copy, target, connection, dimensions, terminal jobs and absent "
        "writer lock verified. No data/job/rule mutation or SQL sync required.",
        ("Ready means reusable native binding within the pinned policy only. Each run still needs "
         "an open, unexpired destination-bound manifest admission and no valid stop marker; "
         "no delivery, recurring arrivals or ten-file runtime acceptance is claimed." if native else
         "Ready means admission for this one-file ten-row demonstration only, not delivered data, "
         "future object access, general deployment readiness or the ten-file acceptance run."),
    ]


def validate(config, prefix, state_dir, *, api=None):
    try:
        if "native_rebind" in config["demo-3"]:
            from native_rebind import validate_rebound

            return validate_rebound(config, prefix, state_dir, api=api)
        return _validate(config, prefix, state_dir, api=api)
    except (KeyError, TypeError, IndexError, RuntimeError, subprocess.SubprocessError) as exc:
        raise CandidateBlocked(f"Customer preflight could not verify its evidence: {type(exc).__name__}.") from exc
