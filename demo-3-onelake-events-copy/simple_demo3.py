"""Reset and run the fixed OneLake FileCreated → native Copy demo."""

import base64
import copy
import json
import time
from datetime import date
from urllib.error import HTTPError
from urllib.parse import urljoin, urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener
from uuid import UUID


_API = "https://api.fabric.microsoft.com"
_LANDING = "Files/incoming/sales"
_FACT = "dbo.factsales"
_EVENT = "Microsoft.Fabric.OneLake.FileCreated"
_GRAPH = "ReflexEntities.json"
_TERMINAL = {"Completed", "Failed", "Cancelled", "Canceled", "Deduped"}
_WAIT_SECONDS = 600
_FACT_SCHEMA = (
    "SalesLineID BIGINT, BatchID STRING, SaleTimestamp TIMESTAMP, CustomerID INT, "
    "ProductID INT, Date DATE, Quantity INT, UnitPriceMinorUnits BIGINT, "
    "SalesAmountMinorUnits BIGINT"
)
_DIMENSIONS = {
    "DimDate": "Date DATE, Year INT, MonthNumber INT, MonthName STRING",
    "DimProduct": "ProductID INT, ProductName STRING, ProductCategory STRING",
    "DimCustomer": "CustomerID INT, CustomerName STRING, CustomerRegion STRING",
}


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def _request(context, method, path, body=None, *, skill="spark-cli"):
    url = urlsplit(urljoin(_API, path))
    if url.scheme != "https" or url.netloc != "api.fabric.microsoft.com" or not url.path.startswith("/v1/"):
        raise ValueError("Unexpected Fabric endpoint")
    request = Request(url.geturl(), method=method,
                      data=None if body is None else json.dumps(body).encode("utf-8"),
                      headers={"Authorization": "Bearer " + context["get_token"](_API),
                               "Content-Type": "application/json", "x-ms-fabric-skill": skill})
    with build_opener(_NoRedirect()).open(request, timeout=30) as response:
        raw = response.read()
        return response.status, response.headers, json.loads(raw) if raw else None


def _wait(deadline, message):
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError(message)
    time.sleep(min(5, remaining))


def _reflex_request(context, path, body, *, read=False):
    skill = "activator-cli"
    status, headers, result = _request(context, "POST", path, body, skill=skill)
    if status == 202:
        location = headers.get("Location")
        operation_id = urlsplit(location).path.rstrip("/").rsplit("/", 1)[-1] if location else headers["x-ms-operation-id"]
        operation = f"/v1/operations/{UUID(operation_id)}"
        deadline = time.monotonic() + 180
        while True:
            _, _, state = _request(context, "GET", operation, skill=skill)
            if state["status"] == "Succeeded":
                break
            if state["status"] not in {"NotStarted", "Running"}:
                raise RuntimeError(f"Demo 3 Activator operation failed: {state.get('error', state['status'])}")
            _wait(deadline, "Demo 3 Activator operation did not finish")
        if not read:
            return None
        status, _, result = _request(context, "GET", operation + "/result", skill=skill)
    if status != 200:
        raise RuntimeError(f"Unexpected Demo 3 Activator response: HTTP {status}")
    return result


def _settings(config):
    settings = config["demo-3"]
    if settings["landing_path"] != _LANDING or settings["destination_table"] != _FACT:
        raise ValueError("Demo 3 reset and Copy require the configured sales folder and dbo.factsales")
    return settings


def _base(settings):
    return f"/v1/workspaces/{UUID(settings['workspace_id'])}"


def _part(parts, name):
    matches = [part for part in parts if part["path"] == name and part["payloadType"] == "InlineBase64"]
    if len(matches) != 1:
        raise ValueError(f"Missing or ambiguous Fabric definition part: {name}")
    return matches[0]


def _read_graph(context, path):
    parts = _reflex_request(context, path + "/getDefinition", {}, read=True)["definition"]["parts"]
    return parts, json.loads(base64.b64decode(_part(parts, _GRAPH)["payload"], validate=True))


def _rule(graph):
    rules = [entity["payload"]["definition"] for entity in graph
             if entity["type"] == "timeSeriesView-v1"
             and entity["payload"]["definition"]["type"] == "Rule"]
    if len(rules) != 1:
        raise ValueError("Expected one rule on the configured Demo 3 Activator")
    return rules[0]


def _bind_subject(graph, config):
    settings = _settings(config)
    sources = [entity["payload"] for entity in graph if entity["type"] == "realTimeHubSource-v1"]
    actions = [entity for entity in graph if entity["type"] == "fabricItemAction-v1"]
    if len(sources) != 1 or len(actions) != 1:
        raise ValueError("Expected one OneLake source and one pipeline action on Demo 3 Activator")
    source, action = sources[0], actions[0]
    for key, value in {
        "scope": "Artifact", "tenantId": config["tenant_id"], "workspaceId": settings["workspace_id"],
        "artifactId": settings["lakehouse_id"], "eventGroupType": "Microsoft.Fabric.OneLakeEvents",
    }.items():
        if source["connection"].get(key) != value:
            raise ValueError(f"Demo 3 Activator OneLake source does not match settings: {key}")
    if (action["payload"]["fabricItem"] != {
            "itemId": settings["pipeline_id"], "workspaceId": settings["workspace_id"], "itemType": "Pipeline",
    } or action["payload"]["jobType"] != "Pipeline"):
        raise ValueError("Demo 3 Activator action does not target its configured Copy pipeline")
    source["filterSettings"] = {
        "eventTypes": [{"name": _EVENT}],
        "filters": [
            {"operatorType": "StringBeginsWith", "key": "subject", "values": [f"/{_LANDING}/sales_"]},
            {"operatorType": "StringEndsWith", "key": "subject", "values": [".csv"]},
        ],
    }
    rule = _rule(graph)
    instance = json.loads(rule["instance"])
    bindings = [row for step in instance["steps"] for row in step["rows"]
                if row["kind"] == "FabricItemInvocation"]
    if len(bindings) != 1:
        raise ValueError("Expected one Fabric item binding on the Demo 3 rule")
    args = {arg["name"]: arg for arg in bindings[0]["arguments"]}
    if args["fabricJobConnectionDocumentId"]["value"] != action["uniqueIdentifier"]:
        raise ValueError("Demo 3 rule does not reference its pipeline action")
    parameters = args["parameters"]["values"]
    subjects = [item for item in parameters
                if any(arg.get("name") == "parameterName" and arg.get("value") == "sourceSubject"
                       for arg in item["arguments"])]
    if len(subjects) != 1 or "___subject" not in {
        arg["value"] for group in subjects[0]["arguments"] for ref in group.get("values", [])
        if ref.get("kind") == "EventFieldReference" for arg in ref["arguments"]
        if arg.get("name") == "fieldName"
    }:
        raise ValueError("Demo 3 Copy must bind sourceSubject to the event's ___subject")
    if subjects != parameters:
        args["parameters"]["values"] = subjects
        rule["instance"] = json.dumps(instance)


def _trigger(context, enabled):
    config = context["config"]
    settings = _settings(config)
    path = _base(settings) + f"/reflexes/{UUID(settings['activator_rule_id'])}"
    parts, graph = _read_graph(context, path)
    before = copy.deepcopy(graph)
    if enabled:
        _bind_subject(graph, config)
    _rule(graph)["settings"].update(shouldRun=enabled, shouldApplyRuleOnUpdate=False)
    if graph != before:
        _part(parts, _GRAPH)["payload"] = base64.b64encode(json.dumps(graph).encode("utf-8")).decode("ascii")
        _reflex_request(context, path + "/updateDefinition", {
            "definition": {"parts": [part for part in parts if part["path"] != ".platform"]},
        })
        _, actual = _read_graph(context, path)
        if (_rule(actual)["settings"]["shouldRun"] is not enabled
                or _rule(actual)["settings"]["shouldApplyRuleOnUpdate"] is not False):
            raise RuntimeError("Demo 3 Activator did not reach the requested state")


def _jobs_path(context):
    settings = _settings(context["config"])
    return _base(settings) + f"/items/{UUID(settings['pipeline_id'])}/jobs/instances"


def _jobs(context):
    jobs_path = _jobs_path(context)
    path = jobs_path + "?jobType=Pipeline"
    seen = set()
    while path:
        if path in seen or urlsplit(path).path != jobs_path:
            raise RuntimeError("Demo 3 pipeline history returned an unexpected continuation")
        seen.add(path)
        _, _, page = _request(context, "GET", path)
        yield from page["value"]
        path = page.get("continuationUri")
        if not path and page.get("continuationToken"):
            raise RuntimeError("Demo 3 pipeline history returned no continuation URI")


def _drain(context, *, cancel):
    deadline = time.monotonic() + _WAIT_SECONDS
    requested = set()
    while True:
        active = [job for job in _jobs(context) if job["status"] not in _TERMINAL]
        if not active:
            return
        if cancel:
            for job in active:
                job_id = UUID(job["id"])
                if job_id in requested:
                    continue
                path = _jobs_path(context) + f"/{job_id}"
                try:
                    status, _, _ = _request(context, "POST", path + "/cancel")
                except HTTPError:
                    _, _, actual = _request(context, "GET", path)
                    if actual["status"] not in _TERMINAL:
                        raise
                else:
                    if status != 202:
                        raise RuntimeError(f"Unexpected Demo 3 Copy cancellation response: HTTP {status}")
                requested.add(job_id)
        _wait(deadline, "Demo 3 Copy jobs did not stop; sales state was not cleared")


def _lakehouse_root(settings):
    return (f"abfss://{UUID(settings['workspace_id'])}@onelake.dfs.fabric.microsoft.com/"
            f"{UUID(settings['lakehouse_id'])}")


def _qualified(context, settings):
    _, _, workspace = _request(context, "GET", _base(settings))
    names = (workspace["displayName"], settings["lakehouse_name"], "dbo")
    return ".".join("`" + name.replace("`", "``") + "`" for name in names)


def reset(context):
    """Stop Copy, clear this demo's sales, and restore its schema and dimensions."""
    from notebookutils import fs
    from simple_data import dimensions

    settings = _settings(context["config"])
    stop(context, failed=True)
    spark = context["spark"]
    namespace = _qualified(context, settings)
    landing = f"{_lakehouse_root(settings)}/{_LANDING}"
    if fs.exists(landing) and not fs.rm(landing, True):
        raise RuntimeError("Could not clear the Demo 3 landing folder")
    if not fs.mkdirs(landing):
        raise RuntimeError("Could not recreate the Demo 3 landing folder")

    fact = f"{namespace}.`factsales`"
    spark.sql(f"DROP TABLE IF EXISTS {fact}")
    spark.sql(f"CREATE TABLE {fact} ({_FACT_SCHEMA}) USING DELTA "
              "TBLPROPERTIES ('delta.enableDeletionVectors' = 'false')")
    generated = dimensions(context["config"]["data"])
    for name, schema in _DIMENSIONS.items():
        columns = [field.split()[0] for field in schema.split(",")]
        values = [tuple(date.fromisoformat(row[col]) if col == "Date" and isinstance(row[col], str)
                        else row[col] for col in columns) for row in generated[name]]
        (spark.createDataFrame(values, schema).write.format("delta").mode("overwrite")
         .saveAsTable(f"{namespace}.`{name}`"))


def start(context):
    """Enable the existing native trigger before the producer publishes sales."""
    _trigger(context, True)


def stop(context, *, failed=False):
    """Retain the results; on failure cancel active Copy rather than waiting for data."""
    if not failed:
        _wait_for_final_batch(context)
    _trigger(context, False)
    _drain(context, cancel=failed)


def _wait_for_final_batch(context):
    from simple_data import batch_id

    config = context["config"]
    root = _lakehouse_root(_settings(config))
    final_batch = batch_id(config["data"]["file_count"])
    spark = context["spark"]
    deadline = time.monotonic() + _WAIT_SECONDS
    while True:
        fact = spark.read.format("delta").load(f"{root}/Tables/dbo/factsales")
        if fact.where(fact["BatchID"] == final_batch).limit(1).take(1):
            return
        _wait(deadline, f"Demo 3 Copy has not delivered final batch {final_batch}; see pipeline run history")


def copy_pipeline(config, existing):
    """Build the one-time pipeline update from the fixed pipeline's exported Copy."""
    settings = _settings(config)

    def copies(activities):
        for activity in activities:
            if activity["type"] == "Copy":
                yield activity
            elif activity["type"] == "IfCondition":
                for branch in ("ifTrueActivities", "ifFalseActivities"):
                    yield from copies(activity["typeProperties"].get(branch, []))

    found = list(copies(existing["properties"]["activities"]))
    if len(found) != 1:
        raise ValueError("Expected one native Copy activity in the fixed Demo 3 pipeline")
    activity = copy.deepcopy(found[0])
    activity.update(name="CopyEventFile", dependsOn=[])
    activity.pop("state", None)
    activity.pop("onInactiveMarkAs", None)
    props = activity["typeProperties"]
    source, sink = props["source"], props["sink"]
    for endpoint in (source, sink):
        connection = endpoint["datasetSettings"]["connectionSettings"]["properties"]
        options = connection["typeProperties"]
        if (connection["type"] != "Lakehouse" or options["workspaceId"] != settings["workspace_id"]
                or options["artifactId"] != settings["lakehouse_id"]
                or connection["externalReferences"]["connection"] != settings["connection_id"]):
            raise ValueError("Demo 3 Copy does not use the fixed Lakehouse and connection")
    location = source["datasetSettings"]["typeProperties"]["location"]
    if (source["type"] != "DelimitedTextSource" or location["type"] != "LakehouseLocation"
            or location["folderPath"] != _LANDING.removeprefix("Files/")
            or source["storeSettings"].get("recursive") is not False
            or any("wildcard" in key.lower() or "filelist" in key.lower()
                   for section in (source, source["storeSettings"], location) for key in section)
            or sink["type"] != "LakehouseTableSink"
            or sink["datasetSettings"]["typeProperties"].get("schema") != "dbo"
            or sink["datasetSettings"]["typeProperties"].get("table") != "factsales"
            or props.get("enableStaging") is not False
            or sink["tableActionOption"] != "Upsert"
            or sink["upsertSettings"]["keys"] != ["BatchID", "SalesLineID"]):
        raise ValueError("Expected exact-file native Lakehouse Copy with the existing FactSales Upsert")
    location["fileName"] = {
        "type": "Expression", "value": "@last(split(pipeline().parameters.sourceSubject, '/'))",
    }
    return {"properties": {
        "concurrency": 1,
        "parameters": {"sourceSubject": {"type": "String"}},
        "activities": [activity],
    }}
