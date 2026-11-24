"""Build an uninstalled Data Factory definition for guarded exact-file Copy."""

import copy

from deploy import _guid


_PREPARED = "json(activity('PrepareCopy').output.result.exitValue)"


def _expr(value):
    return {"type": "Expression", "value": value}


def build_pipeline(workspace_id, lakehouse_id, connection_id, notebook_id, copy_activity,
                   *, timeout_minutes=10):
    """Use a Lakehouse Copy template already pinned to this deployment's connection."""
    workspace_id, notebook_id = _guid(workspace_id), _guid(notebook_id)
    lakehouse_id, connection_id = _guid(lakehouse_id), _guid(connection_id)
    if type(timeout_minutes) is not int or not 1 <= timeout_minutes <= 59:
        raise ValueError("invalid activity timeout")
    activity_timeout = f"0.00:{timeout_minutes:02}:00"

    def notebook(name, phase, parameters, dependencies=()):
        return {
            "name": name, "type": "TridentNotebook",
            "dependsOn": [{"activity": item, "dependencyConditions": ["Succeeded"]}
                          for item in dependencies],
            "policy": {"timeout": activity_timeout, "retry": 0, "secureOutput": True,
                       "secureInput": True},
            "typeProperties": {
                "workspaceId": workspace_id, "notebookId": notebook_id,
                "sessionTag": "demo3-guarded-copy",
                "parameters": {
                    "phase": {"value": phase, "type": "string"},
                    "job_id": {"value": _expr("@pipeline().RunId"), "type": "string"},
                    **{key: {"value": _expr(value), "type": "string"}
                       for key, value in parameters.items()},
                },
            },
        }

    prepare = notebook("PrepareCopy", "prepare", {
        "subject": "@pipeline().parameters.sourceSubject",
        "event_id": "@pipeline().parameters.eventId",
        "origin": "@pipeline().parameters.deliveryOrigin",
    })
    if not isinstance(copy_activity, dict) or copy_activity.get("type") != "Copy":
        raise ValueError("a Lakehouse Copy activity is required")
    activity = copy.deepcopy(copy_activity)
    types = activity["typeProperties"]
    source, sink = types["source"], types["sink"]
    location = source["datasetSettings"]["typeProperties"]["location"]
    target = sink["datasetSettings"]["typeProperties"]
    if (source.get("type") != "DelimitedTextSource" or location.get("type") != "LakehouseLocation"
            or location.get("folderPath") != "incoming/sales"
            or source["storeSettings"].get("recursive") is not False
            or any("wildcard" in key.lower() or "filelist" in key.lower()
                   for section in (source, source["storeSettings"], location) for key in section)
            or sink.get("type") != "LakehouseTableSink"
            or sink.get("tableActionOption") != "Upsert"
            or sink.get("upsertSettings", {}).get("keys") != ["BatchID", "SalesLineID"]
            or target.get("schema") != "dbo" or target.get("table") != "factsales"
            or types.get("enableStaging") is not False):
        raise ValueError("Copy must use one fixed Lakehouse folder and (BatchID, SalesLineID) Upsert")
    for endpoint in (source, sink):
        connection = endpoint["datasetSettings"]["connectionSettings"]["properties"]
        if (connection.get("type") != "Lakehouse"
                or connection["typeProperties"].get("workspaceId") != workspace_id
                or connection["typeProperties"].get("artifactId") != lakehouse_id
                or connection["externalReferences"].get("connection") != connection_id):
            raise ValueError("Copy template connection or Lakehouse does not match the deployment")
    columns = ("SalesLineID", "BatchID", "SaleTimestamp", "CustomerID", "ProductID", "Date",
               "Quantity", "UnitPriceMinorUnits", "SalesAmountMinorUnits")
    mappings = types.get("translator", {}).get("mappings")
    if (not isinstance(mappings, list)
            or [(item.get("source", {}).get("name"), item.get("sink", {}).get("name"))
                for item in mappings] != [(name, name) for name in columns]):
        raise ValueError("Copy must explicitly map every sales column")
    location["fileName"] = _expr(f"@{_PREPARED}.filename")
    activity.update(name="CopyEventFile", dependsOn=[],
                    policy={"timeout": activity_timeout, "retry": 0, "secureInput": True})
    for key in ("state", "onInactiveMarkAs"):
        activity.pop(key, None)

    finish = notebook("VerifyCopy", "finish", {
        "prepared_json": "@activity('PrepareCopy').output.result.exitValue",
        "copy_json": "@string(activity('CopyEventFile').output)",
    }, ("CopyEventFile",))
    condition = {
        "name": "CopyOnlyWhenAdmitted", "type": "IfCondition",
        "dependsOn": [{"activity": "PrepareCopy", "dependencyConditions": ["Succeeded"]}],
        "typeProperties": {
            "expression": _expr(f"@equals({_PREPARED}.decision, 'copy-required')"),
            "ifTrueActivities": [activity, finish],
            "ifFalseActivities": [],
        },
    }
    return {"properties": {
        "concurrency": 1,
        "parameters": {
            "sourceSubject": {"type": "String", "defaultValue": ""},
            "eventId": {"type": "String", "defaultValue": ""},
            "deliveryOrigin": {"type": "String", "defaultValue": "event"},
        },
        "activities": [prepare, condition],
    }}
