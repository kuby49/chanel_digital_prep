"""Build an offline, disabled candidate Activator definition.

The candidate requires a captured event and must not be deployed without the
runtime checks documented in README.md.
"""

import json
import re
import uuid


EVENT_TYPE = "Microsoft.Fabric.OneLake.FileCreated"
FOLDER = "/Files/incoming/sales/"
FILE = re.compile(r"sales_[A-Za-z0-9_-]+_[0-9]+\.csv", re.ASCII)
RUN_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}", re.ASCII)


def validate_event(event):
    if not isinstance(event, dict):
        raise ValueError("event must be a CloudEvents object")
    subject = event.get("subject")
    if event.get("type") != EVENT_TYPE:
        raise ValueError("expected a OneLake FileCreated event")
    if not isinstance(event.get("id"), str) or not event["id"]:
        raise ValueError("event id is missing")
    if not isinstance(subject, str) or not subject.startswith(FOLDER):
        raise ValueError("event subject is outside the sales landing folder")
    if not FILE.fullmatch(subject[len(FOLDER):]):
        raise ValueError("event subject is not an immutable sales CSV filename")
    return subject


def _id(value):
    return str(uuid.UUID(value))


def _string(name, value):
    return {"name": name, "type": "string", "value": value}


def _parameter(name, field):
    return {
        "kind": "FabricItemParameter",
        "type": "complex",
        "arguments": [
            _string("parameterName", name),
            _string("parameterType", "String"),
            {
                "name": "parameterValue",
                "type": "complexArray",
                "values": [{
                    "kind": "EventFieldReference",
                    "type": "complexReference",
                    "arguments": [_string("fieldName", field)],
                }],
            },
        ],
    }


def build_definition(tenant_id, workspace_id, lakehouse_id, pipeline_id, sample, *, run_id):
    """Return a disabled candidate; a representative event is mandatory."""
    validate_event(sample)
    if not isinstance(run_id, str) or not RUN_ID.fullmatch(run_id):
        raise ValueError("invalid producer run ID")
    tenant_id, workspace_id, lakehouse_id, pipeline_id = map(
        _id, (tenant_id, workspace_id, lakehouse_id, pipeline_id)
    )
    expected_source = (
        f"/tenants/{tenant_id}/workspaces/{workspace_id}/items/{lakehouse_id}"
    )
    if sample.get("source") != expected_source:
        raise ValueError("event source does not match the selected Lakehouse")
    container, source, event, rule, action, step1, step2, step3, step4 = (
        str(uuid.uuid4()) for _ in range(9)
    )
    parent = {"targetUniqueIdentifier": container}
    event_instance = {
        "templateId": "SourceEvent", "templateVersion": "1.2.4",
        "steps": [{
            "name": "SourceEventStep", "id": step1,
            "rows": [{
                "name": "SourceSelector", "kind": "SourceReference",
                "arguments": [_string("entityId", source)],
            }],
        }],
    }
    binding = {
        "name": "FabricItemBinding", "kind": "FabricItemInvocation",
        "arguments": [
            _string("workspaceId", workspace_id), _string("itemId", pipeline_id),
            _string("itemType", "Pipeline"), _string("jobType", "Pipeline"),
            _string("fabricJobConnectionDocumentId", action),
            {"name": "additionalInformation", "type": "array", "values": []},
            {"name": "parameters", "type": "array", "values": [
                _parameter("sourceSubject", "___subject"),
                _parameter("eventId", "___id"),
            ]},
        ],
    }
    rule_instance = {
        "templateId": "EventTrigger", "templateVersion": "1.2.4",
        "steps": [
            {"name": "FieldsDefaultsStep", "id": step2, "rows": [{
                "name": "EventSelector", "kind": "Event", "arguments": [{
                    "name": "event", "kind": "EventReference", "type": "complex",
                    "arguments": [_string("entityId", event)],
                }],
            }]},
            {"name": "EventDetectStep", "id": step3, "rows": [
                {"name": "OnEveryValue", "kind": "OnEveryValue", "arguments": []}
            ]},
            {"name": "ActStep", "id": step4, "rows": [binding]},
        ],
    }
    return [
        {"uniqueIdentifier": container, "type": "container-v1",
         "payload": {"name": "Sales file events", "type": "rthSubscriptions"}},
        {"uniqueIdentifier": source, "type": "realTimeHubSource-v1", "payload": {
            "name": "Sales Lakehouse file events", "parentContainer": parent,
            "connection": {"scope": "Artifact", "tenantId": tenant_id,
                           "workspaceId": workspace_id, "artifactId": lakehouse_id,
                           "eventGroupType": "Microsoft.Fabric.OneLakeEvents"},
            "filterSettings": {"eventTypes": [{"name": EVENT_TYPE}], "filters": [
                {"operatorType": "StringBeginsWith", "key": "subject",
                 "values": [f"{FOLDER}sales_{run_id}_"]},
                {"operatorType": "StringEndsWith", "key": "subject", "values": [".csv"]},
            ]},
        }},
        {"uniqueIdentifier": event, "type": "timeSeriesView-v1", "payload": {
            "name": "Sales file event", "parentContainer": parent,
            "definition": {"type": "Event", "instance": json.dumps(event_instance)},
        }},
        {"uniqueIdentifier": rule, "type": "timeSeriesView-v1", "payload": {
            "name": "Copy sales file", "description": "Created by: skills-for-fabric",
            "parentContainer": parent,
            "definition": {"type": "Rule", "instance": json.dumps(rule_instance),
                           "settings": {"shouldRun": False, "shouldApplyRuleOnUpdate": False}},
        }},
        {"uniqueIdentifier": action, "type": "fabricItemAction-v1", "payload": {
            "name": "Run sales copy", "parentContainer": parent,
            "fabricItem": {"itemId": pipeline_id, "workspaceId": workspace_id,
                           "itemType": "Pipeline"}, "jobType": "Pipeline",
        }},
    ]
