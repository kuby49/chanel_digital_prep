"""Read-only, fail-closed preparation for isolated runtime access evidence.

The existing access notebook reads two objects but does not prove a target write,
and current definition readback is not proof of the definition executed by a job.
No owner role, caller identity, or supplied receipt substitutes for runtime proof.
"""

from datetime import datetime, timedelta, timezone

import customer_preflight as original
from deploy import _guid
from native_rebind import _when


REQUIRED_OPERATION = (
    "Separately approve and execute one controlled pipeline notebook job on active F32, "
    "after a fresh capacity check immediately before any write or job. Bind it to the "
    "exact new staging and target IDs and the expected runtime principal. Grant that "
    "principal only the staging read and new-target read/write permissions required for "
    "dedicated non-admitting probe objects; write only to the new target. Independently "
    "establish the executed definition and parameters, actual storage-token principal, "
    "successful scoped read/write probes, and their linkage to the completed job using "
    "authoritative runtime evidence and fresh readback. Connection credential identity "
    "and full deployment readiness require separate proof."
)

MISSING_OPERATION = (
    "No evidenced authoritative operation links a job to its executed notebook definition "
    "and independently readable runtime principal plus new-target write/read results. "
    "The existing native_rebind probe only reads objects; its current definition and "
    "caller-supplied output cannot establish this missing runtime proof."
)


def assess(spec, api=None, *, pipeline_job_id=None, now=None):
    """Assess observable job linkage only; never authorize or claim runtime access.

    ``api`` is an injected Fabric client with get and request methods. No request is
    made without an explicitly identified pipeline job. This is not a job launcher.
    """
    fields = {
        "tenant_id", "staging_workspace_id", "staging_lakehouse_id",
        "target_workspace_id", "target_lakehouse_id", "pipeline_id", "notebook_id",
        "principal_id",
    }
    if not isinstance(spec, dict) or set(spec) != fields:
        raise ValueError("Access specification must contain exactly the required IDs.")
    ids = {key: _guid(value) for key, value in spec.items()}
    if ids["tenant_id"] != original.TENANT:
        raise ValueError("Access specification tenant differs from the approved tenant.")
    if (ids["staging_lakehouse_id"] in (original.STAGING_LAKEHOUSE, original.LAKEHOUSE)
            or ids["target_lakehouse_id"] in (original.STAGING_LAKEHOUSE, original.LAKEHOUSE)
            or ids["staging_lakehouse_id"] == ids["target_lakehouse_id"]):
        raise ValueError("Staging and target must be distinct new Lakehouses.")
    if ids["pipeline_id"] == original.PIPELINE or ids["notebook_id"] == original.NOTEBOOK:
        raise ValueError("Access job must use new pipeline and notebook items.")

    result = {
        "ready": False,
        "runtime_identity_verified": False,
        "staging_read_verified": False,
        "target_read_write_verified": False,
        "connection_credential_identity_verified": False,
        "controlled_job_verified": False,
        "job_linkage_verified": False,
        "job_observation": "not supplied",
        "missing_operation": MISSING_OPERATION,
        "required_operation": REQUIRED_OPERATION,
    }
    if pipeline_job_id is None:
        return result
    job_id = _guid(pipeline_job_id)
    if api is None:
        result["job_observation"] = "independent API client unavailable"
        return result

    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None or now.utcoffset() != timedelta(0):
        raise ValueError("Observation time must be timezone-aware UTC.")
    base = f"/v1/workspaces/{ids['target_workspace_id']}"
    path = f"{base}/items/{ids['pipeline_id']}/jobs/instances/{job_id}"
    job = api.get(path, skill="spark-cli")
    if not isinstance(job, dict) or job.get("id") != job_id:
        result["job_observation"] = "pipeline job missing or mismatched"
        return result
    if job.get("status") != "Completed":
        result["job_observation"] = "pipeline job not confirmed completed"
        return result
    start, end = _when(job.get("startTimeUtc")), _when(job.get("endTimeUtc"))
    if end < start or end > now or end < now - timedelta(minutes=15):
        result["job_observation"] = "pipeline job timing is invalid or stale"
        return result
    status, headers, page = api.request(
        "POST", f"{base}/datapipelines/pipelineruns/{job_id}/queryactivityruns",
        {"filters": [], "lastUpdatedAfter": (start - timedelta(minutes=5)).isoformat(),
         "lastUpdatedBefore": (end + timedelta(minutes=5)).isoformat()}, skill="spark-cli")
    if (status != 200 or not isinstance(page, dict) or not isinstance(page.get("value"), list)
            or page.get("continuationToken") or page.get("continuationUri")
            or any(key.lower() in ("x-ms-continuation-token", "continuationtoken", "link") and value
                   for key, value in headers.items()) or len(page["value"]) != 1):
        result["job_observation"] = "activity history missing, incomplete or ambiguous"
        return result
    activity = page["value"][0]
    if not isinstance(activity, dict):
        result["job_observation"] = "activity is malformed"
        return result
    inputs = activity.get("input")
    if (activity.get("pipelineRunId") != job_id
            or activity.get("activityType") != "TridentNotebook"
            or activity.get("status") != "Succeeded"
            or not isinstance(inputs, dict)
            or inputs.get("workspaceId") != ids["target_workspace_id"]
            or inputs.get("notebookId") != ids["notebook_id"]):
        result["job_observation"] = "activity linkage or notebook identity mismatched"
        return result
    try:
        notebook_job_id = _guid(activity.get("activityRunId"))
    except ValueError:
        result["job_observation"] = "notebook job identity missing or invalid"
        return result
    notebook = api.get(
        f"{base}/items/{ids['notebook_id']}/jobs/instances/{notebook_job_id}",
        skill="spark-cli")
    if (not isinstance(notebook, dict) or notebook.get("id") != notebook_job_id
            or notebook.get("jobType") != "PipelineRunNotebook"
            or notebook.get("status") != "Completed"):
        result["job_observation"] = "linked notebook job not confirmed completed"
        return result
    notebook_start = _when(notebook.get("startTimeUtc"))
    notebook_end = _when(notebook.get("endTimeUtc"))
    if (notebook_end < notebook_start or notebook_end > now
            or notebook_start < start - timedelta(minutes=1)
            or notebook_start > end + timedelta(minutes=1)):
        result["job_observation"] = "linked notebook job timing differs"
        return result
    if api.get(path, skill="spark-cli") != job:
        result["job_observation"] = "pipeline job changed during readback"
        return result
    result["job_observation"] = "fresh completed job and activity link observed; runtime proof absent"
    result["job_linkage_verified"] = True
    return result
