"""Read-only evidence for a replacement; never authorizes publication."""

from datetime import datetime, timedelta, timezone

from deploy import CandidateBlocked, _guid


_WORKSPACE = "68971aa2-f4bc-48a2-8962-98a4cb1e9fc2"
_PIPELINE = "2f42bac0-ccc3-4efc-8ff3-787fc93f3827"
_NOTEBOOK = "7d7f2229-4caf-409b-bee0-296d061e14ec"
_PARENT_JOB = "30b42ef3-a8e9-47eb-83a9-f3782aa5e943"
_NOTEBOOK_JOB = "caea88ad-08d0-4dac-b71f-eec1f3389463"
_TERMINAL = {"Completed", "Failed", "Cancelled", "Canceled", "Deduped"}


def _time(value):
    if not isinstance(value, str):
        raise CandidateBlocked("Job timestamp is missing.")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise CandidateBlocked("Job timestamp is invalid.") from exc
    return parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed.astimezone(timezone.utc)


def _job(api, base, item, kind, expected):
    jobs = list(api.list(f"{base}/items/{item}/jobs/instances?jobType={kind}"))
    rows = [job for job in jobs if job.get("id") == expected]
    if (len(rows) != 1 or rows[0].get("itemId") != item
            or rows[0].get("status") not in _TERMINAL):
        raise CandidateBlocked("Original job is missing, ambiguous or not terminal.")
    start, end = _time(rows[0].get("startTimeUtc")), _time(rows[0].get("endTimeUtc"))
    if end < start:
        raise CandidateBlocked("Original job timestamps are inconsistent.")
    return rows[0], start


def original_parent(api, *, now=None):
    """Verify both exact jobs and their activity link; this is not a writer fence."""
    now = now or datetime.now(timezone.utc)
    if now.utcoffset() != timedelta(0):
        raise ValueError("observation time must be UTC")
    base = f"/v1/workspaces/{_WORKSPACE}"
    pipeline, start = _job(api, base, _PIPELINE, "Pipeline", _PARENT_JOB)
    notebook, _ = _job(api, base, _NOTEBOOK, "RunNotebook", _NOTEBOOK_JOB)
    status, headers, body = api.request(
        "POST", f"{base}/datapipelines/pipelineruns/{_PARENT_JOB}/queryactivityruns",
        {"filters": [], "lastUpdatedAfter": (start - timedelta(minutes=3)).isoformat(),
         "lastUpdatedBefore": (now + timedelta(minutes=3)).isoformat()}, skill="spark-cli")
    if (status != 200 or not isinstance(body, dict) or not isinstance(body.get("value"), list)
            or body.get("continuationToken") or body.get("continuationUri")
            or any(key.lower() in ("x-ms-continuation-token", "continuationtoken", "link") and value
                   for key, value in headers.items())):
        raise CandidateBlocked("Original parent activity history is incomplete.")
    activities = body["value"]
    if len(activities) != 1:
        raise CandidateBlocked("Original parent activity linkage is missing or ambiguous.")
    activity = activities[0]
    if not isinstance(activity, dict):
        raise CandidateBlocked("Original parent activity is malformed.")
    inputs = activity.get("input")
    if (not isinstance(inputs, dict)
            or activity.get("pipelineRunId") != _PARENT_JOB or activity.get("activityName") != "RunProducer"
            or activity.get("activityType") != "TridentNotebook"
            or activity.get("activityRunId") != _NOTEBOOK_JOB
            or inputs.get("workspaceId") != _WORKSPACE or inputs.get("notebookId") != _NOTEBOOK):
        raise CandidateBlocked("Original parent and notebook job linkage differs.")
    return {"observed_at_utc": now.isoformat(), "pipeline_status": pipeline["status"],
            "notebook_status": notebook["status"], "writer_drain_verified": False, "ready": False}


def connection(api, connection_id):
    """Check generic connection metadata and caller access, not runtime target access."""
    connection_id = _guid(connection_id)
    result = api.get("/v1/connections/" + connection_id)
    details = result.get("connectionDetails", {})
    credential = result.get("credentialDetails", {})
    if (not isinstance(details, dict) or not isinstance(credential, dict)
            or result.get("id") != connection_id or result.get("connectivityType") != "ShareableCloud"
            or details.get("type") != "Lakehouse" or details.get("path") != "Lakehouse"
            or credential.get("credentialType") != "OAuth2"):
        raise CandidateBlocked("Lakehouse connection metadata differs from the expected generic OAuth2 connection.")
    roles = list(api.list("/v1/connections/" + connection_id + "/roleAssignments"))
    own_roles = [row for row in roles if row.get("principal", {}).get("id") == api.principal_id]
    if len(own_roles) != 1 or own_roles[0].get("role") not in ("Owner", "User", "UserWithReshare"):
        raise CandidateBlocked("Current caller cannot uniquely establish access to the connection.")
    return {"connection_id": connection_id, "caller_role": own_roles[0]["role"],
            "runtime_identity_verified": False, "target_access_verified": False, "ready": False}
