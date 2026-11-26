"""Reset and run the three Fabric demonstrations."""

import time
from datetime import datetime, timedelta, timezone
from email.utils import formatdate
from urllib.parse import urlencode, urlsplit
from urllib.request import Request, urlopen
from uuid import UUID

import requests

from simple_data import sales_csv


def stop_previous_producer(context, pipeline_run_id):
    """Stop an interrupted producer before the new run resets shared data."""
    shared = context["config"]["shared"]
    workspace = str(UUID(shared["workspace_id"]))
    notebook = str(UUID(shared["producer_notebook_id"]))
    pipeline_run_id = str(UUID(pipeline_run_id))
    api = "https://api.fabric.microsoft.com"
    base = f"{api}/v1/workspaces/{workspace}"
    jobs_url = f"{base}/items/{notebook}/jobs/instances"
    headers = {
        "Authorization": "Bearer " + context["get_token"](api),
        "x-ms-fabric-skill": "spark-cli",
    }

    def request(method, url, **kwargs):
        parsed = urlsplit(url)
        if parsed.scheme != "https" or parsed.netloc != "api.fabric.microsoft.com":
            raise RuntimeError("Unexpected Fabric job history URL")
        response = requests.request(method, url, headers=headers, timeout=60, **kwargs)
        response.raise_for_status()
        return response.json() if response.content else None

    def jobs():
        url = jobs_url
        seen_tokens = set()
        while url:
            page = request("GET", url)
            yield from page["value"]
            token = page.get("continuationToken")
            if token:
                if token in seen_tokens:
                    raise RuntimeError("Notebook job history repeated its continuation token")
                seen_tokens.add(token)
                url = jobs_url + "?" + urlencode({"continuationToken": token})
            elif page.get("continuationUri"):
                raise RuntimeError("Notebook job history omitted its continuation token")
            else:
                url = None

    deadline = time.monotonic() + 600
    now = datetime.now(timezone.utc)
    window = {
        "filters": [],
        "lastUpdatedAfter": (now - timedelta(days=1)).isoformat(),
        "lastUpdatedBefore": (now + timedelta(days=1)).isoformat(),
    }
    activity_url = f"{base}/datapipelines/pipelineruns/{pipeline_run_id}/queryactivityruns"
    while True:
        activity = request("POST", activity_url, json=window)
        current = [item["activityRunId"] for item in activity["value"]
                   if item["activityName"] == "RunProducer" and item["activityType"] == "TridentNotebook"]
        rows = list(jobs())
        if len(current) == 1 and any(row["id"] == current[0] and row.get("startTimeUtc") for row in rows):
            break
        if time.monotonic() >= deadline:
            raise TimeoutError("Could not identify the current producer activity")
        time.sleep(5)

    terminal = {"Completed", "Failed", "Cancelled", "Canceled", "Deduped"}
    cancelled = set()
    while True:
        def started(row):
            stamp = row.get("startTimeUtc")
            if not stamp:
                raise RuntimeError("Cannot determine the order of active producer jobs")
            value = datetime.fromisoformat(stamp.replace("Z", "+00:00"))
            return value if value.tzinfo else value.replace(tzinfo=timezone.utc)

        current_start = started(next(row for row in rows if row["id"] == current[0]))
        previous = []
        for row in rows:
            if (row["id"] == current[0] or row["jobType"] != "PipelineRunNotebook"
                    or row["status"] in terminal):
                continue
            other_start = started(row)
            if other_start == current_start:
                raise RuntimeError("Cannot determine the order of active producer jobs")
            if other_start < current_start:
                previous.append(row)
        if not previous:
            return
        for row in previous:
            job_id = str(UUID(row["id"]))
            if job_id not in cancelled:
                request("POST", f"{jobs_url}/{job_id}/cancel")
                cancelled.add(job_id)
        if time.monotonic() >= deadline:
            raise TimeoutError("The interrupted producer did not stop")
        time.sleep(5)
        rows = list(jobs())


def upload(context, demo, filename, content):
    settings = context["config"][demo]
    path = settings["landing_path"].strip("/")
    url = (f"https://onelake.blob.fabric.microsoft.com/{settings['workspace_id']}/"
           f"{settings['lakehouse_id']}/{path}/{filename}")
    request = Request(url, data=content, method="PUT", headers={
        "Authorization": "Bearer " + context["get_token"]("https://storage.azure.com/"),
        "x-ms-version": "2021-08-06",
        "x-ms-date": formatdate(usegmt=True),
        "x-ms-blob-type": "BlockBlob",
        "Content-Type": "text/csv",
    })
    with urlopen(request, timeout=60) as response:
        if response.status != 201:
            raise RuntimeError(f"OneLake upload failed with HTTP {response.status}")


def _stop_all(context, modules, failed):
    errors = []
    for name, module in modules:
        try:
            module.stop(context, failed=failed)
        except Exception as error:
            errors.append(f"{name}: {type(error).__name__}: {error}")
    if errors:
        raise RuntimeError("Unable to stop demo activity: " + "; ".join(errors))


def refresh_sql_metadata(context, demo, tables):
    settings = context["config"][demo]
    workspace = str(UUID(settings["workspace_id"]))
    lakehouse = str(UUID(settings["lakehouse_id"]))
    base = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace}"
    headers = {
        "Authorization": "Bearer " + context["get_token"]("https://api.fabric.microsoft.com"),
        "x-ms-fabric-skill": "sqldw-cli",
    }

    def request(method, url, **kwargs):
        response = requests.request(method, url, headers=headers, timeout=180, **kwargs)
        response.raise_for_status()
        return response

    lakehouse_info = request("GET", f"{base}/lakehouses/{lakehouse}").json()
    endpoint = str(UUID(lakehouse_info["properties"]["sqlEndpointProperties"]["id"]))
    result = request("POST", f"{base}/sqlEndpoints/{endpoint}/refreshMetadata", json={
        "recreateTables": False,
        "tables": [{"schema": schema, "tableNames": [name]} for schema, name in tables],
        "timeout": {"timeUnit": "Minutes", "value": 2},
    })
    if result.status_code == 202:
        operation = str(UUID(result.headers["x-ms-operation-id"]))
        operation_url = f"https://api.fabric.microsoft.com/v1/operations/{operation}"
        deadline = time.monotonic() + 180
        while True:
            status = request("GET", operation_url).json()
            if status["status"] == "Succeeded":
                result = request("GET", operation_url + "/result")
                break
            if status["status"] not in {"NotStarted", "Running"}:
                raise RuntimeError(f"{demo} SQL metadata refresh failed: {status}")
            if time.monotonic() >= deadline:
                raise TimeoutError(f"{demo} SQL metadata refresh did not finish")
            time.sleep(5)
    if result.status_code != 200:
        raise RuntimeError(f"{demo} SQL metadata refresh returned HTTP {result.status_code}")
    statuses = {item["tableName"].lower(): item["status"] for item in result.json()["value"]}
    for schema, name in tables:
        qualified = f"{schema}.{name}".lower()
        if statuses.get(qualified) != "Success":
            raise RuntimeError(f"{demo} SQL metadata refresh failed for {qualified}: {statuses.get(qualified)}")


def run(context, modules, *, send=upload, sleep=time.sleep):
    data = context["config"]["data"]
    try:
        _stop_all(context, modules, True)
        for _, module in modules:
            module.reset(context)
        for _, module in modules:
            module.start(context)
        for number in range(1, data["file_count"] + 1):
            if number > 1:
                sleep(data["interval_seconds"])
            content = sales_csv(data, number)
            filename = f"sales_{number:04d}.csv"
            send(context, "demo-1", filename, content)
            send(context, "demo-3", filename, content)
            dict(modules)["demo-2"].submit_batch(context, content)
            print(f"Published {filename} to the three demos.", flush=True)
        _stop_all(context, modules, False)
        demo1 = context["config"]["demo-1"]
        refresh_sql_metadata(context, "demo-1", [
            (demo1["bronze_schema"], demo1["bronze_view"]),
            (demo1["silver_schema"], demo1["silver_view"]),
        ])
        refresh_sql_metadata(context, "demo-3", [("dbo", "factsales")])
    except Exception as error:
        try:
            _stop_all(context, modules, True)
        except Exception as stop_error:
            raise RuntimeError(f"Demo failed: {error}; cleanup failed: {stop_error}") from error
        raise
