"""Publish one approved definition-only update to the existing owned measured monitor."""

import argparse
import json
import re
import sys
from datetime import timedelta
from pathlib import Path
from time import monotonic

import deploy
import native_demo2 as native
from notebook_builder import fabric_definition


JOURNAL = "demo-2-monitor-publication.json"
ARCHIVE = "demo-2-monitor-definition-before.json"
FINAL_PREFLIGHT_SECONDS = 120


def require(condition, message):
    if not condition:
        raise deploy.Blocked(message)


class ReadOnlyStore:
    def __init__(self, client, workspace, lakehouse):
        self.client = client
        self.root = f"{deploy.ONELAKE}/{native._id(workspace)}/{native._id(lakehouse)}"

    def read(self, path):
        status, headers, body = self.client.onelake("GET", f"{self.root}/{path}", allow_missing=True)
        if status == 404:
            raise FileNotFoundError(path)
        require(status == 200 and isinstance(body, dict), "Invalid OneLake evidence response.")
        return native.monitor._canonical_json(body), headers.get("etag")


def service_utc(value, label):
    require(isinstance(value, str), f"Invalid {label}.")
    if re.fullmatch(r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d+)?", value):
        value += "Z"  # Fabric can omit the zone on its explicit UTC fields.
    return native.monitor._utc(value, label)


def exact_terminal_job(client, workspace, item, job_id, *, job_type=None):
    ws, item, job_id = map(native._id, (workspace, item, job_id))
    status, _, job = client.fabric("GET", f"/v1/workspaces/{ws}/items/{item}/jobs/instances/{job_id}")
    require(status == 200 and isinstance(job, dict) and job.get("id") == job_id
            and job.get("itemId") == item and job.get("status") in native.monitor.TERMINAL_JOBS
            and (job_type is None or (job.get("jobType") == job_type and job["status"] != "Deduped")),
            "An exact old job is missing, mismatched or not terminal.")
    service_utc(job.get("endTimeUtc"), "old job terminal timestamp")
    return job


def verify_producer_activity(client, workspace, parent, notebook, producer_job):
    start = service_utc(parent.get("startTimeUtc"), "pipeline start time")
    end = service_utc(parent["endTimeUtc"], "pipeline end time")
    require(timedelta(0) <= end - start <= timedelta(days=1), "Invalid parent activity window.")
    margin = timedelta(minutes=3)
    status, headers, body = client.fabric(
        "POST", f"/v1/workspaces/{workspace}/datapipelines/pipelineruns/{parent['id']}/queryactivityruns",
        {"filters": [], "lastUpdatedAfter": deploy._stamp(start - margin),
         "lastUpdatedBefore": deploy._stamp(end + margin)},
    )
    paged = any(headers.get(key) for key in ("x-ms-continuation-token", "continuationtoken", "link"))
    if isinstance(body, dict):
        paged = paged or bool(body.get("continuationToken") or body.get("continuationUri"))
        body = body.get("value")
    require(status == 200 and not paged and isinstance(body, list) and len(body) == 1
            and isinstance(body[0], dict), "Parent activity does not yield one unambiguous producer activity.")
    activity = body[0]
    inputs = activity["input"]
    require(activity.get("pipelineRunId") == parent["id"] != producer_job
            and activity.get("activityName") == "RunProducer"
            and activity.get("activityType") == "TridentNotebook"
            and activity.get("retryAttempt") in (None, 0)
            and activity.get("activityRunId") == producer_job == activity["output"]["result"]["runId"]
            and inputs.get("workspaceId") == workspace and inputs.get("notebookId") == notebook,
            "Parent activity does not identify the exact producer notebook job.")


def terminal_evidence(deployment, approval, monitor_id):
    """Recheck the jobs named by the approved historical attribution and durable run evidence."""
    client = deployment.client
    workspace = native._id(approval["producer_workspace_id"])
    store = ReadOnlyStore(client, workspace, approval["producer_lakehouse_id"])
    run_id = approval["run_id"]
    require(isinstance(run_id, str) and native.monitor.RUN_ID.fullmatch(run_id),
            "Invalid approved old run ID.")
    base = f"Files/producer/runs/{run_id}"
    lifecycle, lifecycle_etag = native._read(store, f"{base}/native-lifecycle.json")
    require(lifecycle is not None, "Original native lifecycle is missing.")
    doc = lifecycle["document"]
    shared = doc["shared"]
    require(lifecycle.get("run_id") == run_id == doc["data"]["run_id"]
            and doc["tenant_id"] == deployment.s["tenant_id"]
            and native._digest(doc) == lifecycle.get("config_sha256") == approval["old_config_sha256"]
            and shared["workspace_id"] == workspace
            and shared["staging_lakehouse_id"] == approval["producer_lakehouse_id"]
            and shared["producer_notebook_id"] == approval["producer_notebook_id"]
            and doc["demo-2"]["monitor_notebook_id"] == monitor_id
            and doc["demo-2"]["monitor_definition_sha256"] == approval["old_definition_sha256"]
            and doc["demo-2"]["workspace_id"] == deployment.resources["workspace"]["id"]
            and doc["demo-2"]["lakehouse_id"] == deployment.resources["lakehouse"]["id"]
            and shared["target_bindings"]["demo-2"] == deploy._journal_target(deployment.s, deployment.ledger),
            "Original native lifecycle differs from the approved run or owned monitor.")

    parent = exact_terminal_job(client, workspace, approval["parent_pipeline_id"], approval["parent_job_id"],
                                job_type="Pipeline")
    verify_producer_activity(client, workspace, parent, shared["producer_notebook_id"], approval["producer_job_id"])
    producer = exact_terminal_job(client, workspace, shared["producer_notebook_id"], approval["producer_job_id"],
                                  job_type="PipelineRunNotebook")
    run = native._Run({
        "document": doc, "store": store, "store_factory": lambda ws, lh: ReadOnlyStore(client, ws, lh),
        "clock": deploy._now, "manifest_path": f"{base}/manifest.json",
        "cleanup_deadline_utc": lifecycle["expires_at_utc"], "expires_at_utc": lifecycle["expires_at_utc"],
        "verification_deadline_utc": lifecycle["verification_deadline_utc"],
    }, "cleanup")
    journal = run.load()
    require(journal is not None and set(journal["jobs"]) == set(run.items)
            and all(entry.get("job_id") for entry in journal["jobs"].values()),
            "Missing or uncertain original Demo 2 job submission.")
    jobs = journal["jobs"]
    ws = doc["demo-2"]["workspace_id"]
    monitor_job = exact_terminal_job(client, ws, monitor_id, jobs["monitor"]["job_id"])
    require(lifecycle["tracks"]["demo-2"]["prepare"]["monitor_job_id"] == monitor_job["id"],
            "Lifecycle does not identify the original Demo 2 monitor job.")
    state, state_etag = run.state_read()
    lock_etag = run.lock_read()
    require(state is not None, "Original monitor state is missing; child submissions are uncertain.")
    children = run.child_jobs(state)
    verifier = run.items["bootstrap"]
    bootstrap = exact_terminal_job(client, ws, verifier, jobs["bootstrap"]["job_id"])
    require(bootstrap["id"] not in children and monitor_job["id"] not in children
            and monitor_job["id"] != bootstrap["id"], "Original Demo 2 job IDs overlap.")
    verified = [exact_terminal_job(client, ws, verifier, job_id) for job_id in sorted(children)]
    require(run.state_read() == (state, state_etag) and run.lock_read() == lock_etag
            and native._read(store, run.journal) == (journal, run.etag)
            and native._read(store, f"{base}/native-lifecycle.json") == (lifecycle, lifecycle_etag),
            "Original run evidence changed during terminality checks.")
    return {"terminal_jobs": [{key: job[key] for key in ("id", "itemId", "status", "endTimeUtc")}
                              for job in (parent, producer, monitor_job, bootstrap, *verified)],
            "child_job_ids": sorted(children),
            "lifecycle_etag": lifecycle_etag, "job_journal_etag": run.etag,
            "state_etag": state_etag, "lock_etag": lock_etag}


def read_definition(deployment, item_path):
    status, headers, definition = deployment.client.fabric("POST", item_path + "/getDefinition?format=ipynb", {})
    if status == 202:
        definition = deployment._poll_operation(deploy._operation_id(headers), headers.get("retry-after"))
    else:
        require(status == 200, "Monitor definition readback was not confirmed.")
    return definition


def publish(deployment, approval, *, resume_only):
    """The only cloud mutation is one updateDefinition POST after durable intent."""
    key = deploy._notebook_key(deployment.s, monitor=True)
    require(all(deployment.resources.get(role, {}).get("confirmed") is True
                for role in ("workspace", "eventhouse", "database", "table", "lakehouse", key)),
            "Publication requires existing confirmed ownership records.")
    deployment.verify_target_ownership()
    target = deployment.reused_synthetic_target() if deployment.s["reuse_synthetic_target"] else None
    lakehouse = deployment.item("lakehouse", "Lakehouse", deployment.s["lakehouse_name"], False)
    name = deployment.s[f"{key}_name"]
    with deploy._spark_client(deployment.client):
        item = deployment.item(key, "Notebook", name, False)
    monitor_id = item["id"]
    ws = deployment.resources["workspace"]["id"]
    require(monitor_id == approval["monitor_notebook_id"], "Approval does not identify the owned monitor.")
    path = f"/v1/workspaces/{ws}/items/{monitor_id}"
    desired = fabric_definition(deploy._load_monitor_builder().build_monitor_notebook(
        workspace_id=ws, lakehouse_id=lakehouse["id"], lakehouse_name=deployment.s["lakehouse_name"],
        **({"reused_synthetic_target": target} if target is not None else {}),
    ))
    new_sha = native.definition_sha256(desired)
    require(new_sha == approval["new_definition_sha256"] != approval["old_definition_sha256"],
            f"Trusted replacement definition SHA-256 is {new_sha}; approval does not match.")

    journal_path = deployment.ledger.directory / JOURNAL
    archive_path = deployment.ledger.directory / ARCHIVE
    with deploy._spark_client(deployment.client):
        live = read_definition(deployment, path)
        live_sha = native.definition_sha256(live)
        expected_metadata = native._definition_projection(desired)[:3]
        require(native._definition_projection(live)[:3] == expected_metadata,
                "Existing monitor Lakehouse binding or execution metadata differs.")
        if journal_path.exists():
            record = json.loads(journal_path.read_text(encoding="utf-8"))
            require(record.get("approval") == approval and record.get("phase") in
                    {"pending", "accepted", "acknowledged", "verified"},
                    "Publication journal does not match the exact approved operation.")
            archive = json.loads(archive_path.read_text(encoding="utf-8"))
            require(native._digest(archive) == record["archive_sha256"]
                    and archive["approval"] == approval
                    and native.definition_sha256(archive["definition"]) == approval["old_definition_sha256"]
                    and native._definition_projection(archive["definition"])[:3] == expected_metadata,
                    "Immutable old definition archive differs from the approval.")
        else:
            require(not resume_only, "No publication intent exists to reconcile.")
            require(live_sha == approval["old_definition_sha256"],
                    f"Current monitor baseline SHA-256 differs ({live_sha}); no update attempted.")
            evidence = terminal_evidence(deployment, approval, monitor_id)
            archive = {"approval": approval, "definition": live, "terminal_evidence": evidence}
            if archive_path.exists():
                saved = json.loads(archive_path.read_text(encoding="utf-8"))
                require(saved == archive,
                        "Existing immutable definition archive differs; no update attempted.")
            else:
                deploy._atomic_create_json(archive_path, archive)
            started = monotonic()
            require(terminal_evidence(deployment, approval, monitor_id) == evidence,
                    "Original run evidence changed after archival; no update attempted.")
            deployment.item(key, "Notebook", name, False)
            require(native.definition_sha256(read_definition(deployment, path)) == live_sha,
                    "Monitor definition changed after archival; no update attempted.")
            require(monotonic() - started < FINAL_PREFLIGHT_SECONDS,
                    "Final terminality and definition checks exceeded the two-minute freshness window.")
            record = {"approval": approval, "phase": "pending", "archive_sha256": native._digest(archive)}
            deploy._atomic_create_json(journal_path, record)
            require(monotonic() - started < FINAL_PREFLIGHT_SECONDS,
                    "Publication intent exists but the final checks expired; do not repeat or reset the intent.")
            try:
                status, headers, _ = deployment.client.fabric(
                    "POST", path + "/updateDefinition?updateMetadata=false", {"definition": desired},
                )
            except (deploy.Failure, deploy.Blocked) as exc:
                raise deploy.Blocked("Definition update unconfirmed; reconcile without repeating the POST.") from exc
            if status == 202:
                try:
                    operation = deploy._operation_id(headers)
                except deploy.Failure as exc:
                    raise deploy.Blocked("Definition operation ID unconfirmed; reconcile without repeating the POST.") from exc
                record.update(phase="accepted", operation_id=operation,
                              retry_after=headers.get("retry-after"), observed_at_utc=deploy._stamp(deploy._now()))
            else:
                require(status == 200, "Definition update unconfirmed; reconcile without repeating the POST.")
                record["phase"] = "acknowledged"
            deploy._atomic_json(journal_path, record)
        if record["phase"] == "accepted":
            # Reuse the full service delay on resume, never shorten a response-relative delay.
            deployment._poll_operation(record["operation_id"], record["retry_after"], fetch_result=False)
        deployment.item(key, "Notebook", name, False)
        final = read_definition(deployment, path)
        require(native.definition_sha256(final) == new_sha,
                "New definition is not confirmed; preserve the intent and reconcile without repeating the POST.")
        if record["phase"] != "verified":
            record["verification_source"] = {"pending": "readback_only", "accepted": "lro_and_readback",
                                             "acknowledged": "response_and_readback"}[record["phase"]]
        record["phase"] = "verified"
        deploy._atomic_json(journal_path, record)
    return {"monitor_notebook_id": monitor_id, "monitor_definition_sha256": new_sha}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--prefix", required=True)
    parser.add_argument("--state-dir", type=Path, required=True)
    parser.add_argument("--approval", type=Path, required=True)
    action = parser.add_mutually_exclusive_group(required=True)
    action.add_argument("--publish", action="store_true")
    action.add_argument("--reconcile", action="store_true")
    args = parser.parse_args(argv)
    try:
        require(all(p.is_absolute() for p in (args.config, args.approval, args.state_dir)), "Use absolute paths.")
        approval = json.loads(args.approval.read_text(encoding="utf-8"))
        require(isinstance(approval, dict) and approval.get("exclusive_window_confirmed") is True
                and isinstance(approval.get("producer_run_evidence"), str)
                and approval["producer_run_evidence"].strip(),
                "Approval must confirm the exclusive publication window and cite exact producer-job-to-run evidence.")
        for field in ("old_config_sha256", "old_definition_sha256", "new_definition_sha256"):
            require(re.fullmatch(r"[a-f0-9]{64}", approval[field]), f"Invalid {field}.")
        for field in ("parent_pipeline_id", "parent_job_id", "producer_workspace_id", "producer_lakehouse_id",
                      "producer_notebook_id", "producer_job_id", "monitor_notebook_id"):
            native._id(approval[field])
        settings = deploy.validate_config(json.loads(args.config.read_text(encoding="utf-8")), args.prefix)
        # Native binding reconstruction must use the shared generator from the integrated checkout.
        sys.path.insert(0, str(deploy.ROOT / "demo-data-generator"))
        deadline = deploy.Deadline(settings["track_timeout"], label="monitor publication")
        with deploy.Ledger(args.state_dir, settings) as ledger:
            client = deploy.FabricClient(settings["tenant_id"], deadline)
            result = publish(deploy.Deployment(settings, client, deadline, ledger), approval,
                             resume_only=args.reconcile)
    except (deploy.Blocked, deploy.Failure, native.monitor.MonitorError, KeyError, TypeError, ValueError,
            OSError, ImportError) as exc:
        print(f"Publication not confirmed ({type(exc).__name__}): {exc}", file=sys.stderr)
        return 1
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
