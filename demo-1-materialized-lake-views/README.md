# Demo 1: file-backed materialized lake views

**Status: one-file admission verified; recurring admission is locally tested.**
The read-only `validate-only` path checks the original owned journal, target,
access, dimensions, existing outputs and native refresh state. The recurring
10-file path additionally requires an owned bounded schedule and has not been
verified on the fresh cloud target. Neither admission establishes new delivery
or measured ten-file acceptance. Live validation created the isolated
`chanel-demo1-mlv` workspace on the MCAP F8 capacity, a schema-enabled
`ChanelDemo1Lakehouse`, Bronze/Silver schemas, and static dimensions. Spark
readback confirmed 365 unique non-null date keys, 20 product keys, and 100
customer keys. A separate header-only bootstrap CSV enabled creation of the
Bronze and Silver MLVs; both initially read back zero rows. A later synthetic
refresh produced three Bronze rows and two Silver facts totaling 3,000 minor
units from two batches and a renamed identical replay. Full measured-run
verification remains unimplemented.

## Architecture and prerequisites

The intended path is: isolated producer identity -> completed CSV files in
`Files/incoming/sales/` -> file-backed bronze MLV -> joined silver MLV ->
Lakehouse queries. Three independently loaded static Delta dimensions feed the
silver MLV. The target is a schema-enabled Lakehouse on an approved active
capacity. Cross-workspace access, runtime and MLV feature eligibility must be
verified before deployment, and permissions require approval.

The shared manifest uses top-level `contract_version: 1` and contains
`run_id`, `config`, `destinations`, and `batches`. Demo 1 checks manifest
version and batch classification in its focused validation helper:
`measured_batches` includes only `batch_class: measured`; `bootstrap` and
`negative-test` batches are excluded from coordinated-run row and total
acceptance checks. Each batch carries `batch_class` (`measured`, `bootstrap`,
or `negative-test`) in addition to the producer's batch ID, checksum, row
count, exact minor-unit totals, timestamps, and destination delivery records.
`batches` is an object keyed by decimal batch number. Grouped totals are carried
as `amount_by_date`, `amount_by_product_category`, and
`amount_by_customer_region`; each must independently sum to the batch's
`sales_amount_minor_units`. The checked-in test fixture was generated using producer commit
`b6e0b6a77c1746ffb4b8081fb3e1b07760fe3ab8`. The shared integration snapshot
`83eb097dd9b265326232a59de516ac1a4e962f39` was also inspected for the config
contract and USD currency setting. USD is specified by the shared
configuration; the producer manifest contains exact minor-unit amounts but
does not repeat the currency field.

The shared version 1 CSV header:
`SalesLineID,BatchID,SaleTimestamp,CustomerID,ProductID,Date,Quantity,UnitPriceMinorUnits,SalesAmountMinorUnits`.
The source publishes UTF-8 header CSV, UTC timestamps and ISO dates; each
immutable file is staged and closed before delivery. No bootstrap file is part
of the measured run.

## Header-only bootstrap and definitions

[Microsoft's file-ingestion SQL reference](https://learn.microsoft.com/en-us/fabric/data-engineering/materialized-lake-views/create-materialized-lake-view)
documents `USING OneLake_Files`, CSV options, `schema_mode=FIXED`,
`refresh_mode=APPEND_ONLY`, and a FROM-less projection including `__filepath__`.
It does not document a way to declare the source column schema explicitly for
an empty folder. `FIXED` pins a schema at creation; it does not declare one. Live validation
confirmed that `Files/incoming/sales/` contained zero source files, then
attempted the required `USING OneLake_Files` definition with an explicit
projection, `inferSchema=false`, and `schema_mode=FIXED`. Fabric returned
`MLV_INGEST_SCHEMA_INFERENCE_FAILED` with nested
`UNABLE_TO_INFER_SCHEMA` (“Unable to infer schema for CSV. It must be specified
manually”). The documented syntax has no source-schema declaration option.

The user authorized a separate header-only bootstrap CSV. Before creating the
Bronze view, write an immutable UTF-8 `bootstrap_header_only_v1.csv` in the
landing folder, containing the exact v1 header above followed by a newline
and no data rows. `definition_plan.bootstrap_csv_bytes()` returns those exact
bytes. Keep it separate from measured producer batches; it contributes zero
facts and must not enter measured batch counts, totals, or latency. In the
isolated test, the 111-byte file's SHA-256 was
`3b8ae33e220d6bf1cc3e5fc025d1fdf28df41d731495a022a9e013b38d5e1549`.
With the file present, the Bronze and Silver definitions were created and
read back with zero rows. Both an on-demand full-lineage refresh and a
scheduled full-lineage refresh completed; after each, both views still
contained zero rows and retained their schemas. Bronze columns were strings,
including corrupt-row and source-path fields; Silver applied the candidate
casts to integer, timestamp, date and decimal fields. The one-row fallback
was not needed.
Leave the bootstrap in place: deleting it and refreshing from a truly empty
folder has not been tested.

`silver-candidate.sql` proposes a fully qualified SQL MLV using casts,
exact monetary values, left joins and failure constraints. The replay-aware
definition passed a synthetic refresh on the fresh Lakehouse; it has not
passed the measured producer run. It groups by `(BatchID, SalesLineID)`, collapses
identical copies from different files and exposes quality flags so the
constraint fails on conflicting raw payloads, duplicate keys within a file,
or missing source paths. Bronze retains the original files and rows. Silver
`source_file` is the lexicographically smallest observed path, not necessarily
the first arrival; derive latency from delivery evidence, not this field.
Temporary Spark-view tests verified query results for a renamed replay, a
separate batch reusing an ID, conflicting payloads and same-file duplicates;
they did not exercise MLV creation or refresh-time constraint enforcement.
`COUNT(DISTINCT ...)` prevents incremental refresh under the documented
supported-construct rules, so expect a full Silver refresh and verify actual
mode and timing. Native Lakehouse scheduling remains the refresh mechanism.
Before measured use, verify that
malformed CSV rows cannot be silently discarded, all constraints evaluate as
intended (including nulls), source schema and timestamp timezone are correct,
and dimensions have unique/non-null keys and non-null descriptive fields.
Validate counts and keys independently: a left join with duplicated dimension
keys can still multiply facts. A failing refresh is not a recorded rejection;
retain diagnostic evidence and reconcile bronze and silver once corrected.
Do not use `IF NOT EXISTS` as evidence that an existing definition matches:
read it back and compare the expected definition and ownership.

## Managed refresh and validation

The [public MLV schedule API](https://learn.microsoft.com/en-us/rest/api/fabric/lakehouse/background-jobs/create-refresh-materialized-lake-views-schedule)
documents schedule create/read/list/update/delete and job history for
`RefreshMaterializedLakeViews`. The documented API supports users, service
principals, and managed identities with Contributor-or-higher role, delegated
execute plus read/write scopes, and a minimum effective five-minute cadence.
The lineage permits one active schedule. The local
`definition_plan.build_refresh_schedule_request` builds its bounded Cron
payload and rejects an end time earlier than last delivery + one full interval
+ processing margin. The isolated Lakehouse initially had no schedule; a
bounded five-minute schedule was created, read back, and produced one
successful scheduled run. It was disabled and the disabled state read back.
The history reported lineage-level success, not per-view refresh mode.
Inspect existing schedules before creating one and read back actual cadence,
lineage scope, and bounds.
Check refresh history and actual mode; append-only input alone does not
establish incremental execution. A refresh overlapping a preceding run may be
skipped. Pause the demo-owned schedule after the bounded run. Event-triggered
refresh is preview and is not the baseline.

For user-controlled Spark verification, use the
[Fabric Livy high-concurrency API](https://learn.microsoft.com/en-us/fabric/data-engineering/high-concurrency-livy).
Acquire under `livyApi/versions/2023-12-01/highConcurrencySessions` in the
Demo 1 Lakehouse, with a run-scoped `sessionTag` and identical Spark settings
for related requests. Poll each HC ID until `Idle` with both `sessionId` and
`replId`; submit statements under `highConcurrencySessions/{sessionId}/repls/{replId}/statements`
and release only the acquired HC IDs. A tag is a packing hint, not a lock.
On the isolated MCAP Lakehouse, two HC IDs with a shared tag returned distinct
REPL IDs on one Livy session. A separate HC read-only Spark statement
identified `silver.sales_joined` as `MATERIALIZED_LAKE_VIEW` with a `bigint`
`SalesLineID`. No data or MLV definitions changed.

This verifies the Livy path only. Demo 1 does not launch a notebook or Spark
session from `deploy.py`, and managed MLV refresh stays on the Lakehouse
scheduler. Its documented schedule and execution definition do not expose
the Livy session-tag or REPL acquisition settings; high concurrency for
managed refresh was not verified. Do not substitute Livy for scheduled refresh.

With the required features and approvals in place, verify: dimension sizes,
unique/non-null dimension keys, file checksums and delivered batch IDs; bronze,
rejected and silver count reconciliation; no orphan dimension keys;
integer minor-unit totals and grouped aggregates by date, product category and
customer region against the source manifest; every batch within the effective
configured verification timeout (900 seconds unless explicitly overridden);
one separately identified malformed batch produces a visible failure or
recorded rejection; and same-batch replay/repeated refresh leaves counts and
totals unchanged. `SalesLineID` may repeat across batches and runs; it is not
a global identity or deduplication key. Within each batch it must be unique;
the producer emits `1..row_count` and reuses the same IDs on replay. The
fact/replay key is `(BatchID, SalesLineID)`. The manifest validator rejects
duplicate batch IDs within a manifest to avoid double accounting. The Silver
candidate handles renamed identical copies in its query, but actual MLV
refresh, constraint failure, replay and source reconciliation remain
unverified. Runtime verification must enforce each batch's `1..row_count`
range and detect incomplete or conflicting repeated payloads.
Record first and subsequent batch arrival-to-silver min/median/max separately,
alongside refresh history and capacity contention. Repeat deployment to verify
idempotency without destructive replacement.

## Reusable native Run with a preconfigured schedule

`native_demo1.py` supplies standalone `prepare`, `check_ready`, `verify` and
`stop` hooks for the native producer orchestrator. It imports only the Python
standard library. **Neither the notebook nor its pipeline creates, updates,
disables or cancels an MLV schedule or refresh job.** Use the [native MLV
schedule API](https://learn.microsoft.com/en-us/fabric/data-engineering/materialized-lake-views/materialized-lake-views-public-api)
through the external controller, once per approved presenter window.

One-time setup, after approval, uses the existing controller and the immutable
setup config with the original local ownership journal. Choose an explicitly
bounded operating window (19-1,440 minutes); the native path can reuse that same
schedule for different fresh run IDs while the window has sufficient coverage.
After `start` confirms its receipt, the following **local-only** command exports
the sanitized schedule receipt without acquiring tokens or calling Fabric:

```powershell
python .\demo-1-materialized-lake-views\managed_refresh.py export-native `
  --config C:\demo\setup-common.json > C:\demo\native-schedule-export.json
```

The one-time native template builder must project the export's `native_schedule`
object into `document["demo-1"]["native_schedule"]`. This is the only additional
Demo 1 profile field; there is no per-click CLI or hand-built schedule request.
The receipt contains `contract_version`, `track`, `status`, `setup_run_id`,
`tenant_id`, `principal_id`, `workspace_id`, `lakehouse_id`, `schedule_id`,
`request` and `create_response`. Its exact ID, owner/type, five-minute UTC
configuration, approved start/end and full-lineage scope are pinned and checked
against live GET/list readbacks. Do not copy the original resource ownership
journal into Fabric. Protect the exported receipt and template: they are
operator-provided evidence, not signed service creator attestation. Exporting
alone does not establish current readiness.

The runtime context supplies the resolved `document`, checked-identity,
telemetry-bearing `fabric.get/list_all` client, active `spark`, conditional
OneLake `store.read/create/replace`, authoritative `read_manifest()`, aware UTC
`clock()`, `sleep(seconds)`, `stop_requested()`, and UTC ISO-string
`preparation_deadline_utc`, `verification_deadline_utc`,
`cleanup_deadline_utc` and `expires_at_utc`. The producer must check its runtime
identity, target/resource/name bindings and permissions and hold its global
native writer lock throughout all hooks and delivery. Demo 1 preparation runs
last, followed by `check_ready` directly before the source clock starts.

Admission checks the original 365/20/100 dimension keys and labels, matching
tenant/principal/target/landing, no pre-existing facts for this fresh run's
BatchIDs, one matching enabled schedule, no enabled/unknown competitors, and
successful idle lineage history. **No active job is assumed to belong to this
run or exempted because it is scheduled.** An active or unknown lineage job
is allowed to finish without cancellation; admission waits only within the
fixed preparation deadline and required schedule/verification coverage.
It rechecks the empty manifest, absent run BatchIDs, schedule and idle lineage
after waiting. If those checks or the bounds fail, the click is blocked.
`prepare` conditionally creates and reads back
`Files/producer/runs/<run_id>/demo1/schedule.json`. That per-run record pins the
receipt, data, target and all fixed deadlines. `check_ready` checks those pins,
the fresh empty manifest, absent run BatchIDs, live schedule and idle jobs again.
No hook extends or silently shortens any deadline.

Demo 1 uses four-part Spark names to read the schema-enabled target across
workspaces. The producer notebook's default lakehouse must also be
schema-enabled, or no default lakehouse may be selected; a non-schema default
causes `REQUIRES_SINGLE_PART_NAMESPACE` during preparation. See
[Lakehouse schemas in notebooks](https://learn.microsoft.com/en-us/fabric/data-engineering/lakehouse-schemas#lakehouse-schemas-in-notebook).
Changing the default must not redirect the producer's explicitly bound staging
and manifest paths.

At source admission the exact timing requirements are:

```text
schedule_start <= now
now + 19 minutes <= verification_deadline <= schedule_end
preparation_deadline > now
preparation_deadline < verification_deadline < cleanup_deadline <= expires_at
```

The 19-minute minimum is nine source intervals plus one five-minute refresh
interval and five minutes of processing margin; it is not a freshness SLA.
The schedule can already be running within its bounded operating window; it
does not need a new future start for each click. After setup, the presenter
clicks **Run** and waits for that run and cleanup to complete before clicking
again. Each click allocates a fresh run ID. The prior run's stop hook leaves the
pinned schedule enabled for the next run.

With the default 120-minute run budget and a 24-hour schedule, allocate each
run within the first **22 hours** to leave its full budget inside the window.
The Demo 1 hook itself checks the default allocation-plus-115-minute verification
deadline, which fits through 22 hours and 5 minutes; follow the earlier cutoff
for full allocation coverage. The prior run must release the writer lock, and
the next run must still pass its own idle, deadline and coverage checks. Once
the cutoff passes or the schedule expires, **Run does not reprovision or renew
it**. The operator must provision and export a new bounded schedule and republish
its receipt in the template before another demo window.

Fabric documents an On/Off control under **Materialized lake views > Manage >
Manage schedules**, but portal pause/resume has not been verified live for this
setup and is not the supported presenter-window lifecycle. Runtime would accept
the same receipt after a resume only if the schedule's pinned ID, owner ID and
type, cadence, scope and bounds remain unchanged and all live admission checks
pass. Fabric identifies the owner as the creator or last modifier, so changing
the schedule under another identity can block admission. If re-enabling starts a
refresh immediately, admission waits within its existing bounds for idle
successful lineage. The fixed expiry would not be extended by pausing.

After durable producer delivery completes, `verify` accepts only the actual
ten measured manifest BatchIDs, 1,000 distinct IDs `1..1000` per batch in both
Bronze and Silver, exact totals, clean source/quality diagnostics and exact
Silver aggregates by date/category/region. It rejects missing or duplicated
fresh-run rows, preserves the bootstrap and synthetic baseline, and waits
boundedly for terminal successful lineage with a completed scheduled refresh
after the **final**, not first, delivery acknowledgement. Job history does not
identify the originating schedule, so it is recorded without claiming that
attribution. Source acknowledgements use the manifest's
`submission_result_at_utc` with `submission_status: submitted`; the generic
`timestamp_utc` is overwritten during producer verification and is not an
arrival timestamp. Unknown submission acknowledgements require reconciliation,
not inferred timing.

Successful verification retains per-batch first **post-delivery verification
observation** times and delivery-to-observation seconds alongside job start/end.
These are latency upper bounds, not the first actual Silver visibility:
earlier batches may already have appeared while the producer was running.
Sixty seconds remains source cadence; managed downstream cadence is five
minutes plus queueing/processing.

Use Spark/Delta as the acceptance source. The SQL analytics endpoint can lag
behind committed MLV data even after the lineage job completes. In the measured
run, legacy (V1) metadata sync caught up without intervention: a later SQL
read showed 10 measured batches with 1,000 distinct IDs each and exact totals
in both layers. This is not a SQL freshness guarantee. For persistent lag,
check the endpoint's [metadata sync version and upgrade readiness](https://learn.microsoft.com/en-us/fabric/data-engineering/sql-analytics-endpoint-metadata-sync).
On V2, Microsoft documents a table-scoped procedure for data-only refreshes;
the REST metadata refresh is for schema changes. Treat any V1-to-V2 upgrade as
separately approved maintenance, not part of **Run**.

`stop` returns `status: stopped` with
`scope: "run-specific; shared bounded setup schedule remains enabled"`.
It does not disable that shared schedule, submit or cancel jobs, or delete
data. It waits only until the fixed cleanup deadline for idle successful
lineage, verifies the unchanged schedule and conditionally persists run
completion, even when cooperative stop was requested. Unresolved evidence,
changed schedule or active/unknown jobs block cleanup and retain the producer's
global lock for reconciliation. Forced notebook termination cannot promise
finally/cleanup; the shared schedule remains under its explicit bound.

At the end of the operating window, the owner uses `managed_refresh.py status/stop --yes`
with the **original setup config and journal**, not a fresh native run ID.
Confirm disabled readback and wait for any active lineage refresh to become
terminal. MLV schedule and job pagination stays on the public Fabric API and
blocks on inconsistent continuations. The setup journal retains the stop
intent, response and disabled readback. Only a documented synchronous HTTP 200
can confirm the PATCH; use read-only status to investigate an uncertain result,
and never resubmit it blindly. There is no automatic job cancellation or
schedule extension. A controller journal already marked `stopped` cannot be
exported or reused for another controller stop after an external portal
re-enable; that state requires operator reconciliation, not a fabricated journal.

## Entry point

Azure CLI calls are bounded at 120 seconds or the remaining Fabric deadline.
On Windows MSI installations the helper invokes the bundled Python CLI
directly, so timeout cleanup does not wait on a CMD wrapper's inherited
pipes. Failures are reported without tokens or command output; acquisition is
not automatically retried.

For the specified 10-file, 1,000-row recurring run, `managed_refresh.py`
provides `plan`, `start`, `status`, and `stop` controls for a receipt-bound
native five-minute full-lineage schedule. `start` and `stop` require `--yes`
and the original ownership state; reconcile ambiguous operations before retry.
The older run-bound CLI admission requires the exact enabled schedule, a
future first refresh, and no conflicting schedules or active lineage jobs.
Source files are targeted at
60-second intervals; scheduled refresh and query visibility are slower.
The controller is locally tested but has not yet been verified on this target.
With a frozen recurring config and coordinated approval, run:

```powershell
python .\demo-1-materialized-lake-views\managed_refresh.py plan --config <absolute-common-config>
python .\demo-1-materialized-lake-views\managed_refresh.py start --config <absolute-common-config> --start-delay-minutes 10 --duration-minutes 30 --yes
python .\demo-1-materialized-lake-views\managed_refresh.py status --config <absolute-common-config>
python .\demo-1-materialized-lake-views\managed_refresh.py stop --config <absolute-common-config> --yes
```

`start` performs preparation checks before creating one future-start schedule,
retains the create receipt, and verifies its owner, scope and bounds. The older
run-bound CLI requires admission before the first scheduled refresh; the
reusable hook instead checks the active bounded window as described above.
Observe the actual delivery times, refresh jobs and Silver BatchIDs, then
disable only the owned schedule after the final batch is reconciled. `stop`
reports blocked while any lineage job is nonterminal; it does not cancel jobs.
The minimum active window is 19 minutes and the default is 30 minutes; these
are bounded allowances, not latency guarantees.

Invoke `python deploy.py --mode MODE --config ABSOLUTE_JSON --prefix PREFIX
--state-dir ABSOLUTE_DIR --result ABSOLUTE_JSON`, where `MODE` is `dry-run`,
`validate-only`, `deploy`, `cleanup`, or `resolve-target-candidate`; cleanup
also requires `--yes`. The
result uses shared contract version 1. Dry-run only checks the local contract
and never reports readiness. Customer-run validation is read-only and can
report readiness after its checks pass. General deployment and cleanup remain
blocked; resource-scoped cleanup is not implemented.
The config must contain `contract_version: 1`. The `--prefix` argument is the
effective prefix; the SQL planning helpers expand `{prefix}` in the workspace
template and render a file-backed Bronze MLV definition and fully qualified
Silver MLV candidate. The effective per-batch verification timeout is the
positive integer in `demo-1.verification_timeout_seconds`, falling back to
`execution.verification_timeout_seconds`, then 900 seconds. Invalid values
fail closed. The Silver definition has been created and its schema read back
in the isolated Lakehouse but has not been tested with measured facts.
The Bronze definition sets `FIXED` and `APPEND_ONLY`, explicit CSV v1 options,
corrupt-row capture, and file traceability; it does not invent a
source-schema declaration. The bootstrap test was performed separately, not by
this entry point. Candidate resolution can make read-only Fabric calls.

The orchestrator supplies a stable prefix/tenant-scoped `--state-dir`, a
distinct per-invocation `--result` path, and an outer lock. Future deploy,
validate, and cleanup implementations must retain ownership and recovery state
in that stable directory across invocations and must not automatically discard
unresolved stale state. `resolve-target-candidate` reads the reserved
`demo-1-state.json` under an exclusive `demo-1-state.lock`. A stale lock or
pending/uncertain journal requires manual recovery; neither is discarded.
It validates tenant, prefix, configuration fingerprint, owner, creation
records, resource identity and parent, MLV fact and explicit configured IDs.
It then uses the active tenant's Azure CLI context to independently GET the
exact Workspace and Lakehouse and check names, types, capacity, Lakehouse
parent and deployment markers. If all ownership checks can be met, the
contract calls for atomically creating a non-overwriting
`demo-1-target-candidate.json` beside the result (`pass`, `ready: false`).
The current verifier blocks publication even if the descriptions match.

The local v1 journal reader expects `contract_version`, `track`, `tenant_id`,
`prefix`, `config_sha256`, empty `pending_operations` and
`uncertain_operations`, an `owner` with UUID `id` and `marker`
`{prefix}-demo1-{owner-id}`, `workspace`, `lakehouse`, and `fact`. The
fingerprint is SHA-256 of UTF-8 compact sorted-key JSON containing `capacity`,
`demo-1`, and the effective `prefix` as implemented in
`ownership._config_fingerprint`. Resource records require `status: confirmed`,
`type`, canonical `id`, `created_by_owner`, canonical `create_intent_id`,
`create_response.id`, and `readback` containing matching `id`, `type`, `name`,
and `owner_marker`; the Lakehouse record and readback also contain the
workspace ID. The confirmed fact names the same workspace and Lakehouse,
`silver.sales_joined`, and type `MATERIALIZED_LAKE_VIEW`.

`provision_synthetic.py` is a separate, opt-in entry point. After coordinating
with the deployment owner, run it with an existing isolated `AZURE_CONFIG_DIR`,
an unbound v1 config, and a dedicated state directory:

```powershell
python .\demo-1-materialized-lake-views\provision_synthetic.py --config <absolute-config> --prefix <prefix> --state-dir <absolute-synthetic-state-dir> --principal-id <expected-Entra-object-ID> --yes-create-synthetic
```

It creates only a fresh Workspace with a unique synthetic suffix and a
schema-enabled Lakehouse. It binds the journal to the configured tenant,
capacity and Fabric token's tenant-local object ID, writes each intent under
the state lock before the POST, retains HTTP and operation evidence, and checks
independent resource readbacks. It does not write data, modify existing
resources, change capacity or permissions, create MLVs, enable schedules, or
certify readiness.

Use `--resume` with the same arguments and state directory to recheck confirmed
resources or finish a pending create with a retained 201 or 202 receipt. A
confirmed Workspace can be followed by the first Lakehouse POST only if no
Lakehouse intent exists. The script never reissues a pending POST. A lost,
unreadable or contradictory receipt blocks progress; reconcile it manually,
without adopting any resource by name or marker. After a process crash, confirm
no process owns `demo-1-state.lock` before manually removing the stale lock;
preserve the journal. The lock coordinates cooperating processes but is not
access control. The operator must keep the state directory on a trusted local
filesystem and protect it from modification by others. This local trust model
is not service creator attestation.

Synthetic provisioning does not publish a producer target candidate. The
`deploy.py` readiness and candidate gates are unchanged; the synthetic journal
does not contain a verified `silver.sales_joined` MLV fact.

On 2026-11-24, the provisioner created MCAP Workspace
`af865b3c-f088-44ad-8816-a5e495b1fbf5` and schema-enabled Lakehouse
`646c26d7-7df8-4962-acaa-4e1386bcba08` on `fabricadbmirror`. Both create
responses were 201; the journal contains no pending or uncertain operations.
Separate GETs matched the exact resource identities, parent, names, types,
capacity and marker and confirmed `defaultSchema=dbo`. The protected local
journal's SHA-256 at independent readback was
`67e449b67be645a1588d22e3f6b4847fb876f11790a4b06ed87e833537655ad4`.
The target was handed to the Demo 1 owner for native MLV validation; this is
not proof that an MLV exists or that measured execution is ready.

The live Workspace, Lakehouse and generic Item GETs returned descriptions, but
not the creating principal or a creation-operation reference. The scoped
preview Admin List Items API returned the Lakehouse `creatorPrincipal`
(documented as item owner); Admin Get Workspace returned neither a creator
nor a creation reference. No tested audit source correlates both create
operations to a deployment. Descriptions are mutable and the manually
provisioned resources have no deployment marker. Do not fabricate a journal,
modify descriptions, or adopt an existing resource to work around that gap.
Before readiness, independently verify live ownership, target definition,
runtime access, batch delivery and replay behavior. No global SalesLineID
allocator, retained-key inventory, or exclusive-writer proof is required solely
to make sales IDs globally unique.

No presenter walkthrough can be executed until the blocker is resolved and
artifacts are deployed. Once available, use Lakehouse lineage and refresh
history for the walkthrough. Cleanup must target only recorded demo-owned
resources after explicit approval and an implemented ownership check.
