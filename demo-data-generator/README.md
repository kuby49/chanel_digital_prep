# Shared data producer

The producer defines **CSV contract v1**: UTF-8, comma-delimited, a header,
minimal RFC 4180 quoting, LF line endings, no nulls, ISO dates and UTC
timestamps ending in `Z`. Amounts are integer USD cents and
`SalesAmountMinorUnits = Quantity * UnitPriceMinorUnits`. `dimensions()`
produces the deterministic date, product and customer datasets.

`batch_bytes(RunConfig(...), batch_number)` returns deterministic bytes,
SHA-256, count, exact total, and grouped date, category and region totals.
The same run ID and configuration reproduce the same batches; use a new run ID
for new data. Every manifest records contract version 1 and a fixed batch class
(`measured`, `bootstrap`, or `negative-test`). Classification alone does not
create malformed data.

## SalesLineID and replay

`SalesLineID` is a positive signed BIGINT, consecutive from 1 to `row_count`
within each batch. Sales IDs may repeat across batches and runs;
`(BatchID, SalesLineID)` is the fact and replay key. Replaying a batch uses the
same bytes and keys. No global allocator or reservation fields are used.

The manifest binds the run configuration, destination selection and generator
source digest (`generator_sha256`, SHA-256 over UTF-8 with LF line endings),
plus physical target bindings for Fabric staging. Legacy manifests with
reserved global IDs are left untouched; use a new run ID rather than replaying
legacy files under the revised identity scheme. Fabric also persists a per-run
initialization marker before the manifest so losing the manifest cannot
silently restart that run.

## Staging and delivery

`stage_batch` and `runner.run_batches` are local single-writer primitives.
`FabricLedger` and `run_fabric_batches` implement the Fabric variant using
conditional OneLake Blob requests rather than filesystem rename or cached mount
reads. The run manifest is at
`https://onelake.blob.fabric.microsoft.com/<shared.workspace_id>/<shared.staging_lakehouse_id>/Files/producer/runs/<data.run_id>/manifest.json`.
`shared.run_state_location` is not an override. The manifest may not exist
until the producer starts. Readers should use Blob GET and must not modify the
single-writer manifest.

An exclusive conditional-create lock protects each run. Staged bytes are
immutable and checked against the manifest; manifest changes use `If-Match`
and readback. A potentially ambiguous storage write retains the lock. After a
crash, release it only after confirming the prior writer is stopped and
reconciling the exact lock and manifest. OneLake conditional create, update and
stale-condition rejection were exercised in MCAP; see `deployment-mcap.json`
for the recorded scope.

Delivery records `submission_started_at_utc`, `submission_result_at_utc`, and
`submission_status`: `submitted`, `retryable_failure`, `unknown`, or
`permanent_failure`; these fields remain available after later verification.
`retryable_failure` means the adapter proved the request was
not accepted. An unknown or stranded `submitting` operation must be reconciled,
not blindly retried. The scheduler records `status` (`running`, `completed`,
or `failed`), `started_at_utc`, the run bound and batch start times; on normal
termination it records `finished_at_utc` and `result`. A missing terminal
marker is not evidence that the remote job stopped.

The Eventhouse adapter receives a checked driver-local CSV. A delivery becomes
`verified` only after a matching source ID has terminal SDK success and KQL
returns the exact row count, distinct keys and total. The observed
`Skipped_IngestByTagAlreadyExists` terminal code requires manual
reconciliation: matching KQL alone does not prove a prior successful source
or serialized replay. Other failures remain failures and missing queue
messages are not success. The separate Demo 2 verifier must also check joins
and grouped totals. Serialized duplicate replay was demonstrated for a
separately accounted bootstrap, not for concurrent submissions.

Measured runs stage the first closed CSV at source start, then schedule each
following CSV one interval apart. The main thread is the only manifest writer
and stages due files while one worker performs serialized destination I/O.
`generated_at_utc` records staging after closed-file readback; submission and
verification timestamps describe delivery separately. The runner submits
file destinations before Eventhouse and defers its own readback until after
submissions so that a slow KQL query cannot delay later publication. Compare
adjacent `submission_result_at_utc` values for the observed destination
interval; `staged_to_submission_ack_seconds` shows the per-batch backlog for
successful submissions. A file acknowledgement means OneLake acknowledged
the write (or matching bytes were already present); Eventhouse's
acknowledgement means the queue accepted the request, not that KQL can see it.
Slow submissions can delay landing, slow
OneLake reads/writes can skew staging, and a blocking SDK call can outlast
cooperative stop or the deadline. Demo 2's separate downstream verifier
applies its own deadline.

## Reusable native launch: implemented interface, track blockers remain

The reusable producer profile allocates a fresh run ID and fixed admission
expiry for each Fabric **Run**, without per-run definition publication,
JSON input or presenter CLI. Enable this template with
`shared.native_launch: true` and `shared.native_reusable: true` in the
resolved deployment config before one-time authorized definition publication.
The configured data controls count, rows and interval; the template's run ID
does not control actual run IDs. Defaults are 10 files of 1,000 rows, 60s
apart. The SHA-256 and exact stop nonce are derived again from the entire
fresh projected runtime document, not reused from the template.

**This is not yet a runnable all-demo launch.** Demo 1 needs its read-only
hook and an approved one-time bounded control-plane schedule receipt;
notebook-resident MLV schedule mutations are not bundled. The shared setup
schedule intentionally remains enabled between runs, and its remaining
coverage must include the fresh run's complete verification deadline.
Demo 3 needs complete native readiness, verification and cooperating-writer
drain hooks. Missing or unsuccessful
hooks fail before sales, rather than assuming a manually started monitor
or accepting recorded readiness. Demo 2's native hook must establish and
observe its exact owned monitor/verifier jobs automatically. No live result
is implied by the local producer implementation.

`native_lifecycle.py` expects track-owned flat modules `native_demo1`,
`native_demo2` and `native_demo3`, each exposing `prepare(context)`,
`verify(context, prepared)` and `stop(context, prepared)`. Return JSON dicts
with status `ready`, `verified` and `stopped` respectively only after proving
the corresponding outcome. The producer preserves the evidence and errors.
Prepare runs inside the per-run ledger after empty manifest initialization,
before scheduler/source start. Demo 1 prepares last and must additionally
implement `check_ready(context, prepared)` immediately before staging.
Verify runs only after successful producer delivery/scheduler completion is
durable. Overall pipeline success additionally requires all track verification
and cleanup; delivery success alone is not end-to-end success.

Context is a dict with immutable-by-contract `document` and `binding`,
conditional producer-staging `store`, checked-token `store_factory(ws, lh)`,
`fabric`, active `spark`, `token_provider(audience)`, `manifest_path`,
`read_manifest()`, `stop_requested()`, UTC `clock()`, and `sleep(seconds)`.
It includes fixed UTC strings `preparation_deadline_utc`,
`verification_deadline_utc`, `cleanup_deadline_utc` and `expires_at_utc`.
The runtime Fabric client's `request(method, path, payload=None, timeout=30)`
supports bounded GET/POST/PATCH/DELETE and returns `(status, headers, body)`
without mutation retries. Each track owns its durable mutation intent,
exact operation/job receipts and bounded reconciliation; a prepare exception
does not mean its mutation failed. Cleanup is called with `prepared=None`
after a partial prepare, and all previously attempted tracks are cleaned
up even if another cleanup fails.

The fixed producer staging object `Files/producer/native/writer.lock`
serializes the complete native lifecycle across different run IDs and
target selections. A second click while it exists is blocked, even after
the receipt expires; there is no automatic lock stealing. The per-run
`native-lifecycle.json` records its immutable runtime document, binding,
hook results and errors; `Files/producer/native/latest.json` identifies the
latest admitted run. Normal completion releases global exclusion only after
all verification and cleanup succeeds. Failure, collision, uncertain
storage/mutation outcome or forced cancellation retains the gate for explicit
reconciliation. Other entrypoints are not magically covered: track readiness
must reject conflicting pre-existing schedules, jobs and writers.

The default window is 7,200 seconds, preparation budget 1,200 seconds, cleanup
reserve 300 seconds; set `shared.native_run_window_seconds`,
`shared.native_preparation_timeout_seconds` and
`shared.native_cleanup_timeout_seconds` before template publication when
needed. The window must cover preparation, producer duration, downstream
verification and cleanup. Expiry is fixed at run allocation, never extended.
In particular, the generous default window cannot fit Demo 1's currently
approved 60-minute setup schedule: activation must choose explicit compatible
budgets, and the read-only hook must reject launches without enough remaining
coverage rather than silently shorten verification or widen the schedule.
Native cleanup first persists and reads back the exact immutable stop marker,
then closes Demo 3 admission only while holding its per-run ledger.
Track cleanup must prove its owned schedules/jobs stopped or its precisely
documented cooperating-writer barrier; a service job listing alone is not
a queue-drain proof. Unproven cleanup remains blocked.

## Interim single-use prepared customer demo

This profile has not been published or verified for the recurring customer
demo. It is not the requested reusable one-click experience: each run needs
new preparation and a separately started monitor. Do not select **Run** until
the coordinated native path is ready.

After the operator confirms the exact run ID and downstream readiness, open
the producer workspace, open its **producer_wrapper** Data Pipeline, and select
**Run**. Accept the defaults; do not edit parameters or paste JSON. This
consumes the prepared run ID on first initialization. A repeat or expired
profile fails closed; the operator must prepare a new ID for another demo.

Before enabling the native default, the operator must verify the selected
tracks and dimensions, start Demo 2's monitor for the exact run ID and
manifest with enough time for startup, execution and verification, and
confirm Demo 3's matching admission policy and enabled rule for that run.
Demo 1's ingestion/MLV path must also be ready. The native job checks runtime
identity, notebook context, destination bindings and write access; it cannot
prove a desktop monitor is running or substitute for these checks.

Render and verify the owned definitions using the same resolved config:
set `shared.native_launch: true`, set `shared.native_launch_expires_at_utc`
to a fixed UTC expiry covering Spark startup, producer duration and downstream
drain, and supply the exact owned producer workspace, staging Lakehouse,
notebook and identity IDs. The default target selection is all three demos;
`shared.native_selected_demos` may override it with a comma-delimited selection
that matches `shared.target_bindings`. A prepared profile pins `data.run_id`
and its generation settings; the customer settings are 10 files, 1,000 rows
each, at 60-second intervals. The definition embeds only the projected runtime
configuration, not desktop paths, journal settings or credentials. Updating
existing owned definitions requires the explicit publication below; ordinary
deploy and validation still refuse to overwrite drift. Guarded CLI `run-data` is
not the launcher for a config reserved for native use.

Observe **Monitor** in Fabric for the parent and `RunProducer` notebook,
then open `Files/producer/runs/<run_id>/manifest.json` in `producer_staging`.
Report staging and delivery progress separately from MLV refresh, Eventhouse
analysis and Copy results. To stop, have the operator disable the Demo 3
rule, stop/cancel the producer as appropriate, confirm both Fabric jobs are
terminal, and drain admitted Copy and Eventhouse work. A native run has no
desktop CLI submission journal; a cancellation request does not confirm
termination or undo external writes. Never remove `writer.lock` on that
basis. The producer marks Demo 3 admission stopped under its writer lock on
cooperative stop or failure when the ledger remains writable. On success the
admission remains open until the fixed expiry so queued copies can finish;
after force cancellation do not rely on an admission update having happened.

### Publish prepared definitions to existing owned items

Only the operator should run this during a maintenance window. Stop or
disable other launchers and schedules and ensure nobody uses Fabric **Run**
or edits either item concurrently. Fabric's definition update does not
atomically exclude native launches or external edits; the controller
serializes the guarded producer CLI using the same state directory and
checks the pipeline and notebook job histories and the local job journals.
Do not use a different state directory to bypass an unresolved intent.

Use the same frozen resolved config and owned state directory for both
commands. Run the notebook first; the wrapper update requires an exact
notebook definition readback.

```powershell
python .\demo-data-generator\deploy.py --mode update-definition --definition-kind notebook --expected-definition-sha256 $ApprovedNotebookDigest --config $Config --prefix chanel --state-dir $State --result $NotebookResult --yes
python .\demo-data-generator\deploy.py --mode update-definition --definition-kind pipeline --expected-definition-sha256 $ApprovedPipelineDigest --config $Config --prefix chanel --state-dir $State --result $PipelineResult --yes
```

`$Config`, `$State`, `$NotebookResult`, and `$PipelineResult` are absolute
paths. The baseline digest is lowercase SHA-256 of the UTF-8 canonical JSON
mapping `{part path: decoded JSON value}` from the approved live definition,
including `.platform` if present. Canonical JSON uses sorted keys, compact
separators, no trailing newline, and unescaped Unicode. Retrieve the notebook
in `ipynb` format. A mismatching digest is reported without writing, but
inspect and approve the actual definition before using that digest.

The controller preserves `.platform` byte-for-byte and does not update item
metadata. If publication is interrupted, rerun the **same command** with
the same config and approved baseline. The durable
`shared-producer-definition-update-v1.json` intent records the target and any
accepted Fabric operation ID. The controller polls only that ID or checks
exact readback when acceptance is unknown; it does not retry an uncertain
update POST. If exact readback cannot establish the outcome, keep the
journal and reconcile manually. Successful publication does not declare demo
readiness or start a producer.

## Isolated Demo 2 synthetic staging

The opt-in notebook operation `synthetic-stage` stages exactly two five-row
`bootstrap` batches for a reserved `demo2synthetic_<12 lowercase hex>` run ID.
It requires the selected destination to be only `demo-2`, an exact
`SyntheticFactSales` target binding, a canonical configuration digest in a
separate bound `runtime_evidence_json` request, and matching pipeline notebook,
Lakehouse, tenant and runtime token identities. Staging uses the ordinary
`Files/producer/runs/<run_id>/manifest.json` path and conditional per-run
ledger; same-run replay must match the immutable configuration and bytes.
Both delivery records remain `pending`. This operation does **not** submit
ingestion, prove target ownership, authorize a measured run or make the
producer `ready`.

The separately gated `synthetic-auth` notebook operation accepts the same
bound bootstrap configuration with a matching `kusto` endpoint request. It
generates exactly one row with batch ID
`demo2synthetic_auth_<12 lowercase hex>_0001` and uses the ordinary
single-attempt Eventhouse adapter and conditional ledger under a **different**
run directory. It will not start if that authorization-probe manifest already
exists. Terminal SDK success plus exact KQL row/total verification are required;
an unknown or failed submission is retained in the distinct manifest for
manual reconciliation, never resubmitted. This operation must run only after
the independent target ownership and read-only runtime preflight gates pass.
It does not stage or modify the monitored two-batch manifest.

After independent verification of that authorization probe, the separate
`synthetic-deliver` operation requires its exact one-row `bootstrap` manifest,
source ID, terminal ingestion success and verified KQL totals in the same
isolated target. Only then may it initialize or resume the two-batch monitored
manifest. A previously staged manifest is accepted only when both deterministic
files and all delivery records are still wholly pending; any prior delivery
state without a scheduler blocks reconciliation. The monitored run uses one
submission attempt per batch and retains unknown outcomes for manual recovery.
The operation does not enable the normal `run-data` path or measured readiness.

The synthetic-target `preflight` branch reads its Kusto database, fact schema
and CSV mapping, and peeks one message on each ingestion status queue without
consuming or disclosing message contents. It does not create the usual storage
sentinel; its conditional-storage check reports `not-run`. The returned
`producer-runtime-preflight` observation remains `blocked` and
`effective-ingestion-authorization` remains `unverified`; a verified queue
peek only establishes queue read access. Bind any observation to the exact
published notebook definition and completed parent and child job before
using it as evidence. Never stage or ingest into an unverified target.

## Fabric deployment and current limits

`deploy.py` accepts the shared v1 `--mode`, `--config`, `--prefix`,
`--state-dir`, and `--result` arguments. `dry-run` is a local plan. Deployment
uses an owned-resource journal, verifies tenant and principal, publishes the
pinned `environment.yml`, and refuses to adopt unknown resources or retry an
ambiguous creation. Host token acquisition has a 120-second timeout and no
automatic retry. On Windows MSI installations it invokes the bundled Azure CLI
Python directly to avoid a CMD wrapper holding captured pipes open. The
isolated `AZURE_CONFIG_DIR` and token tenant and principal checks are retained.
`validate-only` can report ready after checking owned
definitions, selected target metadata and direct workspace write roles;
this is an admission check, not proof of actual notebook runtime identity or
delivery. `cleanup` remains blocked. The
`run-data` launcher requires fresh passing producer and selected-destination
validators before submitting; when Demo 2 is selected it also requires the
bound monitor to be running with enough time left for execution and
verification. Admission honors `execution.max_concurrency` (1-4), and its
deadline includes a timeout budget for each wave of validators and the monitor
status check.

The notebook checks its actual runtime identity, producer and destination
bindings, direct workspace write roles and run bounds before staging. A caller
using the Fabric Jobs API directly must independently ensure the selected
destinations and monitor are ready: the notebook does not perform the CLI's
local validator and monitor checks. The MCAP definitions were updated and
independently read back for the bounded customer run.

`job_lifecycle.py` contains no-sales preflight and measured pipeline job primitives.
They check the effective config hash, selected demos and probe ID against the
run binding, then persist the payload hash and submission intent before POST.
The returned pipeline job identity is persisted before polling; ambiguous
submissions are never repeated. A process-released lock serializes operations
for one run. Legacy unbound journals cannot authorize recovery. Windows
power-loss durability is not established.

`producer-status` and `stop-producer` require the original effective config and
`--selected-demos`. When journal and producer resource binding checks succeed,
they read the recorded parent pipeline job's current status.
The activity query can identify its exact `RunProducer` notebook child; a
separate notebook scheduler read confirms that candidate's job status.
Missing, ambiguous or paginated activity results do not prove the child stopped.
When stopping and the parent is terminal but its linked child remains active,
the controller polls that same parent/activity/child chain for up to 120 seconds.
A timeout or a changed identity leaves termination unconfirmed; an activity
reporting success is not a reason to cancel a lagging child.
Both return `blocked` and `ready: false`; neither requests cancellation.
For a journaled measured run, `stop-producer` writes a run- and
configuration-bound cooperative stop marker and waits up to 120 seconds for
the exact parent and child. The notebook checks the marker between operations,
but cannot interrupt a blocking SDK call or undo an in-flight delivery.
Read the manifest and reconcile unknown delivery outcomes before retrying.
A terminal parent alone does not prove child termination, and neither mode
stops the independent monitor or proves downstream fact ingestion.

For pipeline Spark high concurrency, enable
[`highConcurrency.notebookPipelineRunEnabled`](https://learn.microsoft.com/en-us/rest/api/fabric/spark/workspace-settings/update-spark-settings)
in the producer workspace Spark settings. The in-notebook Spark flag alone
does not determine launch mode. In MCAP an isolated no-sales run reported
`output.result.highConcurrencyModeStatus.sessionSource: created`; its parent
and exact notebook job separately reached `Completed`. While the activity was
active it did not expose a child job ID, and the child scheduler
remained `InProgress` after the parent completed. No cancellation was sent.
Do not cancel the shared Livy session or treat high concurrency as proof of
delivery, writer exclusion, or safe stopping.

The experimental notebook preflight accepts `runtime_evidence_json` separately
from `run_config_json`, so its `effective_config_sha256` covers the latter's
complete canonical JSON without including the probe nonce. It emits only
runtime observations in its notebook exit value and always reports `blocked`.
The observed job and caller-supplied definition and resource identities are
not independently authenticated, and neither queue access nor effective
ingestion authorization is proven by this historical preflight alone.

The effective configuration may carry selected-only `shared.target_bindings`
entries of `{workspace_id, item_id, fact}`. These are candidate bindings,
not evidence of target ownership, permissions, delivery safety or monitor
liveness. When `validate-only` receives `--selected-demos`, it requires exactly
those candidates and reads each workspace and typed target item with the CLI
principal. It checks Demo 2's KQL database, Eventhouse parent and configured
adapter endpoints, database, table and mapping; Demo 3's landing must name its
candidate Lakehouse. Demo 1 may use a separate landing Lakehouse, whose item is
read independently. For Demo 3, a separate Azure Storage-audience token for the
same CLI principal reads the [OneLake Delta metadata API](https://learn.microsoft.com/en-us/fabric/onelake/table-apis/delta-table-apis-overview).
The single-page listing must contain exactly the case-sensitive candidate fact
as a Delta table at its expected workspace and Lakehouse path. A continuation
token or missing, malformed or mismatched metadata blocks this observation;
pagination is not implemented. Missing or mismatched bindings block validation.

These are point-in-time metadata and configuration checks only. Demo 1 and
Demo 2 fact identities, table schemas or contents, the landing-to-fact linkage,
owner provenance and the actual notebook principal's access remain unverified.
A successful target read alone is not a readiness decision; `validate-only`
also checks owned definitions and direct write roles. Actual runtime identity,
permissions and target checks still run inside the notebook before staging.

For a fresh deployment, `deploy.py --mode resolve-candidate-ids` accepts the
normal v1 arguments, reads and validates the owned `shared-producer-v1.json`
journal under its lock, and atomically creates
`shared-producer-candidate-ids.json` beside the per-invocation `--result` file.
The sidecar contains only `version`, `workspace_id` and
`staging_lakehouse_id`; an existing sidecar is never overwritten. The mode
rejects pending, missing or mismatched state and does not change `config`.
Consume the sidecar only when this invocation's result says `pass`. These IDs
are candidates for an orchestrator-owned effective configuration, not evidence
of live ownership, permission or readiness; fresh producer and
selected-destination readbacks are still required. This mode does not adopt the
legacy MCAP manifest.

The isolated MCAP producer workspace, staging Lakehouse, notebook, pipeline and
published Environment are recorded in `deployment-mcap.json`. A no-sales
pipeline probe confirmed parameter and Lakehouse binding, the expected runtime
principal, SDK and bundled adapter imports, and an authenticated Kusto SDK
query. The latest probe also verified conditional create, update and delete,
rejected duplicate and stale conditions, and read back sentinel removal. An
earlier probe exposed an HTTP 200 delete response that the client incorrectly
rejected; the client now accepts the observed 200 and standard 202 responses.
These probes do not establish fresh readiness for future runs.

The producer-owned preflight file
`Files/preflight/producer-monitor-heartbeat-v1.txt` contains exactly
`chanel-producer-storage-readiness-v1` followed by LF. Its SHA-256 is
`aea6faa895eae3072bb28cfacf732a93cfd9968fc8d2c6d2675f415278d6fd55`.
It is immutable by convention, not by a storage lock; monitors must compare the
bytes or digest and fail on change. It proves only access to that object.

Before activation, independently verify selected destination permissions,
configuration and runtime identity; start and verify the Demo 2 controller;
implement remote job identity, cancellation and terminal readback; and
demonstrate storage recovery, cadence and deadline behavior. Demo 3 additionally
requires a demonstrated completed-file event: a rename is not assumed to emit
`FileCreated`. No local `ready` flag or old probe is a security boundary.

Run focused tests with `python -m unittest discover -s demo-data-generator -p "test_*.py"`.
