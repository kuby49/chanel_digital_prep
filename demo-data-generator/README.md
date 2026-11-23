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

The scheduler spaces batch starts but calls adapters synchronously and cannot
interrupt a blocking call. It does not establish a hard run deadline or
guarantee one-minute cadence. The Demo 2 verification controller must run
independently, persist its own outcomes and enforce its own deadlines.

## Fabric deployment and current limits

`deploy.py` accepts the shared v1 `--mode`, `--config`, `--prefix`,
`--state-dir`, and `--result` arguments. `dry-run` is a local plan. Deployment
uses an owned-resource journal, verifies tenant and principal, publishes the
pinned `environment.yml`, and refuses to adopt unknown resources or retry an
ambiguous creation. `validate-only` performs readbacks but still reports
blocked while readiness is incomplete. `cleanup` and `run-data` are blocked;
the notebook independently refuses `run-data`. No measured run is enabled.

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
