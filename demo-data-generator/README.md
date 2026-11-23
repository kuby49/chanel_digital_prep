# Shared data producer

The local Python module defines **CSV contract v1**: UTF-8, comma-delimited,
header present, minimal RFC 4180 quoting, LF line endings, no nullable fields,
ISO dates and UTC timestamps ending in `Z`. Amounts are integer cents in USD;
`SalesAmountMinorUnits = Quantity * UnitPriceMinorUnits`. `dimensions()` produces
the three deterministic dimension datasets. Sales keys cycle through each
dimension; coverage of every dimension key requires the total number of rows
to be at least as large as the largest dimension.

`batch_bytes(RunConfig(...), batch_number)` returns the exact CSV bytes and
reference totals including grouped date, product category, and customer region
totals. The batch timestamp is derived from the logical sale date, not the
delivery time. The same run ID, configuration and batch number give the same
bytes; quantities and prices use SHA-256 rather than runtime-specific random
number generation. Use a new run ID for intentionally new data.
Every manifest has `contract_version: 1` and each batch records `batch_class`
(`measured`, `bootstrap`, or `negative-test`); a run has one fixed batch class.
Classification alone does not generate malformed data for a negative test.

`stage_batch(root, config, batch_number, destinations)` stores immutable CSVs
and a v1 JSON manifest under `root/<run_id>`. It verifies existing files and
rejects configuration changes or untracked/corrupt files instead of silently
regenerating them. It requires a single writer per run; a crash between staging
a file and updating the manifest requires manual recovery. Filesystem rename
and durability behavior have only been tested locally, not on OneLake.

`delivery.py` provides single-writer, persisted delivery transitions.
`begin_submission(root, run_id, batch_number, destination, max_attempts)` returns
the closed staged path, batch ID, SHA-256, row count and exact total after recording
`submitting`. An adapter's `submit(staged_path, batch_id, sha256, row_count,
sales_amount_minor_units)` receives a **driver-readable local file**, not an
ABFSS URI. `fabric_files.materialize_staged` copies a staged file through
`notebookutils.fs.cp` to a temporary driver-local file, checks its SHA-256, and
removes the local copy after submission. This helper is locally tested with a
mock filesystem, not verified in Fabric. Record its
`submitted`, `retryable_failure`, `unknown`, or `permanent_failure` result with
`record_submission`. `retryable_failure` means the adapter has proved that the
request was not accepted. A crash in `submitting` or an `unknown` result requires
reconciliation, not blind retry. Never log secrets in diagnostics. Queue
acceptance is not verified ingestion: `record_verification` checks the observed
row count, exact total and distinct sales-line count before setting `verified`.
Eventhouse must additionally verify the batch-specific query and deduplication
behavior; concurrent in-flight retries cannot rely on tags alone.

`runner.run_batches` is a local, dependency-injected scheduler that records a
run's start, deadline and batch start times, spaces new batches by at least the
configured interval, retries only proven non-acceptance, and continues other
destinations after an ambiguous failure. A resumed run retains the original
deadline and never resubmits a queued or ambiguous request. An adapter receives
the submission request and UTC deadline; it must enforce its own I/O timeout.
The runner cannot interrupt a blocking adapter, and it does not prove a hard
job duration. An exclusive local lock rejects concurrent runners; a lock left by
a crash requires operator verification and explicit recovery. Fabric pipeline/job
timeouts and OneLake persistence, exclusive locking,
event finalization, and connector behavior must be validated before activation.

`deploy.py` implements the shared v1 invocation and result envelope. Dry-run
is a plan only; all other operations explicitly report blocked. The local
notebook and pipeline are **draft assets**, not deployed or end-to-end runnable;
the notebook intentionally fails before generating data. Deploy-time notebook
Lakehouse binding and built-in resources have not been implemented. There is no
destination transport or source scheduler yet. No measured run can be started
until Fabric storage semantics, cross-workspace permissions, Eventhouse
idempotency, and readiness validation are implemented and verified. A rename is
not assumed to emit the `FileCreated` event: the selected completion operation,
event type, and fully readable contents must be demonstrated in the tenant
before Demo 3 is ready. Do not treat these local primitives as a working Fabric
producer.

Preflight has not granted any destination access or created connections.
Producer workspace and notebook execution, pipeline parameter binding, and
Eventhouse least-privilege ingestion roles are unverified. Those are deployment
blockers, not implicit authorization. The preflight identity evidence does not
establish that a workspace/notebook POST or job run succeeds; use the approved
identity only after explicit access decisions and complete a bounded non-data
deployment/readback before measured execution.

Run focused tests with `python -m unittest discover -s demo-data-generator -p "test_*.py"`.
