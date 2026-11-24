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

## SalesLineID reservations

`SalesLineID` is a positive signed BIGINT and the logical fact key across
retained runs, not merely within one run. Every run, including producer-created
bootstrap runs, must reserve its full inclusive range before staging or delivery.
`batch_bytes()` alone only calculates candidate bytes; it does not reserve keys.

The Fabric registry is the single deployment-scoped object at
`https://onelake.blob.fabric.microsoft.com/<producer-workspace-id>/<staging-lakehouse-id>/Files/producer/allocations/sales-line-id-v1.json`.
Its exact v1 shape is:

```json
{
  "version": 1,
  "scope": {
    "version": 1,
    "kind": "fabric",
    "owner": "chanel_digital_prep/demo-data-generator",
    "tenant_id": "<canonical-uuid>",
    "producer_workspace_id": "<canonical-uuid>",
    "staging_lakehouse_id": "<canonical-uuid>"
  },
  "allocator_id": "<sha256>",
  "target_bindings": {
    "demo-2": {"workspace_id": "<canonical-uuid>", "item_id": "<canonical-uuid>", "fact": "FactSales"}
  },
  "retained_ranges": [{"source": "<inventory-identifier>", "start": "1", "end": "5"}],
  "reservations": {
    "<run-id>": {
      "reservation": {
        "version": 1, "allocator_id": "<sha256>", "run_id": "<run-id>", "batch_class": "measured",
        "start": "<decimal>", "end": "<decimal>", "count": "<decimal>",
        "generator_sha256": "<sha256>", "fingerprint": "<sha256>"
      },
      "binding": {
        "config": {
          "run_id": "<run-id>", "batch_class": "measured", "start_date": "2024-01-01", "seed": 42,
          "date_count": 365, "products": 20, "customers": 100, "file_count": 10,
          "rows_per_file": 1000, "interval_seconds": 60
        },
        "selected_demos": ["demo-2"],
        "targets": {
          "demo-2": {"workspace_id": "<canonical-uuid>", "item_id": "<canonical-uuid>", "fact": "FactSales"}
        },
        "generator_sha256": "<sha256>"
      }
    }
  },
  "integrity_sha256": "<sha256>"
}
```

The manifest adds `sales_line_id_reservation`, exactly equal to its registry
record's `reservation`. The `config` is `dataclasses.asdict(RunConfig(...))`;
external `product_count` and `customer_count` map to `products` and
`customers`. Start, end and count are canonical decimal strings; a reservation
has `count = file_count * rows_per_file` and `end = start + count - 1`.
Existing retained ranges can include signed BIGINT values and must be a
non-overlapping union across all retained destinations.

For each logical run, `start` is `1 + int.from_bytes(SHA256(
b"chanel-sales-line-id-v1\0" + run_id.encode("ascii")), "big") %
(9223372036854775807 - count + 1)`. It is only a candidate: the registry
rejects collisions and overflow rather than assuming a hash is unique. All
batches use consecutive IDs within the reserved range.

All hashes are lowercase SHA-256 hex. Canonical JSON means UTF-8 bytes from
`json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
allow_nan=False)` with no trailing newline. `allocator_id` hashes `scope`;
`fingerprint` hashes `{"allocator_id": allocator_id, "binding": binding}`;
`integrity_sha256` hashes the entire registry excluding that field. Registry
storage is canonical JSON plus one LF. `generator_sha256` hashes `generator.py`
after strict UTF-8 decoding and replacing CRLF and lone CR with LF, with no
other normalization. A process refuses staging if that source changed after it
was imported.

The allocator validates the registry and checks the selected target bindings,
retained ranges and all prior reservations. A separate exclusive
`Files/producer/allocations/sales-line-id-v1.lock` serializes Fabric producer
runs; its bytes and ETag are rechecked before staging or a manifest transition.
The allocator conditionally replaces the
registry using its ETag, retries definite conflicts, and reads back the exact
record after a successful or ambiguous write. A rejected conditional operation
is never treated as success. Ambiguous writes retain the allocator and affected
run locks;
missing or corrupt registries, mismatched bindings, a manifest without its
reservation, and a reservation without its manifest all require explicit
reconciliation. If conditional enforcement is rejected or uncertain, halt all
writers and reconcile the entire registry against retained facts before
restarting. A checksum is not an ownership signature.

The scope remains stable when a subset of demos is selected. Adding a target
requires a separately authorized onboarding process that preserves existing
bindings, inventories all retained facts under quiesced writers, updates the
same registry conditionally, and proves no other allocator or writer can bypass
it. This process, runtime target-to-adapter identity and fresh authorization
are **not yet implemented or verified**. No live registry has been initialized.
The previously observed Demo 2 bootstrap had IDs 1–5; its point-in-time inventory
alone does not establish a safe baseline. Demo 1's header-only bootstrap had no
keys. The independent verifier must read the authoritative registry, compare
scope, binding and manifest, verify IDs fall in range and check other retained
fact batches for collisions.

Local staging uses a separate root-scoped `.sales-line-id-v1.json` registry and
an exclusive `.sales-line-id-v1.lock`; it is not the Fabric authority. Local
registry writes use file synchronization and atomic replacement, with directory
synchronization on non-Windows systems. Power-loss durability on Windows and
concurrent external writers have not been established.

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

`job_lifecycle.py` contains local no-sales preflight pipeline job primitives.
They check the effective config hash, selected demos and probe ID against the
run binding, then persist the payload hash and submission intent before POST.
The returned pipeline job identity is persisted before polling; ambiguous
submissions are never repeated. A process-released lock serializes operations
for one run. Legacy unbound journals cannot authorize recovery. Windows
power-loss durability is not established.

`producer-status` and `stop-producer` require the original effective config and
`--selected-demos`. When journal and producer resource binding checks succeed,
they read the recorded no-sales preflight parent pipeline job's current status.
Both return `blocked` and `ready: false`; `stop-producer` does **not** request
cancellation because authenticated job and child provenance are not
established. A terminal parent status does not prove child termination or
reconcile in-flight deliveries, and neither mode authorizes stopping the
independent monitor. The low-level cancellation primitive is not wired to
these CLI modes. The primitives cannot submit `run-data` or establish readiness.

The experimental notebook preflight accepts `runtime_evidence_json` separately
from `run_config_json`, so its `effective_config_sha256` covers the latter's
complete canonical JSON without including the probe nonce. It emits only
runtime observations in its notebook exit value and always reports `blocked`.
The observed job and caller-supplied definition and resource identities are
not independently authenticated, and neither queue access nor effective
ingestion authorization is proven. The updated notebook and pipeline
definitions have not been deployed or verified in Fabric.

The effective configuration may carry selected-only `shared.target_bindings`
entries of `{workspace_id, item_id, fact}`. These are candidate bindings,
not evidence of target ownership, permissions, allocator safety or monitor
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
owner provenance, the actual notebook principal's access and a complete,
quiesced retained-key inventory remain unverified. A successful read is not a
readiness decision; `validate-only` remains blocked.

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
