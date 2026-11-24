# Demo 3: OneLake events and Copy

**Status: blocked for production ingestion.** An isolated MCAP preflight
workspace, Lakehouse, OAuth2 connection, setup notebook, probe pipelines and a
stopped probe Activator exist. They do not constitute a deployable production
pipeline. The entry point remains fail-closed; `dry-run` only validates the
local contract.

## Contract

`python deploy.py --mode {dry-run,validate-only,deploy,cleanup,resolve-target-candidate,provision-owned,verify-owned} --config <absolute JSON> --prefix <prefix> --state-dir <absolute directory> --result <absolute JSON>`. Cleanup and `provision-owned` require `--yes`. A v1 result reports `track: demo-3`; `ready` remains false. Deploy, validation and cleanup currently return `blocked` and make no Fabric requests; cleanup never deletes anything.

`provision-owned` explicitly creates a new workspace and Lakehouse shell, journaling
each creation intent and confirming live readback; `verify-owned` reads that
journal and checks the live resources without creating anything. Both remain
blocked for readiness: they do not create or verify the fact table, guarded
pipeline, Activator, access or writer safety. They never adopt existing resources
or retry an uncertain creation. The existing preflight workspace is not an
owned production deployment journal.

`resolve-target-candidate` is local-only. It reads a confirmed
`demo-3-state.json` under an exclusive `demo-3-state.lock` and creates a fresh
`demo-3-target-candidate.json` beside the result without overwriting an existing
file. Missing, invalid, pending, uncertain or conflicting ownership blocks the
operation; a stale lock is never cleared automatically. A successful candidate is
not proof of live ownership, access, inventory, exclusive writing or readiness.
The standard deployment stub does not adopt the preflight resources.

The journal must identify contract version, track, tenant, prefix, an owner ID
and marker `<prefix>-demo3-<owner-id>`, plus empty `pending_operations` and
`uncertain_operations` arrays. Confirmed `workspace` and `lakehouse` records
must carry canonical nonzero IDs, item type, `created_by_owner`,
`create_intent_id`, and matching independent `readback` identity and owner
marker. The Lakehouse and its readback must name the owned workspace. A confirmed
`fact` record must bind that workspace and Lakehouse to the exact physical name
`dbo.factsales`. The journal must be written by a future deployment/recovery
workflow only after independently establishing creation and readback; it must
not be fabricated from the preflight observations.

The shared producer's v1 CSV has the header
`SalesLineID,BatchID,SaleTimestamp,CustomerID,ProductID,Date,Quantity,UnitPriceMinorUnits,SalesAmountMinorUnits`.
Consume the same immutable bytes and manifest as the other demos; do not generate
or publish a separate data stream from this directory.
Manifest v1 provides top-level `contract_version`, `run_id`, `config`,
`destinations`, and `batches`; batches carry identity, filename, SHA-256, row
count, minor-unit totals, grouped totals, generation time and destination
delivery status/timestamp. Validation must include only `batch_class: measured`
in coordinated-run totals; `bootstrap` and `negative-test` are separate.
`batches` is an object keyed by batch number; group dictionaries are
`amount_by_date`, `amount_by_product_category`, and `amount_by_customer_region`.

## Verified capabilities and unresolved gates

| Operation | Documented API or capability | Required proof before deployment |
|---|---|---|
| Connection | A Lakehouse OAuth2 connection was created in the Fabric UI, read back via API, and used by a two-row Copy probe. The public [create-connection request](https://learn.microsoft.com/en-us/rest/api/fabric/core/connections/create-connection) lists OAuth2 as a credential type but does not define OAuth2 in its create-credentials union. | Automate a supported connection path or explicitly require the UI-created connection; verify identity and access in each environment. Never store credentials. |
| Pipeline | A non-sales probe used the event subject as a dynamic exact filename and Upserted two rows; its successful event-triggered job read one file. A top-level `keyColumns` property was accepted in a definition but failed at runtime; `upsertSettings.keys` worked for the probe. | Deploy and verify the guarded candidate with explicit sales mappings and composite `(BatchID, SalesLineID)` Upsert. The prior probe did not exercise the composite key or crash recovery. |
| Activator | A restricted non-sales `FileCreated` event invoked a parameter-only pipeline. `___subject` and `___id` matched source and activation history in that probe. A later event-triggered dynamic Copy captured the subject and ID after a six-minute warm-up. `activator.py` builds a disabled candidate. | Verify the actual production source and bindings and completed-file readiness. An event published after a 30-second warm-up appeared in source history but produced no activation or job within 300 seconds. A single six-minute success is not a readiness guarantee; definition readback or a Running label alone is insufficient. |
| Audit, recovery, serialization | Two wait-only jobs on one pipeline with `concurrency: 1` ran their activities sequentially. In the isolated Lakehouse, conditional create had one winner, ETag replacement worked, and stale replacement/deletion were rejected. A synthetic audit-journal probe exercised state transitions and duplicate recognition against OneLake. The guarded pipeline and recovery launcher remain uninstalled candidates. | Verify per-file audit, missed events and uncertain writes, safe serialization/exclusive writers, bounded retries, duplicate delivery, and write-then-crash recovery end to end. Neither the single-pipeline observation nor a cooperative lock excludes outside writers or establishes an onboarding barrier. |
| Query surface | Setup verified dimensions and the fact schema. A non-destructive SQL metadata refresh using actual lowercase Delta paths exposed all four tables; the Spark amount view is not exposed as a SQL view. A separate probe had stale SQL results for over 900 seconds until a targeted, non-recreating refresh. | Verify joined queries, DECIMAL(18,2) results, exact totals and visibility with valid sales data. The observed SQL endpoint is case-sensitive: use physical `dbo.factsales`, not `dbo.FactSales`. |

In an isolated runtime 2.0 probe, the default fact table had deletion vectors
enabled (Delta reader 3/writer 7). The Copy connector rejected its writer
features. A new empty test table created with deletion vectors disabled used
reader 1/writer 2. Setup requests that compatible protocol for new fact tables
and blocks on an incompatible existing table; it never downgrades or replaces
one. Composite Copy against that protocol still needs live verification.

On 2026-11-23, read-only MCAP cloud `supportedConnectionTypes` returned one
`Lakehouse` type with creation method `Lakehouse.Contents`, optional Boolean
`HierarchicalNavigation`, `EnableVorder`, and `OutputMetadataRefresh`, and
credential types `OAuth2` and `ServicePrincipal`; it did **not** list
`WorkspaceIdentity`. This generic metadata alone does not establish that the
Copy connector uses the same creation method or that any identity can create
and use that specific connection. The [connector overview](https://learn.microsoft.com/en-us/fabric/data-factory/connector-lakehouse-overview)
lists organizational account authentication for pipelines. `Connection.Read.All`
or `Connection.ReadWrite.All` permits metadata listing; creation requires
`Connection.ReadWrite.All` and appropriate Fabric permissions.

For this demo, notification delivery is assumed to work in general. One
preflight event was visible at the source without a corresponding activation
during a five-minute observation window; this is not proof of a general
delivery failure or a guarantee of later delivery.

OneLake's documented [`FileCreated`](https://learn.microsoft.com/en-us/fabric/real-time-hub/explore-fabric-onelake-events)
can mean create **or update**; rename is a different event. The current producer
conditionally PUTs the complete CSV to its final Blob path; a preflight PUT
emitted `FileCreated` with `data.api=PutBlob`, and a later GET confirmed its
bytes. This does not prove that the bytes were readable at event delivery.
An existing-file replay performs no new PUT and cannot rely on another event.
Event filters use case-insensitive
`subject` prefix and suffix comparisons; the pipeline must additionally reject
anything except a direct immutable `sales_<run>_<batch>.csv` child and safely
extract an exact file name. It must not read a wildcard or whole folder.

The schema-enabled Lakehouse tables REST endpoint returned
`UnsupportedOperationForSchemasEnabledLakehouse`. A read-only OneLake Delta
catalog request enumerated `dbo.factsales` with a Delta format and its
`Tables/dbo/factsales` location. This observation establishes the table identity
only, not ownership, a complete inventory, access by the runtime identity, or
exclusive writes.

Definition filter changes require a new Real-Time Hub source identifier and
rewiring references, per the [filter API guidance](https://learn.microsoft.com/en-us/fabric/real-time-hub/configure-event-filters-rest-api).
On deployment, use `activator-cli` authoring guidance, validate source and
captured fields, include its telemetry header on Fabric calls, handle 202 LROs
and 429 retries, and read back the definitions. Do not enable the rule until
the readiness and safe-write gates pass. Obtain approval before cross-workspace
permissions or chargeable resources. The user approved the UI connection
creation as a course correction; do not silently substitute another connection.

## Local checks

Run `python -m unittest discover -s demo-3-onelake-events-copy/tests -v` from the repository root.
The synthetic events in these tests exercise local structure only. Set
`DEMO3_PRODUCER_PATH` to the shared producer's `generator.py` to also test the
actual producer manifest and notebook packaging when the shared producer is not
present in the repository. No live presenter walkthrough
or automated cleanup is available until deployment is implemented and approved.

`batch_gate.validate_batch` is a local evidence check, not an installed
pipeline activity. Given an independently supplied shared-generator module and
approved `RunConfig`, authoritative manifest bytes, an exact source subject and
landed file bytes, it regenerates the expected batch in memory and compares the
entire byte sequence, batch metadata, grouped totals and generator fingerprint.
It rejects noncanonical paths and duplicate JSON keys. `submitting`, `submitted`,
`unknown` and `verified` are permitted for an existing exact file because the
event may arrive before the producer records the PUT outcome; `verified` is
producer file readback, not Copy completion. Recovery may omit the event ID,
but does not issue another PUT or Copy. The caller must independently verify
manifest and generator provenance, runtime access, file immutability and writer
exclusion; those and execution/target verification are not established by this
check.

`audit.BatchAudit` uses the shared producer's
`OneLakeStore` interface in the target Lakehouse. Its conditional writer lock
is bound to one pipeline run and batch; its per-batch record persists the event,
Copy intent and result, and target-verification evidence. It refuses to release
an in-flight or unverified Copy, refuses automatic replay of an uncertain write,
and recognizes a completed duplicate without a second Copy. Verification must
come from an independent target query, including the symmetric multiset
difference against all nine source columns, unique key range, orphan count, and
grouped totals. `guard_spark.py` supplies a candidate query, but it has not been
run end to end in the guarded pipeline. The implementation does not recover
stale locks or fence writers that ignore the lock. A lost response or crash requires
reconciliation; never automatically remove a stale lock. Isolated journal
probes used the interactive preflight principal, not the pipeline identity,
and synthetic Copy and query evidence. A client wrapper simulated losing the
response after real lock creation and Copy-intent writes; both retained the
lock and rejected a competing writer. The probes did not interrupt the network,
crash a process, perform Copy, or query the target. Their known test objects
were conditionally removed only after confirming no Copy had been sent.

`delivery_runtime.GuardedDelivery.reconcile` reads the authoritative manifest
and dispatches the same guarded pipeline for missing batches, with a durable
conditional submission intent. A lost response leaves that intent in place and
blocks automatic resubmission. Its local tests model missed events, late
duplicates and two batches with overlapping IDs; they do not prove live recovery.
The pipeline high-concurrency setting was enabled in the isolated preflight
workspace, but Spark session reuse has not been verified. This setting does not
establish event delivery.

`python demo-3-onelake-events-copy/reconcile.py --manifest <manifest.json> --query-export <results.json>`
compares measured batches only. The query export must contain `batches` keyed by
batch ID, with each value holding `row_count`, `sales_amount_minor_units` and
the three group dictionaries above. It must also include integer
`duplicate_batch_sales_line_ids` and `orphan_rows`. The query itself must restrict its
results and duplicate/orphan checks to the measured run. Count duplicates of the
pair `(BatchID, SalesLineID)`, not `SalesLineID` alone: every batch starts at ID 1,
and replay reuses the same IDs in that batch. Missing or extra
batches, non-integer values, and mismatches fail. This check does not prove
delivery, checksums, event execution, pipeline success, CSV row validity, or
query provenance; collect those independently.
