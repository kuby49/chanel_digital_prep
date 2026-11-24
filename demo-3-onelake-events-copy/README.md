# Demo 3: OneLake events and Copy

**Status: bounded customer-run admission verified; production ingestion remains blocked.**
With `demo-3.customer_preflight: true`, read-only `validate-only` checks the
original customer binding and the existing run-specific resources. It does not
claim the new file was delivered or that the ten-file acceptance run passed.
An isolated MCAP preflight
workspace, Lakehouse, OAuth2 connection, setup notebook, probe pipelines and a
stopped probe Activator exist. They do not constitute a deployable production
pipeline. The narrowly scoped customer preflight is read-only; `dry-run`
only validates the local contract.

## User-directed event-delivery assumption

For this demonstration, assume notifications are not missed, as directed by the
user. This is a working assumption, not measured evidence or a service
guarantee; implementing or proving missed-notification recovery is not a
readiness prerequisite. Monitor the actual event-to-Copy run and report any
visibly failed notification or Copy honestly. Retain normal Copy correctness,
duplicate-retry safety, target-row validation and safe stopping.

## Contract

`python deploy.py --mode {dry-run,validate-only,deploy,cleanup,resolve-target-candidate} --config <absolute JSON> --prefix <prefix> --state-dir <absolute directory> --result <absolute JSON>`. Cleanup requires `--yes`. The explicit customer preflight can report `ready: true` for its bound run after live checks. Other deployment, validation and cleanup paths remain blocked; cleanup never deletes anything.

The separately approved fresh run `customer_demo_20261124_dc2792654b5b` uses
the original ownership directory and the exclusive
`demo3-customer-customer_demo_20261124_dc2792654b5b-binding-state.json` journal,
configured by `demo-3.customer_binding_journal`. It preserves the stopped
predecessor journal and pins the complete canonical configuration hash. Other
run IDs and journal paths are not admitted. Rebinding the guard and disabled
rule requires coordinated journaled updates and readback before admission;
historical proofs do not establish new delivery.

Customer credential requests have a 120-second limit with no automatic retry.
Windows MSI installations use the bundled Azure CLI Python and temporary
output handles to avoid CMD and inherited-pipe timeout delays. The isolated
Azure configuration and existing tenant and principal checks are retained.

`resolve-target-candidate` is local-only. It reads a confirmed
`demo-3-state.json` under an exclusive `demo-3-state.lock` and creates a fresh
`demo-3-target-candidate.json` beside the result without overwriting an existing
file. Missing, invalid, pending, uncertain or conflicting ownership blocks the
operation; a stale lock is never cleared automatically. A successful candidate is
not proof of live ownership, access, inventory, exclusive writing or readiness.
The current deployment stub does not create an ownership journal or adopt the
preflight resources.

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
| Pipeline | A non-sales probe used the event subject as a dynamic exact filename and Upserted two rows; its successful event-triggered job read one file. A top-level `keyColumns` property was accepted in a definition but failed at runtime; `upsertSettings.keys` worked for the probe. An earlier target Copy failed before writing because its Delta protocol's `appendOnly` writer feature was unsupported (`dataWritten=0`). A new isolated table with deletion vectors disabled passed the direct protocol gate. The first synthetic Copy read one file and reported three rows; VerifyCopy initially rejected an HTTPS-prefixed OneLake endpoint. Fix `dfc89b7c2431369c4e3116c743e920ddda1a3cfc` normalized the endpoint, and a verification-only continuation completed batch `recoveryprobea9bbae81_0001` and its audit without another Copy. A second manually supplied synthetic job (`13f2398f-7599-465a-b1c2-2fc8b8ef5b85`) completed guarded Copy and audit; this was not an Activator-delivery test. Duplicate batch 1 job `eb2f7cb3-d8e4-4093-a510-82e35ab60bb6` ran PrepareCopy only and did not run Copy. Independent target notebook `7be33833-f22c-4fbd-8d5f-92c47c324a32` verified six rows, six unique `(BatchID, SalesLineID)` pairs, IDs 1–3 per batch, exact regenerated contents and dimension totals. | Composite-key Upsert and duplicate replay are proven for the two isolated synthetic batches only. Actual Activator delivery, measured readiness, and actual Spark session reuse remain unverified; do not imply otherwise. The user's no-missed-notification assumption does not waive monitoring or safe-stop checks. Do not downgrade existing table protocols. |
| Activator | A restricted non-sales `FileCreated` event invoked a parameter-only pipeline. `___subject` and `___id` matched source and activation history in that probe. A later event-triggered dynamic Copy captured the subject and ID after a six-minute warm-up. `activator.py` builds a disabled candidate. | Apply the user-directed no-missed-notification assumption for this demo only; do not require missed-notification recovery to be implemented or proven. Monitor actual event-to-Copy runs and report any visible failure honestly. The historical event with no activation/job within 300 seconds remains recorded; a single six-minute success is not readiness. Definition readback or a Running label alone is insufficient. |
| Audit, recovery, serialization | Two wait-only jobs on one pipeline with `concurrency: 1` ran their activities sequentially. In the isolated Lakehouse, conditional create had one winner, ETag replacement worked, and stale replacement/deletion were rejected. The synthetic two-batch run completed per-file audits; its duplicate batch-1 job ran PrepareCopy only, and the independent target notebook verified both batches. `audit.py` is not installed in a pipeline. | Duplicate replay is verified only for this isolated two-batch run. Measured readiness still requires safe serialization/exclusive-writer evidence, bounded retries, uncertain-write recovery, safe stopping and data correctness. Retain monitoring of actual pipeline/Copy outcomes. Neither the single-pipeline observation nor a cooperative lock excludes outside writers or establishes an onboarding barrier. |
| Query surface | Setup verified dimensions and the fact schema. A non-destructive SQL metadata refresh using actual lowercase Delta paths exposed all four tables; the Spark amount view is not exposed as a SQL view. A separate probe had stale SQL results for over 900 seconds until a targeted, non-recreating refresh. | Verify joined queries, DECIMAL(18,2) results, exact totals and visibility with valid sales data. The observed SQL endpoint is case-sensitive: use physical `dbo.factsales`, not `dbo.FactSales`. |

The earlier incompatible fact table had deletion vectors enabled and Delta
reader 3/writer 7; the Copy connector rejected its `appendOnly` writer feature
before writing data. A separately created empty table with deletion vectors
disabled read back reader 1/writer 2. Setup now requests this compatible
protocol for new tables and fails closed on an incompatible existing table;
it never downgrades or replaces that table. The first synthetic Copy against
the new table read one file and reported three rows copied. VerifyCopy
initially failed because the guard rejected an HTTPS-prefixed OneLake endpoint;
commit `dfc89b7c2431369c4e3116c743e920ddda1a3cfc` normalizes the endpoint, and
a verification-only continuation completed batch `recoveryprobea9bbae81_0001`
and its audit without another Copy. A second manually supplied synthetic job
completed guarded Copy and audit. A duplicate batch-1 job ran PrepareCopy only
and did not run Copy. Independent target notebook
`7be33833-f22c-4fbd-8d5f-92c47c324a32` verified six rows, six unique
`(BatchID, SalesLineID)` pairs with IDs 1–3 in each batch, exact regenerated
contents and dimension totals. This proves composite-key handling and replay
idempotency for these two isolated synthetic batches only; it does not prove
Activator delivery, measured readiness or actual Spark session reuse. The
user-directed no-missed-notification assumption is not a platform guarantee
and does not waive monitoring, correctness or safe-stop checks.

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

`audit.BatchAudit` is an unintegrated candidate using the shared producer's
`OneLakeStore` interface in the target Lakehouse. Its conditional writer lock
is bound to one pipeline run and batch; its per-batch record persists the event,
Copy intent and result, and target-verification evidence. It refuses to release
an in-flight or unverified Copy, refuses automatic replay of an uncertain write,
and recognizes a completed duplicate without a second Copy. Verification must
come from an independent target query, including the symmetric multiset
difference against all nine source columns, unique key range, orphan count, and
grouped totals. The implementation does not perform that query, recover stale
locks, or fence writers that ignore the lock. A lost response or crash requires
reconciliation; never automatically remove a stale lock. Isolated journal
probes used the interactive preflight principal, not the pipeline identity,
and synthetic Copy and query evidence. A client wrapper simulated losing the
response after real lock creation and Copy-intent writes; both retained the
lock and rejected a competing writer. The probes did not interrupt the network,
crash a process, perform Copy, or query the target. Their known test objects
were conditionally removed only after confirming no Copy had been sent.

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
