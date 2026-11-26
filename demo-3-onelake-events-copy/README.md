# Demo 3: OneLake events and Copy

For the simplified reset-and-run flow, see [SIMPLE-DEMO.md](SIMPLE-DEMO.md).
The isolated replacement candidate and publication helpers are experimental
and have not been verified against live replacement items.

**Status: the prepared bounded native demo was verified; general deployment remains blocked.**
The 2026-11-25 presenter-profile run had ten completed event-origin Copy jobs
matching the manifest. An independent Delta snapshot comparison matched all
nine typed columns for 10,000 rows; dimension-orphan checks remain
guard-reported. The run used F32, before the later F16 resize. These results do
not prove event-queue drainage, external-writer exclusion, or production
readiness. See the [presenter walkthrough](../README.md#presenter-walkthrough)
for the prepared window and closure.

The narrowly scoped customer and native preflights are read-only admission
checks; neither proves delivery for a future run. `dry-run` only validates the
local contract. A new operating window requires its own confirmed binding,
schedule and rule proofs.

## User-directed event-delivery assumption

For this demonstration, assume notifications are not missed, as directed by the
user. This is a working assumption, not measured evidence or a service
guarantee; implementing or proving missed-notification recovery is not a
readiness prerequisite. Monitor the actual event-to-Copy run and report any
visibly failed notification or Copy honestly. Retain normal Copy correctness,
duplicate-retry safety, target-row validation and safe stopping.

## Contract

`python deploy.py --mode {dry-run,validate-only,deploy,cleanup,resolve-target-candidate,provision-owned,verify-owned} --config <absolute JSON> --prefix <prefix> --state-dir <absolute directory> --result <absolute JSON>`. Cleanup and `provision-owned` require `--yes`. A v1 result reports `track: demo-3`. Deploy, general validation and cleanup return `blocked`; cleanup never deletes anything. Only an explicit customer or native binding preflight can return `ready:true`.

The separately approved fresh run `customer_demo_20261124_dc2792654b5b` uses
the original ownership directory and the exclusive
`demo3-customer-customer_demo_20261124_dc2792654b5b-binding-state.json` journal,
configured by `demo-3.customer_binding_journal`. It preserves the stopped
predecessor journal and pins the complete canonical configuration hash. The fixed-run preflight does not admit other run IDs or
journal paths. Rebinding the guard and disabled
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
`provision-owned` can create a separately journaled workspace and Lakehouse
shell; it does not adopt the preflight resources or establish pipeline readiness.

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

### Isolated replacement candidate

`isolated_candidate.build_candidate(spec, producer_dir, copy_activity, source_sample,
producer_config_path=..., producer_prefix=..., producer_state_dir=...)`
builds definitions in memory only, with `ready: false`. It does not create
cloud resources, publish, authorize or establish live readiness. Supply canonical IDs for `tenant_id`,
`owner_id`, `workspace_id`, `lakehouse_id`, `notebook_id`, `pipeline_id`,
`reflex_id`, `connection_id`, `producer_workspace_id`, and
`producer_lakehouse_id`, plus `prefix`, `lakehouse_name` and `native_policy`.
It resolves producer staging candidates using the frozen producer's
`resolve-candidate-ids` helper and its separate ownership journal. The helper
does not check live ownership or access. The old staging Lakehouse, original
target items and owner marker, inconsistent Copy endpoints, and an event from
another source are rejected. The existing generic OAuth2 connection can be a
candidate; its live type, path and identity, connection sharing and target
access must be independently checked. No Copy runtime success is implied.
The supplied Copy template must satisfy the exact-file, mappings and Upsert
requirements of `guarded_pipeline.build_pipeline`. The rule is disabled.
Returned hashes identify the generated definitions and producer files; they do
not attest to their ownership, validity or observed deployment. Generated
Activator entity IDs vary between builds, so retain the reviewed candidate.
An invented test event is useful only for local tests, not source validation.

`isolated_setup.prepare(config, prefix, state_dir, api, producer_dir)` freshly
reads the owned target shell through `ownership.provision(..., create=False)`
and builds an unexecuted setup notebook for it. It rejects the original
Lakehouse and marker and checks the runtime workspace and default Lakehouse
before setup code executes. It does not publish or run the notebook, and
neither the ownership read nor notebook creation proves setup or readiness.
`isolated_preflight.original_parent(api)` freshly checks both exact incident
jobs and their activity link; missing, active or ambiguous evidence blocks it.
Even terminal jobs do not prove all writers drained. `isolated_preflight.connection`
checks generic OAuth2 connection metadata and the caller's connection role;
it explicitly leaves runtime identity and target access unverified.

A separate Lakehouse isolates its Delta data and cooperative lock; it is a
fresh demonstration, **not recovery of the original run**. Shared capacity is
not compute isolation. The existing original-target publishers must not be
pointed at the new IDs. `isolated_items.create_item` can create individually
approved, owned setup notebook, guard notebook and pipeline shells, followed
by a Reflex shell only after the operator confirms a real new-target event was
observed. It records an intent before each create; `isolated_items.reconcile`
only reads back a pending create and never resubmits it. The event sample hash
binds the candidate to that operator-supplied observation; it is not itself
proof that the event occurred.

With all four owned shells and a reviewed candidate, `isolated_publish.Publisher`
can publish the setup notebook, guard notebook and pipeline definitions one at
a time with `approved=True`, journal each intent and acceptance, and compare
complete readbacks. `reconcile()` only polls or reads back; an unrecorded
acceptance remains blocked and is never resubmitted. These operations remain
unready and require an exclusive operator: the platform update is not
conditional on the preceding readback. They check fresh parent job linkage,
owned target and staging readbacks, connection and caller role, target jobs,
schedules and lock. They require the configured target and staging capacities
to be active F32 immediately before writes. The capacity must remain F32
indefinitely; these controllers never resize or suspend it and cannot prevent
another actor changing it. This requirement applies to this separate isolated
replacement workflow, which is blocked at the simplified demo's F16 default.
The item and publication workflows have only been tested locally, not against
the replacement's live items.

Publishing even the **disabled** Reflex remains blocked. `isolated_access.assess`
can inspect a controlled job's linkage but cannot prove the executed definition,
runtime token principal or target read/write from current evidence. A separate
approved F32 job must read a dedicated new-staging probe, write and read a
non-admitting object in the new target, and provide authoritative, independently
verifiable job-linked definition, identity and results. Establish connection
credential identity and actual Copy behavior separately. No proof writer,
rule enablement or live readiness is implied. Before any future deployment,
coordinate the sequence and authorization, in order:

1. Reconcile the original parent and any possible writers; preserve the old
   lock and incident evidence. Obtain explicit coordinated authorization.
2. Establish independent ownership and live readback of new resources and
   access. Verify a schema-enabled Lakehouse, fact protocol, dimensions and
   Lakehouse connection. Shell provisioning alone is not readiness.
3. Capture a real representative `FileCreated` event from the new target.
   Review a candidate and a purpose-built journaled publisher/controller;
   the existing native publishers and preflights are bound to original IDs.
4. Publish only under separate authorization and read back the notebook,
   pipeline and complete disabled rule against the reviewed definitions.
   Independently verify permissions, job inventory, locks and writer exclusion.
5. Bind the shared producer and fresh run to the new target with its own
   evidence and journal. Require separate coordinated approval before enabling
   admission or launching; verify actual delivery afterward.

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

The following probe observations and gates predate the verified native run
summarized above; they do not establish readiness in another environment.

| Operation | Documented API or capability | Required proof before deployment |
|---|---|---|
| Connection | A Lakehouse OAuth2 connection was created in the Fabric UI, read back via API, and used by a two-row Copy probe. The public [create-connection request](https://learn.microsoft.com/en-us/rest/api/fabric/core/connections/create-connection) lists OAuth2 as a credential type but does not define OAuth2 in its create-credentials union. | Automate a supported connection path or explicitly require the UI-created connection; verify identity and access in each environment. Never store credentials. |
| Pipeline | A non-sales probe used the event subject as a dynamic exact filename and Upserted two rows; its successful event-triggered job read one file. A top-level `keyColumns` property was accepted in a definition but failed at runtime; `upsertSettings.keys` worked for the probe. An earlier target Copy failed before writing because its Delta protocol's `appendOnly` writer feature was unsupported (`dataWritten=0`). A new isolated table with deletion vectors disabled passed the direct protocol gate. The first synthetic Copy read one file and reported three rows; VerifyCopy initially rejected an HTTPS-prefixed OneLake endpoint. Fix `dfc89b7c2431369c4e3116c743e920ddda1a3cfc` normalized the endpoint, and a verification-only continuation completed batch `recoveryprobea9bbae81_0001` and its audit without another Copy. A second manually supplied synthetic job (`13f2398f-7599-465a-b1c2-2fc8b8ef5b85`) completed guarded Copy and audit; this was not an Activator-delivery test. Duplicate batch 1 job `eb2f7cb3-d8e4-4093-a510-82e35ab60bb6` ran PrepareCopy only and did not run Copy. Independent target notebook `7be33833-f22c-4fbd-8d5f-92c47c324a32` verified six rows, six unique `(BatchID, SalesLineID)` pairs, IDs 1–3 per batch, exact regenerated contents and dimension totals. | Composite-key Upsert and duplicate replay are proven for the two isolated synthetic batches only. Actual Activator delivery, measured readiness, and actual Spark session reuse remain unverified; do not imply otherwise. The user's no-missed-notification assumption does not waive monitoring or safe-stop checks. Do not downgrade existing table protocols. |
| Activator | A restricted non-sales `FileCreated` event invoked a parameter-only pipeline. `___subject` and `___id` matched source and activation history in that probe. A later event-triggered dynamic Copy captured the subject and ID after a six-minute warm-up. `activator.py` builds a disabled candidate. | Apply the user-directed no-missed-notification assumption for this demo only; do not require missed-notification recovery to be implemented or proven. Monitor actual event-to-Copy runs and report any visible failure honestly. The historical event with no activation/job within 300 seconds remains recorded; a single six-minute success is not readiness. Definition readback or a Running label alone is insufficient. |
| Audit, recovery, serialization | Two wait-only jobs on one pipeline with `concurrency: 1` ran their activities sequentially. In the isolated Lakehouse, conditional create had one winner, ETag replacement worked, and stale replacement/deletion were rejected. The synthetic two-batch run completed per-file audits; its duplicate batch-1 job ran PrepareCopy only, and the independent target notebook verified both batches. The later native guard packages `audit.py`. | Duplicate replay was verified for the isolated two-batch run. Retain monitoring of actual pipeline/Copy outcomes, uncertain-write reconciliation, safe stopping and data correctness. Neither the single-pipeline observation nor a cooperative lock excludes outside writers or establishes an onboarding barrier. |
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

## Binding a bounded shared run

### Reusable native event path

Use `activator.build_native_definition(...)` and
`guarded_notebook.build_native_notebook(producer_dir, policy, workspace_id,
lakehouse_id, lakehouse_name, producer_workspace_id, producer_lakehouse_id)`
for a one-time, stopped binding on the existing owned items. The unchanged Copy
pipeline stays at concurrency one, with a single admitted filename and
`(BatchID, SalesLineID)` Upsert. The native source filters FileCreated events
under `/Files/incoming/sales/sales_` ending in `.csv`; the guard, not that broad
filter, authorizes the exact run and file. No definition edit is needed for each
subsequent native run or file.

The version-one policy pins `run_template` (batch class `measured`, start date
`2026-01-01`, seed 42, 365 dates, 20 products, 100 customers and 60-second
interval), `max_file_count:10`, `max_rows_per_file:1000`, a positive
`max_admission_seconds` covering publication plus queued Copy/drain, and
`writer_wait_seconds` between 1 and 300 (default 300). The admission lifetime
bound is distinct from the producer's run duration. Dimension/template changes
still need deliberate preparation and a new deployment-bound policy.

For each fresh native ID the producer must publish the authoritative
`Files/producer/runs/<run_id>/manifest.json` before the file PUT. In addition
to the existing v1 configuration, generator fingerprint, destinations and
batches, it contains:

```json
{
  "demo3_admission": {
    "version": 1,
    "state": "open",
    "workspace_id": "<owned Demo 3 workspace>",
    "lakehouse_id": "<owned Demo 3 Lakehouse>",
    "config_sha256": "<canonical complete RunConfig SHA-256>",
    "stop_config_sha256": "<canonical native projected-runtime document SHA-256>",
    "expires_at_utc": "<fixed timezone-aware UTC deadline>"
  }
}
```

Both hashes use sorted-key UTF-8 JSON with separators `(",", ":")`,
`ensure_ascii=False`, `allow_nan=False`, and no newline. They hash different
objects: admission `config_sha256` covers `dataclasses.asdict(RunConfig)`,
including the fresh run ID; `stop_config_sha256` covers the producer's native
runtime document. A stop object at `Files/producer/runs/<run_id>/stop.json`
must exactly contain `{version:1, run_id, config_sha256, nonce}`, with that
runtime-document hash and nonce `native-<first 32 hash characters>`. A valid
stop prevents new admission; a malformed or foreign object also fails closed,
rather than being silently ignored.

Only the producer may change its manifest under `FabricLedger` writer-lock
and ETag ownership. Successful publication leaves admission open through its
original expiry for queued delivery. Cooperative stop/failure closes it.
Force cancellation does not guarantee notebook `finally` executes. Existing
durable Copy intents can finish verification and release their owned lock
after stop/expiry; no new Copy intent is authorized. Legacy manifests without
the native admission remain blocked, including the old stopped attempts.

The native audit independently handles overlapping arrivals: only a definitive
conditional-create conflict is polled, with admission rechecks and observable
writer-wait messages. It never steals or deletes another owner's lock and never
retries an uncertain create. A contended event is persisted under
`Files/demo3-audit/waits/<pipeline_run_id>.json`. At the bounded timeout its job
fails with a durable blocked-event ticket; reconcile the writer and use the same
guarded recovery path. This is not an unlimited queue or a guarantee that every
Copy finishes inside 60 seconds. Keep the 60-second producer cadence; do not
silently stretch it to hide lock contention.

`native_stop.stop` provides an offline-tested barrier for cooperating native
guards after the producer has durably written the exact per-run stop marker.
It waits for their shared writer lock, rechecks that marker while holding the
lock, and conditionally releases only its own lock. This assumes the stop
marker remains immutable and every writer uses the guarded path. A timeout,
changed marker, or uncertain storage outcome requires reconciliation and
retaining the producer's lifecycle lock. The barrier does not establish that
Fabric's event queue or jobs are drained, and it is not a complete native
readiness or delivery-verification hook.

`native_demo3.py` contains producer hooks. Before generating a
reusable producer profile, each new authorized setup must pin
`demo-3.native_proof`: version, owned workspace/Lakehouse/guard/pipeline/Reflex/
connection IDs, owner marker, native policy and generator SHA-256, plus
`guard_sha256`, `pipeline_sha256`, and `graph_disabled_sha256`. The hashes
cover canonical JSON (sorted keys, compact UTF-8, no newline) of respectively
`_guard(notebook)`, `pipeline["properties"]`, and `_graph(graph, enabled=False)`
from **confirmed live readbacks** matching the owned deployment and its
journal. Do not treat the offline candidate as a confirmed readback.
Each native launch checks the pinned live definitions, enabled rule, connection,
ownership, writer quiescence and deterministic dimensions before publishing.
Verification requires matching producer bytes, completed event-origin audits,
completed pipeline jobs, and fresh exact Delta/dimension readback for every
batch. Failed or unavailable evidence blocks the run. These hooks and the
prepared profile were exercised by the verified F32 native runs; a new binding
requires fresh proof and the full F16 workload has not been tested.

`native_binding.py` is a publisher for this specific owned
binding. It checks the stopped predecessor,
ownership, target, job history and writer lock, records an intent before each
update, and requires operation success and matching readback. A lost response
without an operation identity remains blocked: do not resubmit it or create a
success-shaped journal by hand. Keep the immutable candidate and all pinned
inputs and journals together.

After separate authorization and coordination, run from this directory with
absolute paths to the immutable candidate, the native and fresh-run config
files, and the original state directory:

```powershell
python native_binding.py --mode publish --candidate <candidate.json> --native-config <native.json> --previous-config <fresh.json> --state-dir <state-directory> --yes
python native_binding.py --mode reconcile --candidate <candidate.json> --native-config <native.json> --previous-config <fresh.json> --state-dir <state-directory>
python native_binding.py --mode resume --candidate <candidate.json> --native-config <native.json> --previous-config <fresh.json> --state-dir <state-directory> --yes
python native_binding.py --mode proof --candidate <candidate.json> --native-config <native.json> --previous-config <fresh.json> --state-dir <state-directory> --output <new-proof.json>
```

Use `reconcile` only after an uncertain attempted update; it polls the recorded
operation and never resubmits. `resume` only submits a rule update that was
never attempted after a confirmed guard. Generate proof only after both
confirmed readbacks; it cannot overwrite an existing file. These commands do
not enable the rule or start jobs. They assume an exclusive operator: Fabric
definition updates have no established conditional-write protection, so an
external editor or writer can race the checks. A stopped rule, terminal job
listing and absent lock do not prove the event queue is drained.

### Rebinding a distinct staging Lakehouse

`native_rebind.py` prepares one separately journaled recovery after a confirmed
first rule-cycle disable. It keeps the original candidate, proof, publication,
correction, access and rule journals unchanged. It does not enable the rule or
start the sales producer. Use it only after the old producer is fenced and the
coordinator has confirmed fresh ownership, definitions, jobs, audit and locks.
The fence is cooperative, not a storage access-control or immutable-storage
guarantee.

Create a separate absolute-path specification with the independently confirmed
staging and target probe object fingerprints and expected pipeline storage
principal. Probe objects must be existing, non-admitting files; the staging
probe cannot be a run manifest. Do not copy these example placeholders into a
live run:

```json
{
  "version": 1,
  "producer_workspace_id": "<new staging workspace UUID>",
  "producer_lakehouse_id": "<new staging Lakehouse UUID>",
  "principal_id": "<expected pipeline storage principal UUID>",
  "producer_probe": {"path": "Files/probe/staging.json", "sha256": "<64 lowercase hex>", "etag": "<exact ETag>"},
  "target_probe": {"path": "Files/probe/target.csv", "sha256": "<64 lowercase hex>", "etag": "<exact ETag>"}
}
```

With the same immutable `@inputs` as the native publisher, prepare the
following PowerShell array and obtain the complete access-notebook baseline
fingerprint. Add that result as `access_baseline_sha256` to the specification
and freeze it before either update. Execute the later steps only after their
individual gates and separate coordination are satisfied:

```powershell
$rebind = @('--rebind-spec', '<spec.json>', '--previous-proof', '<first-cycle-proof.json>')
python native_rebind.py --mode inspect-access @inputs @rebind
python native_rebind.py --mode access-update @inputs @rebind --yes
python native_rebind.py --mode access-run @inputs @rebind --yes
python native_rebind.py --mode access-reconcile @inputs @rebind
python native_rebind.py --mode update @inputs @rebind --yes
python native_rebind.py --mode proof @inputs @rebind --output <new-proof.json>
```

`access-update` preserves every notebook part and checks complete readback.
`access-run` launches the existing one-activity pipeline **once**. Reconcile
until its exact pipeline run and activity-linked notebook job both complete;
the activity ID must match that notebook job ID. This match was observed for
the previous probe, not guaranteed by the API, so missing or different IDs
block. The notebook verifies pipeline context, storage principal, both
Lakehouse object hashes and exact ETags. It does not write an admission or
prove that a later producer run is authorized.

Only after that confirmed access proof does `update` replace the guard's
staging settings, preserving the rest of the full notebook definition.
`demo3-native-staging-access-rebind-state.json` and
`demo3-native-staging-rebind-state.json` pin the inputs and predecessor
journals. Each attempted mutation is submitted once. After a lost response,
use `--mode access-reconcile` or `--mode reconcile`, respectively; these poll
only recorded operation or job identities and never resubmit. An unknown
acceptance remains blocked. `.platform` is omitted from definition updates
because metadata updates are not requested, then must match in full readback.

For a separately authorized rebound second rule cycle, pass `--cycle 2` and
the same `@rebind` to `native_rule.py`; enable requires the new proof. Its
separate journal retains and verifies the complete Activator definition
before and after each change. For the common profile, set
`demo-3.native_rebind` to absolute paths under keys `candidate`, `native_config`,
`previous_config`, `spec`, `previous_proof`, and `proof`; set
`demo-3.native_proof` to the new proof and shared staging IDs to the new
Lakehouse. The preflight rejects a guard and producer bound to different
staging. A stopped second cycle cannot be admitted again as cycle 2.

After the producer owner reconciles the failed run and its launcher lock,
and the coordinator separately authorizes a retry, set
`demo-3.native_rule_cycle: 3` in the replacement profile. Reuse the same
rebound proof and lineage inputs; run `validate-only` against the retry
profile. It requires confirmed cycle-2 enable and disable, an exact stopped
definition, and fresh target, access and quiescence readbacks. Enable with
`native_rule.py --mode enable --cycle 3 @inputs @rebind --proof <rebound-proof.json> --yes`.
Keep `native_rule_cycle: 3` for later `validate-only` checks, and use
`--cycle 3` for the controller's `disable`, `reconcile` and `drain`
commands. The separate `demo3-native-rule-cycle-3-state.json`
pins the untouched second journal and, through it, the first. An attempted
cycle 3 cannot be relaunched or silently reused. Rule admission does not
reconcile the producer's lock or authorize producer launch.

For each separately authorized retry, select the next successive cycle in
`demo-3.native_rule_cycle` and `--cycle`, after the previous cycle is stopped
and the producer's launcher lock is reconciled. For example, after cycle 3
use cycle 4 and the same rebound inputs and proof. Each cycle has its own
`demo3-native-rule-cycle-<n>-state.json` journal, pins all predecessors, and
uses the same one-shot enable, readback, reconcile, disable and drain checks.
Do not overwrite existing journals or enable before coordination.

For ownership and job inventory, a Fabric Wabi redirect continuation is
accepted only when its collection path and sole opaque token match the response.
The next page is requested through `api.fabric.microsoft.com`, not the redirect
host. Missing, mismatched or repeated continuations block readiness and drain.

ETags and hashes are point-in-time checks, not WORM guarantees. An outside
writer can alter source bytes between guard validation and the Copy's later
filename read. The checks do not prove external-writer exclusion, event-queue
drainage, successful notification delivery or actual high-concurrency reuse.

For an unrebound native profile, set `demo-3.native_preflight:true`,
`native_binding_journal:"demo3-native-binding-state.json"` and `native_policy`
in the common configuration; remove `customer_preflight`. Standard
`deploy.py --mode validate-only ...` then verifies the native notebook, broad
event binding, unchanged exact-file Copy, ownership, dimensions and writer/job
quiescence without pinning a specific `data.run_id`. It cannot pass against
a fixed-run notebook and does not enable the rule or prove delivery.

The separately journaled `native_guard_hc.py` corrects the native guard once
by inserting `spark.conf.set("spark.fabric.session.highConcurrency.enabled",
"true")` into its generated cell. It verifies the original stopped proof,
unchanged candidate and ownership, terminal jobs and absent writer lock before
the update. Its `demo3-native-guard-hc-state.json` preserves both definitions
and the exact receipt; on an uncertain response use only `--mode reconcile`
with the same inputs, never resubmit. `native_binding.py --mode proof` requires
the confirmed correction journal and fresh readback, and creates a new proof
without changing the original. The flag and matching pipeline session tag do
not demonstrate actual high-concurrency execution or session reuse.

**Native operation after that deployment:** open the existing
[Demo 3 workspace](https://app.fabric.microsoft.com/groups/744223de-ceab-4c4f-82ba-f811bcb5c7d7/list?experience=real-time-intelligence).
Once separately authorized, use the same four immutable input arguments as
`native_binding.py` with the journaled rule controller:

```powershell
python native_rule.py --mode enable @inputs --proof <confirmed-proof.json> --yes
python native_rule.py --mode disable @inputs --yes
python native_rule.py --mode drain @inputs
```

Here `@inputs` is a PowerShell array containing `--candidate`, `--native-config`,
`--previous-config`, and `--state-dir`, each followed by its absolute path.
Enable checks the proof against fresh stopped readback and changes only
`Copy sales file`. The separate `demo3-native-rule-state.json` records the
first enable/disable cycle; leave the rule enabled across recurring launches
until it is time to stop it. After confirmed disable and coordinated shutdown,
a separately authorized second cycle can use `--cycle 2` with each of the
same commands. Its separate journal pins the unchanged first journal, requires
confirmed first-cycle enable and disable, and rechecks the stopped graph and
fresh proof before re-enabling. This does not establish that the event queue
has drained; do not reset either journal. The separately authorized third
and subsequent cycles described above are available only for the rebound
deployment. On an
interrupted update run `--mode reconcile @inputs` only: it uses the recorded
operation ID and Location and never resubmits. If acceptance is unknown,
stop and reconcile with the operator; neither a second submission nor a
graph snapshot can prove the first operation will not complete later.

One enabled cycle may cover a separately authorized, bounded operating window
with sequential distinct native runs. Keep the same cycle, deployment proof,
rule and staging binding across the Run clicks. Each run stops its own manifest
admission and executes the cooperating writer barrier through `native_demo3.stop`;
that hook does not disable the shared rule. Preserve previous manifests and stop
markers, use fresh run IDs, and require the existing owned-job/absent-lock
preflight before the next run. A queued old event may still start a guard job,
but its stopped admission cannot authorize a new Copy.

Declare the window's absolute UTC expiry and require each run's admission and
cleanup deadlines to fit within it. The Activator `shouldRun` setting has no
automatic TTL: the window owner must reject later launches and perform the
coordinated rule shutdown below. Expiry alone does not stop the rule or prove
event-queue drainage.

After confirming enable, launch the separately prepared native recurring
producer. Do not manually Run
`Demo3EventE2ePipelineA9bbae81` or `Demo3EventE2eGuardA9bbae81`; their parameters
come from events. Observe Activator history, pipeline monitoring, exact
manifest batch IDs, batch audits and any writer-wait tickets, then inspect
`Demo3EventE2eA9bbae81` / `dbo.factsales` by those batch IDs. At coordinated
window shutdown, stop the rule first, request the producer's cooperative stop,
and check drain after admitted work
has finished. Drain checks terminal owned jobs and the absent writer lock
at that moment; it does not prove that the Fabric event queue has drained.
If jobs or a lock remain, wait and repeat only the read-only drain check.
Do not clear an ambiguous writer lock. SQL metadata can lag Delta, so lack of
immediate SQL visibility is not proof of missing Copy. These are operator
steps, not an instruction to enable or run anything during preparation.

### Existing one-file binding

Choose the run ID once in the shared configuration. Use
`guarded_notebook.run_config_from_shared(generator, config)` with the actual
producer module, then pass that result to `guarded_notebook.build_notebook`
with both the target Lakehouse IDs and the **separate producer staging**
workspace and Lakehouse IDs. The packaged generator fingerprint must match the
authoritative manifest at `Files/producer/runs/<run_id>/manifest.json`.
`activator.build_definition(..., sample, run_id=run["run_id"])` produces a
disabled, run-filtered rule from a representative event in the target
Lakehouse. The pipeline must use the matching target, connection and guard.
These builders do not deploy or enable the assets.

An isolated pipeline notebook using the guard's storage-token mechanism read
an existing manifest from the separate producer staging Lakehouse and an
existing landing file from the target, verifying both hashes and the runtime
principal. This establishes access to those existing objects, not the
existence or permissions of a future run's manifest.

The owned guard and disabled Activator have been bound and read back for the
one-file, ten-row customer run `customer_demo_20261124_e0c409cc05ef`. This is a
small demonstration, not the separate ten-file acceptance run. The existing
guarded Copy pipeline is reused; enable the rule only after the producer and
other destinations are ready, then monitor and stop it after the run.

For this exact run only, `validate-only` accepts `demo-3.customer_preflight:true`,
prefix `chanel`, the original `demo3-customer-binding-state.json` directory,
and the pinned producer snapshot under `demo3-current-producer/demo-data-generator`
there. The `demo-3` object must identify the exact workspace, Lakehouse ID/name,
`destination_table:"dbo.factsales"` and `landing_path:"Files/incoming/sales"`.
The shared staging and target binding must match the notebook, with USD and a
positive `data.max_duration_seconds` no greater than 1800.

The read-only preflight checks live owned identities, exact packaged notebook
and pipeline definitions, connection, original dimension/access proofs, terminal
owned jobs and absent source/audit/writer lock. It accepts a stopped rule before
enable or the exact enabled rule after a confirmed journaled enable; stopped,
failed or uncertain runtime attempts cannot be silently relaunched. It does not
enable anything or require a future manifest/file, SQL synchronization or live
delivery. `ready:true` means admission to this bounded customer demonstration
only, not general deployment or completion of the acceptance run.

Customer preflight and its session controller bound each CLI credential request
to 120 seconds without automatic retry. On Windows they invoke the Azure CLI
MSI's sibling `python.exe -IBm azure.cli` directly, preserving `AZ_INSTALLER=MSI`
and the inherited `AZURE_CONFIG_DIR`; missing MSI Python blocks rather than
falling back to `az.cmd`. Other platforms keep the native `az` entry point.
Temporary output handles, rather than captured pipes, prevent inherited child
handles from delaying timeout cleanup. Fabric credential and JWT tenant/principal
checks and the storage credential tenant check remain required.

The first attempt was enabled and safely stopped after the common producer
blocked before submission; its journal must not be reset or reused. The only
additional approved attempt is `customer_demo_20261124_dc2792654b5b`, with the
same generation parameters and resources. Keep the original state directory,
ownership and proof files; set `data.run_id` to that value and
`demo-3.customer_binding_journal` to
`demo3-customer-customer_demo_20261124_dc2792654b5b-binding-state.json`.
No arbitrary run or journal path is admitted.

Create that fresh journal exclusively only when performing the coordinated
rebind, preserving the old definitions and predecessor journal. It must include
`config_sha256` for the final complete common config, `previous_journal` naming
`demo3-customer-binding-state.json`, and `previous_journal_sha256`. Both hashes
use UTF-8 JSON with sorted keys, separators `(",", ":")`, `ensure_ascii=False`,
and `allow_nan=False`, without a trailing newline. The predecessor must retain
its confirmed disabled/terminal/lock-absent result. The validator checks the
new run, full config hash, preserved predecessor and current definitions.
Changing any common config field after binding requires explicit reconciliation,
not overwriting the journal. Historical dimension/access proofs are not new
delivery evidence.

Before that rebind, a stopped read-only preparation check can verify the exact
old notebook/rule, terminal owned jobs, absent writer lock, and absent new
source/audit/staging-manifest paths, but it cannot report `ready:true` for the
new run. The guard must subsequently be rebound and read back for the new run,
and the Activator must get a new source/filter graph that remains disabled.
Enable only after the new run's stopped admission passes and the coordinator
provides a fresh deadline and all-paths signal.

The owned SQL endpoint's `dbo.factsales` visibility was restored with one
targeted `refreshMetadata` call using `recreateTables:false`: SQL subsequently
returned the three bootstrap facts totaling 116125 minor units. No tables or
data were recreated. This is a verified point-in-time recovery, not proof that
automatic SQL synchronization will stay current for the customer batch.

`setup_notebook.py --allow-dimension-expansion` is an explicit insert-only
option for a quiescent, owned existing target. It validates existing and
generated keys and all overlapping rows before writing, appends only missing
rows, and checks old rows and facts remain unchanged. It does not exclude
outside writers. If interrupted, inspect the Delta versions and partial results
before retrying. In the isolated target, the 2026 configuration added 365
dates, 17 products and 95 customers while retaining four earlier dates and
three bootstrap facts; exclude those facts from measured totals.

## Local checks

Run `python -m unittest discover -s demo-3-onelake-events-copy/tests -v` from the repository root.
The synthetic events in these tests exercise local structure only. Set
`DEMO3_PRODUCER_PATH` to the shared producer's `generator.py` to also test the
actual producer manifest and notebook packaging when the shared producer is not
present in the repository. The root
[presenter walkthrough](../README.md#presenter-walkthrough) covers the prepared
live window; general resource cleanup remains blocked.

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

`audit.BatchAudit` is used by the guard notebook with the shared producer's
`OneLakeStore` interface in the target Lakehouse. Its conditional writer lock
is bound to one pipeline run and batch; its per-batch record persists the event,
Copy intent and result, and target-verification evidence. It refuses to release
an in-flight or unverified Copy, refuses automatic replay of an uncertain write,
and recognizes a completed duplicate without a second Copy. Verification must
come from an independent target query, including the symmetric multiset
difference against all nine source columns, unique key range, orphan count, and
grouped totals. `guard_spark.py` performs the target query; the guard does not
recover stale locks or fence writers that ignore the lock. A lost response or crash requires
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
