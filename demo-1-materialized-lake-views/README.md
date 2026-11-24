# Demo 1: file-backed materialized lake views

**Status: measured readiness blocked.** Live validation created the isolated
`chanel-demo1-mlv` workspace on the MCAP F8 capacity, a schema-enabled
`ChanelDemo1Lakehouse`, Bronze/Silver schemas, and static dimensions. Spark
readback confirmed 365 unique non-null date keys, 20 product keys, and 100
customer keys. A separate header-only bootstrap CSV enabled creation of the
Bronze and Silver MLVs; both initially read back zero rows. The executable
deployment and measured-run verification remain unimplemented.

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

`silver-candidate.sql` illustrates a fully qualified, SQL MLV using casts,
exact monetary values, and left joins with failure constraints. Its definition
and empty schema were validated in the isolated Fabric Lakehouse. Before
measured use, verify that
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

With the required features and approvals in place, verify: dimension sizes,
unique/non-null dimension keys, file checksums and delivered batch IDs; bronze,
rejected and silver count reconciliation; unique fact IDs and no orphans;
integer minor-unit totals and grouped aggregates by date, product category and
customer region against the source manifest; every batch within the effective
configured verification timeout (900 seconds unless explicitly overridden);
one separately identified malformed batch produces a visible failure or
recorded rejection; and replay/repeated refresh leaves counts unchanged.
Record first and subsequent batch arrival-to-silver min/median/max separately,
alongside refresh history and capacity contention. Repeat deployment to verify
idempotency without destructive replacement.

## Entry point

Invoke `python deploy.py --mode MODE --config ABSOLUTE_JSON --prefix PREFIX
--state-dir ABSOLUTE_DIR --result ABSOLUTE_JSON`, where `MODE` is `dry-run`,
`validate-only`, `deploy`, `cleanup`, or `resolve-target-candidate`; cleanup
also requires `--yes`. The
result uses shared contract version 1. Dry-run only checks the local contract
and never reports readiness. Deployment, validation and cleanup remain
blocked and perform no cloud operations. Cleanup is unavailable because
resource-scoped cleanup is not implemented.
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
source-schema declaration. All entry-point modes are local-only; the bootstrap
test was performed separately, not by this entry point.

The orchestrator supplies a stable prefix/tenant-scoped `--state-dir`, a
distinct per-invocation `--result` path, and an outer lock. Future deploy,
validate, and cleanup implementations must retain ownership and recovery state
in that stable directory across invocations and must not automatically discard
unresolved stale state. `resolve-target-candidate` reads the reserved
`demo-1-state.json` under an exclusive `demo-1-state.lock`. A stale lock or
pending/uncertain journal requires manual recovery; neither is discarded.
It validates tenant, prefix, configuration fingerprint, owner, creation
records, resource identity and parent, MLV fact and explicit configured IDs.
It atomically creates a non-overwriting `demo-1-target-candidate.json` beside
the result. Success means `pass` with `ready: false`, not cloud readiness.

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

**No deployment journal writer or recovery implementation exists yet.**
The journal schema defines what a future deployment must persist from
verifiable creation and readback; it must not be manufactured to adopt the
manual workspace. The supported source of an ownership marker still needs to
be established by that deployment. Local journal contents and the candidate
are not authenticated ownership evidence. Before readiness or registry
initialization, independently verify live target identity, definition and
access, inventory all retained keys with writers quiesced, and prove no writer
can bypass the shared allocator.

No presenter walkthrough can be executed until the blocker is resolved and
artifacts are deployed. Once available, use Lakehouse lineage and refresh
history for the walkthrough. Cleanup must target only recorded demo-owned
resources after explicit approval and an implemented ownership check.
