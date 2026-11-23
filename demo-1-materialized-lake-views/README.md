# Demo 1: file-backed materialized lake views

**Status: blocked, not deployed.** Live validation created the isolated
`chanel-demo1-mlv` workspace on the MCAP F8 capacity, a schema-enabled
`ChanelDemo1Lakehouse`, Bronze/Silver schemas, and static dimensions. Spark
readback confirmed 365 unique non-null date keys, 20 product keys, and 100
customer keys. The empty-folder Bronze MLV was not created; no Silver MLV,
schedule, producer run, or sales data exists.

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

## Blocking gap and proposed definitions

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
Do not populate the folder with a seed file, invent an unsupported option, or
substitute notebook/Copy ingestion. The MLV, Silver definition, and managed
schedule cannot be validated until Microsoft documents or enables a supported
no-seed schema declaration; the deployment remains blocked.

`silver-candidate.sql` illustrates a fully qualified, SQL MLV using casts,
exact monetary values, and left joins with failure constraints. It is not
validated against Fabric and is **not deployed**. Before use, verify that
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
+ processing margin. Because the Bronze MLV does not exist, no schedule was
created and tenant-specific schedule readback remains untested. After the
blocker is resolved, inspect existing schedules before creating one and read
back actual cadence, lineage scope, and bounds.
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
`validate-only`, `deploy`, or `cleanup`; cleanup also requires `--yes`. The
result uses shared contract version 1. Dry-run only checks the local contract
and never reports readiness. Other modes are blocked and perform no cloud
operations. Cleanup is unavailable because resource-scoped cleanup is not
implemented.
The config must contain `contract_version: 1`. The `--prefix` argument is the
effective prefix; the SQL planning helpers expand `{prefix}` in the workspace
template and render a file-backed Bronze MLV definition and fully qualified
Silver MLV candidate. The effective per-batch verification timeout is the
positive integer in `demo-1.verification_timeout_seconds`, falling back to
`execution.verification_timeout_seconds`, then 900 seconds. Invalid values
fail closed. The Silver definition is not executed or validated by Fabric.
The Bronze definition sets `FIXED` and `APPEND_ONLY`, explicit CSV v1 options,
corrupt-row capture, and file traceability; it does not invent a
source-schema declaration. Dry-run is local-only; other entry-point modes
remain blocked and do not perform cloud operations. The approved live test
created isolated schemas and dimensions, but Bronze MLV creation failed with
the schema-inference error above.

No presenter walkthrough can be executed until the blocker is resolved and
artifacts are deployed. Once available, use Lakehouse lineage and refresh
history for the walkthrough. Cleanup must target only recorded demo-owned
resources after explicit approval and an implemented ownership check.
