# Demo 1: file-backed materialized lake views

**Status: blocked, not deployed.** This directory contains a fail-closed
integration entry point and a candidate silver definition, not an operational
demo. No Fabric resources, schedules, producer runs, or data were created.

## Architecture and prerequisites

The intended path is: isolated producer identity -> completed CSV files in
`Files/incoming/sales/` -> file-backed bronze MLV -> joined silver MLV ->
Lakehouse queries. Three independently loaded static Delta dimensions feed the
silver MLV. The target is a schema-enabled Lakehouse on an approved active
capacity. Cross-workspace access, runtime and MLV feature eligibility must be
verified before deployment, and permissions require approval.

Use the shared version 1 CSV header:
`SalesLineID,BatchID,SaleTimestamp,CustomerID,ProductID,Date,Quantity,UnitPriceMinorUnits,SalesAmountMinorUnits`.
The source publishes UTF-8 header CSV, UTC timestamps and ISO dates; each
immutable file is staged and closed before delivery. No bootstrap file is part
of the measured run.

## Blocking gap and proposed definitions

[Microsoft's file-ingestion SQL reference](https://learn.microsoft.com/en-us/fabric/data-engineering/materialized-lake-views/create-materialized-lake-view)
documents `USING OneLake_Files`, CSV options, `schema_mode=FIXED`,
`refresh_mode=APPEND_ONLY`, and a FROM-less projection including `__filepath__`.
It does not document a way to declare the source column schema explicitly for
an empty folder. `FIXED` pins a schema; it is not documented as a schema
declaration. Empty-folder creation and materialization with the nine declared
columns remain unverified. Do not populate the folder with a seed file, invent
an unsupported option, or substitute notebook/Copy ingestion. Obtain supported
syntax and verify both creation and materialization in the approved environment
before marking this track ready. Specify header, comma, UTF-8, quote, escape,
null handling, `inferSchema=false`, and the corrupt-record behavior explicitly
once supported syntax is established.

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

The [public MLV API](https://learn.microsoft.com/en-us/fabric/data-engineering/materialized-lake-views/materialized-lake-views-public-api)
documents schedule create/read/list/update and job history for
`RefreshMaterializedLakeViews`. This is a documented path, not a tested
deployment. Configure a five-minute time-based schedule covering both views
in lineage order. End time must be at least planned last delivery plus the
effective interval plus processing margin; record the calculation and read
back the actual cadence and bounds. Check refresh history and actual mode;
append-only input alone does not establish incremental execution. A refresh
overlapping a preceding run may be skipped. Pause the demo-owned schedule after
the bounded run. Event-triggered refresh is preview and is not the baseline.

With the required features and approvals in place, verify: dimension sizes,
unique/non-null dimension keys, file checksums and delivered batch IDs; bronze,
rejected and silver count reconciliation; unique fact IDs and no orphans;
integer minor-unit totals and grouped aggregates by date, product category and
customer region against the source manifest; every batch within 900 seconds;
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
operations. Cleanup is unavailable because ownership has not been verified.
The config must contain `contract_version: 1`. The `--prefix` argument is the
effective prefix; deployment will need to expand workspace name templates with it.

No presenter walkthrough or cleanup can be executed until the blocker is
resolved, artifacts are deployed, and their ownership is verified. Once
available, use the Lakehouse lineage and refresh history for the walkthrough;
cleanup must target only recorded demo-owned resources after explicit approval.
