# Chanel Fabric Demo Specification

## 1. Objective

Build, deploy, test, and document three reproducible demonstrations:

1. **CSV ingestion using Fabric Materialized Lake Views:** A source generates sales CSV files in a OneLake landing folder. A file-backed bronze materialized lake view ingests them into Delta format, and a silver materialized lake view validates, transforms, and joins the sales data to static dimensions.
2. **CSV ingestion into Eventhouse with OneLake availability:** The same source submits identical CSV batches to an Eventhouse KQL database. Enable OneLake availability and query the resulting Delta representation from a dedicated Lakehouse alongside the static dimensions.
3. **Event-driven CSV ingestion using Data Factory Copy activity:** The same source writes identical CSV batches to a OneLake landing folder. OneLake file events trigger a Fabric Data Factory pipeline through Activator, and its Copy activity loads the new file into a Lakehouse fact table.

Use one deterministic data generator and the same CSV contents for all three demos. Generate one new sales file every 60 seconds and preload three dimension tables (`DimCustomer`, `DimProduct`, and `DimDate`) into each demo's Lakehouse. Join facts to dimensions using `CustomerID`, `ProductID`, and `Date`.

The Fabric feature in Demo 1 is **Materialized Lake Views**, not Databricks Lakeflow. All three scenarios demonstrate recurring file-based ingestion; a one-minute source cadence does not imply one-minute downstream visibility or continuous row-level streaming.

## 2. Environment and resource isolation

Default to:

| Setting | Default |
|---|---|
| Environment | MCAP |
| Fabric capacity | `fabricadbmirror` |
| Deployment prefix | `chanel` |

Reuse only the approved tenant/subscription context and existing Fabric capacity. These demos do not require a Databricks workspace.

Create one dedicated Fabric workspace and Lakehouse per demo, plus one isolated shared producer workspace:

| Workload | Workspace name template | Required artifacts |
|---|---|---|
| 1 | `<prefix>-demo1-mlv` | Bronze and silver materialized lake views; managed refresh configuration |
| 2 | `<prefix>-demo2-eventhouse` | Eventhouse, KQL database, sales table, CSV mapping, and OneLake table shortcut |
| 3 | `<prefix>-demo3-copy-events` | Data Factory pipeline, connections, Activator rule, and ingestion audit state |
| Shared CSV producer | `<prefix>-demo-data-generator` | Producer notebook, Data Factory wrapper pipeline, staging Lakehouse, run manifest table/files, and bounded execution parameters |

The producer must run in Fabric in its own workspace by default. Do not place the recurring producer in any demo workspace because its lifecycle, credentials, and compute consumption must remain isolated from the three ingestion patterns. Its pipeline and notebook may be demo-owned infrastructure, but they must not write demo-specific transformed data or implement a demo's ingestion logic.

All demo tables, notebooks, pipelines, connections, and other artifacts must be newly created and independently owned. Apply the same rule to semantic models and reports if included. Use least-privilege access for the producer identity to the Demo 1 and Demo 3 landing folders and the Demo 2 ingestion endpoint.

List any additional shared infrastructure or identity required by the implementation and obtain approval before reusing or modifying it. Do not modify existing workloads, shared permissions, or capacity settings without approval.

Use a configurable deployment prefix and stable per-demo names. Reruns may reuse resources created by the same demo deployment but must not adopt unrelated existing resources.

## 3. Feasibility and prerequisites

Before deploying any demo:

- Confirm the active tenant, target workspace identifiers, and capacity assignment.
- Verify required permissions, authentication, network access, licensing, and feature availability.
- Verify that the Fabric producer identity can run the notebook/pipeline, write and read its staging Lakehouse, write the two destination landing folders, and submit CSV ingestion requests to Eventhouse. Obtain approval for every cross-workspace permission and connection.
- For Demo 1, verify Lakehouse schema support, a supported Fabric runtime, `USING OneLake_Files` CSV ingestion, and managed refresh support. Record feature-specific preview status and refresh restrictions.
- For Demo 2, verify automated CSV ingestion into the KQL database, OneLake availability, table-level mirroring-policy configuration, and Lakehouse access to the exposed Delta data.
- For Demo 3, verify OneLake event delivery for the chosen upload method, Activator pipeline actions, dynamic parameter binding, and Lakehouse Copy source/destination support. Keep the OneLake event source and consumer in the same region.
- Verify API coverage for each deployment/configuration step, including connector-specific authentication, supported identities, permissions, and feature status. Default to API automation using the applicable Fabric skills; identify any genuine API gaps before proposing UI interaction.
- Record additional compute or infrastructure needs and obtain approval before provisioning chargeable resources beyond the stated environment.

If a required capability is unavailable, report the exact blocker and supporting error or documentation. Do not silently replace materialized lake views with notebook ingestion, OneLake availability with a custom export, or event-driven Copy with scheduled polling. Continue the other demos if they are independent and feasible.

## 4. Configuration

Provide a documented configuration template covering:

- Tenant ID and subscription ID where applicable.
- Fabric capacity ID, per-demo workspace and Lakehouse names/IDs, schemas, and relevant item names/IDs.
- Demo 1 landing-folder URI, materialized lake view names, CSV options, and refresh configuration.
- Demo 2 Eventhouse/KQL database IDs, ingestion endpoint, table and CSV mapping names, delivery method, OneLake table path, shortcut name, and target write latency.
- Demo 3 landing-folder URI, pipeline and connection IDs, event source/filter, Activator rule ID, event-to-parameter mapping, destination table, and retry/idempotency settings.
- Shared producer workspace, staging Lakehouse, producer notebook, wrapper pipeline, destination connection/identity IDs, run-state storage, and maximum producer job duration.
- Deployment prefix, logical data run ID, sample-data seed, dimension sizes, date range, rows per file, file count, and generation interval.
- Per-demo verification timeout, validation polling interval, and bounded generator/refresh operating window.
- Authentication method and secret references.

Defaults must select the environment above. Resolve and record identifiers rather than relying solely on display names. Use ID-based OneLake paths where supported, such as `abfss://<WorkspaceID>@onelake.dfs.fabric.microsoft.com/<LakehouseID>/Files/incoming/sales/`.

Record the actual Eventhouse OneLake table path returned by Fabric; do not assume it has the same item identifier as a Lakehouse. Never store tokens or secrets in source control, configuration templates, logs, or diagrams.

## 5. Sample star schema and recurring CSV source

Use the same logical model and deterministic generator in all three demos, but create independent physical data and dimensions in each demo.

| Table | Grain | Key relationships |
|---|---|---|
| `DimDate` | One row per calendar date | Logical primary key: `Date` |
| `DimProduct` | One row per product | Logical primary key: `ProductID` |
| `DimCustomer` | One row per customer | Logical primary key: `CustomerID` |
| `FactSales` | One row per sales order line | Logical primary key: `SalesLineID`; foreign keys to each dimension |

### Required columns and types

| Table | Columns and Lakehouse types |
|---|---|
| `DimDate` | `Date DATE`, `Year INT`, `MonthNumber INT`, `MonthName STRING` |
| `DimProduct` | `ProductID INT`, `ProductName STRING`, `ProductCategory STRING` |
| `DimCustomer` | `CustomerID INT`, `CustomerName STRING`, `CustomerRegion STRING` |
| `FactSales` | `SalesLineID BIGINT`, `BatchID STRING`, `SaleTimestamp TIMESTAMP`, `CustomerID INT`, `ProductID INT`, `Date DATE`, `Quantity INT`, `UnitPriceMinorUnits BIGINT`, `SalesAmountMinorUnits BIGINT` |

Derive Lakehouse `UnitPrice` and `SalesAmount` as `DECIMAL(18,2)` from the integer minor-unit values using exact decimal arithmetic. Use one documented currency with two decimal places. Require `SalesAmountMinorUnits = Quantity * UnitPriceMinorUnits`.

Eventhouse must retain monetary values as KQL `long`, not floating-point `real`, so reference totals remain exact. Map integer IDs and quantities to `int`/`long` as appropriate, timestamps to `datetime`, and strings to `string`. Store `Date` as a normalized midnight date value or ISO date string and document its conversion for the Lakehouse join.

Treat keys as logical constraints and validate them independently of engine enforcement. Use fully qualified Lakehouse table/view names in Spark reads, writes, and SQL.

### Generator and delivery contract

- Default to 365 dates starting on a configurable date, 20 products, 100 customers, and seed `42`. Generate facts that exercise all dimension keys.
- Default to 10 files of 1,000 sales lines each, producing 10,000 sales lines. Publish the first file when the run starts and each subsequent file 60 seconds later. Make file count and rows per file configurable.
- Preload and validate dimensions before starting the live source. Keep them unchanged throughout the comparison.
- Run the generator as a Fabric notebook invoked by a Data Factory wrapper pipeline in `<prefix>-demo-data-generator`. Start it through the Fabric Jobs API or the top-level orchestrator's `--run-data` mode. A single invocation generates the configured number of batches and exits; no long-lived notebook session or open-ended schedule is required.
- Generate a batch in the producer Lakehouse staging area, validate and close it, calculate its checksum and reference totals, and only then deliver it to the three selected destinations. Publish the manifest alongside the staged CSV.
- Generate each batch once and deliver the same staged bytes to all three destinations. Demo 2 may use a different transport, but must ingest the CSV file contents rather than a different event payload.
- Use UTF-8 CSV with a header, comma delimiter, fixed column order, ISO dates, UTC timestamps, and documented quoting/null handling.
- Include a stable `BatchID` in every row and assign unique `SalesLineID` values across the entire run. Use immutable filenames such as `sales_<run-id>_<batch-number>.csv`.
- Record each file's checksum, row count, expected monetary totals, and generation/delivery timestamps in a source manifest. Delivery failures must be reported and retried using the same batch identity.
- Define a coordinated run explicitly. Deployment/bootstrap artifacts are not measured batch data. Complete all destinations, ingestion definitions, dimensions, Demo 1 MLVs, and event/schedule configuration before `--run-data` starts the timed 10-file run. The coordinated run still consists only of its measured batches. Demo 1 may use a separate bootstrap file before that run, first attempting a header-only CSV with the declared schema and then, only if needed, a valid one-row CSV.
- Classify the Demo 1 bootstrap separately as `bootstrap`, with its own immutable file and batch identity outside the coordinated run. Do not send it to the other demos. Exclude it from measured counts, exact reference totals, grouped aggregates and latency metrics; account for it explicitly in bronze, silver, rejections and all measured queries before the timed run begins. In particular, do not assume an append-only bronze output can discard a bootstrap row. If a valid row is required, use valid dimension keys and a distinct reserved fact key that cannot collide with measured-run keys. Verify actual behavior and record any bootstrap cleanup or exclusion rather than assuming either works.
- If the one-row fallback was needed and the views have been created, test removing only that bootstrap CSV and issuing a full refresh before the measured run. Verify the actual refresh mode and clean bronze and silver counts. If empty-source full refresh fails or leaves bootstrap data, retain explicit bootstrap accounting and exclusion; do not assume file deletion removed previously materialized rows.
- Complete and close uploads before processing. Validate the upload method's file-event behavior; do not treat initial file creation as proof that all bytes are readable.
- For Demo 1 and Demo 3, prefer a temporary location outside the subscribed folder followed by an atomic final-name move/copy into the OneLake landing folder. Event filters must ignore staging extensions such as `.tmp` and fire only for completed `.csv` files.
- Use notebook parameters for run ID, seed, file count, interval, batch size, enabled demo destinations, and maximum run duration. Default to 10 files at 60-second intervals. Persist progress after each completed delivery so a rerun resumes with the same batch identity rather than regenerating a batch.
- Replaying an existing logical run must not generate new batch identities or silently double-count sales. Intentional new data requires a new run ID.

Use a default verification timeout of 900 seconds per batch from successful delivery to that demo's required query surface. Configure changes before execution. Record source-delivery skew separately when comparing latency.

## 6. Demo 1: CSV ingestion using Fabric Materialized Lake Views

### Required flow

1. Create the dedicated workspace, schema-enabled Lakehouse, landing folder, bronze/silver schemas, and static dimension tables.
2. Create a file-backed bronze materialized lake view with `USING OneLake_Files`. Configure the fixed schema and CSV parsing explicitly, use `APPEND_ONLY` file refresh mode, and preserve `BatchID` and `__filepath__` for traceability. The empty-folder attempt failed in the target environment; a separately classified bootstrap is permitted before creation. Try a header-only CSV first; if that does not work, try a valid one-row CSV, subject to the isolation and accounting rules above. Verify whether each attempt succeeds and document its actual result. If neither works, report the feature as blocked rather than replacing the required MLV mechanism.
3. Create a silver materialized lake view that reads bronze, casts and validates the fact columns, derives exact decimal amounts, and joins all three dimensions. Preserve fact keys and dimension attributes in the joined output.
4. Use explicit failure or recorded rejection handling for malformed rows, invalid keys, and invalid quantities/amounts. Do not allow inner joins or quality filters to hide lost facts; reconcile bronze, rejected, and silver row counts, identifying bootstrap rows separately from measured rows.
5. Configure Fabric-managed lineage refresh for bronze and silver in dependency order. After a one-row bootstrap, test the user-requested bootstrap-file removal and full refresh as described above; record the mode and results rather than assuming it succeeds. Default to a five-minute time-based schedule with a bounded end time; record the actual supported cadence. Do not implement ongoing refresh as a custom notebook polling loop. Set the schedule end to at least `last delivery time + one effective refresh interval + processing margin`; record that calculation so the final batch receives a complete refresh opportunity.
6. After all demos report ready, start the 10-file coordinated CSV run. Show that subsequent managed refreshes discover new files and update both layers without recreating the views.
7. Query the silver output from the Lakehouse and verify batch contents, totals, and dimension joins. Demonstrate a repeated refresh without duplicate sales rows.

Document the view definitions, lineage, source folder, CSV schema, data quality handling, refresh history, and observed arrival-to-silver latency. Current documentation states a minimum effective scheduled refresh interval of five minutes; do not promise that every one-minute file arrival produces a separate refresh.

Event-triggered managed MLV refresh may be demonstrated only as an explicitly configured variation, with its preview prerequisites and generated supporting artifacts documented. It must not replace the baseline without recording the configuration change.

### Acceptance criteria

- Bronze ingests CSV files directly through the file-backed MLV feature, without an intermediate Copy activity or notebook load.
- Bronze and silver materialized outputs exist as queryable Delta data, and lineage shows their dependency.
- Every valid measured batch appears exactly once in the silver output within the configured timeout; any bootstrap row is separately identified and accounted for.
- All three dimension joins preserve the expected fact row count, with no duplicate fact keys or orphaned foreign keys.
- Exact totals and aggregates by date, product category, and customer region match the source manifest.
- Refresh evidence identifies actual refresh modes and timings; do not claim incremental execution solely because source files are append-only.

## 7. Demo 2: CSV ingestion into Eventhouse with OneLake availability

### Required flow

1. Create the dedicated workspace, Eventhouse, KQL database, sales table, and Lakehouse. Preload the same three dimensions into this Lakehouse.
2. Define the KQL table schema and explicit CSV ingestion mapping, including header handling and the integer monetary columns.
3. Configure a supported automated file-ingestion route, such as the Kusto ingestion SDK submitting the generated CSV files. Record the endpoint, authentication, staging/storage requirements, batching settings, and ingestion status tracking. A manual file upload alone does not satisfy recurring ingestion.
4. Enable OneLake availability for the sales table. Set `TargetLatencyInMinutes=5`, the minimum supported configured synchronization target, on this demo-owned table and record the applied mirroring policy. Treat this as a target rather than a guaranteed SLA. If the configuration or applied policy cannot be verified, report a blocker rather than assuming OneLake visibility.
5. Start the source and ingest each CSV batch at the one-minute cadence. Track ingestion success and validate facts in KQL using exact integer totals. Use documented batch-deduplication support and verify replay behavior; do not assume exactly-once ingestion.
6. Discover the exposed OneLake Delta table path and create a table shortcut in the dedicated Lakehouse. Read it without a custom export or second Lakehouse data load.
7. Join the shortcut facts to the three Lakehouse dimensions using documented type conversions. Derive decimal sales values on the Lakehouse query side.
8. Verify each new batch in both KQL and the Lakehouse query result. Successful verification requires data visibility within the configured timeout, but latency measurements are not a deliverable for this demo.

Document that OneLake availability exposes a managed, read-only Delta representation of Eventhouse data, not the original CSV files. A shortcut avoids an additional user-managed copy into the Lakehouse; it does not mean the Eventhouse-to-OneLake materialization itself is instantaneous or copy-free.

Current documentation allows a OneLake target latency of five minutes to three hours and describes adaptive batching. The configured minimum of five minutes cannot be reduced to three minutes; a three-minute request is outside the documented supported range. The target is not a guaranteed deadline. Document retention coupling, relevant operation restrictions, and the small-file trade-off of selecting the minimum target.

### Acceptance criteria

- The same generated CSV batches are ingested automatically into the intended KQL table with the specified schema and mapping.
- KQL batch counts and exact monetary totals match the source manifest.
- OneLake availability is enabled and evidenced by a readable Delta table and recorded mirroring policy/status.
- The Lakehouse shortcut reads that managed Delta representation, and all three static dimension joins produce the expected results.
- Each batch becomes visible on the required Lakehouse query surface within the configured timeout; report failures without collecting or reporting propagation-latency statistics.
- Replaying a batch does not double-count sales in the validated fact result.

## 8. Demo 3: OneLake event-triggered Data Factory Copy ingestion

### Required flow

1. Create the dedicated workspace, Lakehouse, landing folder, static dimension tables, and typed destination fact table.
2. Create the required demo-owned Lakehouse connections through the Fabric Connections REST API using a supported authentication method. Deploy a `DataPipeline` item with a Base64-encoded `pipeline-content.json` definition containing the Copy activity, a Lakehouse `Files` source using `DelimitedText`, a Lakehouse `Tables` destination, and explicit column mappings.
3. Parameterize the exact source file path and event identity. Configure an idempotent write strategy, preferably Copy upsert keyed by `SalesLineID` where supported, and prevent conflicting writes to the same Delta table. Record any supporting audit/validation activities.
4. Use the `activator-cli` skill and Activator REST create/update-definition APIs to deploy the OneLake event trigger. Its `ReflexEntities.json` must include a `realTimeHubSource-v1` source for `Microsoft.Fabric.OneLakeEvents`, the source Lakehouse item ID, a `FileCreated` subscription, an enabled event rule, and a pipeline invocation action. Filter out unrelated folders, dimension files, temporary files, and destination-table changes to avoid loops.
5. Bind the event's file location and identity to the pipeline's declared parameters. Capture a real event payload and document the exact property mapping; do not assume Azure Blob trigger expressions apply to OneLake events.
6. Read back and validate the deployed pipeline and Activator definitions, including connection IDs, source filters, enabled rule state, action target, and parameter bindings. Start the source and write each completed CSV file at the one-minute cadence. Verify readable file readiness before Copy. Use bounded retries with explicit failure reporting when data is not yet available.
7. Copy only the event-associated file, not the entire accumulated folder. Preserve `BatchID`, validate the copied row count, and record event ID, source path, pipeline run ID, and batch outcome.
8. Query the loaded facts joined to the three static dimensions and derive exact decimal monetary values. Demonstrate duplicate event/retry handling without duplicate sales.

The live ingestion path must be driven by OneLake events through Activator, not a timed pipeline schedule or manual trigger. Copy activity must perform the CSV-to-Delta load; supporting checks must not silently replace it with notebook ingestion.

The pipeline UI's **Trigger** wizard is an alternative authoring experience, not a required deployment step. For API-first deployment, the Activator definition owns the underlying event subscription; do not invent a separate pipeline-subscription API. Use the Job Scheduler API to inspect the event-triggered pipeline job and confirm the Copy result. A successful definition upload alone does not prove event delivery or pipeline execution.

Do not claim exactly-once event delivery. A completed-file processing record alone is not sufficient protection against a retry after a write succeeds but before the record is committed; the destination write/recovery strategy must also be idempotent.

### Acceptance criteria

- A new file produces a captured OneLake event, an Activator action, and a correlated pipeline run without manual intervention.
- The Copy activity reads the intended file and loads its rows into the intended Delta fact table.
- Every valid batch is present within the configured timeout, and all previously loaded batches remain intact.
- Repeated delivery of an event or a retry after a successful write leaves fact counts and totals unchanged.
- Exact totals and dimension-filtered aggregates match the source manifest, with no duplicate fact keys or orphaned foreign keys.
- Evidence separates file-delivery time, event arrival, pipeline start, Copy completion, and query visibility.
- Pipeline, connection, and trigger setup is reproducible through documented APIs. Record any approved UI-only exception and its exact API limitation; do not count an untested API definition as a completed deployment.

## 9. Deployment and parallel execution

### API-first and skill-driven implementation

- Prefer supported Fabric REST/data-plane APIs over UI configuration for all three demos, including resource creation, artifact definitions, ingestion, configuration, validation, and cleanup.
- Use the applicable available Fabric skills as the primary implementation guidance, including `activator-cli` for event triggers, `eventhouse-cli` for KQL ingestion and policies, and `spark-cli` for supported Spark/MLV operations. Apply the Fabric notebook workflow skill whenever notebook authoring or publishing is involved.
- Where no applicable skill covers an operation, use the documented Fabric APIs and available tools. Do not use a similarly named skill for a different workload or assume that a skill's presence proves API support.
- During preflight, maintain a step-by-step automation matrix for every required operation with: supported API or skill operation, endpoint/command, identity support, required permission, idempotency behavior, response/LRO handling, and a verification readback. A linked UI feature page is not evidence of API support. Managed MLV schedule creation/update must remain unclaimed until a supported automation path is verified.
- For Demo 3, automate pipeline creation/update through item-definition APIs, connection creation through `POST /v1/connections`, and trigger creation/update through Activator (`Reflex`) APIs. Keep pipeline and trigger definitions as parameterized deployment assets.
- Handle pagination, `202` long-running operations, throttling, and explicit errors. Retrieve deployed definitions and verify runtime behavior after changes.
- If no supported automation path exists for a step, classify that step as an explicit approved exception before deployment. Record the documentation or sanitized error, obtain approval for the smallest necessary UI interaction, and document the manual procedure plus a readback or runtime verification. Manual schedule configuration must be identified as a limitation in the final report, not as successful API deployment.
- Do not silently fall back to browser automation or undocumented portal endpoints. If the deployment orchestrator cannot safely pause at and resume after an approved manual step, report that demo as blocked rather than marking it deployed.

### Execution and isolation

- Place each demo in its own subdirectory under the main demo directory.
- Use the following repository layout:
  - `demo-1-materialized-lake-views/` for Demo 1.
  - `demo-2-eventhouse/` for Demo 2.
  - `demo-3-onelake-events-copy/` for Demo 3.
  - `demo-data-generator/` for the shared deterministic CSV generator, schemas, source manifest writer, and delivery utilities.
  - `scripts/` for top-level orchestration, prerequisite checks, deployment, validation, and bounded cleanup helpers.
- Keep demo-specific code, configuration, definitions, validation, documentation, and evidence inside the corresponding demo folder. Only reusable source-generation and common delivery primitives belong in `demo-data-generator/`.
- Do not duplicate the generator under individual demos. Each demo must consume it through a documented, versioned interface and retain its own configuration and delivery state.
- Run the three implementation/deployment tracks in parallel where tooling permits and their prerequisites are satisfied. Coordinate the common generator only after all active destinations are ready.
- Develop and implement the three demo tracks in parallel as independent workstreams with separate ownership boundaries, configurations, and validation paths. Changes to shared contracts must be reviewed against all three demos before use.
- Create a top-level `scripts/deploy-demos` deployment entry point that accepts a demo selector and supports `all`, `demo-1`, `demo-2`, `demo-3`, and any explicitly documented comma-separated combination. The selected demos must deploy in one command using the same configuration schema and authentication context.
- In `all` mode, execute independent demo deployments in parallel by default, with a bounded configurable concurrency limit. In single-demo or partial-selection mode, deploy only the selected demo tracks. Do not require a user to manually orchestrate separate scripts.
- The orchestrator has two phases: deploy/verify all selected destinations and the shared producer, then optionally use the Fabric Jobs API to start the producer pipeline with `--run-data`. It must not start measured data while any required selected demo is still deploying, awaiting approval, or reporting not-ready. For a subset whose remaining demos are intentionally excluded, readiness applies only to the selected set.
- Each demo must expose an idempotent deployment interface consumed by the top-level script. The orchestrator must aggregate exit codes and diagnostics, preserve per-demo logs and manifests, and report the final pass/fail/blocked status for every selected demo without masking failures in another parallel track.
- Support `--dry-run`, `--validate-only`, `--deploy`, and `--cleanup` modes, an explicit `--yes` confirmation for cleanup or destructive replacement, and a common deployment prefix/configuration file. Runtime defaults must remain bounded and must not invoke the producer pipeline unless `--run-data` is supplied. The producer pipeline run must terminate after the configured file count or maximum duration and must not rely on an always-on notebook schedule.
- Resolve shared prerequisites once where safe, but do not allow one demo's optional failure to block independent demos unless the failed prerequisite is truly shared. Dependency failures must be attributed to `shared`, `demo-1`, `demo-2`, or `demo-3`.
- Keep resource names, configuration outputs, and execution state separate.
- Serialize any unavoidable changes to shared infrastructure or permissions.
- Make reruns safe: detect owned resources, avoid duplicates, and require explicit confirmation before destructive replacement.
- Do not let one demo's cleanup or failure disrupt the others. Track failed deliveries per destination so healthy demos can continue.

The source code in `demo-data-generator/` is shared repository code that is deployed to the dedicated producer workspace as the producer notebook/pipeline. It is not a local runtime dependency during the presentation.

## 10. Validation and evidence

For each demo, provide executable checks where supported and explicit manual checks otherwise:

- Validate the preloaded dimension sizes, unique/non-null dimension keys, unique fact keys, and zero orphaned foreign keys.
- Verify each expected batch, source checksum/delivery record, accepted row count, and final total of 10,000 valid sales lines for the default run.
- Compare exact integer minor-unit totals and corresponding decimal Lakehouse values. Validate grouped aggregates by date, product category, and customer region.
- Verify the required ingestion mechanism, refresh/event history, and downstream-change tests for that demo.
- Test at least one malformed or schema-incompatible CSV in a separately identified negative-test batch. Require an explicit failure or recorded rejection; exclude it from successful-run reference totals.
- Replay a successful batch or equivalent retry and prove there is no double counting.
- A second deployment run without duplicate resources or unintended data loss.
- Evidence of resource isolation and cleanup scope.

Provide a cross-demo comparison:

| Measure | Demo 1 | Demo 2 | Demo 3 |
|---|---|---|---|
| Ingestion mechanism | File-backed MLV | KQL CSV ingestion | Pipeline Copy |
| Activation | Managed lineage refresh | Source submission and ingestion service | OneLake event and Activator |
| Required final query surface | Silver MLV | Lakehouse join over OneLake shortcut | Lakehouse join over copied facts |
| Data storage/movement | CSV to bronze/silver Delta | CSV to KQL, then managed OneLake Delta representation | CSV to Lakehouse Delta table |
| Observed outcomes | Counts, totals, latency, failures | Counts, totals, failures | Counts, totals, latency, failures |

For Demo 1 and Demo 3, measure first-batch and subsequent-batch latency separately and report minimum, median, and maximum batch latency. Do not measure or report synchronization latency for Demo 2. Record shared-capacity contention when tracks run concurrently; do not present these small demo measurements as a controlled performance benchmark.

Record pass/fail/blocked status, actual results, UTC timestamps, configured timeouts, and sanitized error details. Set and document a numeric verification timeout before execution; do not extend it silently to turn a failed check into a pass.

## 11. Deliverables and completion

Each demo directory must include:

- A README with prerequisites, configuration, deployment steps, a presenter walkthrough, known limitations, and troubleshooting.
- API-based deployment scripts/notebooks, parameterized item definitions, and documented manual steps only for approved API gaps.
- A per-demo deployment entry point that can be invoked independently and through the top-level all-demo orchestrator. It must honor the same dry-run, validation, configuration, logging, and idempotency contracts.
- Access to the common CSV generator, source manifest, and demo-specific validation scripts/queries.
- A sanitized manifest of created resource names/IDs and relevant configuration.
- An architecture diagram in editable Mermaid form showing resource boundaries, identity/authentication, storage, compute, and data access or movement.
- Test results and supporting evidence.
- Cleanup instructions or scripts limited to that demo's owned resources, with an explicit confirmation step.

Provide a top-level README linking all three demos, explaining the ingestion patterns and latency trade-offs, and describing how the Fabric-hosted producer is deployed and started through `--run-data` using the same staged CSV bytes for every selected destination.

Completion requires deployed resources, executed validation, recorded outcomes, and reproducible documentation. Clearly distinguish successful demonstrations from blocked or unmet objectives. Leave successful demo resources and ingested data available for presentation; do not run cleanup automatically.

After the bounded test run, verify that the producer pipeline job has completed and no producer notebook session remains active. Pause demo-owned schedules/event rules to avoid ongoing compute use. If a recurring producer schedule is added as an optional variation, it must have explicit start/end bounds and be disabled by default. Document how to start a new bounded producer run for a presentation without rebuilding resources.

## 12. Authoritative references

- [Overview of Fabric Materialized Lake Views](https://learn.microsoft.com/en-us/fabric/data-engineering/materialized-lake-views/overview-materialized-lake-view)
- [Spark SQL reference, including `USING OneLake_Files`](https://learn.microsoft.com/en-us/fabric/data-engineering/materialized-lake-views/create-materialized-lake-view)
- [Get started with Materialized Lake Views](https://learn.microsoft.com/en-us/fabric/data-engineering/materialized-lake-views/get-started-with-materialized-lake-views)
- [Schedule a Materialized Lake View refresh](https://learn.microsoft.com/en-us/fabric/data-engineering/materialized-lake-views/schedule-lineage-run)
- [Ingest file data into a KQL database](https://learn.microsoft.com/en-us/fabric/real-time-intelligence/get-data-local-file)
- [Enable OneLake availability for Eventhouse](https://learn.microsoft.com/en-us/fabric/real-time-intelligence/event-house-onelake-availability)
- [Build event-driven pipelines with OneLake events](https://learn.microsoft.com/en-us/fabric/real-time-hub/tutorial-build-event-driven-data-pipelines)
- [Fabric OneLake events in Real-Time hub](https://learn.microsoft.com/en-us/fabric/real-time-hub/create-streams-fabric-onelake-events)
- [Trigger Fabric items and pass parameters with Activator](https://learn.microsoft.com/en-us/fabric/real-time-intelligence/data-activator/activator-trigger-fabric-items)
- [Configure the Lakehouse connector in a Copy activity](https://learn.microsoft.com/en-us/fabric/data-factory/connector-lakehouse-copy-activity)
- [REST API capabilities for Fabric Data Factory pipelines](https://learn.microsoft.com/en-us/fabric/data-factory/pipeline-rest-api-capabilities)
- [Create a Fabric connection through REST](https://learn.microsoft.com/en-us/rest/api/fabric/core/connections/create-connection)
- [Automate Azure and Fabric event consumption through REST APIs](https://learn.microsoft.com/en-us/fabric/real-time-hub/automate-event-consumption-rest-api-cicd)
- [Deploy OneLake event subscriptions and pipeline actions with Activator REST APIs](https://learn.microsoft.com/en-us/fabric/real-time-hub/consume-events-activator-rest-api)

Recheck preview status and prerequisites during deployment; documentation is not evidence that a feature is enabled in the target tenant.

## 13. User decisions and course corrections

- Use the signed-in MCAP identity where the relevant operation supports it.
- The user permits starting and stopping resources needed for the demonstrations and scaling the approved capacity up to F64. Isolation, least-privilege and verification requirements remain in effect.
- After testing, leave the capacity running at its final size. This does not change the requirement to stop the bounded producer and pause demo-owned schedules and event rules.
- The user granted upfront approvals needed to test these demos live, including cross-workspace access. Limit permissions and changes to what the demos actually require, record them, and do not modify unrelated workloads. This approval does not establish feature availability or permit skipping readiness and runtime verification.
- Course correction: the original specification required Demo 1 MLV creation against an empty folder and prohibited seed data. After the empty-folder fixed-schema attempt failed, the user directed a separately classified bootstrap CSV, initially header-only: "do the thing with the separate bootstrap CSV (try to use an empty file, headers only. Shoudl work as you know the sample data column types, isnt it ? )". The user then explicitly allowed a fallback: "if empty file not enough, than use a CSV with one row". These decisions supersede the earlier empty-folder hard blocker, not the measured-run, isolation, accounting or verification requirements.
- The user further requested: "After that you can remove the one file with one row (if you needed it) and do a FULL reload, isnt it". Test this only if the one-row fallback was used; confirm actual full-refresh behavior and clean outputs, or retain separately classified bootstrap accounting when it fails.
- Demo 1 owner-reported result: an immutable 111-byte header-only CSV allowed creation of bronze and silver MLV definitions. Bronze readback reported fixed schema, append-only refresh and 11 string fields including `_corrupt_record` and `__filepath__`; Silver readback had the typed casts. An on-demand full-lineage refresh and a scheduled five-minute lineage run completed, with both outputs retaining their schemas and zero rows. The test schedule was disabled and read back disabled. The header-only file remains; the one-row fallback and deletion/full-refresh test were not needed or performed. Actual per-view refresh modes, measured-data ingestion, deployment automation and end-to-end validation still require verification.
- Implementation finding, independently confirmed by the producer and Demo 3 owners: the earlier generator restarted `SalesLineID` at 1 for every logical run. A second retained run therefore reused fact keys: Upsert keyed on `SalesLineID` could overwrite prior facts and `BatchID`, while Append could create duplicate logical keys. A producer-generated one-row bootstrap would also collide with the first measured key; the verified header-only bootstrap produced no rows. Retained multi-run behavior remains blocked. `SalesLineID` remains the required logical fact primary key; this finding does not authorize changing it to a composite key.
- Remedy, implemented locally but not verified live: atomically reserve durable, disjoint BIGINT ranges for logical runs, including any one-row bootstrap, in a scope that protects all retained destinations receiving the shared rows. Persist each reservation with its run manifest, reuse it on replay, and fail closed on conflicts or overflow. Preserve single-demo and partial-selection support without allowing separate allocators to issue overlapping ranges to the same retained destination; safely reconcile existing keys and writers when onboarding a target. The revised producer includes a registry, reservation binding and deployment-wide writer lock, with local tests; no live registry or reservation has been created. Allocation, ownership, cross-track target binding, concurrency, recovery, existing-data collision checks and end-to-end behavior must be verified before claiming readiness. The verifier's generator provenance must be rebound and revalidated against the finalized source; do not start measured data while the required gates remain blocked.
- Integration course correction: resolve a candidate `{workspace_id, item_id, fact}` from each selected track's own durable ownership journal and bind exactly the selected map in the effective configuration, producer reservation and monitor identity. Conflicting explicit configuration, missing or uncertain ownership state, and shared workspaces block. The `fact` identifies the actual destination with its exact case where the service is case-sensitive; a logical Spark name does not establish a physical Copy or SQL target. Candidates and local result files are not authenticated proof: require independent fresh target ownership, actual measured-principal runtime access before writes, and complete retained-key inventory under a verified writer-quiescence barrier held through registry onboarding. Establish that other writers and allocators cannot bypass the registry. The Demo 2 item is the KQL Database, not the Eventhouse or shortcut Lakehouse. The manually provisioned Demo 1 and Demo 2 resources lack the required ownership journals and must not be silently adopted.
- Ownership handoff gap: the v1 candidate does not bind the exact journal owner or marker, and a prefix-shaped live marker or another local field alone is not ownership proof. Before measured execution, establish an independently trusted, owner-bound provenance interface and verify it against fresh authoritative target readbacks. No such cross-track interface is currently implemented.
- Demo 3 owner-reported preflight: one non-sales OneLake event passed its exact subject and ID to a completed pipeline using `___subject` and `___id`. Other events appeared in source-event history without rule activation after reconfiguration, including one with no activation or Copy job within 300 seconds; after that attempt, the owner disabled the probe and restored its original target with readback. A later isolated event-triggered exact-filename Copy read one file and upserted two non-sales rows after a six-minute warm-up; the owner subsequently read back `shouldRun=false` and the restored original pipeline references. Another probe's SQL results remained stale for over 900 seconds until a targeted metadata refresh. Neither isolated success nor a rule-enabled readback establishes reliable event processing or immediate SQL visibility; missed events must be detected and reconciled before claiming measured-run readiness.
