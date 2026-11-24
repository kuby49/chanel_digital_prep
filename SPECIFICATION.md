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
- For each Spark-processing notebook, verify the supported high-concurrency configuration at both notebook and workspace/pipeline level where applicable. Confirm session-sharing compatibility (user, workspace, default Lakehouse and Spark settings); use pipeline session tags where supported. Do not claim high-concurrency execution from a notebook property alone.
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
| `FactSales` | One row per sales order line | Logical primary key: `(BatchID, SalesLineID)`; foreign keys to each dimension |

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
- Include a stable `BatchID` in every row and assign `SalesLineID` values `1..row_count` within each batch. The `(BatchID, SalesLineID)` pair is unique; `SalesLineID` may repeat across different batches and runs. A replay uses the same batch ID, line IDs, filename, and staged bytes. Use immutable filenames such as `sales_<run-id>_<batch-number>.csv`.
- Record each file's checksum, row count, expected monetary totals, and generation/delivery timestamps in a source manifest. Delivery failures must be reported and retried using the same batch identity.
- Define a coordinated run explicitly. Deployment/bootstrap artifacts are not measured batch data. Complete all destinations, ingestion definitions, dimensions, Demo 1 MLVs, and event/schedule configuration before `--run-data` starts the timed 10-file run. The coordinated run still consists only of its measured batches. Demo 1 may use a separate bootstrap file before that run, first attempting a header-only CSV with the declared schema and then, only if needed, a valid one-row CSV.
- Classify the Demo 1 bootstrap separately as `bootstrap`, with its own immutable file and batch identity outside the coordinated run. Do not send it to the other demos. Exclude it from measured counts, exact reference totals, grouped aggregates and latency metrics; account for it explicitly in bronze, silver, rejections and all measured queries before the timed run begins. In particular, do not assume an append-only bronze output can discard a bootstrap row. If a valid row is required, give it a stable batch ID and valid dimension keys. Verify actual behavior and record any bootstrap cleanup or exclusion rather than assuming either works.
- If the one-row fallback was needed and the views have been created, test removing only that bootstrap CSV and issuing a full refresh before the measured run. Verify the actual refresh mode and clean bronze and silver counts. If empty-source full refresh fails or leaves bootstrap data, retain explicit bootstrap accounting and exclusion; do not assume file deletion removed previously materialized rows.
- Complete and close uploads before processing. Validate the upload method's file-event behavior; do not treat initial file creation as proof that all bytes are readable.
- For Demo 1 and Demo 3, prefer a temporary location outside the subscribed folder followed by an atomic final-name move/copy into the OneLake landing folder. Event filters must ignore staging extensions such as `.tmp` and fire only for completed `.csv` files.
- Use notebook parameters for run ID, seed, file count, interval, batch size, enabled demo destinations, and maximum run duration. Default to 10 files at 60-second intervals. Persist progress after each completed delivery so a rerun resumes with the same batch identity rather than regenerating a batch.
- Replaying an existing logical run must reuse the same batch identities and staged bytes; retries of `(BatchID, SalesLineID)` must not double-count or overwrite a different batch. A new run uses new batch IDs, but may reuse the same `SalesLineID` values. Intentional new data requires a new run ID.

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
- All three dimension joins preserve the expected fact row count, with no duplicate `(BatchID, SalesLineID)` keys or orphaned foreign keys.
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
3. Parameterize the exact source file path and event identity. Configure an idempotent write strategy keyed by `(BatchID, SalesLineID)` where supported. If composite-key upsert is unavailable, combine a batch-identity audit gate with destination behavior that prevents replay duplication without overwriting a different batch. Prevent conflicting writes to the same Delta table. Record any supporting audit/validation activities.
4. Use the `activator-cli` skill and Activator REST create/update-definition APIs to deploy the OneLake event trigger. Its `ReflexEntities.json` must include a `realTimeHubSource-v1` source for `Microsoft.Fabric.OneLakeEvents`, the source Lakehouse item ID, a `FileCreated` subscription, an enabled event rule, and a pipeline invocation action. Filter out unrelated folders, dimension files, temporary files, and destination-table changes to avoid loops.
5. Bind the event's file location and identity to the pipeline's declared parameters. Capture a real event payload and document the exact property mapping; do not assume Azure Blob trigger expressions apply to OneLake events.
6. Read back and validate the deployed pipeline and Activator definitions, including connection IDs, source filters, enabled rule state, action target, and parameter bindings. Start the source and write each completed CSV file at the one-minute cadence. Verify readable file readiness before Copy. Use bounded retries with explicit failure reporting when data is not yet available.
7. Copy only the event-associated file, not the entire accumulated folder. Preserve `BatchID`, validate the copied row count, and record event ID, source path, pipeline run ID, and batch outcome.
8. Query the loaded facts joined to the three static dimensions and derive exact decimal monetary values. Demonstrate duplicate event/retry handling without duplicate sales.

The live ingestion path must be driven by OneLake events through Activator, not a timed pipeline schedule or manual trigger. Copy activity must perform the CSV-to-Delta load; supporting checks must not silently replace it with notebook ingestion.

The pipeline UI's **Trigger** wizard is an alternative authoring experience, not a required deployment step. For API-first deployment, the Activator definition owns the underlying event subscription; do not invent a separate pipeline-subscription API. Use the Job Scheduler API to inspect the event-triggered pipeline job and confirm the Copy result. A successful definition upload alone does not prove event delivery or pipeline execution.

Do not claim exactly-once event delivery. A completed-file processing record alone is not sufficient protection against a retry after a write succeeds but before the record is committed; the destination write/recovery strategy must also be idempotent. For this demonstration, use the user-directed assumption that notifications are not missed. This is an assumption, not measured evidence or a service guarantee, and missed-notification recovery does not need to be implemented or proven as a prerequisite. Monitor the actual event-to-Copy run and report any visibly failed notification or Copy honestly; retain file binding, Copy correctness, duplicate-retry safety, data validation and safe stopping.

### Acceptance criteria

- A new file produces a captured OneLake event, an Activator action, and a correlated pipeline run without manual intervention.
- The Copy activity reads the intended file and loads its rows into the intended Delta fact table.
- Every valid batch is present within the configured timeout, and all previously loaded batches remain intact.
- Repeated delivery of an event or a retry after a successful write leaves fact counts and totals unchanged.
- Exact totals and dimension-filtered aggregates match the source manifest, with no duplicate `(BatchID, SalesLineID)` keys or orphaned foreign keys.
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
- For every Spark-processing activity, record Fabric run evidence for the actual session mode and session ID; when claiming reuse, verify compatible notebook activities share the same session ID. If unsupported, document the specific activity, limitation and actual session mode. Treat session-start improvements as distinct from event delivery, ingestion, and query visibility.
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

- Parallel-work clarification: the user explained, "actaully when i told to paralelliye the demos in the spec, i meant YOU can work on the 3 demos in parallel, cause they are pretty independent in nature. What did you understood ?" This means parallel implementation by the agents. Serial deployment is acceptable; concurrent all-demo deployment is not a readiness requirement. This clarification supersedes section 9's parallel-deployment requirement, while preserving its other requirements. Existing safe parallel-deployment support may be retained.
- Coordinated bounded-run configuration: use one shared run with seed 42, start date 2026-01-01, 365 dates, 20 products, 100 customers, 10 files of 1,000 rows and a 60-second cadence, unless a real limitation requires a documented change. The proven isolated Demo 2 target is preferred; exclude existing bootstrap rows from measured totals. For Demo 3, expand existing owned dimensions only after verifying that overlapping rows match the generator, appending only missing compatible rows; otherwise use a fresh demo-owned target and preserve the old test resources. Assign a unique run ID at execution. Do not start measured data before the actual bindings and readiness are verified.
- Immediate customer-demo course correction: first prepare a fresh shared run `customer_demo_20261124_e0c409cc05ef` with the same seed and dimensions, one file of 10 rows, a 60-second configured interval and a 1,800-second maximum duration. Show actual data through all three mechanisms and stop owned jobs and rules afterward. This one-file run does not demonstrate recurring arrivals or complete the separate 10-file acceptance run. It has not yet started.
- Integration clarification: the presenter-facing goal is a safely repeatable
  Fabric **Run** action that creates a fresh run identity without per-run
  definition rebinding or manually starting desktop monitoring. A prepared,
  expiring single-use profile is interim only. Preserve the three ingestion
  mechanisms, replay correctness, bounded lifecycle and verification; report
  any unsupported or unverified native automation rather than claiming success.
- MLV cadence and window course correction: the user requested, "I would like
  the MLVs to run every 2.5 minutes, not 5 And if 60 bothers you, make it 115
  or whatever you think its right, just lets finish finally". The 115-minute
  suggestion permits a longer bounded operating window; it is separate from
  the requested 2-minute-30-second refresh cadence. The documented native MLV
  schedule has a [five-minute minimum effective interval](https://learn.microsoft.com/en-us/fabric/data-engineering/materialized-lake-views/materialized-lake-views-public-api),
  and its [Cron interval is expressed in integer minutes](https://learn.microsoft.com/en-us/rest/api/fabric/lakehouse/background-jobs/create-refresh-materialized-lake-views-schedule).
  Exact 2.5-minute native scheduling is therefore not established. Preserve
  explicit window bounds, per-run verification deadlines and safe stopping.
- Subsequent cadence request: "OK, if cron dose accpet only integers only,
  make it 3 minutes". Three is an integer, but the documented five-minute
  minimum effective interval still applies; a three-minute configuration may
  be adjusted to five.
- Final MLV cadence decision: "OK, dont do preview stuff , accept the 5
  minutes". Use the documented five-minute scheduled refresh, not the
  event-triggered MLV preview. This supersedes the earlier 2.5- and
  three-minute requests; do not claim a shorter effective interval.
- Simplified implementation focus: after the user asked, "can we pivot  now in our approach, or too late ?", prioritize the shortest safe path to runnable demonstrations, reusing verified work rather than adding new hardening or ownership-proof frameworks. Retain the specified correctness checks, protection against writing to the wrong target, and stopping owned jobs. Report unsupported or unverified behavior honestly; this focus does not silently waive other explicit requirements.
- Use the signed-in MCAP identity where the relevant operation supports it.
- The user permits starting and stopping resources needed for the demonstrations and scaling the approved capacity up to F64. Isolation, least-privilege and verification requirements remain in effect.
- After testing, leave the capacity running at its final size. This does not change the requirement to stop the bounded producer and pause demo-owned schedules and event rules.
- The user granted upfront approvals needed to test these demos live, including cross-workspace access. Limit permissions and changes to what the demos actually require, record them, and do not modify unrelated workloads. This approval does not establish feature availability or permit skipping readiness and runtime verification.
- Course correction: the original specification required Demo 1 MLV creation against an empty folder and prohibited seed data. After the empty-folder fixed-schema attempt failed, the user directed a separately classified bootstrap CSV, initially header-only: "do the thing with the separate bootstrap CSV (try to use an empty file, headers only. Shoudl work as you know the sample data column types, isnt it ? )". The user then explicitly allowed a fallback: "if empty file not enough, than use a CSV with one row". These decisions supersede the earlier empty-folder hard blocker, not the measured-run, isolation, accounting or verification requirements.
- The user further requested: "After that you can remove the one file with one row (if you needed it) and do a FULL reload, isnt it". Test this only if the one-row fallback was used; confirm actual full-refresh behavior and clean outputs, or retain separately classified bootstrap accounting when it fails.
- Demo 1 owner-reported result: an immutable 111-byte header-only CSV allowed creation of bronze and silver MLV definitions. Bronze readback reported fixed schema, append-only refresh and 11 string fields including `_corrupt_record` and `__filepath__`; Silver readback had the typed casts. An on-demand full-lineage refresh and a scheduled five-minute lineage run completed, with both outputs retaining their schemas and zero rows. The test schedule was disabled and read back disabled. The header-only file remains; the one-row fallback and deletion/full-refresh test were not needed or performed. Actual per-view refresh modes, measured-data ingestion, deployment automation and end-to-end validation still require verification.
- User-directed ID course correction: "activate also pipeline high concurrency if needed.\n\nAnd dont overomplicate the Unique Sales ID question, this is  just a demo, its fine to have duplicate IDs". `SalesLineID` is now unique within a batch, may repeat across batches/runs, and the logical key is `(BatchID, SalesLineID)`. Emit IDs `1..row_count` per batch and reuse them exactly for a replay. Retire global range allocation and any retained-key census or cross-destination writer fence required solely for run-global ID uniqueness. This does not authorize duplicate rows on replay, cross-batch overwrite, or unsafe concurrent destination writes; each destination still needs a verified idempotent replay/write strategy. The append-only Demo 1 MLV path may not support replay deduplication and must remain blocked unless its supported path is demonstrated.
- Integration course correction: resolve a candidate `{workspace_id, item_id, fact}` from each selected track's own durable ownership journal and bind exactly the selected map in the effective configuration and monitor identity. Conflicting explicit configuration, missing or uncertain ownership state, and shared workspaces block. The `fact` identifies the actual destination with its exact case where the service is case-sensitive; a logical Spark name does not establish a physical Copy or SQL target. Candidates and local result files are not authenticated proof: require independent fresh target ownership and actual measured-principal runtime access before writes. Retained-key census and cross-destination writer fencing are not prerequisites solely for global ID uniqueness; destination-specific write serialization, batch replay protection, and lifecycle/safe-stop checks remain required. The Demo 2 item is the KQL Database, not the Eventhouse or shortcut Lakehouse. The manually provisioned Demo 1 and Demo 2 resources lack the required ownership journals and must not be silently adopted.
- Demo 1 replay candidate: temporary Spark views verified that the replay-aware Silver query collapses identical rows from a renamed file while retaining repeated IDs across batches, and flags conflicts, duplicate rows within one file and missing paths. Bronze retains the raw rows. The candidate's actual MLV DDL, refresh-time failure constraints, and incomplete or changed replay reconciliation are unverified. Expect a full Silver refresh because of `COUNT(DISTINCT ...)`; verify refresh mode and timing before measured use.
- Demo 2 isolated synthetic proof: the earlier `SyntheticFactSales` test does not establish measured readiness; keep its `bootstrap` and authorization rows out of measured totals. The producer's synthetic path separates a two-batch staging operation, a one-row authorization probe with its own run and manifest, and monitored delivery. The owner reports that a no-sales preflight verified the producer runtime identity, target KQL/schema/mapping reads, and non-destructive success and failure queue peeks; the observation was fresh when the separately journaled one-row authorization parent `92e67d1f-be86-4820-b235-c2db34c66591` was submitted. The producer owner reports terminal ingestion success for that distinct source and an exact KQL readback of one row totaling 7,020 minor units. Demo 2 reports its bounded synthetic run independently reconciled both five-row batches, their deadlines and a run-wide Spark check. The monitor, two verifier jobs and run-verification job completed without a retained monitor writer lock; all four runtime records reported `isHighConcurrency:false` and separate Spark applications. This does not establish measured readiness or session reuse. Require independently verified ownership and fresh runtime authorization for any new monitored delivery.
- Ownership handoff: the v1 candidate does not bind the exact journal owner or marker, and a prefix-shaped live marker or another local field alone is not ownership proof. Before writing, use existing trusted deployment records and fresh target readbacks to verify the intended resources and runtime access. A new cross-track provenance framework is not a prerequisite when those existing checks suffice; the current normal-run gates are not yet implemented.
- Fresh Demo 1 synthetic-resource provenance: the separate opt-in container provisioner relies on an operator-protected local deployment journal with uniquely marked intent durably recorded before each create POST, the actual accepted response or operation evidence and exact IDs retained, and independent readback of tenant, capacity, resource, parent and marker. A lost or ambiguous response remains unresolved; never resubmit an uncertain create or adopt a historical resource by name. On 2026-11-24, it created isolated MCAP Workspace `af865b3c-f088-44ad-8816-a5e495b1fbf5` and schema-enabled Lakehouse `646c26d7-7df8-4962-acaa-4e1386bcba08`, each with a retained 201 response. Independent GETs matched their identities, parent, capacity and markers; no operation remained pending or uncertain. The target was handed to the Demo 1 owner for native MLV validation. This is a local-state trust assumption, not Fabric creator attestation: journal tampering and mutable descriptions are not excluded by service readback, and a lock is not access control. Provisioning alone does not authenticate the v1 sidecar or establish the MLV fact, runtime permissions or measured readiness. Cross-track verification and those gates remain separate and incomplete.
- Demo 1 owner-reported native synthetic result: on that fresh target, Bronze and replay-aware Silver were read back as materialized lake views. A managed lineage refresh of synthetic batch A, its identical renamed replay and batch B reusing `SalesLineID=1` yielded three Bronze rows and two Silver rows keyed by the two batch IDs, totaling 3,000 minor units. Both owned high-concurrency handles were released; no measured run or schedule was started. Refresh modes, replay across separate refreshes, negative constraint enforcement and measured readiness remain unverified.
- Demo 3 owner-reported preflight: one non-sales OneLake event passed its exact subject and ID to a completed pipeline using `___subject` and `___id`. Other events appeared in source-event history without rule activation after reconfiguration, including one with no activation or Copy job within 300 seconds; after that attempt, the owner disabled the probe and restored its original target with readback. A later isolated event-triggered exact-filename Copy read one file and upserted two non-sales rows after a six-minute warm-up; the owner subsequently read back `shouldRun=false` and the restored original pipeline references. Another probe's SQL results remained stale for over 900 seconds until a targeted metadata refresh. These are historical outcomes, not a delivery guarantee; under the user-directed assumption, missed-notification recovery is not a readiness prerequisite, but visible run failures must still be reported honestly.
- Demo 3 owner-reported synthetic end-to-end result: a real FileCreated event reached Activator with the correct file and event parameters, triggered Copy of one file and three rows, and completed its audit; independent Delta readback matched all three rows and composite keys. After the rule was disabled, a second observed event launched no pipeline within 180 seconds, and all probe jobs reached terminal states. The separate source-only preview Eventstream was soft-deleted after its evidence was saved. This bounded event and stop check does not prove shared-producer stopping or measured-run readiness.
- Demo 3 dimension preparation: bounded notebook job `2fb0677a-d4c6-4740-9a3c-f5083423f06a` completed on the existing owned target. After verifying compatible existing dimensions, it inserted only missing keys and read back 369 dates (including four earlier dates), 20 products and 100 customers. The three bootstrap facts and their Delta version were unchanged. Normal run-specific binding, producer staging access and measured readiness remain unverified.
- User-directed Demo 3 notification assumption: "then can you assume there are no notifications missed., if i general the notifications arrive". For this demonstration, assume notifications are not missed; this is not measured evidence, a service guarantee or exactly-once semantics. Retain actual event-to-Copy monitoring, Copy correctness, duplicate-retry safety, target-row validation and safe stopping, but do not require implementation or proof of missed-notification recovery. The Demo 3 owner reports an earlier target with deletion vectors and Delta reader 3/writer 7 failed before writing because the Copy connector did not support its `appendOnly` writer feature (`dataWritten=0`). A new isolated table with deletion vectors disabled read back reader 1/writer 2 and passed the direct Delta protocol gate. The first synthetic Copy read one file and reported three rows copied; VerifyCopy initially rejected an HTTPS-prefixed OneLake endpoint. Fix `dfc89b7c2431369c4e3116c743e920ddda1a3cfc` normalized the endpoint, and a verification-only continuation completed batch `recoveryprobea9bbae81_0001` and its audit without repeating Copy. A second manually supplied synthetic job (`13f2398f-7599-465a-b1c2-2fc8b8ef5b85`, not an Activator-delivery test) completed guarded Copy and audit. Duplicate batch 1 job `eb2f7cb3-d8e4-4093-a510-82e35ab60bb6` ran PrepareCopy only and did not run Copy. Independent target notebook `7be33833-f22c-4fbd-8d5f-92c47c324a32` verified six rows, six unique `(BatchID, SalesLineID)` pairs with IDs 1–3 in each batch, exact regenerated contents and dimension totals. These two manual tests establish composite-key behavior and guarded replay, not Activator delivery; the separate event test is described above. Measured readiness, missed-notification recovery and actual Spark session reuse remain unproven. Do not downgrade an existing table's protocol.
- User-directed Spark course correction: "For all demos that need Spark  use high concurrency sessions for processing, maybe this will help with demo3 missing events too". Use supported Fabric high-concurrency sessions for Spark processing in applicable demos and in the shared producer, except for the Demo 2 monitor and verification jobs as specified below. For pipeline-triggered notebooks, verify the workspace's pipeline high-concurrency setting and use compatible session tags/configuration where supported. Respect Fabric's sharing boundaries: same user, workspace, default Lakehouse, Spark configuration, and applicable libraries; do not force sharing across isolated workspaces. Keep the platform's default sharing limit unless a separately justified requirement needs a change. The notebook property `spark.fabric.session.highConcurrency.enabled=true` is configuration intent only; it does not prove a run used a high-concurrency session. Record runtime session type and IDs, and show shared IDs across compatible notebook activities when claiming reuse. If the supported high-concurrency route is unavailable for a Spark activity, document the exact limitation and the activity's actual session mode rather than silently falling back or claiming readiness.
- User-directed Demo 2 exception: "They dont have to (\"The monitor and verification jobs did not use high-concurrency Spark\"), no need to investigate". Demo 2 monitor and verification jobs do not need high-concurrency Spark; do not investigate or block their readiness solely because these jobs use separate Spark applications. This exception does not apply to other workloads and does not waive data correctness, monitoring, ownership or safe-stop requirements.
- High concurrency may reduce Spark session startup time for compatible processing, but it does not establish or repair Demo 3 OneLake event delivery, Activator rule execution, Copy activity execution, missed-event recovery, or SQL endpoint freshness. Treat any improvement to Demo 3 latency as a hypothesis until separately measured; preserve its event, Copy, reconciliation, and lifecycle safeguards.
- Current evidence is mixed and does not establish high-concurrency use for every Spark activity. The shared producer notebook, Demo 2's generated verifier and monitor notebooks, and Demo 3's setup notebook set `spark.fabric.session.highConcurrency.enabled=true`; Demo 1 has no Spark processing implementation in its deployment entry point. The Demo 1 owner reports an isolated MCAP Livy probe in which two high-concurrency session IDs with one tag shared a Livy session ID but had distinct REPL IDs; a separate read-only Spark query identified the MLV type. This verifies user-controlled Spark verification only, not scheduled MLV refresh. Demo 2's evidence reports two concurrent verifier jobs with `isHighConcurrency:false`; its owner-created `chanel-demo2-eventhouse` workspace (`aab2ed7b-fd2f-4696-84b3-b34ef931681f`) currently has `notebookPipelineRunEnabled=false`, and its monitor uses on-demand notebook jobs rather than pipeline activities. Read-only reconciliation confirmed two separate MCAP setting changes: the producer workspace `chanel-demo-data-generator` (`48593f2f-d3ac-474b-ba96-08282d6f69df`) has pipeline high concurrency enabled after a one-property change with a full before/after snapshot; the session-created Demo 3 preflight workspace `chanel-demo3-copy-events` (`744223de-ceab-4c4f-82ba-f811bcb5c7d7`) also has it enabled after a separate one-property change, but its full before snapshot and comprehensive usage inventory were not retained. No further settings changes were made. Neither setting proves high-concurrency runtime use; no post-change session evidence or Demo 3 session evidence is available. Record actual mode and IDs for each supported Spark path; do not claim a runtime or latency benefit without the relevant evidence. See [Fabric high concurrency mode](https://learn.microsoft.com/en-us/fabric/data-engineering/high-concurrency-overview), [notebook session configuration](https://learn.microsoft.com/en-us/fabric/data-engineering/configure-high-concurrency-session-notebooks), [pipeline session configuration](https://learn.microsoft.com/en-us/fabric/data-engineering/configure-high-concurrency-session-notebooks-in-pipelines), and the [Livy high-concurrency API](https://learn.microsoft.com/en-us/fabric/data-engineering/high-concurrency-livy).
