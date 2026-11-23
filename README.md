# Chanel Fabric ingestion demonstrations

The [specification](SPECIFICATION.md) defines three isolated comparisons using the same staged CSV bytes: [file-backed materialized lake views](demo-1-materialized-lake-views/), [Eventhouse and OneLake availability](demo-2-eventhouse/), and [OneLake events with Copy](demo-3-onelake-events-copy/). The [shared producer](demo-data-generator/) runs as a bounded Fabric notebook through a wrapper pipeline in its own workspace, not as a local presentation-time process.

The default one-minute source cadence is not a downstream visibility guarantee. Demo 1 uses managed MLV refresh (document the actual supported interval); Demo 2's five-minute OneLake target is not an SLA and its propagation latency is not reported; Demo 3 requires a real event and Copy run. See the specification for acceptance criteria and feature-specific blockers.

## Configuration and prerequisites

Copy `config.example.json` to a private, untracked location and fill in the resolved IDs, names, feature settings and bounded run parameters. The `workspace_name` defaults contain `{prefix}`; each track expands it using the effective `--prefix`. The config is version 1 and consists of shared `environment`, `tenant_id`, `subscription_id`, `capacity`, `authentication`, `execution`, and `data` objects and `demo-1`, `demo-2`, `demo-3`, and `shared` track objects. Null IDs must be resolved and recorded by each track before it reports readiness. Configuration may contain secret **references**, never tokens or secret values. State files and logs must contain only sanitized data.

Use the signed-in MCAP identity where supported. In each PowerShell process set `$env:AZURE_CONFIG_DIR = "$HOME\.azure-mcap"` before invoking Azure CLI-backed operations; verify the active tenant, subscription and capacity. The configured identity alone does not grant cross-workspace permissions. All feature, permission, API-coverage, charge and approval checks in the specification remain required. Do not start cloud deployment until coordinated preflight and required approvals are complete.

## Orchestration

Requires Python 3.9 or later. From the repository root, for example:

```powershell
python scripts\deploy-demos all --dry-run --config C:\path\to\config.json
python scripts\deploy-demos demo-1,demo-3 --validate-only --config C:\path\to\config.json
python scripts\deploy-demos all --deploy --config C:\path\to\config.json
python scripts\deploy-demos all --deploy --run-data --config C:\path\to\config.json
python scripts\deploy-demos demo-1 --cleanup --yes --config C:\path\to\config.json
```

`--concurrency` overrides the configured limit (1–4); `--prefix` overrides the configured prefix. Selected tracks run independently in parallel up to that limit, and the shared producer is included in deployment and validation. Partial cleanup excludes the shared producer to avoid disrupting other demos; `all --cleanup --yes` includes it. Each track must restrict cleanup to resources it owns and require `--yes` for destructive replacement. No cleanup is automatic.

Each invocation creates a separate directory under `.deployment-state` (override with `--state-dir`) with per-track logs, result JSON and an aggregate `summary.json`. A missing entry point, invalid response, timeout, failure or blocked track makes the command fail without masking the other selected results. A timeout does not prove that work already submitted to a remote service has stopped; inspect its job state before retrying.

The producer is never started unless `--deploy --run-data` is explicitly supplied. This first deploys all selected destinations and the shared producer, then invokes fresh `validate-only` checks for every selected track. It starts the shared producer only if all report `pass` and `ready: true`; missing, awaiting-approval or unverified capabilities must report blocked. A nonempty stable `data.run_id`, bounded file count, batch size and sufficient positive maximum duration are required. The producer owns enforcing its runtime deadline, persisting progress and terminating its Fabric job; orchestration cannot establish that the job finished merely from submission. After a run, verify job/session completion and pause demo-owned schedules and rules, leaving the capacity running at its final size.

## Version 1 integration contract

Each track supplies `deploy.py`, invoked with `--mode`, `--config` (absolute JSON path), `--prefix`, `--state-dir` and `--result` (absolute JSON path). Modes are `dry-run`, `validate-only`, `deploy`, and `cleanup`; cleanup receives `--yes`. A user-provided `--yes` is also forwarded in other modes to authorize explicitly requested destructive replacement. The shared producer additionally supports `run-data` and receives `--selected-demos` as a comma-separated list. It must not start a job in any other mode. Track scripts must support safe reruns, ownership checks, bounded execution, explicit approvals and per-track manifests as specified.

Each invocation writes the requested result JSON:

```json
{"contract_version":1,"track":"demo-1","mode":"validate-only","status":"pass","ready":true,"diagnostics":[]}
```

`track` is `demo-1`, `demo-2`, `demo-3`, or `shared`; status is `pass`, `fail`, or `blocked`. Exit zero only for `pass`. Failures and blockers require nonempty sanitized diagnostics and `ready: false`. A pass for `deploy` or `validate-only` requires `ready: true`, based on actual readbacks of deployed artifacts, dimensions, configuration, identity and permissions, not just a successful API submission. Dry-run and cleanup need not report readiness. Never claim an unsupported automation or unapproved manual step is ready. Track logs are captured independently.

The CSV contract uses UTF-8 without BOM, header and comma delimiter, standard double-quote escaping, LF line endings, no null fields, ISO dates and UTC ISO timestamps ending in `Z`. Fixed header order:

```text
SalesLineID,BatchID,SaleTimestamp,CustomerID,ProductID,Date,Quantity,UnitPriceMinorUnits,SalesAmountMinorUnits
```

Minor units are integer USD cents in the current producer; do not select another currency without updating and verifying the producer contract. Each generated file has a stable run ID, batch ID, immutable filename and SHA-256 checksum over its exact staged bytes. The source manifest has top-level `contract_version: 1`, `run_id`, `config`, `destinations`, and `batches` (an object keyed by batch number as a string). Each batch records `batch_id`, `batch_number`, `batch_class` (`measured`, `bootstrap`, or `negative-test`), `filename`, `sha256`, `row_count`, `sales_amount_minor_units`, grouped totals in `amount_by_date`, `amount_by_product_category`, and `amount_by_customer_region`, `generated_at_utc`, and destination-keyed `deliveries` containing `status` and `timestamp_utc`. Amount totals are exact integer sums; measured totals exclude bootstrap and negative-test batches. Delivery retries use the same staged bytes and batch identity. Destination-specific validation and state remain in the corresponding track.

The current Demo 1 manifest validator expects an array and a single `grouped_totals` field instead of the producer's object and three named totals; this integration is blocked until the validator and its tests are aligned. Producer primitives also do not yet deploy a Fabric notebook or pipeline, deliver files, or update delivery status.

The implementation must verify shared contract changes against all tracks before use. The specification remains the authoritative source for the data model, source manifest, isolation, verification and lifecycle requirements.
