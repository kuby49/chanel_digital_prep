# Chanel Fabric demos: reset and run

## Authoritative requirements

This specification replaces the earlier recovery, proof and presenter-window
requirements. Earlier implementation history does not impose additional runtime
requirements. The goal is a simple, repeatable demonstration, not a production
ingestion framework or a performance benchmark.

The presenter opens the existing combined Fabric pipeline and presses **Run**,
without entering technical parameters. Every Run:

1. Stops any remaining work of these demos that would interfere with reset.
2. Resets the existing demo targets to their clean initial state.
3. Prepares and starts the native prerequisites automatically.
4. Generates the sales files and demonstrates the three ingestion mechanisms.
5. Stops the producer and native refresh/event activity automatically, leaving
   the results available to inspect until the next reset and Run.

If a demo fails, report the actual error. The next attempt resets and restarts
from the beginning; it does not resume a partially completed logical run.
Do not require the presenter to prepare a 24-hour window, promise a no-touch
window, publish a profile, start a monitor, edit JSON or use a local launcher.

## Environment and existing targets

Use the signed-in MCAP identity where the relevant operation supports it.
Keep `fabricadbmirror` running at **F32**; do not resize, restart or suspend it.
Do not change unrelated workloads, resources or permissions.

`demo-settings.json` contains the fixed settings shared by the wrapper and
demo helpers. These bindings come from the existing presenter setup; inclusion
here does not claim that a new implementation has been deployed or run.

| Component | Workspace ID | Existing item IDs |
|---|---|---|
| Combined producer | `68971aa2-f4bc-48a2-8962-98a4cb1e9fc2` | Pipeline `2f42bac0-ccc3-4efc-8ff3-787fc93f3827`; notebook `7d7f2229-4caf-409b-bee0-296d061e14ec`; staging Lakehouse `95a05004-09b1-4a3f-b8b3-2af9e68dd935` |
| Demo 1 | `af865b3c-f088-44ad-8816-a5e495b1fbf5` | Lakehouse `646c26d7-7df8-4962-acaa-4e1386bcba08` |
| Demo 2 | `0a0499fe-51b5-42b1-bc99-31c121cc74a6` | Eventhouse `a585c196-beb8-432a-bf11-f0f2530de92c`; KQL database `25159007-a6dd-4020-8afc-01d393378727`; Lakehouse `f565aeac-f321-46f2-8262-076e06490707` |
| Demo 3 | `744223de-ceab-4c4f-82ba-f811bcb5c7d7` | Lakehouse `6bba6989-5cfd-4192-8e75-00274afb66c1`; Copy pipeline `bab19f65-b5ab-4edd-8066-89b4fc00f120`; Activator `195d1691-08f6-4140-a6d5-18c332696fef` |

Reuse these targets rather than creating replacements to preserve interrupted
activities. Reset only this demo's data and artifacts. Preserve unrelated data.
Create an additional item only when it is actually necessary to implement the
demo, not as another observer, probe or recovery target. Report a concrete
unsupported operation or missing permission instead of building speculative
workarounds. Never put credentials or access tokens in repository files or logs.

## Shared sample data

Use one deterministic generator, seed `42`, dates starting `2026-01-01`,
365 dates, 20 products and 100 customers. Restore the same dimensions in each
demo's Lakehouse during reset:

| Table | Columns |
|---|---|
| `DimDate` | `Date DATE`, `Year INT`, `MonthNumber INT`, `MonthName STRING` |
| `DimProduct` | `ProductID INT`, `ProductName STRING`, `ProductCategory STRING` |
| `DimCustomer` | `CustomerID INT`, `CustomerName STRING`, `CustomerRegion STRING` |
| Sales CSV/facts | `SalesLineID BIGINT`, `BatchID STRING`, `SaleTimestamp TIMESTAMP`, `CustomerID INT`, `ProductID INT`, `Date DATE`, `Quantity INT`, `UnitPriceMinorUnits BIGINT`, `SalesAmountMinorUnits BIGINT` |

Default to ten files of 1,000 rows, publishing the first immediately after setup
and subsequent files at 60-second intervals. Generate each file once and deliver
the same complete UTF-8 CSV bytes to all three destinations. Use a header,
commas, fixed column order, ISO dates and UTC timestamps. Simple filenames such
as `sales_0001.csv` can be reused after reset. `BatchID` identifies the numbered
sample batch; it is not a generated execution identity or a recovery key.

Retain integer USD minor units and derive decimal monetary values for display
using exact decimal arithmetic. Join the facts to all three dimensions using
`CustomerID`, `ProductID` and `Date`. Use fully qualified Lakehouse names in
Spark reads, writes and SQL. The generator runs in Fabric, not on the
presenter's computer, and exits after producing the configured files.

Do not add manifests, file checksums, delivery receipts, per-file timings,
duplicate-recovery logic or exhaustive reconciliation. Basic readable progress
and normal Fabric job history are sufficient.

## Demo 1: file-backed Materialized Lake Views

Keep the actual Fabric MLV mechanism:

- Bronze `bronze.sales_files` ingests the landing CSV files directly with
  `USING OneLake_Files` and the fixed CSV schema.
- Silver `silver.sales_joined` casts the sales fields, derives amounts and joins
  the static dimensions.
- Recurring refresh uses the native **Lakehouse lineage scheduler**, with the
  accepted five-minute cadence. Do not replace it with a periodic notebook or
  pipeline refresh loop or the event-triggered MLV preview.

Reset the landing data and the materialized outputs, preserving or recreating
`Files/incoming/sales/bootstrap_header_only_v1.csv`. This header-only file was
needed for fixed-schema creation and contributes no sales rows. Bronze is
append-only: deleting CSV files alone is not a reset of materialized history.
Use a supported same-target rebuild/reset so both outputs are empty before
new sales arrive, then restore their definitions and dimensions as needed.

At Run startup, create or update and enable the native schedule using supported
Fabric operations. Any API-required start/end dates are calculated internally
for this execution, not presenter admission windows. Allow the last generated
file to receive its native refresh before stopping the schedule. Leave the
resulting tables available. Report a real API limitation if schedule automation
is unavailable; do not silently substitute another ingestion mechanism.

## Demo 2: Eventhouse and OneLake availability

Reset `SyntheticFactSales` in the existing KQL database. Keep or recreate its
schema and `SalesCsvV1` mapping and configure OneLake availability with the
five-minute target. Restore the Lakehouse dimensions and retain or repair the
`EventhouseFactSales` shortcut using the actual OneLake location.

Submit the generated CSV files through the supported Kusto ingestion SDK.
Show the facts in KQL and through the Lakehouse shortcut joined to the
dimensions. OneLake availability exposes managed Delta data, not the original
CSV files. Ingestion and OneLake propagation are asynchronous; five minutes is
a configuration target, not a guaranteed visibility deadline or an SLA.

Do not require a separate monitor notebook, receipt-queue observer or per-batch
proof pipeline. Keep the final facts and shortcut available after completion.

## Demo 3: OneLake events and a single Copy activity

Reset `Files/incoming/sales` and `dbo.factsales` in the existing Lakehouse and
restore the dimensions. Disable its trigger and finish or stop its known
active Copy work before clearing its data. Do not scan unrelated workspaces.

Use OneLake `FileCreated` events for completed `sales_*.csv` files in
`/Files/incoming/sales/`. The existing Activator starts the existing pipeline,
whose ingestion path is a **single Copy activity** loading the event's file
into `dbo.factsales`. Bind the file location from the actual event. Do not
replace event-triggered Copy with scheduled polling or notebook ingestion.

Enable the native event trigger before producing sales, then let the final
Copy finish and disable the trigger automatically. Retain the copied facts and
dimension joins for inspection. No guard notebook, audit store, staging-access
probe, event observer or replay/rebind subsystem is required.

## Minimal helper interface for parallel implementation

Each demo owner supplies exactly one uniquely named Python module in its own
directory, for bundling into the existing producer notebook:

| Directory | Module |
|---|---|
| `demo-1-materialized-lake-views` | `simple_demo1.py` |
| `demo-2-eventhouse` | `simple_demo2.py` |
| `demo-3-onelake-events-copy` | `simple_demo3.py` |

Each module exposes these ordinary functions:

```python
def reset(context):
    """Restore this fixed demo's empty facts/landing and initial dimensions."""

def start(context):
    """Prepare and enable this demo's native schedule/trigger, if applicable."""

def stop(context, *, failed=False):
    """Stop this demo's native work without deleting its result data."""
```

`context` is a plain dictionary containing only:

- `config`: the settings dictionary loaded from `demo-settings.json`. Helpers
  use their own `demo-1`, `demo-2` or `demo-3` section plus common `data` and
  `tenant_id` settings. They do not mutate it.
- `spark`: the producer notebook's Spark session, available when appropriate.
  An owner may use a necessary demo-local reset notebook for operations that
  cannot run in that session.
- `get_token`: a callable `get_token(audience)` returning an access-token
  string for the resource URL requested by the helper. The producer implements
  it using the signed-in Fabric identity. Helpers never log its result.

Functions return `None` when finished and raise a normal exception on failure.
There is no helper result schema, version negotiation, persistent lifecycle
document or proof object. Any necessary demo-local reset notebook IDs are
ordinary fields in that demo's settings, not a new orchestration subsystem.
`start` may do nothing for Demo 2 when reset has already prepared its ingestion.

The wrapper uses the three modules in order:

1. Call `stop(context, failed=True)` for each demo to stop its prior activity.
   These helpers operate only on their own known assets, never the currently
   executing wrapper or producer.
2. Call every `reset`, then every `start`, before publishing any sales.
3. Generate and deliver the configured files.
4. Call every `stop(context)` on normal completion. Each helper allows its
   final ordinary downstream processing to finish before disabling its native
   activity; this is not per-file reconciliation or latency measurement.
5. On failure, attempt `stop(context, failed=True)` for every demo. This mode
   stops or cancels activity without waiting for successful result production.
   Surface the original error and any actual stopping error in Fabric output.

Use normal service status checks and a reasonable finite wait where processing
is asynchronous. Do not introduce deadlines as a new SLA/admission framework.
Do not hide an error behind a successful return or an indefinite wait.
Stopping never clears result data; only the next reset does.

## Implementation boundaries and acceptance

The shared owner maintains this specification, root settings and root scripts.
The producer owner maintains `demo-data-generator`; each demo owner maintains
its demo directory. Implement these independent tracks in parallel against the
helper interface above. A local deployment tool may publish assets, but it is
not the presentation-time launcher.

Remove the old runtime dependencies on run IDs, profile hashes, locks,
sentinels, bounded access checks, ownership proofs, recovery journals and extra
monitors. Old code/configuration that remains temporarily is legacy, not an
additional requirement of this specification.

Validate the actual presenter experience: Run from a clean or interrupted
state, see new data through each specified mechanism and dimension join, see
the native schedule/trigger stop, inspect the retained results, then Run again
and see reset rather than accumulation. Do not claim a live outcome from local
tests or from a successful parent status alone.

Do not add superfluous unit tests, negative-data suites, schema-drift handling,
record-loss/duplication recovery or detailed processing measurements. Keep only
checks needed for the changed behavior and the practical live demonstration.
Document the one Run action, the three result locations, automatic stopping
and the reset-and-restart response to failure. Distinguish implemented,
deployed and actually demonstrated behavior.

## Fabric references

- [File-backed Materialized Lake Views](https://learn.microsoft.com/en-us/fabric/data-engineering/materialized-lake-views/create-materialized-lake-view)
- [Schedule MLV lineage refresh](https://learn.microsoft.com/en-us/fabric/data-engineering/materialized-lake-views/schedule-lineage-run)
- [Eventhouse OneLake availability](https://learn.microsoft.com/en-us/fabric/real-time-intelligence/event-house-onelake-availability)
- [OneLake event-driven pipelines](https://learn.microsoft.com/en-us/fabric/real-time-hub/tutorial-build-event-driven-data-pipelines)
- [Lakehouse Copy activity](https://learn.microsoft.com/en-us/fabric/data-factory/connector-lakehouse-copy-activity)
