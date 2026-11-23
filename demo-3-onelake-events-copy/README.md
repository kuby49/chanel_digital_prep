# Demo 3: OneLake events and Copy

**Status: blocked; not deployed.** This directory provides an offline candidate
Activator builder and a fail-closed orchestrator entry point, not a working
ingestion deployment. No connections, pipeline, rule, tables or resources have
been created or verified. `dry-run` only validates the local contract.

## Contract

`python deploy.py --mode {dry-run,validate-only,deploy,cleanup} --config <absolute JSON> --prefix <prefix> --state-dir <absolute directory> --result <absolute JSON>`. Cleanup requires `--yes`. A v1 result reports `track: demo-3`; `ready` is false until a live deployment and all prerequisites are verified. Non-dry-run modes currently return `blocked` and make no Fabric requests; cleanup also never deletes anything.

The shared producer's v1 CSV has the header
`SalesLineID,BatchID,SaleTimestamp,CustomerID,ProductID,Date,Quantity,UnitPriceMinorUnits,SalesAmountMinorUnits`.
Consume the same immutable bytes and manifest as the other demos; do not generate
data in this directory.
Manifest v1 provides top-level `contract_version`, `run_id`, `config`,
`destinations`, and `batches`; batches carry identity, filename, SHA-256, row
count, minor-unit totals, grouped totals, generation time and destination
delivery status/timestamp. Validation must include only `batch_class: measured`
in coordinated-run totals; `bootstrap` and `negative-test` are separate.
Manifest-to-destination reconciliation is not yet implemented.

## Verified capabilities and unresolved gates

| Operation | Documented API or capability | Required proof before deployment |
|---|---|---|
| Connection | [`POST /v1/connections`](https://learn.microsoft.com/en-us/rest/api/fabric/core/connections/create-connection), delegated `Connection.ReadWrite.All`; users and permitted service principals supported | Discover the Lakehouse-specific connection creation method, parameters and supported credential type. Test and read back a demo-owned connection. Never store credentials in config or evidence. |
| Pipeline | [Item definitions](https://learn.microsoft.com/en-us/fabric/data-factory/pipeline-rest-api-capabilities), `pipeline-content.json` in Base64 | Implement and read back the exact connector JSON and explicit mappings. [Lakehouse Copy](https://learn.microsoft.com/en-us/fabric/data-factory/connector-lakehouse-copy-activity) documents exact `folderPath`/`fileName`, `DelimitedText`, and `Upsert` with `keyColumns`; feature documentation is not proof of the pipeline JSON or tenant runtime behavior. |
| Activator | [Reflex item definition](https://learn.microsoft.com/en-us/fabric/real-time-hub/consume-events-activator-rest-api), `ReflexEntities.json` | Capture an actual event and verify the binding, filters, source identity, region, deployed readback and invocation using a pipeline job and activity run. `activator.py` constructs a **disabled, untested candidate** only. Reference `subject` and `id` are documented event fields, but their resolution as pipeline parameters must be verified. |
| Audit, recovery, serialization | No implementation yet | A durable per-file audit is insufficient by itself. Verify Copy upsert keyed by `SalesLineID` after a successful write followed by a failed audit commit and on duplicate events. Implement a durable single-writer mechanism or prove a supported safe concurrency strategy, including crash recovery and bounded retries. Do not equate activity concurrency with cross-run serialization. |
| Completion | [Job Scheduler and activity runs](https://learn.microsoft.com/en-us/fabric/data-factory/pipeline-rest-api-capabilities) | Capture event, pipeline and activity timestamps, file identity, count and totals; validate replay, malformed CSV, joins, and visibility before marking ready. |

OneLake's documented [`FileCreated`](https://learn.microsoft.com/en-us/fabric/real-time-hub/explore-fabric-onelake-events)
can mean create **or update**; rename is a different event. A move of a staged
file may therefore not fire this subscription. Validate the actual producer
operation and completed-file readiness from captured events; do not infer either
from the filename or definition upload. Event filters use case-insensitive
`subject` prefix and suffix comparisons; the pipeline must additionally reject
anything except a direct immutable `sales_<run>_<batch>.csv` child and safely
extract an exact file name. It must not read a wildcard or whole folder.

Definition filter changes require a new Real-Time Hub source identifier and
rewiring references, per the [filter API guidance](https://learn.microsoft.com/en-us/fabric/real-time-hub/configure-event-filters-rest-api).
On deployment, use `activator-cli` authoring guidance, validate source and
captured fields, include its telemetry header on Fabric calls, handle 202 LROs
and 429 retries, and read back the definitions. Do not enable the rule until
the readiness and safe-write gates pass. Obtain approval before cross-workspace
permissions or chargeable resources; no UI fallback without an approved exception.

## Local checks

Run `python -m unittest discover -s demo-3-onelake-events-copy/tests -v` from the repository root.
The synthetic events in these tests exercise local structure only and are not
evidence of Fabric behavior. No live presenter walkthrough or cleanup is
available until deployment is implemented and approved.
