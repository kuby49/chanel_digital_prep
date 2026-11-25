# Demo 2: simple reset and run

The shared Fabric Run resets the fixed Eventhouse sales table, restores its
dimensions, then submits the same generated CSV bytes as the other demos.
Eventhouse exposes its managed Delta table through the existing Lakehouse
shortcut; the completion step shows both KQL sales and the shortcut joined to
customer, product and date dimensions.

The producer bundles `simple_demo2.py`, `eventhouse_core.py` and the shared
`simple_data.py`, and calls `reset(context)`, `start(context)`,
`submit_batch(context, csv_bytes)` for each file and `stop(context)`. On failure
it calls `stop(context, failed=True)` and reports the error. The next Run resets
and starts again. No results are deleted by `stop`.

The signed-in identity needs KQL Table Admin for reset, KQL ingestion and read
permissions, and read/write access to the configured Lakehouse and shortcut.
The notebook environment needs `azure-kusto-data`. Settings come from `demo-2`
and `data` in the shared `demo-settings.json`. Capacity stays F32.

Reset turns OneLake availability off, clears the KQL table, then turns it back on
with the configured five-minute target. It never deletes managed Delta files.
The normal stop waits for native mirroring status and displays sample results;
OneLake propagation is asynchronous and five minutes is not an SLA.

CSV uses synchronous `.ingest inline`, which Microsoft documents for
demonstrations and prototyping rather than production or high-volume ingestion.
An interrupted or timed-out request may have an uncertain outcome. Stop any
older queued ingestion before the first simplified Run; cancelling its producer
does not necessarily cancel work already accepted by the service.
