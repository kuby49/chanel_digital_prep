# Demo 3: reset and run

Open the existing combined Fabric pipeline and select **Run**. The producer
uses `simple_demo3.py` to stop the fixed Activator and any active runs of its
Copy pipeline, clear `Files/incoming/sales`, and recreate `dbo.factsales` and
the sample dimensions in the configured Lakehouse. It enables the Activator
and automatically waits 180 seconds before publishing sales. This measured
startup mitigation is not a service-readiness guarantee.
Each native `FileCreated` event passes `___subject`
to a single Copy activity, which reads that exact CSV and appends to the
Delta fact table. There is no guard notebook, audit store or replay step.

On normal completion the helper checks that every expected numbered batch is
present in the fact table, then disables the trigger and lets active Copy jobs
finish. A missing batch raises an error after the normal finite completion wait.
Failure cleanup disables the trigger and cancels active Copy jobs. Both leave
the results in place. Inspect `dbo.factsales` and join `dbo.DimCustomer`,
`dbo.DimProduct` and `dbo.DimDate` on `CustomerID`, `ProductID` and `Date`;
divide the minor-unit fields by 100 using decimal arithmetic for display.
If the run fails, check Fabric pipeline history and press **Run** again to
reset and start over.

## One-time integration

1. Stop other producers targeting this Lakehouse and stop the existing
   Activator. Finish or cancel any jobs from the old guarded pipeline,
   including any still-running guard notebooks. Do not clear the old
   audit or lock data.
2. Read and decode `pipeline-content.json` from the fixed pipeline identified
   in `demo-settings.json`. Pass that JSON object to
   `simple_demo3.copy_pipeline(config, existing)` and publish the returned
   `pipeline-content.json` to the **same pipeline**. This retains the existing
   Lakehouse connection and column mappings and selects Append without key
   settings. Leave the
   existing Reflex linked to that pipeline; `start()` retains the event
   subject binding and removes the obsolete event ID argument.
3. Bundle `simple_demo3.py` and the producer's `simple_data.py` in the
   producer notebook. The helper uses Fabric's `notebookutils.fs` and the
   provided workspace-crossing Spark session. The notebook identity needs
   access to edit the configured Reflex, list/cancel the configured pipeline's
   jobs, and write the target's landing folder, fact and dimensions. Reuse
   the configured OAuth2 connection; no new resource or root setting is needed.

The end-of-run batch presence check does not reconcile individual rows or files. Stopping the
Reflex and draining listed jobs does not prove that the service discarded
every queued event. The default capacity size is F16. A manual
combined-pipeline Run completed at that size; independent destination
verification and two consecutive presenter Runs remain unverified. These
helpers are published to the existing demo; the older `README.md` documents
the legacy flow, not prerequisites for this one.
