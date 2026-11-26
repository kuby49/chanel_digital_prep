# Chanel Fabric ingestion demonstrations

The [specification](SPECIFICATION.md) describes the three demos and their
shared reset-and-run lifecycle. The simplified implementation has been
published to the existing Fabric items. A manual presenter Run completed on
F16; independent end-to-end result verification remains pending.
For the data flows and their closest Databricks counterparts, open the
[six-slide interactive architecture briefing](architecture/ingestion-scenarios.html).

## Run the demos

Sign in to MCAP and open the
[combined pipeline](https://app.fabric.microsoft.com/groups/68971aa2-f4bc-48a2-8962-98a4cb1e9fc2/pipelines/2f42bac0-ccc3-4efc-8ff3-787fc93f3827?experience=power-bi).
Choose **Home > Run**; no technical parameters or separate monitor are needed.
The pipeline resets the demo data, starts its native prerequisites and produces
ten sample files. On completion, it stops the schedule and trigger, leaving
the results for inspection. If a run fails, report the error and restart from
the beginning; the next run resets the demos rather than resuming old data.

| Demo | Result |
|---|---|
| Materialized Lake Views | [ChanelDemo1Lakehouse](https://app.fabric.microsoft.com/groups/af865b3c-f088-44ad-8816-a5e495b1fbf5/lakehouses/646c26d7-7df8-4962-acaa-4e1386bcba08?experience=power-bi): `bronze.sales_files` and `silver.sales_joined`. |
| Eventhouse and OneLake | [SalesDatabase](https://app.fabric.microsoft.com/groups/0a0499fe-51b5-42b1-bc99-31c121cc74a6/databases/25159007-a6dd-4020-8afc-01d393378727?experience=power-bi): `SyntheticFactSales`; [SalesLakehouse](https://app.fabric.microsoft.com/groups/0a0499fe-51b5-42b1-bc99-31c121cc74a6/lakehouses/f565aeac-f321-46f2-8262-076e06490707?experience=power-bi): `EventhouseFactSales` shortcut. |
| OneLake events and Copy | [Activator](https://app.fabric.microsoft.com/groups/744223de-ceab-4c4f-82ba-f811bcb5c7d7/reflexes/195d1691-08f6-4140-a6d5-18c332696fef?experience=power-bi) and [target Lakehouse](https://app.fabric.microsoft.com/groups/744223de-ceab-4c4f-82ba-f811bcb5c7d7/lakehouses/6bba6989-5cfd-4192-8e75-00274afb66c1?experience=power-bi): `dbo.factsales`. |

Database-level OneLake availability is enabled for the demo Eventhouse
database. If it is manually disabled, enable it again in the database details
UI; the reset does not configure that database-level switch. The five-minute
table mirroring target is not a visibility guarantee.

The default demo capacity size is F16. Earlier live runs used F32. On
2026-11-26, a manual Run of the combined pipeline and its producer notebook
both completed on F16; that status alone does not independently verify every
destination's results. The capacity was suspended after the run. Retained
results are accessible again after it is resumed. The published notebook's
bundled capacity metadata may lag the settings file; it does not control or
validate the live capacity size.
