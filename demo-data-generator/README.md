# Shared data producer

The local Python module defines **CSV contract v1**: UTF-8, comma-delimited,
header present, minimal RFC 4180 quoting, LF line endings, no nullable fields,
ISO dates and UTC timestamps ending in `Z`. Amounts are integer cents in USD;
`SalesAmountMinorUnits = Quantity * UnitPriceMinorUnits`. `dimensions()` produces
the three deterministic dimension datasets. Sales keys cycle through each
dimension; coverage of every dimension key requires the total number of rows
to be at least as large as the largest dimension.

`batch_bytes(RunConfig(...), batch_number)` returns the exact CSV bytes and
reference totals including grouped date, product category, and customer region
totals. The batch timestamp is derived from the logical sale date, not the
delivery time. The same run ID, configuration and batch number give the same
bytes; quantities and prices use SHA-256 rather than runtime-specific random
number generation. Use a new run ID for intentionally new data.
Every manifest has `contract_version: 1` and each batch records `batch_class`
(`measured`, `bootstrap`, or `negative-test`); a run has one fixed batch class.
Classification alone does not generate malformed data for a negative test.

`stage_batch(root, config, batch_number, destinations)` stores immutable CSVs
and a v1 JSON manifest under `root/<run_id>`. It verifies existing files and
rejects configuration changes or untracked/corrupt files instead of silently
regenerating them. It requires a single writer per run; a crash between staging
a file and updating the manifest requires manual recovery. Filesystem rename
and durability behavior have only been tested locally, not on OneLake.

`deploy.py` implements the shared v1 invocation and result envelope. Dry-run
is a plan only; all other operations explicitly report blocked. In particular,
there is no deployed notebook, pipeline, destination transport, delivery retry,
delivery-state update, or source scheduler yet. No measured run can be started
until Fabric storage semantics, cross-workspace permissions, Eventhouse
idempotency, and readiness validation are implemented and verified. A rename is
not assumed to emit the `FileCreated` event: the selected completion operation,
event type, and fully readable contents must be demonstrated in the tenant
before Demo 3 is ready. Do not treat these local primitives as a working Fabric
producer.

Run focused tests with `python -m unittest discover -s demo-data-generator -p "test_*.py"`.
