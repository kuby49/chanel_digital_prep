"""Local checks for the shared producer manifest contract v1."""


MANIFEST_FIELDS = {"run_id", "config", "destinations", "batches"}
BATCH_FIELDS = {
    "batch_id",
    "batch_number",
    "filename",
    "sha256",
    "row_count",
    "sales_amount_minor_units",
    "grouped_totals",
    "generated_at_utc",
    "deliveries",
    "batch_class",
}
BATCH_CLASSES = {"measured", "bootstrap", "negative-test"}


def validate_manifest(manifest):
    """Return diagnostics for contract-version and batch-class violations."""
    diagnostics = []
    if not isinstance(manifest, dict):
        return ["manifest must be a JSON object"]

    if manifest.get("contract_version") != 1:
        diagnostics.append("manifest contract_version must be 1")
    missing = MANIFEST_FIELDS - manifest.keys()
    if missing:
        diagnostics.append(f"manifest is missing fields: {', '.join(sorted(missing))}")

    batches = manifest.get("batches")
    if not isinstance(batches, list):
        diagnostics.append("manifest batches must be an array")
        return diagnostics

    for index, batch in enumerate(batches):
        if not isinstance(batch, dict):
            diagnostics.append(f"batch {index} must be an object")
            continue
        missing_batch_fields = BATCH_FIELDS - batch.keys()
        if missing_batch_fields:
            diagnostics.append(
                f"batch {index} is missing fields: {', '.join(sorted(missing_batch_fields))}"
            )
        if batch.get("batch_class") not in BATCH_CLASSES:
            diagnostics.append(
                f"batch {index} batch_class must be measured, bootstrap, or negative-test"
            )
    return diagnostics


def measured_batches(manifest):
    """Select only measured batches for the coordinated-run acceptance totals."""
    return [
        batch
        for batch in manifest.get("batches", [])
        if isinstance(batch, dict) and batch.get("batch_class") == "measured"
    ]
