"""Local checks for the shared producer manifest contract v1."""

import re


MANIFEST_FIELDS = {"run_id", "config", "destinations", "batches"}
BATCH_FIELDS = {
    "batch_id",
    "batch_number",
    "filename",
    "sha256",
    "row_count",
    "sales_amount_minor_units",
    "amount_by_date",
    "amount_by_product_category",
    "amount_by_customer_region",
    "generated_at_utc",
    "deliveries",
    "batch_class",
}
BATCH_CLASSES = {"measured", "bootstrap", "negative-test"}
DESTINATIONS = {"demo-1", "demo-2", "demo-3"}
GROUPED_TOTAL_FIELDS = (
    "amount_by_date",
    "amount_by_product_category",
    "amount_by_customer_region",
)


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
    if not isinstance(manifest.get("config"), dict):
        diagnostics.append("manifest config must be an object")
    elif manifest.get("config", {}).get("run_id") != manifest.get("run_id"):
        diagnostics.append("manifest config run_id must match the manifest run_id")

    destinations = manifest.get("destinations")
    if not isinstance(destinations, list) or any(
        not isinstance(destination, str) or destination not in DESTINATIONS
        for destination in destinations
    ):
        diagnostics.append("manifest destinations must be an array of demo-1/demo-2/demo-3")
        destinations = []
    elif len(destinations) != len(set(destinations)):
        diagnostics.append("manifest destinations must not contain duplicates")

    batches = manifest.get("batches")
    if not isinstance(batches, dict):
        diagnostics.append("manifest batches must be an object keyed by batch number")
        return diagnostics

    for key, batch in batches.items():
        if not isinstance(key, str) or not key.isdecimal():
            diagnostics.append(f"batch key {key!r} must be a decimal batch number")
            continue
        if not isinstance(batch, dict):
            diagnostics.append(f"batch {key} must be an object")
            continue
        missing_batch_fields = BATCH_FIELDS - batch.keys()
        if missing_batch_fields:
            diagnostics.append(
                f"batch {key} is missing fields: {', '.join(sorted(missing_batch_fields))}"
            )
        if batch.get("batch_class") not in BATCH_CLASSES:
            diagnostics.append(
                f"batch {key} batch_class must be measured, bootstrap, or negative-test"
            )
        if (
            not isinstance(batch.get("batch_number"), int)
            or isinstance(batch.get("batch_number"), bool)
            or batch["batch_number"] < 1
            or batch.get("batch_number") != int(key)
        ):
            diagnostics.append(f"batch {key} batch_number must match its positive object key")
        if not isinstance(batch.get("sha256"), str) or not re.fullmatch(
            r"[0-9a-f]{64}", batch["sha256"]
        ):
            diagnostics.append(f"batch {key} sha256 must be a 64-character lowercase hex digest")
        if (
            not isinstance(batch.get("row_count"), int)
            or isinstance(batch.get("row_count"), bool)
            or batch["row_count"] <= 0
        ):
            diagnostics.append(f"batch {key} row_count must be a positive integer")
        if (
            not isinstance(batch.get("sales_amount_minor_units"), int)
            or isinstance(batch.get("sales_amount_minor_units"), bool)
        ):
            diagnostics.append(f"batch {key} sales_amount_minor_units must be an integer")
        if not isinstance(batch.get("generated_at_utc"), str) or not batch["generated_at_utc"]:
            diagnostics.append(f"batch {key} generated_at_utc must be a nonempty string")
        for field in GROUPED_TOTAL_FIELDS:
            totals = batch.get(field)
            if not isinstance(totals, dict) or not totals or any(
                not isinstance(amount, int) or isinstance(amount, bool)
                for amount in totals.values()
            ):
                diagnostics.append(f"batch {key} {field} must map group names to integer totals")
            elif totals and sum(totals.values()) != batch.get("sales_amount_minor_units"):
                diagnostics.append(
                    f"batch {key} {field} must sum to sales_amount_minor_units"
                )
        deliveries = batch.get("deliveries")
        if not isinstance(deliveries, dict) or set(deliveries) != set(destinations):
            diagnostics.append(f"batch {key} deliveries must match manifest destinations")
        else:
            for destination, delivery in deliveries.items():
                if not isinstance(delivery, dict) or not {
                    "status", "timestamp_utc"
                }.issubset(delivery):
                    diagnostics.append(
                        f"batch {key} delivery for {destination} requires status and timestamp_utc"
                    )
    return diagnostics


def measured_batches(manifest):
    """Select only measured batches for the coordinated-run acceptance totals."""
    batches = manifest.get("batches", {})
    if not isinstance(batches, dict):
        return []
    return [
        batches[key]
        for key in sorted(batches, key=lambda value: int(value) if str(value).isdecimal() else -1)
        if isinstance(batches[key], dict) and batches[key].get("batch_class") == "measured"
    ]
