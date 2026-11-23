"""Reconcile a measured-only query export with the producer's v1 manifest."""

import argparse
import json
import sys
from pathlib import Path


GROUPS = ("amount_by_date", "amount_by_product_category", "amount_by_customer_region")


def _integer(value):
    return isinstance(value, int) and not isinstance(value, bool)


def reconcile(manifest, observed):
    """Compare a query export, without claiming delivery or event evidence."""
    if not isinstance(manifest, dict) or manifest.get("contract_version") != 1:
        raise ValueError("manifest must use contract_version 1")
    batches = manifest.get("batches")
    if not isinstance(batches, dict):
        raise ValueError("manifest batches must be an object")
    expected = {}
    for batch in batches.values():
        if not isinstance(batch, dict) or batch.get("batch_class") not in (
            "measured", "bootstrap", "negative-test"
        ):
            raise ValueError("batch class is missing or invalid")
        if batch["batch_class"] != "measured":
            continue
        batch_id = batch.get("batch_id")
        if not isinstance(batch_id, str) or not batch_id or batch_id in expected:
            raise ValueError("measured batch IDs must be nonempty and unique")
        expected[batch_id] = batch
    if not expected:
        raise ValueError("manifest contains no measured batches")
    if not isinstance(observed, dict) or not isinstance(observed.get("batches"), dict):
        raise ValueError("query export batches must be an object keyed by batch ID")
    actual = observed["batches"]
    errors = []
    for name in ("duplicate_sales_line_ids", "orphan_rows"):
        value = observed.get(name)
        if not _integer(value) or value != 0:
            errors.append(f"{name} must be zero")
    missing, extra = set(expected) - set(actual), set(actual) - set(expected)
    if missing:
        errors.append("missing measured batches: " + ", ".join(sorted(missing)))
    if extra:
        errors.append("unexpected query batches: " + ", ".join(sorted(extra)))
    for batch_id in sorted(set(expected) & set(actual)):
        reference, result = expected[batch_id], actual[batch_id]
        if not isinstance(result, dict):
            errors.append(f"{batch_id}: query batch must be an object")
            continue
        for field in ("row_count", "sales_amount_minor_units"):
            if not _integer(reference.get(field)) or not _integer(result.get(field)):
                errors.append(f"{batch_id}: {field} must be an integer")
            elif reference[field] != result[field]:
                errors.append(f"{batch_id}: {field} does not match")
        if _integer(reference.get("row_count")) and reference["row_count"] <= 0:
            errors.append(f"{batch_id}: row_count must be positive")
        for field in GROUPS:
            left, right = reference.get(field), result.get(field)
            if (not isinstance(left, dict) or not isinstance(right, dict)
                    or any(not isinstance(k, str) or not _integer(v) for k, v in left.items())
                    or any(not isinstance(k, str) or not _integer(v) for k, v in right.items())):
                errors.append(f"{batch_id}: {field} must map strings to integers")
            elif left != right:
                errors.append(f"{batch_id}: {field} does not match")
            elif _integer(reference.get("sales_amount_minor_units")) and sum(left.values()) != reference["sales_amount_minor_units"]:
                errors.append(f"{batch_id}: {field} does not sum to the batch total")
    return errors


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", required=True, type=Path)
    parser.add_argument("--query-export", required=True, type=Path)
    args = parser.parse_args(argv)
    try:
        manifest = json.loads(args.manifest.read_text(encoding="utf-8"))
        observed = json.loads(args.query_export.read_text(encoding="utf-8"))
        errors = reconcile(manifest, observed)
    except (OSError, ValueError) as exc:
        print(f"Invalid reconciliation input: {type(exc).__name__}", file=sys.stderr)
        return 1
    if errors:
        for error in errors:
            print(error, file=sys.stderr)
        return 1
    print("Measured batch counts, integer totals and groups match; delivery and event evidence remain unverified.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
