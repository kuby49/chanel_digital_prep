"""Offline evidence checks for one producer batch, not a Copy authorization."""

import csv
import io
import json
import re
from dataclasses import asdict


_SEGMENT = re.compile(r"[A-Za-z0-9_-][A-Za-z0-9._-]*\Z")
_PUBLISHED_OR_UNCERTAIN = {"submitting", "submitted", "unknown", "verified"}


def source_number(expected_config, source_subject, landing_directory="incoming/sales"):
    if not isinstance(landing_directory, str) or not all(
        part not in (".", "..") and _SEGMENT.fullmatch(part)
        for part in landing_directory.split("/")
    ):
        raise ValueError("invalid landing directory")
    if not isinstance(source_subject, str):
        raise ValueError("source subject must be a string")
    prefix = f"/Files/{landing_directory}/sales_{expected_config.run_id}_"
    if not source_subject.startswith(prefix) or not source_subject.endswith(".csv"):
        raise ValueError("source subject is outside the expected run and landing directory")
    number_text = source_subject[len(prefix):-4]
    if (not number_text.isascii() or not number_text.isdigit()
            or len(number_text) > max(4, len(str(expected_config.file_count)))):
        raise ValueError("invalid source batch number")
    number = int(number_text)
    if not 1 <= number <= expected_config.file_count or number_text != f"{number:04d}":
        raise ValueError("noncanonical or out-of-range source batch number")
    return number


def _same_json(left, right):
    return json.dumps(left, sort_keys=True, separators=(",", ":"), allow_nan=False) == json.dumps(
        right, sort_keys=True, separators=(",", ":"), allow_nan=False
    )


def validate_batch(generator, expected_config, manifest_bytes, source_subject, source_bytes,
                   *, event_id=None, origin="event", landing_directory="incoming/sales"):
    """Compare exact bytes and metadata to an independently supplied producer configuration.

    The caller must independently obtain the authoritative manifest and landed bytes. A
    successful comparison does not establish ownership, access, source immutability,
    writer exclusion, Copy execution, or delivery to the fact table.
    """
    if not isinstance(expected_config, generator.RunConfig):
        raise ValueError("an independently supplied producer RunConfig is required")
    try:
        expected_config.validate()
    except (TypeError, OverflowError) as exc:
        raise ValueError("invalid expected producer configuration") from exc
    if not isinstance(manifest_bytes, bytes) or not isinstance(source_bytes, bytes):
        raise ValueError("manifest and landed file must be byte strings")
    if origin not in ("event", "recovery"):
        raise ValueError("invalid validation origin")
    if event_id is not None and (
        not isinstance(event_id, str) or not event_id or len(event_id) > 512
        or not event_id.isprintable()
    ):
        raise ValueError("invalid event ID")
    if origin == "event" and event_id is None:
        raise ValueError("an event ID is required for an event")
    number = source_number(expected_config, source_subject, landing_directory)

    manifest = generator.strict_json_loads(manifest_bytes.decode("utf-8"))
    if (not isinstance(manifest, dict) or type(manifest.get("contract_version")) is not int
            or manifest["contract_version"] != 1
            or manifest.get("run_id") != expected_config.run_id
            or not _same_json(manifest.get("config"), asdict(expected_config))
            or manifest.get("generator_sha256") != generator.generator_source_sha256()):
        raise ValueError("manifest version, run ID, configuration, or generator fingerprint differs")
    destinations = manifest.get("destinations")
    if (not isinstance(destinations, list) or not all(
        isinstance(item, str) and item in ("demo-1", "demo-2", "demo-3")
        for item in destinations
    ) or len(destinations) != len(set(destinations)) or "demo-3" not in destinations):
        raise ValueError("manifest does not select Demo 3 exactly once")

    batches = manifest.get("batches")
    batch = batches.get(str(number)) if isinstance(batches, dict) else None
    if not isinstance(batch, dict):
        raise ValueError("batch metadata is absent")
    deliveries = batch.get("deliveries")
    delivery = deliveries.get("demo-3") if isinstance(deliveries, dict) else None
    status = delivery.get("status") if isinstance(delivery, dict) else None
    if not isinstance(status, str) or status not in _PUBLISHED_OR_UNCERTAIN:
        raise ValueError("Demo 3 delivery is not submitted or reconcilable")

    generated_bytes, expected = generator.batch_bytes(expected_config, number)
    if (not _same_json({key: batch.get(key) for key in expected}, expected)
            or source_bytes != generated_bytes):
        raise ValueError("landed bytes or batch metadata differs from deterministic input")
    reader = csv.reader(io.StringIO(generated_bytes.decode("utf-8")))
    if tuple(next(reader, ())) != generator.HEADER:
        raise ValueError("producer header differs from the v1 source mapping")
    keys = [int(row[0]) for row in reader]
    if (len(keys) != expected["row_count"] or not keys
            or any(key != index for index, key in enumerate(keys, start=1))):
        raise ValueError("SalesLineID must be contiguous from 1 within the batch")
    return {
        "origin": origin, "event_id": event_id, "source_subject": source_subject,
        "batch_id": expected["batch_id"], "sha256": expected["sha256"],
        "row_count": expected["row_count"],
        "sales_amount_minor_units": expected["sales_amount_minor_units"],
        "first_sales_line_id": keys[0],
        "last_sales_line_id": keys[-1],
        **{key: expected[key] for key in
           ("amount_by_date", "amount_by_product_category", "amount_by_customer_region")},
        "delivery_status": status,
    }
