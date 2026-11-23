"""Deterministic v1 sales data and recoverable local staging primitives."""

import csv
import hashlib
import io
import json
import os
import re
import tempfile
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path


HEADER = (
    "SalesLineID", "BatchID", "SaleTimestamp", "CustomerID", "ProductID",
    "Date", "Quantity", "UnitPriceMinorUnits", "SalesAmountMinorUnits",
)


@dataclass(frozen=True)
class RunConfig:
    run_id: str
    batch_class: str = "measured"
    start_date: str = "2024-01-01"
    seed: int = 42
    date_count: int = 365
    products: int = 20
    customers: int = 100
    file_count: int = 10
    rows_per_file: int = 1000
    interval_seconds: int = 60

    def validate(self):
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}", self.run_id):
            raise ValueError("run_id must contain only letters, digits, underscores or hyphens")
        if self.batch_class not in ("measured", "bootstrap", "negative-test"):
            raise ValueError("batch_class must be measured, bootstrap or negative-test")
        date.fromisoformat(self.start_date)
        if any(not isinstance(value, int) or isinstance(value, bool) or value <= 0
               for value in (self.date_count, self.products, self.customers,
                             self.file_count, self.rows_per_file)):
            raise ValueError("dimension sizes, file count and rows per file must be positive integers")
        if not isinstance(self.seed, int) or isinstance(self.seed, bool):
            raise ValueError("seed must be an integer")
        if not isinstance(self.interval_seconds, int) or isinstance(self.interval_seconds, bool) or self.interval_seconds < 0:
            raise ValueError("interval_seconds must be a nonnegative integer")
        if self.products > 2147483647 or self.customers > 2147483647:
            raise ValueError("dimension IDs exceed INT")
        if self.file_count * self.rows_per_file > 9223372036854775807:
            raise ValueError("sales line IDs exceed BIGINT")
        date.fromisoformat(self.start_date) + timedelta(days=self.date_count - 1)


def dimensions(config):
    config.validate()
    start = date.fromisoformat(config.start_date)
    dates = []
    for offset in range(config.date_count):
        day = start + timedelta(days=offset)
        dates.append({"Date": day.isoformat(), "Year": day.year, "MonthNumber": day.month,
                      "MonthName": ("January", "February", "March", "April", "May", "June",
                                    "July", "August", "September", "October", "November",
                                    "December")[day.month - 1]})
    categories = ("Beauty", "Fashion", "Fragrance", "Accessories")
    regions = ("Americas", "Europe", "Asia Pacific")
    products = [{"ProductID": n, "ProductName": f"Product {n:03d}",
                 "ProductCategory": categories[(n - 1) % len(categories)]}
                for n in range(1, config.products + 1)]
    customers = [{"CustomerID": n, "CustomerName": f"Customer {n:03d}",
                  "CustomerRegion": regions[(n - 1) % len(regions)]}
                 for n in range(1, config.customers + 1)]
    return {"DimDate": dates, "DimProduct": products, "DimCustomer": customers}


def batch_bytes(config, batch_number):
    config.validate()
    if not isinstance(batch_number, int) or isinstance(batch_number, bool) or not 1 <= batch_number <= config.file_count:
        raise ValueError("batch_number is outside the configured run")
    batch_id = f"{config.run_id}_{batch_number:04d}"
    output = io.StringIO(newline="")
    writer = csv.writer(output, lineterminator="\n", quoting=csv.QUOTE_MINIMAL)
    writer.writerow(HEADER)
    first = (batch_number - 1) * config.rows_per_file
    start = date.fromisoformat(config.start_date)
    total = 0
    by_date, by_category, by_region = {}, {}, {}
    categories = ("Beauty", "Fashion", "Fragrance", "Accessories")
    regions = ("Americas", "Europe", "Asia Pacific")
    for offset in range(config.rows_per_file):
        index = first + offset
        day = start + timedelta(days=index % config.date_count)
        customer = index % config.customers + 1
        product = index % config.products + 1
        digest = hashlib.sha256(f"{config.seed}:{index}".encode("ascii")).digest()
        quantity = int.from_bytes(digest[:8], "big") % 5 + 1
        price = int.from_bytes(digest[8:16], "big") % 99901 + 100
        amount = quantity * price
        timestamp = datetime.combine(day, datetime.min.time(), tzinfo=timezone.utc) + timedelta(seconds=index % 86400)
        writer.writerow((index + 1, batch_id, timestamp.strftime("%Y-%m-%dT%H:%M:%SZ"),
                         customer, product, day.isoformat(), quantity, price, amount))
        total += amount
        for groups, key in ((by_date, day.isoformat()),
                            (by_category, categories[(product - 1) % len(categories)]),
                            (by_region, regions[(customer - 1) % len(regions)])):
            groups[key] = groups.get(key, 0) + amount
    payload = output.getvalue().encode("utf-8")
    return payload, {"batch_id": batch_id, "batch_number": batch_number, "batch_class": config.batch_class,
                     "filename": f"sales_{batch_id}.csv", "sha256": hashlib.sha256(payload).hexdigest(),
                     "row_count": config.rows_per_file, "sales_amount_minor_units": total,
                     "amount_by_date": by_date, "amount_by_product_category": by_category,
                     "amount_by_customer_region": by_region}


def _atomic_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(prefix=".manifest-", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as file:
            json.dump(value, file, sort_keys=True, indent=2)
            file.write("\n")
            file.flush()
            os.fsync(file.fileno())
        os.replace(name, path)
    finally:
        if os.path.exists(name):
            os.unlink(name)


def new_manifest(config, destinations):
    config.validate()
    if not destinations or len(set(destinations)) != len(destinations) or any(
        name not in ("demo-1", "demo-2", "demo-3") for name in destinations
    ):
        raise ValueError("select distinct demo destinations")
    return {"contract_version": 1, "run_id": config.run_id, "config": asdict(config),
            "destinations": sorted(destinations), "batches": {}}


def initialize_run(root, config, destinations):
    """Create or validate a run manifest; callers must serialize access."""
    expected = new_manifest(config, destinations)
    root = Path(root) / config.run_id
    root.mkdir(parents=True, exist_ok=True)
    manifest_path = root / "manifest.json"
    if manifest_path.exists():
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        if manifest["config"] != expected["config"] or manifest["destinations"] != expected["destinations"]:
            raise ValueError("existing run configuration or destinations differ; use a new run ID")
    else:
        if any(path.name != ".producer.lock" for path in root.iterdir()):
            raise RuntimeError("run contains untracked files; manual recovery required")
        manifest = expected
        _atomic_json(manifest_path, manifest)
    return root, manifest


def stage_batch(root, config, batch_number, destinations):
    """Stage once; callers must serialize access to a run directory."""
    root, manifest = initialize_run(root, config, destinations)
    manifest_path = root / "manifest.json"
    key = str(batch_number)
    if key in manifest["batches"]:
        entry = manifest["batches"][key]
        path = root / entry["filename"]
        if not path.is_file() or hashlib.sha256(path.read_bytes()).hexdigest() != entry["sha256"]:
            raise RuntimeError("staged batch missing or corrupt; manual recovery required")
        return entry
    payload, entry = batch_bytes(config, batch_number)
    path = root / entry["filename"]
    if path.exists():
        raise RuntimeError("untracked staged batch exists; manual recovery required")
    with path.open("xb") as file:
        file.write(payload)
        file.flush()
        os.fsync(file.fileno())
    entry["generated_at_utc"] = datetime.now(timezone.utc).isoformat()
    entry["deliveries"] = {name: {"status": "pending", "timestamp_utc": None}
                           for name in sorted(destinations)}
    manifest["batches"][key] = entry
    _atomic_json(manifest_path, manifest)
    return entry
