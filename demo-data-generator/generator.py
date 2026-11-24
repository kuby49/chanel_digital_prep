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
MAX_SALES_LINE_ID = 9223372036854775807


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
        if self.file_count * self.rows_per_file > MAX_SALES_LINE_ID:
            raise ValueError("sales line IDs exceed BIGINT")
        date.fromisoformat(self.start_date) + timedelta(days=self.date_count - 1)


def sales_line_id_range(config):
    config.validate()
    count = config.file_count * config.rows_per_file
    digest = hashlib.sha256(b"chanel-sales-line-id-v1\0" + config.run_id.encode("ascii")).digest()
    start = int.from_bytes(digest, "big") % (MAX_SALES_LINE_ID - count + 1) + 1
    return start, start + count - 1


def _source_sha256(source):
    source = source.decode("utf-8").replace("\r\n", "\n").replace("\r", "\n")
    return hashlib.sha256(source.encode("utf-8")).hexdigest()


_IMPORTED_GENERATOR_SHA256 = _source_sha256(Path(__file__).read_bytes())


def generator_source_sha256():
    try:
        current = _source_sha256(Path(__file__).read_bytes())
    except (OSError, UnicodeError):
        raise RuntimeError("Generator source is unavailable or invalid; staging is blocked") from None
    if current != _IMPORTED_GENERATOR_SHA256:
        raise RuntimeError("Generator source changed after import; restart before staging")
    return _IMPORTED_GENERATOR_SHA256


def _unique_pairs(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON key")
        result[key] = value
    return result


def _invalid_constant(value):
    raise ValueError(f"invalid JSON constant: {value}")


def strict_json_loads(value):
    return json.loads(value, object_pairs_hook=_unique_pairs, parse_constant=_invalid_constant)


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
    first_sales_line_id, _ = sales_line_id_range(config)
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
        writer.writerow((first_sales_line_id + index, batch_id, timestamp.strftime("%Y-%m-%dT%H:%M:%SZ"),
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


def batch_metadata_matches(entry, expected):
    from sales_ids import canonical_bytes

    return (isinstance(entry, dict)
            and canonical_bytes({name: entry.get(name) for name in expected}) == canonical_bytes(expected))


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


def new_manifest(config, destinations, reservation):
    config.validate()
    if not destinations or len(set(destinations)) != len(destinations) or any(
        name not in ("demo-1", "demo-2", "demo-3") for name in destinations
    ):
        raise ValueError("select distinct demo destinations")
    if not isinstance(reservation, dict):
        raise ValueError("a durable SalesLineID reservation is required")
    return {"contract_version": 1, "run_id": config.run_id, "config": asdict(config),
            "destinations": sorted(destinations), "sales_line_id_reservation": dict(reservation), "batches": {}}


def initialize_run(root, config, destinations):
    """Create or validate a run manifest; callers must serialize access."""
    from sales_ids import canonical_bytes, local_reservation

    config.validate()
    run_root = Path(root) / config.run_id
    manifest_path = run_root / "manifest.json"
    exists = manifest_path.exists()
    if exists:
        manifest = strict_json_loads(manifest_path.read_text(encoding="utf-8"))
        if (not isinstance(manifest, dict) or type(manifest.get("contract_version")) is not int
                or manifest["contract_version"] != 1 or manifest.get("run_id") != config.run_id
                or canonical_bytes(manifest.get("config")) != canonical_bytes(asdict(config))
                or manifest.get("destinations") != sorted(destinations)
                or not isinstance(manifest.get("batches"), dict)):
            raise ValueError("existing run configuration or destinations differ; use a new run ID")
    elif run_root.exists() and any(path.name != ".producer.lock" for path in run_root.iterdir()):
        raise RuntimeError("run contains untracked files; manual recovery required")
    reservation = local_reservation(root, config, destinations, require_existing=exists, require_new=not exists)
    expected = new_manifest(config, destinations, reservation)
    if exists:
        if canonical_bytes(manifest.get("sales_line_id_reservation")) != canonical_bytes(reservation):
            raise RuntimeError("run manifest reservation differs from the allocator")
    else:
        run_root.mkdir(parents=True, exist_ok=True)
        manifest = expected
        _atomic_json(manifest_path, manifest)
    return run_root, manifest


def stage_batch(root, config, batch_number, destinations):
    """Stage once; callers must serialize access to a run directory."""
    payload, expected = batch_bytes(config, batch_number)
    root, manifest = initialize_run(root, config, destinations)
    manifest_path = root / "manifest.json"
    key = str(batch_number)
    if key in manifest["batches"]:
        entry = manifest["batches"][key]
        if not batch_metadata_matches(entry, expected):
            raise RuntimeError("staged batch metadata differs from deterministic input")
        path = root / entry["filename"]
        if not path.is_file() or hashlib.sha256(path.read_bytes()).hexdigest() != expected["sha256"]:
            raise RuntimeError("staged batch missing or corrupt; manual recovery required")
        return entry
    entry = expected
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
