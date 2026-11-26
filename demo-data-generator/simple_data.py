"""Shared deterministic dimensions and sales CSV files."""

import csv
import io
import random
from datetime import date, datetime, time, timedelta, timezone


HEADER = (
    "SalesLineID", "BatchID", "SaleTimestamp", "CustomerID", "ProductID",
    "Date", "Quantity", "UnitPriceMinorUnits", "SalesAmountMinorUnits",
)


def dimensions(data):
    start = date.fromisoformat(data["start_date"])
    months = ("January", "February", "March", "April", "May", "June", "July",
              "August", "September", "October", "November", "December")
    categories = ("Beauty", "Fashion", "Fragrance", "Accessories")
    regions = ("Americas", "Europe", "Asia Pacific")
    dates = []
    for offset in range(data["date_count"]):
        day = start + timedelta(days=offset)
        dates.append({"Date": day.isoformat(), "Year": day.year, "MonthNumber": day.month,
                      "MonthName": months[day.month - 1]})
    products = [{"ProductID": n, "ProductName": f"Product {n:03d}",
                 "ProductCategory": categories[(n - 1) % len(categories)]}
                for n in range(1, data["product_count"] + 1)]
    customers = [{"CustomerID": n, "CustomerName": f"Customer {n:03d}",
                  "CustomerRegion": regions[(n - 1) % len(regions)]}
                 for n in range(1, data["customer_count"] + 1)]
    return {"DimDate": dates, "DimProduct": products, "DimCustomer": customers}


def batch_id(number):
    return f"batch_{number:04d}"


def sales_csv(data, number):
    start = date.fromisoformat(data["start_date"])
    rng = random.Random(data["seed"] + number - 1)
    output = io.StringIO(newline="")
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow(HEADER)
    for offset in range(data["rows_per_file"]):
        index = (number - 1) * data["rows_per_file"] + offset
        day = start + timedelta(days=index % data["date_count"])
        stamp = datetime.combine(day, time(), tzinfo=timezone.utc) + timedelta(seconds=index % 86400)
        quantity, price = rng.randint(1, 5), rng.randint(100, 100000)
        writer.writerow((offset + 1, batch_id(number), stamp.strftime("%Y-%m-%dT%H:%M:%SZ"),
                         index % data["customer_count"] + 1, index % data["product_count"] + 1,
                         day.isoformat(), quantity, price, quantity * price))
    return output.getvalue().encode("utf-8")
