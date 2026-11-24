"""Initialize and verify the Demo 3 Lakehouse in a Fabric notebook."""

import json
from datetime import date

from pyspark.sql import functions as F
from pyspark.sql import types as T


spark.conf.set("spark.fabric.session.highConcurrency.enabled", "true")
config = RunConfig(
    run_id="demo3setup",
    start_date=setup_start_date,
    seed=setup_seed,
    date_count=setup_date_count,
    products=setup_product_count,
    customers=setup_customer_count,
    file_count=1,
    rows_per_file=1,
    interval_seconds=0,
)
generated = dimensions(config)

dimension_schemas = {
    "DimDate": T.StructType([
        T.StructField("Date", T.DateType()),
        T.StructField("Year", T.IntegerType()),
        T.StructField("MonthNumber", T.IntegerType()),
        T.StructField("MonthName", T.StringType()),
    ]),
    "DimProduct": T.StructType([
        T.StructField("ProductID", T.IntegerType()),
        T.StructField("ProductName", T.StringType()),
        T.StructField("ProductCategory", T.StringType()),
    ]),
    "DimCustomer": T.StructType([
        T.StructField("CustomerID", T.IntegerType()),
        T.StructField("CustomerName", T.StringType()),
        T.StructField("CustomerRegion", T.StringType()),
    ]),
}
keys = {"DimDate": "Date", "DimProduct": "ProductID", "DimCustomer": "CustomerID"}
fact_schema = T.StructType([
    T.StructField("SalesLineID", T.LongType()),
    T.StructField("BatchID", T.StringType()),
    T.StructField("SaleTimestamp", T.TimestampType()),
    T.StructField("CustomerID", T.IntegerType()),
    T.StructField("ProductID", T.IntegerType()),
    T.StructField("Date", T.DateType()),
    T.StructField("Quantity", T.IntegerType()),
    T.StructField("UnitPriceMinorUnits", T.LongType()),
    T.StructField("SalesAmountMinorUnits", T.LongType()),
])


def qualified(name):
    return f"`{setup_lakehouse_name}`.`dbo`.`{name}`"


def column_types(schema):
    return [(field.name, field.dataType.simpleString()) for field in schema.fields]


def create_table(name, schema, properties=""):
    columns = ", ".join(
        f"`{field.name}` {field.dataType.simpleString()}" for field in schema.fields
    )
    spark.sql(f"CREATE TABLE IF NOT EXISTS {qualified(name)} ({columns}) USING DELTA {properties}")
    actual = spark.table(qualified(name))
    if column_types(actual.schema) != column_types(schema):
        raise RuntimeError(f"{name} has a different schema")
    return actual


expected = {}
missing = []
for name, schema in dimension_schemas.items():
    rows = generated[name]
    if name == "DimDate":
        rows = [{**row, "Date": date.fromisoformat(row["Date"])} for row in rows]
    expected[name] = spark.createDataFrame(rows, schema)
    actual = create_table(name, schema)
    count = actual.count()
    if count == 0:
        missing.append(name)
    elif (
        count != len(rows)
        or actual.exceptAll(expected[name]).limit(1).count()
        or expected[name].exceptAll(actual).limit(1).count()
    ):
        raise RuntimeError(f"{name} does not match the shared deterministic dimensions")

create_table("FactSales", fact_schema, "TBLPROPERTIES ('delta.enableDeletionVectors' = 'false')")
fact_details = spark.sql(f"DESCRIBE DETAIL {qualified('FactSales')}").first().asDict()
validate_copy_protocol(fact_details)
for name in missing:
    expected[name].write.format("delta").mode("append").saveAsTable(qualified(name))

counts = {}
for name, schema in dimension_schemas.items():
    actual = spark.table(qualified(name))
    key = keys[name]
    counts[name] = actual.count()
    if (
        counts[name] != len(generated[name])
        or actual.filter(F.col(key).isNull()).limit(1).count()
        or actual.select(key).distinct().count() != counts[name]
        or actual.exceptAll(expected[name]).limit(1).count()
        or expected[name].exceptAll(actual).limit(1).count()
    ):
        raise RuntimeError(f"{name} failed dimension validation")

view_query = f"""SELECT *,
    CAST(CAST(`UnitPriceMinorUnits` AS DECIMAL(20,0)) / 100 AS DECIMAL(18,2)) AS `UnitPrice`,
    CAST(CAST(`SalesAmountMinorUnits` AS DECIMAL(20,0)) / 100 AS DECIMAL(18,2)) AS `SalesAmount`
    FROM {qualified("FactSales")}"""
spark.sql(f"CREATE VIEW IF NOT EXISTS {qualified('FactSalesAmounts')} AS {view_query}")
if spark.catalog.getTable(qualified("FactSalesAmounts")).tableType != "VIEW":
    raise RuntimeError("FactSalesAmounts is not a view")
view_ddl = " ".join(
    row[0] for row in spark.sql(f"SHOW CREATE TABLE {qualified('FactSalesAmounts')}").collect()
)
if not matches_amount_view(
    view_ddl, view_query, qualified("FactSalesAmounts"), qualified("FactSales")
):
    raise RuntimeError("FactSalesAmounts has an unexpected definition")

landing_path = (
    f"abfss://{setup_workspace_id}@onelake.dfs.fabric.microsoft.com/"
    f"{setup_lakehouse_id}/Files/incoming/sales"
)
mssparkutils.fs.mkdirs(landing_path)
mssparkutils.fs.ls(landing_path)
print(json.dumps({"dimensions": counts, "fact_table": qualified("FactSales"),
                  "amount_view": qualified("FactSalesAmounts"), "landing_path": landing_path}))
_setup_generator_dir.cleanup()
_setup_sys.modules.pop(_setup_generator_name, None)
