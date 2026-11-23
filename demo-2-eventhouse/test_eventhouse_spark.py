import csv
import importlib.util
import io
import os
import sys
import tempfile
import unittest
from datetime import datetime
from pathlib import Path

from lakehouse_validation import (
    NotVisible,
    VerificationError,
    _dimension_frames,
    verify_dimensions,
    verify_lakehouse_batch,
    verify_run_keys,
)


class SparkVerificationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        default = Path(__file__).resolve().parent.parent / "demo-data-generator" / "generator.py"
        source = Path(os.environ.get("CHANEL_PRODUCER_GENERATOR", str(default)))
        if not source.is_file():
            raise unittest.SkipTest("The shared producer has not been merged into this worktree")
        try:
            from pyspark.sql import SparkSession
            from pyspark.sql.types import (
                IntegerType, LongType, StringType, StructField, StructType, TimestampType,
            )
        except ImportError as exc:
            raise unittest.SkipTest("PySpark is not installed") from exc

        spec = importlib.util.spec_from_file_location("spark_test_generator", source)
        cls.generator = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = cls.generator
        spec.loader.exec_module(cls.generator)
        os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
        cls.warehouse = tempfile.TemporaryDirectory()
        cls.addClassCleanup(cls.warehouse.cleanup)
        cls.spark = (
            SparkSession.builder.master("local[1]")
            .appName("chanel-eventhouse-tests")
            .config("spark.ui.enabled", "false")
            .config("spark.ui.showConsoleProgress", "false")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.warehouse.dir", Path(cls.warehouse.name).as_uri())
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")
        config = cls.generator.RunConfig(
            run_id="spark_contract", start_date="2026-01-01", date_count=4,
            products=3, customers=3, file_count=1, rows_per_file=12,
        )
        payload, cls.batch = cls.generator.batch_bytes(config, 1)
        cls.data = {
            "run_id": config.run_id, "start_date": config.start_date,
            "date_count": config.date_count, "product_count": config.products,
            "customer_count": config.customers, "seed": config.seed,
        }
        schema = StructType([
            StructField("SalesLineID", LongType(), False),
            StructField("BatchID", StringType(), False),
            StructField("SaleTimestamp", TimestampType(), False),
            StructField("CustomerID", IntegerType(), False),
            StructField("ProductID", IntegerType(), False),
            StructField("Date", StringType(), False),
            StructField("Quantity", IntegerType(), False),
            StructField("UnitPriceMinorUnits", LongType(), False),
            StructField("SalesAmountMinorUnits", LongType(), False),
        ])
        rows = []
        for row in csv.DictReader(io.StringIO(payload.decode("utf-8"))):
            rows.append((
                int(row["SalesLineID"]), row["BatchID"],
                datetime.fromisoformat(row["SaleTimestamp"].replace("Z", "+00:00")),
                int(row["CustomerID"]), int(row["ProductID"]), row["Date"],
                int(row["Quantity"]), int(row["UnitPriceMinorUnits"]),
                int(row["SalesAmountMinorUnits"]),
            ))
        cls.facts = cls.spark.createDataFrame(rows, schema).cache()
        cls.facts.count()
        cls.dimensions = {
            name: frame.cache()
            for name, frame in _dimension_frames(cls.spark, cls.data, cls.generator).items()
        }
        for frame in cls.dimensions.values():
            frame.count()

    @classmethod
    def tearDownClass(cls):
        if hasattr(cls, "spark"):
            cls.spark.stop()
        sys.modules.pop("spark_test_generator", None)

    def setUp(self):
        self.facts.createOrReplaceGlobalTempView("EventhouseFactSales")
        for name, frame in self.dimensions.items():
            frame.hint("broadcast").createOrReplaceGlobalTempView(name)

    def test_exact_facts_and_three_dimension_joins(self):
        verify_dimensions(self.spark, self.data, self.generator, "global_temp")
        result = verify_lakehouse_batch(
            self.spark, self.batch, "global_temp", "EventhouseFactSales"
        )
        self.assertEqual(result["row_count"], self.batch["row_count"])
        self.assertEqual(result["sales_amount_minor_units"], self.batch["sales_amount_minor_units"])
        verify_run_keys(self.spark, [self.batch], "global_temp", "EventhouseFactSales")

    def test_incomplete_or_duplicate_facts_do_not_pass(self):
        self.facts.limit(1).createOrReplaceGlobalTempView("EventhouseFactSales")
        with self.assertRaises(NotVisible):
            verify_lakehouse_batch(self.spark, self.batch, "global_temp", "EventhouseFactSales")
        self.facts.union(self.facts.limit(1)).createOrReplaceGlobalTempView("EventhouseFactSales")
        with self.assertRaises(VerificationError):
            verify_lakehouse_batch(self.spark, self.batch, "global_temp", "EventhouseFactSales")

    def test_run_keys_ignore_bootstrap_but_reject_cross_batch_duplicates(self):
        from pyspark.sql import functions as F

        bootstrap = self.facts.withColumn("BatchID", F.lit("bootstrap_0001"))
        self.facts.union(bootstrap).createOrReplaceGlobalTempView("EventhouseFactSales")
        verify_run_keys(self.spark, [self.batch], "global_temp", "EventhouseFactSales")
        second = dict(self.batch, batch_id="spark_contract_0002")
        duplicate = self.facts.withColumn("BatchID", F.lit(second["batch_id"]))
        self.facts.union(bootstrap).union(duplicate).createOrReplaceGlobalTempView("EventhouseFactSales")
        with self.assertRaises(VerificationError):
            verify_run_keys(self.spark, [self.batch, second], "global_temp", "EventhouseFactSales")

    def test_orphaned_dimension_is_rejected(self):
        from pyspark.sql import functions as F

        self.dimensions["DimProduct"].where(F.col("ProductID") != 1).createOrReplaceGlobalTempView(
            "DimProduct"
        )
        with self.assertRaises(VerificationError):
            verify_lakehouse_batch(self.spark, self.batch, "global_temp", "EventhouseFactSales")

    def test_grouped_total_mismatch_is_rejected(self):
        batch = dict(self.batch)
        batch["amount_by_customer_region"] = {"Americas": batch["sales_amount_minor_units"]}
        with self.assertRaises(VerificationError):
            verify_lakehouse_batch(self.spark, batch, "global_temp", "EventhouseFactSales")

    def test_incorrect_decimal_amount_is_rejected(self):
        from pyspark.sql import functions as F

        class CorruptDecimalSpark:
            def __init__(self, spark):
                self.spark = spark

            def __getattr__(self, name):
                return getattr(self.spark, name)

            def sql(self, query):
                return self.spark.sql(query).withColumn(
                    "SalesAmount", F.col("SalesAmount") + F.lit(1).cast("decimal(18,2)")
                )

        with self.assertRaises(VerificationError):
            verify_lakehouse_batch(
                CorruptDecimalSpark(self.spark), self.batch, "global_temp", "EventhouseFactSales"
            )


if __name__ == "__main__":
    unittest.main()
