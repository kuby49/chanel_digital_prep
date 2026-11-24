import csv
import importlib.util
import io
import os
import sys
import tempfile
import unittest
from datetime import datetime
from pathlib import Path

from dimension_expansion import dimension_gaps
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
            products=3, customers=3, file_count=2, rows_per_file=12,
        )
        payload, cls.batch = cls.generator.batch_bytes(config, 1)
        second_payload, cls.second_batch = cls.generator.batch_bytes(config, 2)
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
        def fact_rows(data):
            return [
                (
                    int(row["SalesLineID"]), row["BatchID"],
                    datetime.fromisoformat(row["SaleTimestamp"].replace("Z", "+00:00")),
                    int(row["CustomerID"]), int(row["ProductID"]), row["Date"],
                    int(row["Quantity"]), int(row["UnitPriceMinorUnits"]),
                    int(row["SalesAmountMinorUnits"]),
                )
                for row in csv.DictReader(io.StringIO(data.decode("utf-8")))
            ]

        rows, second_rows = fact_rows(payload), fact_rows(second_payload)
        cls.facts = cls.spark.createDataFrame(rows, schema).cache()
        cls.second_facts = cls.spark.createDataFrame(second_rows, schema).cache()
        cls.facts.count()
        cls.second_facts.count()
        cls.sales_ids = sorted(row[0] for row in rows)
        cls.second_sales_ids = sorted(row[0] for row in second_rows)
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

    def test_dimension_expansion_preflight_counts_only_missing_compatible_rows(self):
        larger = {**self.data, "date_count": 365, "product_count": 20, "customer_count": 100}
        self.assertEqual(dimension_gaps(self.spark, larger, self.generator, "global_temp"), {
            "global_temp.DimDate": 361,
            "global_temp.DimProduct": 17,
            "global_temp.DimCustomer": 97,
        })

    def test_dimension_expansion_preflight_rejects_duplicate_existing_key(self):
        actual = self.dimensions["DimCustomer"]
        actual.unionByName(actual.limit(1)).createOrReplaceGlobalTempView("DimCustomer")
        larger = {**self.data, "customer_count": 100}
        with self.assertRaisesRegex(VerificationError, "DimCustomer.*duplicate"):
            dimension_gaps(self.spark, larger, self.generator, "global_temp")

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

    def test_run_keys_allow_reused_ids_in_other_batches_and_runs(self):
        from pyspark.sql import functions as F

        bootstrap = self.facts.withColumn("BatchID", F.lit("bootstrap_0001"))
        other_run = self.facts.withColumn("BatchID", F.lit("other_run_0001"))
        self.assertEqual(self.sales_ids, list(range(1, self.batch["row_count"] + 1)))
        self.assertEqual(self.second_sales_ids, self.sales_ids)
        self.facts.union(bootstrap).union(other_run).union(self.second_facts).createOrReplaceGlobalTempView(
            "EventhouseFactSales"
        )
        verify_lakehouse_batch(self.spark, self.batch, "global_temp", "EventhouseFactSales")
        verify_lakehouse_batch(self.spark, self.second_batch, "global_temp", "EventhouseFactSales")
        verify_run_keys(self.spark, [self.batch, self.second_batch], "global_temp", "EventhouseFactSales")

    def test_run_keys_reject_duplicate_ids_within_a_batch(self):
        from pyspark.sql import functions as F

        self.facts.withColumn(
            "SalesLineID",
            F.when(F.col("SalesLineID") == self.sales_ids[1],
                   F.lit(self.sales_ids[0])).otherwise(F.col("SalesLineID")),
        ).createOrReplaceGlobalTempView("EventhouseFactSales")
        with self.assertRaisesRegex(VerificationError, "duplicate"):
            verify_run_keys(self.spark, [self.batch], "global_temp", "EventhouseFactSales")
        with self.assertRaisesRegex(VerificationError, "duplicate"):
            verify_lakehouse_batch(self.spark, self.batch, "global_temp", "EventhouseFactSales")

    def test_synthetic_run_keys_reconcile_only_reserved_bootstrap_batches(self):
        from pyspark.sql import functions as F

        run = "demo2synthetic_123456abcdef"
        fixture = {**self.batch, "batch_id": f"{run}_0001", "batch_class": "bootstrap"}
        self.facts.withColumn("BatchID", F.lit(fixture["batch_id"])).createOrReplaceGlobalTempView(
            "EventhouseFactSales"
        )
        verify_run_keys(self.spark, [fixture], "global_temp", "EventhouseFactSales", synthetic_run_id=run)
        with self.assertRaisesRegex(VerificationError, "measured batches only"):
            verify_run_keys(self.spark, [fixture], "global_temp", "EventhouseFactSales")

    def test_run_keys_reject_rows_shifted_between_batches(self):
        from pyspark.sql import functions as F

        first = self.facts.where(F.col("SalesLineID") != self.sales_ids[0])
        shifted = self.facts.where(F.col("SalesLineID") == self.sales_ids[0]).withColumn(
            "BatchID", F.lit(self.second_batch["batch_id"])
        ).withColumn("SalesLineID", F.lit(self.sales_ids[-1] + 1))
        first.union(self.second_facts).union(shifted).createOrReplaceGlobalTempView("EventhouseFactSales")
        with self.assertRaisesRegex(VerificationError, "missing or duplicate"):
            verify_run_keys(self.spark, [self.batch, self.second_batch], "global_temp", "EventhouseFactSales")

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
