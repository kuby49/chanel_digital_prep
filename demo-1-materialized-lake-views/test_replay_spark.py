"""Exercise the Silver query against temporary Spark views, without cloud writes."""

import unittest
from pathlib import Path

try:
    from pyspark.sql import SparkSession
except ImportError:
    SparkSession = None


@unittest.skipIf(SparkSession is None, "PySpark is required")
class ReplaySparkTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.getOrCreate()
        cls.spark.createDataFrame([(1, "Customer", "Region")],
                                  ["CustomerID", "CustomerName", "CustomerRegion"]) \
            .createOrReplaceTempView("test_replay_customer")
        cls.spark.createDataFrame([(1, "Product", "Category")],
                                  ["ProductID", "ProductName", "ProductCategory"]) \
            .createOrReplaceTempView("test_replay_product")
        cls.spark.sql(
            "SELECT DATE '2026-01-01' AS `Date`, 2026 AS Year, "
            "1 AS MonthNumber, 'January' AS MonthName"
        ).createOrReplaceTempView("test_replay_date")
        template = globals().get("SQL_TEMPLATE")
        if template is None:
            template = Path(__file__).with_name("silver-candidate.sql").read_text(encoding="utf-8")
        cls.query = template.replace("\r\n", "\n").split("\nAS\n", 1)[1]
        for key, name in {
            "bronze": "test_replay_bronze", "dim_customer": "test_replay_customer",
            "dim_product": "test_replay_product", "dim_date": "test_replay_date",
        }.items():
            cls.query = cls.query.replace("{{" + key + "}}", name)

    @classmethod
    def tearDownClass(cls):
        for name in ("test_replay_customer", "test_replay_product",
                     "test_replay_date", "test_replay_bronze"):
            cls.spark.catalog.dropTempView(name)

    def run_rows(self, rows):
        columns = ["SalesLineID", "BatchID", "SaleTimestamp", "CustomerID", "ProductID",
                   "Date", "Quantity", "UnitPriceMinorUnits", "SalesAmountMinorUnits",
                   "_corrupt_record", "__filepath__"]
        schema = ", ".join(f"`{column}` STRING" for column in columns)
        self.spark.createDataFrame(rows, schema).createOrReplaceTempView("test_replay_bronze")
        return [row.asDict(recursive=True) for row in self.spark.sql(self.query).collect()]

    def test_replay_and_conflicting_rows(self):
        def fact(batch, line, path, amount="100"):
            return (str(line), batch, "2026-01-01T12:00:00Z", "1", "1", "2026-01-01",
                    "1", amount, amount, None, path)

        original = [fact("A", 1, "file-a"), fact("A", 2, "file-a")]
        replay = [fact("A", 1, "file-copy"), fact("A", 2, "file-copy")]
        rows = self.run_rows(original + replay + [fact("B", 1, "file-b")])
        self.assertEqual({(row["BatchID"], row["SalesLineID"]) for row in rows},
                         {("A", 1), ("A", 2), ("B", 1)})
        self.assertEqual(sum(row["SalesAmountMinorUnits"] for row in rows), 300)
        self.assertTrue(all(row["payload_variants"] == row["max_file_rows"] == 1
                            and row["missing_source"] == 0 for row in rows))

        conflict = self.run_rows([fact("A", 1, "file-a"), fact("A", 1, "file-copy", "200")])
        self.assertEqual(conflict[0]["payload_variants"], 2)
        repeated_in_file = self.run_rows([fact("A", 1, "file-a"), fact("A", 1, "file-a")])
        self.assertEqual(repeated_in_file[0]["max_file_rows"], 2)
        missing_path = self.run_rows([fact("A", 1, "file-a"), fact("A", 1, None)])
        self.assertEqual(missing_path[0]["missing_source"], 1)


if __name__ == "__main__":
    unittest.main()
