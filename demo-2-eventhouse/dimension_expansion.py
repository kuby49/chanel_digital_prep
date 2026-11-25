"""Insert missing rows into compatible, quiescent Demo 2 dimensions."""

from lakehouse_validation import (
    VerificationError, _dimension_frames, _table_name, verify_dimensions,
)


def _compatible_dimensions(spark, data, generator, lakehouse, schema):
    plans = []
    for name, expected in _dimension_frames(spark, data, generator).items():
        qualified = _table_name(lakehouse, name, schema)
        if not spark.catalog.tableExists(qualified):
            raise VerificationError(f"Required dimension {qualified} does not exist")
        actual = spark.table(qualified)
        if actual.columns != expected.columns or [field.dataType for field in actual.schema] != [
            field.dataType for field in expected.schema
        ]:
            raise VerificationError(f"Dimension {qualified} has an unexpected schema")
        if actual.exceptAll(expected).limit(1).count():
            raise VerificationError(
                f"Dimension {qualified} contains conflicting, unexpected or duplicate rows"
            )
        plans.append((qualified, actual, expected))
    return plans


def dimension_gaps(spark, data, generator, lakehouse, schema=None):
    """Read-only check: report missing rows after verifying every existing dimension."""
    return {
        qualified: expected.exceptAll(actual).count()
        for qualified, actual, expected in _compatible_dimensions(spark, data, generator, lakehouse, schema)
    }


def expand_dimensions(spark, data, generator, lakehouse, schema=None):
    """Run only in the owned Lakehouse while other dimension writers are stopped."""
    from delta.tables import DeltaTable

    compatible = _compatible_dimensions(spark, data, generator, lakehouse, schema)
    plans = [(DeltaTable.forName(spark, qualified), expected, expected.columns[0])
             for qualified, _, expected in compatible]
    for table, expected, key in plans:
        (table.alias("existing")
         .merge(expected.alias("desired"), f"existing.`{key}` = desired.`{key}`")
         .whenNotMatchedInsertAll()
         .execute())
    verify_dimensions(spark, data, generator, lakehouse, schema)
