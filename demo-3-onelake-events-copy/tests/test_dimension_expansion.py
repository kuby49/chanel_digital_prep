import sys
import unittest
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from dimension_expansion import plan_dimension_append, verify_dimension_append


class Column:
    def __init__(self, name):
        self.name = name

    def isNull(self):
        return lambda row: row[self.name] is None


class Functions:
    col = Column


class Rows:
    def __init__(self, values):
        self.values = list(values)

    def count(self):
        return len(self.values)

    def select(self, key):
        return Rows([{key: row[key]} for row in self.values])

    def distinct(self):
        return Rows(dict(items) for items in dict.fromkeys(tuple(row.items()) for row in self.values))

    def filter(self, predicate):
        return Rows(row for row in self.values if predicate(row))

    def limit(self, number):
        return Rows(self.values[:number])

    def join(self, other, key, kind):
        keys = {row[key] for row in other.values}
        return Rows(row for row in self.values if (row[key] in keys) == (kind == "left_semi"))

    def exceptAll(self, other):
        remaining = Counter(tuple(row.items()) for row in other.values)
        result = []
        for row in self.values:
            item = tuple(row.items())
            if remaining[item]:
                remaining[item] -= 1
            else:
                result.append(row)
        return Rows(result)

    def unionByName(self, other):
        return Rows(self.values + other.values)


class DimensionExpansionTests(unittest.TestCase):
    def test_only_new_rows_are_added_and_old_rows_remain(self):
        before = Rows([{"id": 1, "name": "same"}, {"id": 3, "name": "old"}])
        selected = Rows([{"id": 1, "name": "same"}, {"id": 2, "name": "new"}])
        added = plan_dimension_append("Dim", before, selected, "id", Functions)
        self.assertEqual(added.values, [{"id": 2, "name": "new"}])
        after = before.unionByName(added)
        self.assertEqual(verify_dimension_append("Dim", before, added, after, "id", Functions), 3)
        with self.assertRaisesRegex(RuntimeError, "preserve exactly"):
            verify_dimension_append("Dim", before, added, added, "id", Functions)
        with self.assertRaisesRegex(RuntimeError, "preserve exactly"):
            verify_dimension_append(
                "Dim", before, added, after.unionByName(Rows([{"id": 4, "name": "extra"}])),
                "id", Functions)

    def test_conflicting_overlap_and_invalid_keys_stop_before_append(self):
        selected = Rows([{"id": 1, "name": "expected"}])
        for before in (
            Rows([{"id": 1, "name": "different"}]),
            Rows([{"id": 1, "name": "expected"}, {"id": 1, "name": "expected"}]),
            Rows([{"id": None, "name": "invalid"}]),
        ):
            with self.subTest(before=before.values), self.assertRaises(RuntimeError):
                plan_dimension_append("Dim", before, selected, "id", Functions)


if __name__ == "__main__":
    unittest.main()
