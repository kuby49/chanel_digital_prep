"""Plan and verify insert-only updates to deterministic dimensions."""


def _unique_key_count(name, frame, key, functions):
    count = frame.count()
    if (frame.filter(functions.col(key).isNull()).limit(1).count()
            or frame.select(key).distinct().count() != count):
        raise RuntimeError(f"{name} has null or duplicate dimension keys")
    return count


def plan_dimension_append(name, before, expected, key, functions):
    _unique_key_count(name, before, key, functions)
    _unique_key_count(name, expected, key, functions)
    before_keys = before.select(key)
    expected_keys = expected.select(key)
    existing_overlap = before.join(expected_keys, key, "left_semi")
    expected_overlap = expected.join(before_keys, key, "left_semi")
    if (existing_overlap.exceptAll(expected_overlap).limit(1).count()
            or expected_overlap.exceptAll(existing_overlap).limit(1).count()):
        raise RuntimeError(f"{name} has incompatible overlapping dimension rows")
    return expected.join(before_keys, key, "left_anti")


def verify_dimension_append(name, before, added, after, key, functions):
    count = _unique_key_count(name, after, key, functions)
    expected = before.unionByName(added)
    if (count != before.count() + added.count()
            or expected.exceptAll(after).limit(1).count()
            or after.exceptAll(expected).limit(1).count()):
        raise RuntimeError(f"{name} did not preserve exactly the existing and added rows")
    return count
