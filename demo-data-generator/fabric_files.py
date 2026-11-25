"""Fabric file primitives. Runtime behavior still requires tenant verification."""

import hashlib
import tempfile
from contextlib import contextmanager
from pathlib import Path


@contextmanager
def materialize_staged(fs, staged_uri, expected_sha256):
    """Copy closed OneLake CSV to a temporary driver-local file and verify bytes."""
    if not isinstance(staged_uri, str) or not (
        staged_uri.startswith("abfss://") or staged_uri.startswith("Files/")
    ):
        raise ValueError("staged_uri must be a OneLake or default Lakehouse path")
    if (not isinstance(expected_sha256, str) or len(expected_sha256) != 64
            or any(c not in "0123456789abcdef" for c in expected_sha256)):
        raise ValueError("expected_sha256 must be lowercase SHA-256 hex")
    with tempfile.TemporaryDirectory(prefix="producer-") as directory:
        target = Path(directory) / "batch.csv"
        if not fs.exists(staged_uri):
            raise FileNotFoundError("staged CSV does not exist")
        if not fs.cp(staged_uri, target.as_uri(), False):
            raise RuntimeError("OneLake to driver copy returned false")
        if not target.is_file() or hashlib.sha256(target.read_bytes()).hexdigest() != expected_sha256:
            raise RuntimeError("materialized CSV does not match staged checksum")
        yield str(target)
