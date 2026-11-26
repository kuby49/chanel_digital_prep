import os
import sys
import tempfile
import unittest
import uuid
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import customer_preflight as original
import isolated_setup
from deploy import CandidateBlocked


class IsolatedSetupTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        source = os.environ.get("DEMO3_PRODUCER_PATH")
        if source:
            self.producer = Path(source).resolve().parent
        else:
            self.producer = self.root
            (self.root / "generator.py").write_text(
                "class RunConfig: pass\ndef dimensions(): pass\n", encoding="utf-8")
        self.workspace, self.lakehouse = str(uuid.uuid4()), str(uuid.uuid4())
        self.owner = {"workspace": {"id": self.workspace}, "lakehouse": {"id": self.lakehouse},
                      "owner": {"marker": "new-demo3-" + str(uuid.uuid4())},
                      "deployment_spec": {"lakehouse_name": "Replacement", "capacity_id": str(uuid.uuid4())}}
        self.config = {"contract_version": 1, "data": {"start_date": "2026-01-01",
                       "date_count": 365, "product_count": 20, "customer_count": 100, "seed": 42}}
        capacity = mock.patch.object(isolated_setup.isolated_preflight, "require_f32")
        capacity.start()
        self.addCleanup(capacity.stop)

    def test_setup_requires_owned_readback_and_pins_context_before_execution(self):
        with mock.patch.object(isolated_setup.ownership, "provision", return_value=self.owner) as verify:
            result = isolated_setup.prepare(self.config, "new", self.root, object(), self.producer)
        self.assertFalse(result["ready"])
        self.assertEqual(result["status"], "unexecuted-setup-candidate")
        self.assertFalse(verify.call_args.kwargs["create"])
        self.assertEqual(result["notebook"]["metadata"]["trident"]["lakehouse"]["default_lakehouse"],
                         self.lakehouse)
        source = "".join(result["notebook"]["cells"][-1]["source"])
        self.assertLess(source.index("Replacement setup context differs"), source.index("spark.conf.set"))
        compile(source, "replacement-setup", "exec")

    def test_original_target_and_marker_are_never_packaged(self):
        for field in ("lakehouse", "owner"):
            record = self.owner[field].copy()
            self.owner[field] = ({"id": original.LAKEHOUSE} if field == "lakehouse"
                                 else {"marker": original.MARKER})
            with mock.patch.object(isolated_setup.ownership, "provision", return_value=self.owner), \
                    self.assertRaises(CandidateBlocked):
                isolated_setup.prepare(self.config, "new", self.root, object(), self.producer)
            self.owner[field] = record

    def test_unconfirmed_ownership_stops_before_packaging(self):
        with mock.patch.object(isolated_setup.ownership, "provision", side_effect=CandidateBlocked("pending")), \
                mock.patch.object(isolated_setup.setup_notebook, "build_notebook") as build, \
                self.assertRaisesRegex(CandidateBlocked, "pending"):
            isolated_setup.prepare(self.config, "new", self.root, object(), self.producer)
        build.assert_not_called()
