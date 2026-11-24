"""One-shot correction of the published native guard's high-concurrency flag."""

import argparse
import base64
import copy
import json
from pathlib import Path

import customer_preflight as check
import guarded_notebook
import native_binding as binding
from deploy import CandidateBlocked, _load_json, _state_lock
from ownership import FabricControl


class GuardCorrection:
    def __init__(self, publisher):
        self.publisher = publisher
        self.root = publisher.root
        self.api = publisher.api
        self.journal = self.root / binding.HC_JOURNAL

    def _expected(self):
        producer, _, _ = check._producer(self.root)
        return guarded_notebook.build_native_notebook(
            producer, self.publisher.template["native_policy"], check.WORKSPACE, check.LAKEHOUSE,
            check.HOUSE_NAME, check.STAGING_WORKSPACE, check.STAGING_LAKEHOUSE)

    def _baseline(self, proof_path, published):
        if (self.root / binding.RULE_JOURNAL).exists():
            raise CandidateBlocked("Native rule journal exists; do not correct the guard.")
        pipeline = binding._quiescent(self.api, self.root)
        graph = binding._graph(self.api)
        binding._stopped(graph, self.publisher.graph)
        notebook = binding._guard(self.api)
        if binding._notebook(notebook) != binding._notebook(self.publisher.notebook):
            raise CandidateBlocked("Published guard differs from the frozen predecessor.")
        expected_proof = {
            "version": 1, "workspace_id": check.WORKSPACE, "lakehouse_id": check.LAKEHOUSE,
            "guard_notebook_id": check.NOTEBOOK, "pipeline_id": check.PIPELINE,
            "reflex_id": check.REFLEX, "connection_id": check.CONNECTION, "marker": check.MARKER,
            "native_policy": published["native_policy"], "generator_sha256": check.GENERATOR_SHA,
            "guard_sha256": check.config_sha256(binding._notebook(notebook)),
            "pipeline_sha256": check.config_sha256(pipeline["properties"]),
            "graph_disabled_sha256": check.config_sha256(check._canonical(graph)),
        }
        if _load_json(proof_path) != expected_proof:
            raise CandidateBlocked("Old proof does not match the fresh stopped baseline.")
        target = copy.deepcopy(notebook)
        target["cells"][1]["source"].insert(0, binding.HC_FLAG)
        if binding._notebook(target) != binding._notebook(self._expected()):
            raise CandidateBlocked("Correction is not the single expected native guard change.")
        self.publisher._unchanged()
        return notebook, target

    def _state(self, published):
        return self.publisher._correction(published)

    def _confirm(self, state):
        record = state["update"]
        if record.get("http") == 202:
            operation, location = binding._operation_location({
                "x-ms-operation-id": record.get("operation_id"), "location": record.get("location")})
            self.publisher.poll(self.api, location, operation, "spark-cli")
        elif record.get("http") != 200:
            raise CandidateBlocked("Guard update acceptance unknown; never resubmit.")
        if binding._guard(self.api) != state["target"]:
            raise CandidateBlocked("Corrected guard readback differs; do not retry.")
        binding._stopped(binding._graph(self.api), self.publisher.graph)
        if check.config_sha256(self.publisher._published()) != state["binding_sha256"]:
            raise CandidateBlocked("Native publication changed during correction.")
        record.update(status="confirmed-readback", confirmed_at_utc=binding._utc())
        binding._save(self.journal, state)

    def update(self, proof_path):
        if not proof_path.is_absolute():
            raise ValueError("An absolute predecessor proof path is required.")
        with _state_lock(self.root):
            if self.journal.exists():
                raise CandidateBlocked("Guard correction already attempted; reconcile only.")
            published = self.publisher._published()
            old, target = self._baseline(proof_path, published)
            state = {"binding_sha256": check.config_sha256(published),
                     "previous_proof_sha256": check.config_sha256(_load_json(proof_path)),
                     "old_sha256": check.config_sha256(old), "target_sha256": check.config_sha256(target),
                     "before": copy.deepcopy(old), "target": target,
                     "update": {"status": "in-flight", "time_utc": binding._utc()}}
            binding._create(self.journal, state)
            binding._save(self.journal, state)
            part = {"path": "notebook-content.ipynb", "payloadType": "InlineBase64",
                    "payload": base64.b64encode(json.dumps(target).encode("utf-8")).decode("ascii")}
            status, headers, _ = self.api.request(
                "POST", binding._BASE + "/items/" + check.NOTEBOOK + "/updateDefinition",
                {"definition": {"format": "ipynb", "parts": [part]}}, timeout=120, skill="spark-cli")
            state["update"]["http"] = status
            if status == 202:
                values = {key.lower(): value for key, value in headers.items()}
                state["update"].update(operation_id=values.get("x-ms-operation-id"),
                                       location=values.get("location"))
            binding._save(self.journal, state)
            self._confirm(state)
            return state

    def reconcile(self):
        with _state_lock(self.root):
            state = self._state(self.publisher._published())
            if state["update"]["status"] == "in-flight":
                self._confirm(state)
            elif binding._guard(self.api) != state["target"]:
                raise CandidateBlocked("Corrected guard changed after confirmation.")
            return state


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", required=True, choices=("update", "reconcile"))
    for arg in ("candidate", "native-config", "previous-config", "state-dir"):
        parser.add_argument("--" + arg, required=True, type=Path)
    parser.add_argument("--previous-proof", type=Path)
    parser.add_argument("--yes", action="store_true")
    args = parser.parse_args(argv)
    if args.mode == "update" and (not args.yes or args.previous_proof is None):
        parser.error("update requires --yes and --previous-proof")
    if args.mode == "reconcile" and args.previous_proof is not None:
        parser.error("reconcile does not take --previous-proof")
    api = FabricControl.from_cli(check.TENANT, timeout=120)
    correction = GuardCorrection(binding.NativeBinding(args.state_dir, args.candidate, args.native_config,
                                                       args.previous_config, api))
    state = correction.update(args.previous_proof) if args.mode == "update" else correction.reconcile()
    print(json.dumps({"mode": args.mode, "journal": str(correction.journal),
                      "update": state["update"]}, sort_keys=True))


if __name__ == "__main__":
    main()
