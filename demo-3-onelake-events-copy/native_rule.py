"""Journal up to two explicitly selected cycles for a published Demo 3 native rule."""

import argparse
import base64
import copy
import json
import re
from pathlib import Path

import customer_preflight as check
import native_binding as binding
from deploy import CandidateBlocked, _load_json, _state_lock
from ownership import FabricControl


_RESOURCE = binding._BASE + "/reflexes/" + check.REFLEX
_SKILL = "activator-cli"


class NativeRule:
    def __init__(self, publisher, *, cycle=1):
        if type(cycle) is not int or cycle not in (1, 2):
            raise ValueError("Only rule cycles 1 and 2 are supported.")
        self.publisher = publisher
        self.api = publisher.api
        self.graph = publisher.graph
        self.cycle = cycle
        self.predecessor = publisher.root / binding.RULE_JOURNAL
        self.journal = publisher.root / (binding.RULE_JOURNAL if cycle == 1
                                          else binding.RULE_SECOND_JOURNAL)

    def _binding_hash(self):
        return check.config_sha256(self.publisher._published())

    def _readback(self, enabled):
        check._owned(self.api, binding._BASE + "/reflexes", "Demo3EventE2eActivatorA9bbae81",
                     check.REFLEX, _SKILL)
        graph = binding._graph(self.api)
        running = check._rule_state(graph, {"definition_disabled": self.graph}, reusable=True)
        if running is not enabled:
            raise CandidateBlocked("Native rule is not in the expected state.")

    def _validate_state(self, state, fingerprint):
        actions = state.get("actions")
        if (state.get("binding_sha256") != fingerprint
                or not isinstance(state.get("proof_sha256"), str)
                or not re.fullmatch(r"[a-f0-9]{64}", state["proof_sha256"])
                or not isinstance(actions, dict)
                or set(actions) not in ({"enable"}, {"enable", "disable"})
                or any(not isinstance(record, dict) or not record.get("time_utc")
                       or record.get("status") not in ("in-flight", "confirmed-readback")
                       for record in actions.values())
                or ("disable" in actions and actions["enable"]["status"] != "confirmed-readback")):
            raise CandidateBlocked("Native rule journal differs; reconcile manually.")
        for record in actions.values():
            if record["status"] == "confirmed-readback":
                if record.get("http") not in (200, 202) or not record.get("confirmed_at_utc"):
                    raise CandidateBlocked("Confirmed rule update is missing a receipt.")
                self._operation(record)
        return state

    def _predecessor_hash(self, fingerprint):
        raw = binding._bytes(self.predecessor)
        previous = self._validate_state(binding._json_bytes(raw), fingerprint)
        if (previous["actions"].get("enable", {}).get("status") != "confirmed-readback"
                or previous["actions"].get("disable", {}).get("status") != "confirmed-readback"):
            raise CandidateBlocked("First rule cycle must have confirmed enable and disable.")
        return binding._sha(raw)

    def _assert_predecessor(self, state, fingerprint):
        if self.cycle == 2 and (state.get("previous_journal") != binding.RULE_JOURNAL
                                or state.get("previous_journal_sha256") != self._predecessor_hash(fingerprint)):
            raise CandidateBlocked("First rule-cycle journal changed; reconcile manually.")

    def _state(self, fingerprint):
        state = self._validate_state(_load_json(self.journal), fingerprint)
        self._assert_predecessor(state, fingerprint)
        return state

    @staticmethod
    def _operation(record):
        if record.get("http") == 202:
            return binding._operation_location({"x-ms-operation-id": record.get("operation_id"),
                                                "location": record.get("location")})
        if record.get("http") != 200:
            raise CandidateBlocked("Rule update acceptance is unknown; do not resubmit.")
        return None

    def _confirm(self, state, action):
        record = state["actions"][action]
        operation = self._operation(record)
        if operation:
            operation_id, location = operation
            self.publisher.poll(self.api, location, operation_id, _SKILL)
        self._readback(action == "enable")
        if self._binding_hash() != state["binding_sha256"]:
            raise CandidateBlocked("Native binding changed since the rule intent.")
        self._assert_predecessor(state, state["binding_sha256"])
        record.update(status="confirmed-readback", confirmed_at_utc=binding._utc())
        binding._save(self.journal, state)

    def _verify_proof(self, proof_path):
        proof = _load_json(proof_path)
        if proof != self.publisher._read_proof(self.publisher._published()):
            raise CandidateBlocked("Native deployment proof differs from fresh stopped readback.")
        return check.config_sha256(proof)

    def change(self, action, proof_path=None):
        if action not in ("enable", "disable"):
            raise ValueError("Invalid native rule action")
        enabled = action == "enable"
        with _state_lock(self.publisher.root):
            fingerprint = self._binding_hash()
            if enabled:
                if self.journal.exists():
                    raise CandidateBlocked("Native rule enable was already attempted; never resubmit.")
                if proof_path is None or not proof_path.is_absolute():
                    raise ValueError("Enable requires an absolute confirmed proof path.")
                proof_sha = self._verify_proof(proof_path)
                state = {"binding_sha256": fingerprint, "proof_sha256": proof_sha, "actions": {}}
                if self.cycle == 2:
                    state.update(previous_journal=binding.RULE_JOURNAL,
                                 previous_journal_sha256=self._predecessor_hash(fingerprint))
            else:
                state = self._state(fingerprint)
                if "disable" in state["actions"] or state["actions"]["enable"]["status"] != "confirmed-readback":
                    raise CandidateBlocked("Disable already attempted or enable unconfirmed; reconcile first.")
            self._readback(not enabled)
            if self._binding_hash() != fingerprint:
                raise CandidateBlocked("Native binding changed before the rule update.")
            self._assert_predecessor(state, fingerprint)
            graph = copy.deepcopy(self.graph)
            for entity in graph:
                definition = entity.get("payload", {}).get("definition", {})
                if entity.get("type") == "timeSeriesView-v1" and definition.get("type") == "Rule":
                    definition["settings"]["shouldRun"] = enabled
            part = {"path": "ReflexEntities.json", "payloadType": "InlineBase64",
                    "payload": base64.b64encode(json.dumps(graph).encode("utf-8")).decode("ascii")}
            record = {"status": "in-flight", "time_utc": binding._utc()}
            state["actions"][action] = record
            if enabled:
                binding._create(self.journal, state)
            binding._save(self.journal, state)
            status, headers, _ = self.api.request("POST", _RESOURCE + "/updateDefinition",
                                                  {"definition": {"parts": [part]}},
                                                  timeout=120, skill=_SKILL)
            record["http"] = status
            if status == 202:
                values = {key.lower(): value for key, value in headers.items()}
                record.update(operation_id=values.get("x-ms-operation-id"), location=values.get("location"))
            binding._save(self.journal, state)
            self._confirm(state, action)
            return state

    def reconcile(self):
        with _state_lock(self.publisher.root):
            state = self._state(self._binding_hash())
            action = "disable" if "disable" in state["actions"] else "enable"
            if state["actions"][action]["status"] == "in-flight":
                self._confirm(state, action)
            else:
                self._readback(action == "enable")
            return state

    def drain(self):
        with _state_lock(self.publisher.root):
            state = self._state(self._binding_hash())
            if state["actions"].get("disable", {}).get("status") != "confirmed-readback":
                raise CandidateBlocked("Native rule disable must be confirmed before checking drain.")
            self._readback(False)
            binding._quiescent(self.api, self.publisher.root)
            self._readback(False)
            return {"rule": "stopped", "owned_jobs": "terminal", "writer_lock": "absent",
                    "event_queue": "unverified", "scope": "point-in-time"}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", required=True, choices=("enable", "disable", "reconcile", "drain"))
    for arg in ("candidate", "native-config", "previous-config", "state-dir"):
        parser.add_argument("--" + arg, required=True, type=Path)
    parser.add_argument("--proof", type=Path)
    parser.add_argument("--cycle", type=int, choices=(1, 2), default=1)
    parser.add_argument("--yes", action="store_true")
    args = parser.parse_args(argv)
    if args.mode in ("enable", "disable") and not args.yes:
        parser.error("Rule updates require explicit --yes")
    if (args.mode == "enable") != (args.proof is not None):
        parser.error("--proof is required only for enable")
    api = FabricControl.from_cli(check.TENANT, timeout=120)
    rule = NativeRule(binding.NativeBinding(args.state_dir, args.candidate, args.native_config,
                                            args.previous_config, api), cycle=args.cycle)
    if args.mode == "drain":
        result = rule.drain()
    else:
        state = rule.reconcile() if args.mode == "reconcile" else rule.change(args.mode, args.proof)
        result = {"journal": str(rule.journal), "actions": state["actions"]}
    print(json.dumps({"mode": args.mode, **result}, sort_keys=True))


if __name__ == "__main__":
    main()
