"""Journal explicitly selected cycles for a published Demo 3 native rule."""

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
_JOURNALS = (binding.RULE_JOURNAL, binding.RULE_SECOND_JOURNAL, binding.RULE_THIRD_JOURNAL)


class NativeRule:
    def __init__(self, publisher, *, cycle=1):
        if type(cycle) is not int or cycle not in (1, 2, 3):
            raise ValueError("Only rule cycles 1, 2 and 3 are supported.")
        rebound = getattr(publisher, "rebound", False)
        if rebound and cycle == 1:
            raise ValueError("A staging rebind is eligible only for rule cycle 2 or 3.")
        if cycle == 3 and not rebound:
            raise ValueError("Rule cycle 3 requires the confirmed staging rebind.")
        self.publisher = publisher
        self.api = publisher.api
        self.graph = publisher.graph
        self.cycle = cycle
        self.predecessor = publisher.root / _JOURNALS[cycle - 2] if cycle > 1 else None
        self.journal = publisher.root / _JOURNALS[cycle - 1]

    def _binding_hash(self):
        return check.config_sha256(self.publisher._published())

    def _readback(self, enabled, expected_definition=None):
        check._owned(self.api, binding._BASE + "/reflexes", "Demo3EventE2eActivatorA9bbae81",
                     check.REFLEX, _SKILL)
        if getattr(self.publisher, "rebound", False):
            definition = binding._reflex_definition(self.api)
            _, graph = binding._reflex_parts(definition)
            if expected_definition is not None and not binding._same_reflex_definition(
                    definition, expected_definition):
                raise CandidateBlocked("Complete native rule definition changed.")
        else:
            definition = None
            graph = binding._graph(self.api)
        running = check._rule_state(graph, {"definition_disabled": self.graph}, reusable=True)
        if running is not enabled:
            raise CandidateBlocked("Native rule is not in the expected state.")
        return definition

    def _rebound_target(self, before, enabled):
        _, graph = binding._reflex_parts(before)
        running = check._rule_state(graph, {"definition_disabled": self.graph}, reusable=True)
        if running is enabled:
            raise CandidateBlocked("Rule definition does not have the expected predecessor state.")
        target = copy.deepcopy(graph)
        for entity in target:
            definition = entity.get("payload", {}).get("definition", {})
            if entity.get("type") == "timeSeriesView-v1" and definition.get("type") == "Rule":
                definition["settings"]["shouldRun"] = enabled
        return binding._replace_reflex(before, target)

    def _validate_state(self, state, fingerprint, *, rebound_definitions=False):
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
        if rebound_definitions:
            for action, record in actions.items():
                if (not isinstance(record.get("before"), dict) or not isinstance(record.get("target"), dict)
                        or not binding._same_reflex_definition(
                            record["target"], self._rebound_target(record["before"], action == "enable"))):
                    raise CandidateBlocked("Rebound rule journal definitions differ.")
            if ("disable" in actions and not binding._same_reflex_definition(
                    actions["disable"]["before"], actions["enable"]["target"])):
                raise CandidateBlocked("Rebound rule predecessor definition changed.")
        return state

    def _predecessor(self, fingerprint):
        raw = binding._bytes(self.predecessor)
        previous = binding._json_bytes(raw)
        if self.cycle == 3:
            controller = NativeRule(self.publisher, cycle=2)
            controller._validate_state(previous, fingerprint, rebound_definitions=True)
            controller._assert_predecessor(previous, fingerprint)
        else:
            previous_fingerprint = self.publisher.predecessor_binding_hash()
            if not getattr(self.publisher, "rebound", False) and previous_fingerprint != fingerprint:
                raise CandidateBlocked("Native binding changed while checking the first rule cycle.")
            self._validate_state(previous, previous_fingerprint)
        if (previous["actions"].get("enable", {}).get("status") != "confirmed-readback"
                or previous["actions"].get("disable", {}).get("status") != "confirmed-readback"):
            label = "First" if self.cycle == 2 else "Second"
            raise CandidateBlocked(f"{label} rule cycle must have confirmed enable and disable.")
        return previous, binding._sha(raw)

    def _assert_predecessor(self, state, fingerprint):
        if self.cycle > 1:
            previous, digest = self._predecessor(fingerprint)
            if (state.get("previous_journal") != self.predecessor.name
                    or state.get("previous_journal_sha256") != digest
                    or (self.cycle == 3 and (
                        previous["proof_sha256"] != state["proof_sha256"]
                        or not binding._same_reflex_definition(
                            state["actions"]["enable"]["before"], previous["actions"]["disable"]["target"])))):
                label = "First" if self.cycle == 2 else "Second"
                raise CandidateBlocked(f"{label} rule-cycle journal changed; reconcile manually.")

    def _state(self, fingerprint):
        state = self._validate_state(
            _load_json(self.journal), fingerprint,
            rebound_definitions=getattr(self.publisher, "rebound", False))
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
        if getattr(self.publisher, "rebound", False):
            self._readback(action == "enable", record["target"])
        else:
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
            expected_before = None
            if enabled:
                if self.journal.exists():
                    raise CandidateBlocked("Native rule enable was already attempted; never resubmit.")
                if proof_path is None or not proof_path.is_absolute():
                    raise ValueError("Enable requires an absolute confirmed proof path.")
                predecessor = self._predecessor(fingerprint) if self.cycle == 3 else None
                proof_sha = self._verify_proof(proof_path)
                state = {"binding_sha256": fingerprint, "proof_sha256": proof_sha, "actions": {}}
                if self.cycle > 1:
                    if predecessor is None:
                        predecessor = self._predecessor(fingerprint)
                    state.update(previous_journal=self.predecessor.name,
                                 previous_journal_sha256=predecessor[1])
                    if self.cycle == 3:
                        if predecessor[0]["proof_sha256"] != proof_sha:
                            raise CandidateBlocked("Second rule cycle is bound to a different proof.")
                        expected_before = predecessor[0]["actions"]["disable"]["target"]
            else:
                state = self._state(fingerprint)
                if "disable" in state["actions"] or state["actions"]["enable"]["status"] != "confirmed-readback":
                    raise CandidateBlocked("Disable already attempted or enable unconfirmed; reconcile first.")
            before = (self._readback(not enabled) if expected_before is None
                      else self._readback(not enabled, expected_before))
            if (not enabled and getattr(self.publisher, "rebound", False)
                    and not binding._same_reflex_definition(before, state["actions"]["enable"]["target"])):
                raise CandidateBlocked("Complete enabled rule definition changed before disable.")
            if self._binding_hash() != fingerprint:
                raise CandidateBlocked("Native binding changed before the rule update.")
            record = {"status": "in-flight", "time_utc": binding._utc()}
            if getattr(self.publisher, "rebound", False):
                target = self._rebound_target(before, enabled)
                record.update(before=before, target=target)
                body = binding._reflex_update_body(target)
            else:
                graph = copy.deepcopy(self.graph)
                for entity in graph:
                    definition = entity.get("payload", {}).get("definition", {})
                    if entity.get("type") == "timeSeriesView-v1" and definition.get("type") == "Rule":
                        definition["settings"]["shouldRun"] = enabled
                part = {"path": "ReflexEntities.json", "payloadType": "InlineBase64",
                        "payload": base64.b64encode(json.dumps(graph).encode("utf-8")).decode("ascii")}
                body = {"definition": {"parts": [part]}}
            state["actions"][action] = record
            self._assert_predecessor(state, fingerprint)
            if enabled:
                binding._create(self.journal, state)
            binding._save(self.journal, state)
            status, headers, _ = self.api.request("POST", _RESOURCE + "/updateDefinition",
                                                  body,
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
                if getattr(self.publisher, "rebound", False):
                    self._readback(action == "enable", state["actions"][action]["target"])
                else:
                    self._readback(action == "enable")
            return state

    def drain(self):
        with _state_lock(self.publisher.root):
            state = self._state(self._binding_hash())
            if state["actions"].get("disable", {}).get("status") != "confirmed-readback":
                raise CandidateBlocked("Native rule disable must be confirmed before checking drain.")
            if getattr(self.publisher, "rebound", False):
                self._readback(False, state["actions"]["disable"]["target"])
            else:
                self._readback(False)
            binding._quiescent(self.api, self.publisher.root)
            if getattr(self.publisher, "rebound", False):
                self._readback(False, state["actions"]["disable"]["target"])
            else:
                self._readback(False)
            return {"rule": "stopped", "owned_jobs": "terminal", "writer_lock": "absent",
                    "event_queue": "unverified", "scope": "point-in-time"}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", required=True, choices=("enable", "disable", "reconcile", "drain"))
    for arg in ("candidate", "native-config", "previous-config", "state-dir"):
        parser.add_argument("--" + arg, required=True, type=Path)
    parser.add_argument("--proof", type=Path)
    parser.add_argument("--cycle", type=int, choices=(1, 2, 3), default=1)
    parser.add_argument("--rebind-spec", type=Path)
    parser.add_argument("--previous-proof", type=Path)
    parser.add_argument("--yes", action="store_true")
    args = parser.parse_args(argv)
    if args.mode in ("enable", "disable") and not args.yes:
        parser.error("Rule updates require explicit --yes")
    if (args.mode == "enable") != (args.proof is not None):
        parser.error("--proof is required only for enable")
    if (args.rebind_spec is None) != (args.previous_proof is None) or (
            args.rebind_spec is not None and args.cycle == 1):
        parser.error("--rebind-spec and --previous-proof must be supplied together for cycle 2 or 3")
    if args.cycle == 3 and args.rebind_spec is None:
        parser.error("--cycle 3 requires --rebind-spec and --previous-proof")
    api = FabricControl.from_cli(check.TENANT, timeout=120)
    publisher = binding.NativeBinding(args.state_dir, args.candidate, args.native_config,
                                      args.previous_config, api)
    if args.rebind_spec is not None:
        from native_rebind import NativeRebind

        publisher = NativeRebind(publisher, args.rebind_spec, args.previous_proof)
    rule = NativeRule(publisher, cycle=args.cycle)
    if args.mode == "drain":
        result = rule.drain()
    else:
        state = rule.reconcile() if args.mode == "reconcile" else rule.change(args.mode, args.proof)
        result = {"journal": str(rule.journal), "actions": state["actions"]}
    print(json.dumps({"mode": args.mode, **result}, sort_keys=True))


if __name__ == "__main__":
    main()
