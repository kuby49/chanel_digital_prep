"""Fail-closed Demo 1 entry point for deployment contract version 1."""

import argparse
import json
import sys
from pathlib import Path

from definition_plan import effective_verification_timeout_seconds
from customer_validation import validate_customer
from ownership import (
    CANDIDATE_FILE, LOCK_FILE, STATE_FILE, CandidateBlocked, resolve_target_candidate,
)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--mode", required=True,
        choices=("dry-run", "validate-only", "deploy", "cleanup", "resolve-target-candidate"),
    )
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--prefix", required=True)
    parser.add_argument("--state-dir", required=True, type=Path)
    parser.add_argument("--result", required=True, type=Path)
    parser.add_argument("--yes", action="store_true")
    args = parser.parse_args()

    protected = (args.config, args.state_dir / STATE_FILE, args.state_dir / LOCK_FILE,
                 args.result.parent / CANDIDATE_FILE)
    if args.result.resolve() in {path.resolve() for path in protected}:
        print("Result path conflicts with input, state, or candidate", file=sys.stderr)
        return 1

    result = {
        "contract_version": 1,
        "track": "demo-1",
        "mode": args.mode,
        "status": "blocked",
        "ready": False,
        "diagnostics": [],
    }
    try:
        if not all(path.is_absolute() for path in (args.config, args.state_dir, args.result)):
            raise ValueError("config, state-dir, and result paths must be absolute")
        with args.config.open(encoding="utf-8") as file:
            config = json.load(file)
        if (not isinstance(config, dict) or type(config.get("contract_version")) is not int
                or config["contract_version"] != 1):
            raise ValueError("config must be an object with contract_version 1")
        verification_timeout = effective_verification_timeout_seconds(config)
        result["diagnostics"].append(
            f"Per-batch verification timeout: {verification_timeout} seconds."
        )
        if not args.prefix:
            raise ValueError("prefix must not be empty")
        if args.mode == "cleanup" and not args.yes:
            raise ValueError("cleanup requires --yes")

        if args.mode == "dry-run":
            result["status"] = "pass"
            result["diagnostics"].append(
                "Local contract check only; deployment and readiness are not verified."
            )
        elif args.mode == "resolve-target-candidate":
            args.result.parent.mkdir(parents=True, exist_ok=True)
            resolve_target_candidate(config, args.prefix, args.state_dir, args.result)
            result["status"] = "pass"
            result["diagnostics"].append(
                "Provisional ownership candidate only; runtime access and readiness "
                "are not established."
            )
        elif args.mode == "validate-only":
            result["diagnostics"].extend(validate_customer(config, args.prefix, args.state_dir))
            result["status"] = "pass"
            result["ready"] = True
        else:
            result["diagnostics"].append(
                "Demo 1 readiness remains blocked: header-only bootstrap MLV definitions "
                "exist in an isolated test environment, but automated deployment, "
                "configured-environment schedule and runtime validation, and measured-data "
                "verification are not implemented."
            )
            if args.mode == "deploy":
                result["diagnostics"].append(
                    "No resources were created: no verified workspace creation provenance "
                    "source is configured for the ownership journal."
                )
            if args.mode == "cleanup":
                result["diagnostics"].append(
                    "Cleanup is unavailable because resource-scoped cleanup is not implemented."
                )
    except CandidateBlocked as error:
        result["diagnostics"].append(str(error))
    except (OSError, ValueError, json.JSONDecodeError) as error:
        result["status"] = "fail"
        result["diagnostics"].append(str(error))

    try:
        args.result.parent.mkdir(parents=True, exist_ok=True)
        args.result.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    except OSError as error:
        print(f"Cannot write result: {error}", file=sys.stderr)
        return 1
    print(json.dumps(result))
    return 0 if result["status"] == "pass" else 1


if __name__ == "__main__":
    sys.exit(main())
