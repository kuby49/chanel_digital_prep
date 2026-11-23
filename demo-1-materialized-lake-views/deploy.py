"""Fail-closed Demo 1 entry point for deployment contract version 1."""

import argparse
import json
import sys
from pathlib import Path

from definition_plan import effective_verification_timeout_seconds


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", required=True, choices=("dry-run", "validate-only", "deploy", "cleanup"))
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--prefix", required=True)
    parser.add_argument("--state-dir", required=True, type=Path)
    parser.add_argument("--result", required=True, type=Path)
    parser.add_argument("--yes", action="store_true")
    args = parser.parse_args()

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
        if not isinstance(config, dict) or config.get("contract_version") != 1:
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
        else:
            result["diagnostics"].append(
                "Demo 1 readiness remains blocked: header-only bootstrap MLV definitions "
                "exist in an isolated test environment, but automated deployment, "
                "configured-environment schedule and runtime validation, and measured-data "
                "verification are not implemented."
            )
            if args.mode == "cleanup":
                result["diagnostics"].append(
                    "Cleanup is unavailable because resource-scoped cleanup is not implemented."
                )
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
