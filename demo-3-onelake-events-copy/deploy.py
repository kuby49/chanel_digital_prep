"""Fail-closed Demo 3 orchestration contract; no Fabric writes are performed."""

import argparse
import json
import re
import sys
from pathlib import Path


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", required=True, choices=("dry-run", "validate-only", "deploy", "cleanup"))
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--prefix", required=True)
    parser.add_argument("--state-dir", required=True, type=Path)
    parser.add_argument("--result", required=True, type=Path)
    parser.add_argument("--yes", action="store_true")
    args = parser.parse_args(argv)
    result = {
        "contract_version": 1, "track": "demo-3", "mode": args.mode,
        "status": "blocked", "ready": False, "diagnostics": [],
    }
    if not args.result.is_absolute():
        print("Result path must be absolute", file=sys.stderr)
        return 1
    try:
        if not all(path.is_absolute() for path in (args.config, args.state_dir)):
            raise ValueError("config and state-dir paths must be absolute")
        if not re.fullmatch(r"[a-z0-9](?:[a-z0-9-]*[a-z0-9])?", args.prefix):
            raise ValueError("prefix must contain lowercase letters, digits, or interior hyphens")
        with args.config.open(encoding="utf-8") as file:
            config = json.load(file)
        if not isinstance(config, dict) or config.get("contract_version") != 1:
            raise ValueError("configuration contract_version must be 1")
        if args.mode == "cleanup" and not args.yes:
            raise ValueError("cleanup requires --yes; no resources were removed")
        if args.mode == "dry-run":
            result["status"] = "pass"
            result["diagnostics"].append(
                "Offline contract check only; deployment and runtime readiness are not established."
            )
        else:
            result["diagnostics"].append(
                "Blocked: connection authentication, exact-file Copy definition, serialized writes, "
                "event binding and runtime verification are not implemented or verified; no Fabric changes made."
            )
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        result["status"] = "fail"
        result["diagnostics"].append(f"Invalid local input: {type(exc).__name__}")
    try:
        args.result.parent.mkdir(parents=True, exist_ok=True)
        args.result.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    except OSError as exc:
        print(f"Could not write result: {type(exc).__name__}", file=sys.stderr)
        return 1
    print(json.dumps(result))
    return 0 if result["status"] == "pass" else 1


if __name__ == "__main__":
    sys.exit(main())
