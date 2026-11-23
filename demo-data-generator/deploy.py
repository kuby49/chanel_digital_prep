"""Shared deployment contract. Cloud operations remain explicitly blocked."""

import argparse
import json
import sys
from pathlib import Path


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", required=True, choices=("dry-run", "validate-only", "deploy", "cleanup", "run-data"))
    parser.add_argument("--config", required=True)
    parser.add_argument("--prefix", required=True)
    parser.add_argument("--state-dir", required=True)
    parser.add_argument("--result", required=True)
    parser.add_argument("--selected-demos")
    parser.add_argument("--yes", action="store_true")
    args = parser.parse_args(argv)
    result = {"contract_version": 1, "track": "shared", "mode": args.mode,
              "status": "blocked", "ready": False, "diagnostics": []}
    try:
        if not all(Path(p).is_absolute() for p in (args.config, args.state_dir, args.result)):
            raise ValueError("config, state-dir and result must be absolute paths")
        config = json.loads(Path(args.config).read_text(encoding="utf-8"))
        if not isinstance(config, dict) or config.get("contract_version") != 1:
            raise ValueError("config contract_version must be 1")
        if not args.prefix:
            raise ValueError("prefix must not be empty")
        if args.mode == "cleanup" and not args.yes:
            raise ValueError("cleanup requires --yes")
        if args.mode == "run-data":
            selected = args.selected_demos.split(",") if args.selected_demos else []
            if not selected or len(selected) != len(set(selected)) or any(
                name not in ("demo-1", "demo-2", "demo-3") for name in selected
            ):
                raise ValueError("run-data requires distinct --selected-demos")
        if args.mode == "dry-run":
            result.update(status="pass", diagnostics=["Plan only; no resources were changed."])
        else:
            result["diagnostics"].append(
                "Blocked: Fabric producer deployment, destination delivery, and fresh readiness verification are not implemented or verified."
            )
    except (ValueError, OSError, json.JSONDecodeError) as exc:
        result.update(status="fail", diagnostics=[str(exc)])
    result_path = Path(args.result)
    result_path.parent.mkdir(parents=True, exist_ok=True)
    result_path.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    return 0 if result["status"] == "pass" else 1


if __name__ == "__main__":
    sys.exit(main())
