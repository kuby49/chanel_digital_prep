"""Shared deployment contract. Cloud operations remain explicitly blocked."""

import argparse
import json
import sys
from pathlib import Path

from generator import RunConfig


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
            data = config.get("data")
            if not isinstance(data, dict):
                raise ValueError("run-data requires a data configuration object")
            required = ("run_id", "seed", "start_date", "date_count", "product_count",
                        "customer_count", "file_count", "rows_per_file", "interval_seconds",
                        "max_duration_seconds", "currency")
            if any(key not in data or data[key] is None for key in required):
                raise ValueError("run-data requires all data parameters")
            if not isinstance(data["run_id"], str) or not isinstance(data["start_date"], str):
                raise ValueError("run_id and start_date must be strings")
            run = RunConfig(run_id=data["run_id"], start_date=data["start_date"],
                            seed=data["seed"], date_count=data["date_count"],
                            products=data["product_count"], customers=data["customer_count"],
                            file_count=data["file_count"], rows_per_file=data["rows_per_file"],
                            interval_seconds=data["interval_seconds"])
            run.validate()
            duration = data["max_duration_seconds"]
            if (not isinstance(duration, int) or isinstance(duration, bool)
                    or duration <= (run.file_count - 1) * run.interval_seconds):
                raise ValueError("max_duration_seconds must exceed the final scheduled batch offset")
            if data["currency"] != "USD":
                raise ValueError("producer currently supports USD only")
        if args.mode == "dry-run":
            result.update(status="pass", diagnostics=["Plan only; no resources were changed."])
        else:
            result["diagnostics"].append(
                "Blocked: Fabric producer deployment, destination delivery, and fresh readiness verification are not implemented or verified."
            )
    except (ValueError, OverflowError, OSError) as exc:
        result.update(status="fail", diagnostics=[str(exc)])
    result_path = Path(args.result)
    result_path.parent.mkdir(parents=True, exist_ok=True)
    result_path.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    return 0 if result["status"] == "pass" else 1


if __name__ == "__main__":
    sys.exit(main())
