#!/usr/bin/env python3
"""Print or write the JustHodl AI infrastructure plan without contacting AWS."""

from __future__ import annotations

import argparse
import importlib.util
import json
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
VALIDATOR_PATH = (
    REPO_ROOT
    / "aws"
    / "lambdas"
    / "justhodl-ai"
    / "iam"
    / "validate_production_gates.py"
)


def load_validator():
    spec = importlib.util.spec_from_file_location(
        "justhodl_ai_validate_production_gates", VALIDATOR_PATH
    )
    if spec is None or spec.loader is None:
        raise RuntimeError(f"could not load {VALIDATOR_PATH}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output",
        type=Path,
        help="optional path for the deterministic JSON plan; stdout is always populated",
    )
    args = parser.parse_args()
    gates = load_validator()
    checks = gates.validate()
    plan = gates.change_plan()
    payload = {
        "ok": all(item.passed for item in checks),
        "checks": [
            {"name": item.name, "passed": item.passed, "detail": item.detail}
            for item in checks
        ],
        "change_plan": plan,
    }
    rendered = json.dumps(payload, indent=2) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered, encoding="utf-8")
    print(rendered, end="")
    return 0 if payload["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
