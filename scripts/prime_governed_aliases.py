#!/usr/bin/env python3
"""Protect batch dependencies before any caller or producer code is replaced."""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "aws/shared"))
from governed_targets import GOVERNED_FUNCTIONS

CALLERS = {"justhodl-scheduler", "justhodl-event-coordinator", "justhodl-schedule-liveness", "justhodl-backend-agent"}


def targets_to_prime(selected, configs):
    targets = set()
    names = set()
    for folder in selected:
        config = configs.get(folder, {})
        name = config.get("function_name", folder)
        names.add(name)
        if name in GOVERNED_FUNCTIONS or (config.get("release_validation") or {}).get("schema_version"):
            targets.add(name)
    if names & CALLERS:
        # A router update can immediately invoke any governed dependency, even
        # when that producer has no code change in this particular push.
        targets.update(GOVERNED_FUNCTIONS)
    if "justhodl-portfolio-admin" in names:
        targets.add("justhodl-portfolio-snapshot")
    return sorted(targets)


if __name__ == "__main__":
    import boto3
    from protect_lambda_alias import protect
    region, *selected = sys.argv[1:]
    configs = {}
    for target in selected:
        path = ROOT / "aws/lambdas" / target / "config.json"
        if path.exists():
            configs[target] = json.loads(path.read_text())
    clients = [boto3.client(service, region_name=region) for service in ("lambda", "scheduler", "events")]
    for function in targets_to_prime(selected, configs):
        # An absent dependency fails closed. Newly introduced services require
        # a validated bootstrap before automated callers may target them.
        print(json.dumps(protect(*clients, function), sort_keys=True))
