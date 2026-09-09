#!/usr/bin/env python3
"""Move automated callers onto alias-aware code before staging producers."""
from __future__ import annotations

import base64
import hashlib
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
from prime_governed_aliases import CALLERS

ROUTING_CALLERS = CALLERS | {"justhodl-portfolio-admin"}


def configured_name(folder):
    path = ROOT / "aws/lambdas" / folder / "config.json"
    return json.loads(path.read_text()).get("function_name", folder) if path.exists() else folder


def ordered_targets(targets):
    # Stable within each phase; retain repository folder names for packaging.
    return sorted(dict.fromkeys(targets), key=lambda target: configured_name(target) not in ROUTING_CALLERS)


def verify_caller(lam, function, archive):
    expected = base64.b64encode(hashlib.sha256(Path(archive).read_bytes()).digest()).decode()
    config = lam.get_function_configuration(FunctionName=function)
    if (config.get("State") != "Active" or config.get("LastUpdateStatus") != "Successful"
            or config.get("CodeSha256") != expected):
        raise RuntimeError("Caller code or state does not match this release; producers must remain unstaged")
    return {"phase": "caller_ready", "function": function, "code_sha256": expected,
            "state": config["State"], "last_update_status": config["LastUpdateStatus"]}


if __name__ == "__main__":
    command, *args = sys.argv[1:]
    if command == "order":
        print(" ".join(ordered_targets(args)))
    elif command == "is-caller":
        sys.exit(0 if configured_name(args[0]) in ROUTING_CALLERS else 1)
    elif command == "verify-caller":
        import boto3
        function, region, archive = args
        print(json.dumps(verify_caller(boto3.client("lambda", region_name=region), function, archive), sort_keys=True))
    else:
        raise ValueError("Unknown release-order command")
