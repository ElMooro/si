#!/usr/bin/env python3
"""ops 3084 -- v3.6 crash diagnosis: sync invoke w/ LogType=Tail,
print the traceback verbatim. [skip-deploy]"""
import base64
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import boto3
from ops_report import report

L = boto3.client("lambda", region_name="us-east-1")
AWS_DIR = Path(__file__).resolve().parents[2]


def main():
    fails, warns = [], []
    with report("3084_v36_tail") as rep:
        r = L.invoke(FunctionName="justhodl-industry-rotation",
                     InvocationType="RequestResponse",
                     LogType="Tail", Payload=b"{}")
        tail = base64.b64decode(r.get("LogResult") or b"").decode(
            "utf-8", "replace")
        err = r.get("FunctionError")
        rep.kv(function_error=err or "none")
        for ln in tail.splitlines()[-30:]:
            rep.log(ln[:220])
        if err:
            fails.append("engine errored: see tail")
        (AWS_DIR / "ops" / "reports" / "3084.json").write_text(
            json.dumps({"ops": 3084,
                        "verdict": "FAIL" if fails else "PASS",
                        "fails": fails, "warns": warns,
                        "tail_last": tail.splitlines()[-12:],
                        "ts": datetime.now(
                            timezone.utc).isoformat()}, indent=1))
        rep.kv(verdict="FAIL" if fails else "PASS")
        if fails:
            sys.exit(1)


main()
sys.exit(0)
