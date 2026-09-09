#!/usr/bin/env python3
"""Ops5241: Census Lambda URL owner metadata only; no invokes or code fetch."""
import json
import os
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws/ops/checks"))
from audit_20260909_census_owner import discover


def main():
    import boto3
    from botocore.config import Config
    # Never enable SDK debug logging: configuration responses can contain secrets.
    checkout = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
    result = {"ops": 5241, "checkout_sha": checkout, "ops_target_sha": os.environ.get("OPS_TARGET_SHA")}
    try:
        config = Config(connect_timeout=10, read_timeout=20, retries={"mode": "adaptive", "total_max_attempts": 10}, max_pool_connections=2)
        clients = {name: boto3.client(name, region_name="us-east-1", config=config) for name in ("lambda", "sts")}
        result.update(discover(clients["lambda"], clients["sts"]))
    except Exception:
        # Deliberately never stringify SDK errors, requests or response objects.
        result.update(ok=False, status="RUNNER_FAILED", error_count=1,
                      error_types={"runner:UNCLASSIFIED_ERROR": 1})
    path = ROOT / "aws/ops/reports/5241_census_url_owner.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps({"ops": 5241, "ok": result["ok"], "status": result["status"],
                      "matches": len(result.get("matches", [])), "error_count": result["error_count"],
                      "report": str(path.relative_to(ROOT))}))
    if not result["ok"]:
        sys.exit(1)


if __name__ == "__main__":
    main()
