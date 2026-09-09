#!/usr/bin/env python3
"""Ops5238: install confidentiality policies before the full source release.

Permission changes and cache purge only. All originals/versions are preserved;
no Lambda invocation, private payload read, mirror seed or service config change.
Full ops5230 must later complete before temporary public-feed containment lifts.
"""
import json
import os
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / "aws/ops/checks"), str(ROOT / "aws/shared")]
from audit_20260909_containment import contain


def main():
    import boto3
    from botocore.config import Config
    clients = {name: boto3.client(name, region_name="us-east-1", config=Config(
        connect_timeout=15, read_timeout=30, retries={"max_attempts": 4})) for name in ("s3", "sts")}
    checkout = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
    result = {"ops": 5238, "checkout_sha": checkout, "ops_target_sha": os.environ.get("OPS_TARGET_SHA"), **contain(ROOT, clients)}
    path = ROOT / "aws/ops/reports/5238_audit_privacy_containment.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps({"ops": 5238, "ok": result["ok"], "policy_verified": result.get("policy_verified", False),
                      "edge_purge_verified": result.get("edge_purge_verified", False),
                      "temporary_containment_retained": result.get("temporary_containment_retained", False),
                      "temporary_containment_state": result["temporary_containment_state"],
                      "policy_write_acknowledged": result["policy_write_acknowledged"],
                      "checks": len(result["checks"]), "report": str(path.relative_to(ROOT))}))
    if not result["ok"]:
        sys.exit(1)


if __name__ == "__main__":
    main()
