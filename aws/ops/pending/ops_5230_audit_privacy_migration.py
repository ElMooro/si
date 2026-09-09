#!/usr/bin/env python3
"""Ops5230: apply only after the reviewed Worker and Lambda release has deployed.

Retry after Worker vault-slug cache protection: canonical hyphenated shard required; legacy underscore alias optional.
Safe to retry. Private originals and object versions are retained. Temporary
external-read containment stays installed on failure. No source prose, secret,
environment value, signed URL, or raw Lambda response is written to reports.
The provider rebuild is pinned to a source-verified numbered Lambda version.
"""
import json
import os
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / "aws/ops/checks"), str(ROOT / "aws/shared")]
from audit_20260909_privacy_migration import Migration, MigrationError, REGION, verify_core_receipt


def main():
    import boto3
    from botocore.config import Config
    clients = {name: boto3.client(name, region_name=REGION,
        config=Config(read_timeout=950, connect_timeout=15, tcp_keepalive=True, retries={"max_attempts": 2}) if name == "lambda" else Config(retries={"max_attempts": 4}))
        for name in ("s3", "lambda", "iam", "ssm", "sts")}
    migration = Migration(ROOT, clients)
    checkout = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
    result = {"ops": 5230, "checkout_sha": checkout, "ops_target_sha": os.environ.get("OPS_TARGET_SHA"), "ok": False}
    try:
        result["core_layer_prerequisite"] = verify_core_receipt(ROOT)
        result.update(migration.run())
    except Exception as exc:
        # SDK/HTTP errors can contain sensitive request context; suppress text/trace.
        result.update({"failed_step": migration.step, "error_class": type(exc).__name__,
                       "reason": str(exc) if isinstance(exc, MigrationError) else "operation_failed_details_withheld",
                       "checks": migration.rows, "private_payloads_reported": 0})
        if migration.step != "initialization":
            try:
                migration.policy(True)
                migration.purge()
                result["temporary_containment_retained"] = True
            except Exception:
                result["temporary_containment_recheck_failed"] = True
    path = ROOT / "aws/ops/reports/5230_audit_privacy_migration.json"
    path.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps({"ops": 5230, "ok": result["ok"], "checks": len(migration.rows), "failed_step": result.get("failed_step"), "report": str(path.relative_to(ROOT))}))
    if not result["ok"]:
        sys.exit(1)


if __name__ == "__main__":
    main()
