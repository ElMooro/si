#!/usr/bin/env python3
"""Step 310 — Verify Sprint 5 (sector-tilt) and Sprint 6 (pairs-scanner)
deployment state in AWS.

Checks:
  1. Both Lambdas exist
  2. EventBridge rules wired
  3. S3 outputs produced + ages
  4. Sample inspect both outputs to know what's working
"""
import json
import os
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
REPORT = "aws/ops/reports/310_sprint5_6_audit.json"

lam = boto3.client("lambda", region_name=REGION)
events = boto3.client("events", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)


def check_lambda(name):
    out = {"name": name}
    try:
        cfg = lam.get_function_configuration(FunctionName=name)
        out["exists"] = True
        out["runtime"] = cfg.get("Runtime")
        out["timeout"] = cfg.get("Timeout")
        out["memory"] = cfg.get("MemorySize")
        out["last_modified"] = cfg.get("LastModified")
        out["state"] = cfg.get("State")
    except ClientError as e:
        out["exists"] = False
        out["err"] = e.response["Error"]["Code"]
    return out


def check_eb_rule(rule_name):
    try:
        r = events.describe_rule(Name=rule_name)
        return {
            "exists": True,
            "schedule": r.get("ScheduleExpression"),
            "state": r.get("State"),
        }
    except ClientError as e:
        return {"exists": False, "err": e.response["Error"]["Code"]}


def check_s3(key):
    try:
        obj = s3.head_object(Bucket=BUCKET, Key=key)
        age_h = (datetime.now(timezone.utc) - obj["LastModified"]).total_seconds() / 3600
        return {
            "exists": True,
            "size_kb": round(obj["ContentLength"] / 1024, 1),
            "last_modified": obj["LastModified"].isoformat(),
            "age_hours": round(age_h, 1),
        }
    except ClientError as e:
        return {"exists": False, "err": e.response["Error"]["Code"]}


def sample_s3(key, max_chars=2000):
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=key)
        data = json.loads(obj["Body"].read())
        s = json.dumps(data, indent=2, default=str)
        if len(s) > max_chars:
            return s[:max_chars] + "…"
        return s
    except Exception as e:
        return f"<err: {str(e)[:200]}>"


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}

    # SPRINT 5 — sector-tilt
    out["sprint5"] = {
        "lambda": check_lambda("justhodl-sector-tilt"),
        "eb_rules": {},
        "s3_output": check_s3("data/sector-tilt.json"),
    }
    # Try common rule names
    for rn in ["sector-tilt-6hourly", "sector-tilt-hourly", "sector-tilt-schedule"]:
        r = check_eb_rule(rn)
        if r.get("exists"):
            out["sprint5"]["eb_rules"][rn] = r

    # SPRINT 6 — pairs-scanner
    out["sprint6"] = {
        "lambda": check_lambda("justhodl-pairs-scanner"),
        "eb_rules": {},
        "s3_output": check_s3("data/pairs-scanner.json"),
    }
    for rn in ["pairs-scanner-hourly", "pairs-scanner-6hourly", "pairs-scanner-schedule"]:
        r = check_eb_rule(rn)
        if r.get("exists"):
            out["sprint6"]["eb_rules"][rn] = r

    # If sector-tilt has data, capture sample for HTML design
    if out["sprint5"]["s3_output"].get("exists"):
        out["sprint5"]["sample"] = sample_s3("data/sector-tilt.json", 4000)
    if out["sprint6"]["s3_output"].get("exists"):
        out["sprint6"]["sample"] = sample_s3("data/pairs-scanner.json", 4000)

    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f:
        json.dump(out, f, indent=2, default=str)

    # Print summary
    print("═" * 70)
    print("  SPRINT 5 — Macro→Sector Tilt Engine")
    print("═" * 70)
    s5 = out["sprint5"]
    print(f"  Lambda exists: {s5['lambda'].get('exists')}")
    if s5["lambda"].get("exists"):
        print(f"    State: {s5['lambda'].get('state')} · last modified: {s5['lambda'].get('last_modified')}")
    print(f"  EB rules: {list(s5.get('eb_rules', {}).keys()) or 'NONE'}")
    print(f"  S3 output exists: {s5['s3_output'].get('exists')}")
    if s5["s3_output"].get("exists"):
        print(f"    Size: {s5['s3_output'].get('size_kb')} KB · Age: {s5['s3_output'].get('age_hours')}h")

    print()
    print("═" * 70)
    print("  SPRINT 6 — Pairs-Trading Scanner")
    print("═" * 70)
    s6 = out["sprint6"]
    print(f"  Lambda exists: {s6['lambda'].get('exists')}")
    if s6["lambda"].get("exists"):
        print(f"    State: {s6['lambda'].get('state')} · last modified: {s6['lambda'].get('last_modified')}")
    print(f"  EB rules: {list(s6.get('eb_rules', {}).keys()) or 'NONE'}")
    print(f"  S3 output exists: {s6['s3_output'].get('exists')}")
    if s6["s3_output"].get("exists"):
        print(f"    Size: {s6['s3_output'].get('size_kb')} KB · Age: {s6['s3_output'].get('age_hours')}h")

    print()
    if out["sprint5"].get("sample"):
        print("─── sector-tilt.json sample ───")
        print(out["sprint5"]["sample"][:2200])
        print()
    if out["sprint6"].get("sample"):
        print("─── pairs-scanner.json sample ───")
        print(out["sprint6"]["sample"][:2200])


if __name__ == "__main__":
    main()
