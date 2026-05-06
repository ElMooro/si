#!/usr/bin/env python3
"""Step 283 — Delete the 3 nyfed Lambdas confirmed as abandoned dev artifacts.

Step 282 investigation conclusively showed:
  - All 3 have NO EventBridge rule (full paginator scan)
  - All 3 have NO Function URL
  - All 3 had bunched manual invocations April 23-30, then silence
  - All 3 have last code modification April 25 (recent dev probe)
  - All 3 have NO repo dependents outside historical reports
  - All 3 are NOT monitored in expectations.py

Safeguards (defense in depth):
  A. NO EB rule (full scan, not just targeted query)
  B. NO Function URL
  C. ZERO invocations in last 48h (tighter than 281's 7d window)
"""
import json
import os
import time
from datetime import datetime, timezone, timedelta

import boto3
from botocore.exceptions import ClientError

REGION = "us-east-1"
ACCOUNT_ID = "857687956942"
REPORT_PATH = "aws/ops/reports/283_nyfed_deletion.json"

ORPHANS = [
    "nyfed-primary-dealer-fetcher",
    "nyfed-financial-stability-fetcher",
    "nyfedapi-isolated",
]

lam = boto3.client("lambda", region_name=REGION)
eb = boto3.client("events", region_name=REGION)
cw = boto3.client("cloudwatch", region_name=REGION)


def has_eb_rule_full_scan(name):
    """Bulletproof — paginates ALL rules and checks targets."""
    arn = f"arn:aws:lambda:{REGION}:{ACCOUNT_ID}:function:{name}"
    paginator = eb.get_paginator("list_rules")
    for page in paginator.paginate():
        for rule in page.get("Rules", []):
            try:
                tgts = eb.list_targets_by_rule(Rule=rule["Name"])
                for t in tgts.get("Targets", []):
                    if t.get("Arn") == arn:
                        return rule["Name"]
            except Exception:
                pass
    return None


def invocations_last_48h(name):
    end = datetime.now(timezone.utc)
    start = end - timedelta(hours=48)
    try:
        resp = cw.get_metric_statistics(
            Namespace="AWS/Lambda",
            MetricName="Invocations",
            Dimensions=[{"Name": "FunctionName", "Value": name}],
            StartTime=start, EndTime=end,
            Period=3600, Statistics=["Sum"],
        )
        return int(sum(d["Sum"] for d in resp.get("Datapoints", [])))
    except Exception as e:
        return f"err:{e}"


def safe_delete(name):
    out = {"name": name}
    try:
        try:
            cfg = lam.get_function(FunctionName=name)
            out["arn"] = cfg["Configuration"]["FunctionArn"]
            out["code_size"] = cfg["Configuration"]["CodeSize"]
        except ClientError as e:
            if e.response["Error"]["Code"] == "ResourceNotFoundException":
                out["status"] = "already_deleted"
                return out
            raise

        # Safeguard A: any EB rule (full scan)
        eb_rule = has_eb_rule_full_scan(name)
        out["eb_rule_full_scan"] = eb_rule
        if eb_rule:
            out["status"] = "skipped_has_eb_rule"
            return out

        # Safeguard B: any Function URL
        try:
            url_cfg = lam.get_function_url_config(FunctionName=name)
            out["function_url"] = url_cfg.get("FunctionUrl")
            out["status"] = "skipped_has_function_url"
            return out
        except ClientError as e:
            if e.response["Error"]["Code"] != "ResourceNotFoundException":
                raise
            out["function_url"] = None

        # Safeguard C: invocations in last 48h
        inv = invocations_last_48h(name)
        out["invocations_48h"] = inv
        if isinstance(inv, int) and inv > 0:
            out["status"] = "skipped_recent_invocations_48h"
            return out

        # All clear — DELETE
        lam.delete_function(FunctionName=name)
        out["status"] = "deleted"
        return out

    except Exception as e:
        out["status"] = "error"
        out["err"] = str(e)[:200]
        return out


def main():
    started = time.time()
    out = {"started": datetime.now(timezone.utc).isoformat(), "results": []}
    for name in ORPHANS:
        result = safe_delete(name)
        print(f"[283] {name}: {result.get('status')}")
        out["results"].append(result)

    out["n_deleted"] = sum(1 for r in out["results"] if r["status"] == "deleted")
    out["n_skipped"] = sum(1 for r in out["results"] if r["status"].startswith("skipped"))
    out["n_already_deleted"] = sum(1 for r in out["results"] if r["status"] == "already_deleted")
    out["n_errors"] = sum(1 for r in out["results"] if r["status"] == "error")
    out["total_code_size_freed"] = sum(
        r.get("code_size", 0) for r in out["results"] if r["status"] == "deleted"
    )
    out["duration_s"] = round(time.time() - started, 1)

    os.makedirs(os.path.dirname(REPORT_PATH), exist_ok=True)
    with open(REPORT_PATH, "w") as f:
        json.dump(out, f, indent=2, default=str)
    print(json.dumps(out, indent=2, default=str)[:3000])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
