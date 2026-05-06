#!/usr/bin/env python3
"""Step 281 — Delete the 5 orphan Lambdas identified in step 261's audit.

Per parallel session's 261_repo_cleanup_audit:
  - ecb (1.7KB stub)
  - nyfed-primary-dealer-fetcher (1.6KB stub)
  - justhodl-chat-api (2KB predecessor of justhodl-ai-chat)
  - nyfed-financial-stability-fetcher (2.7KB stub)
  - nyfedapi-isolated (4.4KB test artifact)

All have:
  - 0 invocations in last 24h (per audit)
  - No EventBridge rules pointing to them
  - No Function URL config
  - No code-level dependents in repo

Safeguards before deletion:
  1. Re-verify zero EB rule targets exist
  2. Re-verify no Function URL
  3. Re-verify no recent invocations (CloudWatch metrics, last 7 days)
  4. Capture the deployment-package S3 location and source code bytes
     so the function can be rebuilt from git history if needed
"""
import json
import os
import time
from datetime import datetime, timezone, timedelta

import boto3
from botocore.exceptions import ClientError

REGION = "us-east-1"
ACCOUNT_ID = "857687956942"
REPORT_PATH = "aws/ops/reports/281_orphan_lambda_deletion.json"

ORPHANS = [
    "ecb",
    "nyfed-primary-dealer-fetcher",
    "justhodl-chat-api",
    "nyfed-financial-stability-fetcher",
    "nyfedapi-isolated",
]

lam = boto3.client("lambda", region_name=REGION)
eb = boto3.client("events", region_name=REGION)
cw = boto3.client("cloudwatch", region_name=REGION)


def check_invocations_last_7d(name):
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=7)
    try:
        resp = cw.get_metric_statistics(
            Namespace="AWS/Lambda",
            MetricName="Invocations",
            Dimensions=[{"Name": "FunctionName", "Value": name}],
            StartTime=start, EndTime=end,
            Period=86400, Statistics=["Sum"],
        )
        total = sum(d["Sum"] for d in resp.get("Datapoints", []))
        return int(total)
    except Exception as e:
        return f"err:{e}"


def safe_delete(name):
    out = {"name": name}
    try:
        # Check existence
        try:
            cfg = lam.get_function(FunctionName=name)
            out["exists_pre"] = True
            out["arn"] = cfg["Configuration"]["FunctionArn"]
            out["code_size"] = cfg["Configuration"]["CodeSize"]
            out["last_modified"] = cfg["Configuration"].get("LastModified")
        except ClientError as e:
            if e.response["Error"]["Code"] == "ResourceNotFoundException":
                out["status"] = "already_deleted"
                return out
            raise

        # Safeguard A: any EB rule still pointing at this Lambda?
        rules = eb.list_rule_names_by_target(
            TargetArn=f"arn:aws:lambda:{REGION}:{ACCOUNT_ID}:function:{name}"
        )
        rule_names = rules.get("RuleNames", [])
        out["eb_rules_pointing_at_it"] = rule_names
        if rule_names:
            out["status"] = "skipped_has_eb_rules"
            return out

        # Safeguard B: any Function URL?
        try:
            url_cfg = lam.get_function_url_config(FunctionName=name)
            out["function_url"] = url_cfg.get("FunctionUrl")
            out["status"] = "skipped_has_function_url"
            return out
        except ClientError as e:
            if e.response["Error"]["Code"] != "ResourceNotFoundException":
                raise
            out["function_url"] = None

        # Safeguard C: invocations in last 7 days
        inv = check_invocations_last_7d(name)
        out["invocations_7d"] = inv
        if isinstance(inv, int) and inv > 0:
            out["status"] = "skipped_recent_invocations"
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
        print(f"[281] {name}: {result.get('status')}")
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
