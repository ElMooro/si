#!/usr/bin/env python3
"""Step 285 — Post-import verify of justhodl-ka-metrics + delete cdn-diag-temp.

After step 284 downloaded the source and step 285 commits it to the repo,
deploy-lambdas.yml will redeploy from repo (same code, near no-op). This
script:

  1. Sync invokes justhodl-ka-metrics to confirm it still works post-import
  2. Verifies data/khalid-metrics.json and data/ka-metrics.json are
     fresh (last write within last 24h, since cron runs at 11 UTC daily)
  3. Deletes justhodl-cdn-diag-temp with safeguards:
     - No EB rule (full scan)
     - No Function URL
     - 0 invocations 48h
"""
import json
import os
import time
from datetime import datetime, timezone, timedelta

import boto3
from botocore.exceptions import ClientError

REGION = "us-east-1"
ACCOUNT_ID = "857687956942"
BUCKET = "justhodl-dashboard-live"
REPORT_PATH = "aws/ops/reports/285_ka_metrics_import_and_cdn_delete.json"

lam = boto3.client("lambda", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)
eb = boto3.client("events", region_name=REGION)
cw = boto3.client("cloudwatch", region_name=REGION)


def has_eb_rule_full_scan(name):
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


def s3_age_hours(key):
    try:
        head = s3.head_object(Bucket=BUCKET, Key=key)
        last_mod = head["LastModified"]
        return round((datetime.now(timezone.utc) - last_mod).total_seconds() / 3600, 1)
    except Exception as e:
        return f"err:{e}"


def main():
    started = time.time()
    out = {"started": datetime.now(timezone.utc).isoformat()}
    try:
        # 1. Sync invoke ka-metrics
        print("[285] sync invoking justhodl-ka-metrics…")
        inv_started = time.time()
        try:
            inv = lam.invoke(FunctionName="justhodl-ka-metrics",
                              InvocationType="RequestResponse",
                              Payload=b'{}')
            payload = json.loads(inv["Payload"].read())
            out["ka_metrics_invoke"] = {
                "status": inv.get("StatusCode"),
                "func_err": inv.get("FunctionError"),
                "elapsed_s": round(time.time() - inv_started, 2),
                "body_preview": str(payload)[:300],
            }
        except Exception as e:
            out["ka_metrics_invoke"] = {"err": str(e)[:300]}

        # 2. Verify the data files it writes are fresh
        out["data_freshness"] = {
            "data/khalid-metrics.json": s3_age_hours("data/khalid-metrics.json"),
            "data/ka-metrics.json": s3_age_hours("data/ka-metrics.json"),
            "data/khalid-config.json": s3_age_hours("data/khalid-config.json"),
        }

        # 3. Delete cdn-diag-temp with safeguards
        print("[285] checking justhodl-cdn-diag-temp for deletion…")
        try:
            cfg = lam.get_function(FunctionName="justhodl-cdn-diag-temp")
            cdn_size = cfg["Configuration"]["CodeSize"]
        except ClientError as e:
            if e.response["Error"]["Code"] == "ResourceNotFoundException":
                out["cdn_diag_delete"] = {"status": "already_deleted"}
                cfg = None
            else:
                raise

        if cfg:
            cdn_eb = has_eb_rule_full_scan("justhodl-cdn-diag-temp")
            try:
                cdn_url = lam.get_function_url_config(FunctionName="justhodl-cdn-diag-temp").get("FunctionUrl")
            except ClientError as e:
                cdn_url = None if e.response["Error"]["Code"] == "ResourceNotFoundException" else f"err:{e}"
            cdn_inv = invocations_last_48h("justhodl-cdn-diag-temp")

            check = {
                "code_size": cdn_size,
                "eb_rule": cdn_eb,
                "function_url": cdn_url,
                "invocations_48h": cdn_inv,
            }

            if cdn_eb:
                check["status"] = "skipped_has_eb_rule"
            elif cdn_url:
                check["status"] = "skipped_has_function_url"
            elif isinstance(cdn_inv, int) and cdn_inv > 0:
                check["status"] = "skipped_recent_invocations_48h"
            else:
                lam.delete_function(FunctionName="justhodl-cdn-diag-temp")
                check["status"] = "deleted"
                check["bytes_freed"] = cdn_size

            out["cdn_diag_delete"] = check

        out["duration_s"] = round(time.time() - started, 1)

    except Exception as e:
        import traceback
        out["fatal_error"] = str(e)
        out["traceback"] = traceback.format_exc()

    os.makedirs(os.path.dirname(REPORT_PATH), exist_ok=True)
    with open(REPORT_PATH, "w") as f:
        json.dump(out, f, indent=2, default=str)
    print(json.dumps(out, indent=2, default=str)[:3000])
    return 0 if "fatal_error" not in out else 1


if __name__ == "__main__":
    raise SystemExit(main())
