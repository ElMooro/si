"""
Ops 1087: re-sweep the five degraded upgrades after schedule recreation.

Same green criteria as 1083:
 (a) EventBridge schedule enabled
 (b) At least one successful invocation after 2026-10-01T13:54Z
 (c) Fresh data artifact in S3
 (d) Zero errors in recent CloudWatch window

Writes: aws/ops/reports/1087_resweep.json. Never raises.
"""
from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone

OUT_PATH = os.path.join("aws", "ops", "reports", "1087_resweep.json")
BUCKET = "justhodl-dashboard-live"
REGION = "us-east-1"
FIX_WINDOW = datetime(2026, 10, 1, 13, 54, tzinfo=timezone.utc)

UNITS = [
    {"name": "SEC 8-K enrichment",
     "lambda": "justhodl-sec-8k-enrich",
     "schedule": "justhodl-sec-8k-enrich-schedule",
     "artifact": "data/sec-8k.json"},
    {"name": "FINRA short-interest",
     "lambda": "justhodl-short-interest",
     "schedule": "justhodl-short-interest-schedule",
     "artifact": "data/short-interest-tickers.json"},
    {"name": "SEC XBRL",
     "lambda": "justhodl-xbrl-fundamentals",
     "schedule": "justhodl-xbrl-fundamentals-schedule",
     "artifact": "data/xbrl-fundamentals.json"},
    {"name": "Corporate actions",
     "lambda": "justhodl-corporate-actions",
     "schedule": "justhodl-corporate-actions-schedule",
     "artifact": "data/corporate-actions.json"},
    {"name": "ETF issuer holdings",
     "lambda": "justhodl-etf-issuer-holdings",
     "schedule": "justhodl-etf-issuer-holdings-daily",
     "artifact": "data/etf-issuer-holdings.json"},
]

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                "..", "..", "shared"))
import boto3  # noqa: E402
from botocore.exceptions import ClientError  # noqa: E402

sched = boto3.client("scheduler", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)
cw = boto3.client("cloudwatch", region_name=REGION)


def check_schedule(name):
    """Check if schedule enabled. Never raises."""
    try:
        r = sched.get_schedule(Name=name)
        state = r.get("State")
        return {"enabled": state == "ENABLED", "state": state}
    except ClientError:
        return {"enabled": False, "error": "not found"}
    except Exception as e:  # noqa: BLE001
        return {"enabled": "unknown", "error": str(e)[:60]}


def check_invocation(fn):
    """Check invocations/errors since fix window. Never raises."""
    try:
        end = datetime.now(timezone.utc)
        inv = cw.get_metric_statistics(
            Namespace="AWS/Lambda", MetricName="Invocations",
            Dimensions=[{"Name": "FunctionName", "Value": fn}],
            StartTime=FIX_WINDOW, EndTime=end, Period=3600,
            Statistics=["Sum"])["Datapoints"]
        err = cw.get_metric_statistics(
            Namespace="AWS/Lambda", MetricName="Errors",
            Dimensions=[{"Name": "FunctionName", "Value": fn}],
            StartTime=FIX_WINDOW, EndTime=end, Period=3600,
            Statistics=["Sum"])["Datapoints"]
        inv_sum = sum(d["Sum"] for d in inv)
        err_sum = sum(d["Sum"] for d in err)
        return {"invocations_since_fix": int(inv_sum),
                "errors_since_fix": int(err_sum),
                "has_success": inv_sum > 0 and err_sum == 0}
    except Exception as e:  # noqa: BLE001
        return {"error": str(e)[:80]}


def check_artifact(key):
    """Check S3 artifact freshness. Never raises."""
    try:
        r = s3.head_object(Bucket=BUCKET, Key=key)
        lm = r["LastModified"]
        age_h = (datetime.now(timezone.utc) - lm).total_seconds() / 3600
        return {"exists": True,
                "last_modified": lm.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "age_hours": round(age_h, 1),
                "fresh": age_h < 48}
    except ClientError:
        return {"exists": False}
    except Exception as e:  # noqa: BLE001
        return {"exists": "unknown", "error": str(e)[:60]}


def main():
    """Run resweep. Write report."""
    rep = {"script": "1087_resweep",
           "read_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
           "units": []}
    for u in UNITS:
        res = {"name": u["name"], "lambda": u["lambda"]}
        res["schedule"] = check_schedule(u["schedule"])
        res["invocation"] = check_invocation(u["lambda"])
        res["artifact"] = check_artifact(u["artifact"])
        sched_ok = res["schedule"].get("enabled") is True
        inv_ok = res["invocation"].get("has_success", False)
        art_ok = res["artifact"].get("fresh", False)
        res["green"] = sched_ok and inv_ok and art_ok
        rep["units"].append(res)

    rep["green_count"] = sum(1 for u in rep["units"] if u["green"])
    rep["total"] = len(rep["units"])

    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    with open(OUT_PATH, "w") as f:
        json.dump(rep, f, indent=2)
    print(json.dumps({"green": rep["green_count"], "total": rep["total"]}))


if __name__ == "__main__":
    main()
