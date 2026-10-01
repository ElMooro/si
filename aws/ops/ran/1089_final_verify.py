"""
Ops 1089: final verification with CORRECT artifact paths.

From 1088 diagnosis, the actual artifacts are:
 - 8-K: data/sec-filings-intel.json (not data/sec-8k.json)
 - Short-interest: data/short-book.json (not data/short-interest-tickers.json)
 - Corporate actions: data/corporate-actions-index.json (not data/corporate-actions.json)
 - XBRL: data/xbrl-fundamentals/ (per-ticker, not single file)
 - ETF: data/etf-issuer-holdings.json (correct)

Writes: aws/ops/reports/1089_final_verify.json. Never raises.
"""
from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone

OUT_PATH = os.path.join("aws", "ops", "reports", "1089_final_verify.json")
BUCKET = "justhodl-dashboard-live"
REGION = "us-east-1"

UNITS = [
    {"name": "SEC 8-K enrichment",
     "lambda": "justhodl-sec-8k-enrich",
     "schedule": "justhodl-sec-8k-enrich-schedule",
     "artifact": "data/sec-filings-intel.json"},
    {"name": "FINRA short-interest",
     "lambda": "justhodl-short-interest",
     "schedule": "justhodl-short-interest-schedule",
     "artifact": "data/short-book.json"},
    {"name": "SEC XBRL",
     "lambda": "justhodl-xbrl-fundamentals",
     "schedule": "justhodl-xbrl-fundamentals-schedule",
     "artifact": "data/xbrl-fundamentals/A.json"},
    {"name": "Corporate actions",
     "lambda": "justhodl-corporate-actions",
     "schedule": "justhodl-corporate-actions-schedule",
     "artifact": "data/corporate-actions-index.json"},
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
FIX_WINDOW = datetime(2026, 10, 1, 13, 54, tzinfo=timezone.utc)


def check_schedule(name):
    try:
        r = sched.get_schedule(Name=name)
        return {"enabled": r.get("State") == "ENABLED"}
    except Exception:  # noqa: BLE001
        return {"enabled": False}


def check_invocation(fn):
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
        return {"invocations": int(inv_sum), "errors": int(err_sum),
                "clean": inv_sum > 0 and err_sum == 0}
    except Exception as e:  # noqa: BLE001
        return {"error": str(e)[:60]}


def check_artifact(key):
    try:
        r = s3.head_object(Bucket=BUCKET, Key=key)
        lm = r["LastModified"]
        age_h = (datetime.now(timezone.utc) - lm).total_seconds() / 3600
        return {"exists": True, "age_hours": round(age_h, 1),
                "fresh": age_h < 48, "size": r.get("ContentLength", 0)}
    except ClientError:
        return {"exists": False}
    except Exception as e:  # noqa: BLE001
        return {"exists": "unknown", "error": str(e)[:60]}


def main():
    rep = {"script": "1089_final_verify",
           "read_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
           "units": []}
    for u in UNITS:
        res = {"name": u["name"]}
        res["schedule_enabled"] = check_schedule(u["schedule"])["enabled"]
        inv = check_invocation(u["lambda"])
        res["invocations"] = inv.get("invocations", 0)
        res["errors"] = inv.get("errors", 0)
        art = check_artifact(u["artifact"])
        res["artifact_exists"] = art.get("exists") is True
        res["artifact_fresh"] = art.get("fresh", False)
        res["artifact_size"] = art.get("size", 0)
        res["green"] = (res["schedule_enabled"] and inv.get("clean", False)
                        and res["artifact_fresh"])
        rep["units"].append(res)

    rep["green_count"] = sum(1 for u in rep["units"] if u["green"])
    rep["total"] = len(rep["units"])
    rep["status"] = "COMPLETE"

    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    with open(OUT_PATH, "w") as f:
        json.dump(rep, f, indent=2)
    print(json.dumps({"green": rep["green_count"], "total": rep["total"]}))


if __name__ == "__main__":
    main()
