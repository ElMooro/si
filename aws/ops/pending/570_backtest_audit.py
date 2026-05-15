#!/usr/bin/env python3
"""570 — Pre-build audit for backtesting framework. Look for existing
Lambdas, DDB tables, and S3 keys related to backtesting / accuracy / signals."""
import io, json, os
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/570_backtest_audit.json"
lam = boto3.client("lambda", region_name="us-east-1")
ddb = boto3.client("dynamodb", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}

    # 1. Search for backtest-related Lambdas
    try:
        keywords = ["backtest", "back-test", "replay", "historical-test",
                     "accuracy", "calibrator", "signal-logger", "outcome-checker",
                     "predict", "ranker", "rank", "conviction"]
        paginator = lam.get_paginator("list_functions")
        matches = []
        for page in paginator.paginate():
            for f in page.get("Functions", []):
                n = f["FunctionName"].lower()
                for kw in keywords:
                    if kw in n:
                        matches.append({
                            "name": f["FunctionName"],
                            "memory": f.get("MemorySize"),
                            "timeout": f.get("Timeout"),
                            "last_modified": f.get("LastModified"),
                        })
                        break
        out["matching_lambdas"] = matches
    except Exception as e:
        out["lambda_err"] = str(e)[:200]

    # 2. DynamoDB tables
    try:
        tables = ddb.list_tables()
        out["ddb_tables"] = [t for t in tables.get("TableNames", [])
                              if any(kw in t.lower() for kw in ["signal", "outcome", "calibr", "backtest", "predict"])]
    except Exception as e:
        out["ddb_err"] = str(e)[:200]

    # 3. S3 keys for signals/outcomes/calibration
    try:
        prefixes_to_check = [
            "signals/", "outcomes/", "calibration/", "predictions/",
            "backtest/", "learning/", "archive/", "data/calibr",
        ]
        s3_keys = {}
        for pfx in prefixes_to_check:
            try:
                resp = s3.list_objects_v2(
                    Bucket="justhodl-dashboard-live", Prefix=pfx,
                    MaxKeys=15,
                )
                keys = [{"key": o["Key"], "size_kb": round(o["Size"]/1024,1),
                          "modified": o["LastModified"].isoformat()[:19]}
                          for o in resp.get("Contents", [])]
                s3_keys[pfx] = keys
            except Exception as e:
                s3_keys[pfx] = f"err: {e}"
        out["s3_keys_by_prefix"] = s3_keys
    except Exception as e:
        out["s3_err"] = str(e)[:200]

    # 4. Check FRED cache + historical bars availability
    try:
        for k in ["data/fred-cache.json", "data/fred-cache-secretary.json",
                   "data/report.json", "data/historical-bars.json",
                   "data/signals.json", "learning/calibration_weights.json",
                   "learning/prompt_templates.json"]:
            try:
                obj = s3.head_object(Bucket="justhodl-dashboard-live", Key=k)
                out.setdefault("sidecar_check", {})[k] = {
                    "size_kb": round(obj["ContentLength"]/1024, 1),
                    "modified": obj["LastModified"].isoformat()[:19],
                }
            except: out.setdefault("sidecar_check", {})[k] = "missing"
    except Exception as e:
        out["sidecar_err"] = str(e)[:200]

    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
