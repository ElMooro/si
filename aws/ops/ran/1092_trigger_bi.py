"""
Ops 1092: trigger best-ideas to get fresh t360 artifact, verify flow-confluence ticker_map.
Writes: aws/ops/reports/1092_trigger_bi.json. Never raises.
"""
from __future__ import annotations
import json, os, sys
from datetime import datetime, timezone
OUT_PATH = os.path.join("aws", "ops", "reports", "1092_trigger_bi.json")
REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "shared"))
import boto3
from botocore.exceptions import ClientError
lam = boto3.client("lambda", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)

def main():
    rep = {"script": "1092_trigger_bi",
           "read_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")}
    # Trigger best-ideas
    try:
        r = lam.invoke(FunctionName="justhodl-best-ideas",
                       InvocationType="Event",
                       Payload=json.dumps({"source": "ops-1092", "trigger": "t360-verify"}))
        rep["best_ideas_triggered"] = r["StatusCode"] == 202
    except Exception as e:
        rep["best_ideas_triggered"] = False
        rep["trigger_error"] = str(e)[:100]
    # Check flow-confluence ticker_map for t360 fields
    try:
        r = s3.get_object(Bucket=BUCKET, Key="data/flow-confluence.json")
        d = json.loads(r["Body"].read())
        tm = d.get("ticker_map", {})
        if tm:
            fk = list(tm.keys())[0]
            sample = tm[fk]
            rep["flow_confluence"] = {
                "version": d.get("version"),
                "n_tickers": len(tm),
                "sample_ticker": fk,
                "sample_has_t360_coverage": "t360_coverage" in sample,
                "sample_has_t360_domains": "t360_domains" in sample,
                "counts_keys": list(d.get("counts", {}).keys())[:10],
                "has_cross_validated_tickers": "cross_validated_tickers" in d,
                "has_ticker_360_context": "ticker_360_context" in d,
            }
        else:
            rep["flow_confluence"] = {"error": "empty ticker_map"}
    except Exception as e:
        rep["flow_confluence"] = {"error": str(e)[:100]}
    rep["status"] = "COMPLETE"
    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    with open(OUT_PATH, "w") as f:
        json.dump(rep, f, indent=2)
    print(json.dumps(rep, indent=2)[:2500])

if __name__ == "__main__":
    main()
