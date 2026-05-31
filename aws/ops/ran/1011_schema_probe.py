#!/usr/bin/env python3
"""Step 1011 — Probe live S3 schemas for universe.json + master-ranker.json
+ get a fresh sample of justhodl-signals to confirm DDB metadata fields.

Used to inform the design of:
  - engine-signal-map (DDB → engine→signal_types)
  - miss-detector refinement (universe coverage check)
  - miss-calibrator (which signals have near-miss thresholds)
"""
import json, os
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/1011_schema_probe.json"
REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
s3 = boto3.client("s3", region_name=REGION)
ddb = boto3.resource("dynamodb", region_name=REGION)


def grab(key, byte_limit=4000):
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=key)
        body = obj["Body"].read().decode("utf-8", errors="replace")
        return {
            "size": obj["ContentLength"],
            "modified": str(obj["LastModified"]),
            "preview": body[:byte_limit],
            "parsed_keys": list(json.loads(body).keys()) if body else [],
        }
    except Exception as e:
        return {"err": str(e)[:200]}


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    
    out["universe.json"] = grab("data/universe.json")
    out["master-ranker.json"] = grab("data/master-ranker.json", byte_limit=3000)
    out["conviction.json"] = grab("data/conviction.json", byte_limit=4000)
    out["signal-scorecard.json"] = grab("data/signal-scorecard.json", byte_limit=3000)
    out["miss-summary.json"] = grab("data/miss-summary.json", byte_limit=2000)
    
    # Also peek 5 fresh signals from DDB to confirm metadata fields (source_engine?)
    try:
        table = ddb.Table("justhodl-signals")
        resp = table.scan(Limit=10)
        items = resp.get("Items", [])
        out["ddb_sample"] = []
        for it in items[:5]:
            out["ddb_sample"].append({
                "signal_id":    str(it.get("signal_id",""))[:30],
                "signal_type":  str(it.get("signal_type","")),
                "status":       it.get("status"),
                "metadata_keys": list((it.get("metadata") or {}).keys())[:10],
                "metadata_sample": dict(list((it.get("metadata") or {}).items())[:5]),
            })
    except Exception as e:
        out["ddb_err"] = str(e)[:200]
    
    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f:
        json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
