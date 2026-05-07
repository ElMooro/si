#!/usr/bin/env python3
"""Step 307 — Schema inspection for the Daily Alpha Scoreboard.

Pulls ONE per-ticker row from each major signal feed so the scoreboard
can correctly join + render the columns:
  - compound-signals.json (the unifier — primary key list of tickers)
  - asymmetric-scorer.json (asymmetric setups detail)
  - eps-revision-velocity.json (EPS revision per ticker)
  - insider-clusters.json (insider cluster per ticker)
  - smart-money-clusters.json (13F per ticker)
  - deep-value.json (Ben Graham screener per ticker)
"""
import json
import os
from datetime import datetime, timezone

import boto3

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
KEYS = [
    "data/compound-signals.json",
    "data/asymmetric-scorer.json",
    "data/eps-revision-velocity.json",
    "data/insider-clusters.json",
    "data/smart-money-clusters.json",
    "data/deep-value.json",
    "data/macro-nowcast.json",
    "data/sector-rotation.json",
]
REPORT = "aws/ops/reports/307_alpha_scoreboard_schemas.json"

s3 = boto3.client("s3", region_name=REGION)


def main():
    out = {"started": datetime.now(timezone.utc).isoformat(), "files": {}}
    for key in KEYS:
        try:
            obj = s3.get_object(Bucket=BUCKET, Key=key)
            data = json.loads(obj["Body"].read())
            entry = {
                "size_kb": round(obj["ContentLength"] / 1024, 1),
                "age_hours": round((datetime.now(timezone.utc) - obj["LastModified"]).total_seconds() / 3600, 1),
            }
            # Top-level keys
            if isinstance(data, dict):
                entry["top_keys"] = list(data.keys())[:20]
                # Find the main per-ticker list
                ticker_lists = []
                for k, v in data.items():
                    if isinstance(v, list) and v and isinstance(v[0], dict):
                        sample = v[0]
                        # Heuristic: per-ticker if has ticker/symbol field
                        ticker_field = None
                        for tf in ["ticker", "symbol", "sym"]:
                            if tf in sample:
                                ticker_field = tf
                                break
                        if ticker_field:
                            ticker_lists.append({
                                "key": k, "len": len(v),
                                "ticker_field": ticker_field,
                                "sample_keys": list(sample.keys())[:15],
                                "first_full": sample,
                            })
                entry["ticker_lists"] = ticker_lists[:3]
            elif isinstance(data, list):
                entry["is_list"] = True
                entry["len"] = len(data)
                if data and isinstance(data[0], dict):
                    entry["sample_keys"] = list(data[0].keys())[:15]
                    entry["first_full"] = data[0]
            out["files"][key] = entry
        except Exception as e:
            out["files"][key] = {"err": str(e)[:200]}
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f:
        json.dump(out, f, indent=2, default=str)
    print(json.dumps(out, indent=2, default=str)[:7000])


if __name__ == "__main__":
    main()
