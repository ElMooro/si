#!/usr/bin/env python3
"""Step 320 — Schema sniff for inputs the earnings-whisper Lambda will consume.

Inputs:
  data/earnings-tracker.json  (calendar — upcoming earnings)
  data/insider-trades.json    (per-ticker insider activity)
  data/news-sentiment.json    (per-ticker sentiment)
  data/8k-filings.json        (recent material 8-Ks)
  data/options-flow.json      (unusual options activity)
"""
import json
import os
from datetime import datetime, timezone

import boto3

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
KEYS = [
    "data/earnings-tracker.json",
    "data/insider-trades.json",
    "data/news-sentiment.json",
    "data/8k-filings.json",
    "data/options-flow.json",
    "data/redflag-alerts.json",
]
REPORT = "aws/ops/reports/320_earnings_whisper_schemas.json"

s3 = boto3.client("s3", region_name=REGION)


def main():
    out = {"started": datetime.now(timezone.utc).isoformat(), "files": {}}
    for key in KEYS:
        try:
            obj = s3.get_object(Bucket=BUCKET, Key=key)
            data = json.loads(obj["Body"].read())
            entry = {
                "size_kb": round(obj["ContentLength"] / 1024, 1),
                "age_h": round((datetime.now(timezone.utc) - obj["LastModified"]).total_seconds() / 3600, 2),
            }
            if isinstance(data, dict):
                entry["top_keys"] = list(data.keys())[:25]
                # Find per-ticker lists
                ticker_lists = {}
                for k, v in data.items():
                    if isinstance(v, list) and v and isinstance(v[0], dict):
                        sample = v[0]
                        for tf in ("ticker", "symbol", "sym"):
                            if tf in sample:
                                ticker_lists[k] = {
                                    "len": len(v),
                                    "ticker_field": tf,
                                    "sample_keys": list(sample.keys())[:18],
                                    "first_full": sample,
                                }
                                break
                entry["ticker_lists"] = ticker_lists
            elif isinstance(data, list):
                entry["is_list"] = True
                entry["len"] = len(data)
                if data and isinstance(data[0], dict):
                    entry["sample_keys"] = list(data[0].keys())[:18]
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
