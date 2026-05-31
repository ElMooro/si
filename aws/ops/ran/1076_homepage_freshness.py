#!/usr/bin/env python3
"""1076 — check freshness of all homepage data files.

Some files may be stale (frozen with $0 / null data) because the producer
Lambda ran during the FRED 429 storm and won't re-run until its next
scheduled invocation. This identifies them so we can trigger refreshes.
"""
import json, os, pathlib, time
from datetime import datetime, timezone, timedelta
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1076_homepage_freshness.json"
REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"

s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION,
                    config=Config(read_timeout=180))

# Homepage data files mapped to their producing Lambda
HOMEPAGE_FILES = {
    "cot/extremes/current.json":           "cftc-futures-positioning-agent",  # probably
    "crypto-intel.json":                   "justhodl-crypto-intel",
    "data/signal-board.json":              "justhodl-signal-board",
    "divergence/current.json":             "justhodl-divergence-engine-v2",
    "edge-data.json":                      "justhodl-edge-engine",
    "flow-data.json":                      "justhodl-options-flow",
    "intelligence-report.json":            "justhodl-morning-intelligence",
    "liquidity-data.json":                 "justhodl-liquidity-agent",
    "opportunities/asymmetric-equity.json": "justhodl-opportunities",
    "portfolio/pnl-daily.json":            "justhodl-portfolio-snapshot",
    "regime/current.json":                 "justhodl-regime-engine",
    "risk/recommendations.json":           "justhodl-risk-engine",
}


def main():
    out = {"started": datetime.now(timezone.utc).isoformat(), "files": []}
    
    now = datetime.now(timezone.utc)
    
    for key, producer in HOMEPAGE_FILES.items():
        entry = {"key": key, "producer": producer}
        try:
            obj = s3.get_object(Bucket=BUCKET, Key=key)
            body = obj["Body"].read()
            last_mod = obj["LastModified"]
            age = now - last_mod
            
            entry["size_kb"]       = round(len(body) / 1024, 1)
            entry["last_modified"] = last_mod.isoformat()
            entry["age_hours"]     = round(age.total_seconds() / 3600, 1)
            entry["age_days"]      = round(age.total_seconds() / 86400, 1)
            
            # Detect "all zero" / "all null" — common stale-data signature
            try:
                d = json.loads(body)
                
                # Heuristics for zero/null detection
                content_str = json.dumps(d)
                zero_count = content_str.count(': 0,') + content_str.count(': 0}') + content_str.count(': null')
                total_values = content_str.count(':')
                entry["null_or_zero_ratio"] = round(zero_count / max(total_values, 1), 2)
                
                # Top-level meta if present
                if isinstance(d, dict):
                    meta = d.get("meta") or {}
                    entry["meta_generated_at"] = meta.get("generated_at")
                    entry["top_keys"] = list(d.keys())[:8]
            except Exception:
                entry["parse_err"] = True
            
            # Flag if stale: >24h for cron, >2h for hourly, etc.
            entry["is_stale"] = age > timedelta(hours=26)
            
        except s3.exceptions.NoSuchKey:
            entry["err"] = "NoSuchKey"
        except Exception as e:
            entry["err"] = str(e)[:200]
        
        out["files"].append(entry)
    
    # Find which producers exist as Lambdas (some may have different names)
    print("[1076] resolving producer Lambdas…")
    paginator = lam.get_paginator("list_functions")
    all_lambdas = set()
    for page in paginator.paginate():
        for f in page["Functions"]:
            all_lambdas.add(f["FunctionName"])
    
    for entry in out["files"]:
        prod = entry.get("producer")
        if prod in all_lambdas:
            entry["producer_exists"] = True
        else:
            entry["producer_exists"] = False
            # Try fuzzy match
            matches = [L for L in all_lambdas if prod.replace("justhodl-", "") in L]
            entry["fuzzy_candidates"] = matches[:5]
    
    out["finished"] = datetime.now(timezone.utc).isoformat()
    pathlib.Path(os.path.dirname(REPORT)).mkdir(parents=True, exist_ok=True)
    pathlib.Path(REPORT).write_text(json.dumps(out, indent=2, default=str))
    print(f"[1076] DONE — {len(out['files'])} files checked")


if __name__ == "__main__":
    main()
