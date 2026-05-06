#!/usr/bin/env python3
"""
Step 187 — Sample S3 JSON shapes for landing page rebuild.

Need to know exact field names + nesting before writing the new
index.html so I get accessors right on the first try (avoid the
broken-AAPL-parser pattern from step 178).

Pull all the data sources the new landing page will use, print
top-level keys + sample values for each.
"""
import json
from ops_report import report
import boto3

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"

s3 = boto3.client("s3", region_name=REGION)

KEYS = [
    "intelligence-report.json",
    "edge-data.json",
    "liquidity-data.json",
    "flow-data.json",
    "crypto-intel.json",
    "regime/current.json",
    "divergence/current.json",
    "cot/extremes/current.json",
    "risk/recommendations.json",
    "opportunities/asymmetric-equity.json",
    "portfolio/pnl-daily.json",
    "screener/data.json",
    "valuations.json",
]


def summarize(obj, prefix="", max_depth=2, depth=0):
    """Print structure summary with sample values."""
    out = []
    if isinstance(obj, dict):
        for k in list(obj.keys())[:30]:
            v = obj[k]
            if isinstance(v, dict):
                out.append(f"{prefix}{k}: dict({len(v)} keys)")
                if depth < max_depth:
                    out.extend(summarize(v, prefix + "  ", max_depth, depth+1))
            elif isinstance(v, list):
                if v and isinstance(v[0], dict):
                    out.append(f"{prefix}{k}: list[{len(v)} dicts]")
                    if depth < max_depth and v:
                        out.append(f"{prefix}  [0]:")
                        out.extend(summarize(v[0], prefix + "    ", max_depth, depth+2))
                else:
                    sample = json.dumps(v[:3])[:100] if v else "[]"
                    out.append(f"{prefix}{k}: list[{len(v)}] {sample}")
            else:
                vs = json.dumps(v)[:80] if v is not None else "null"
                out.append(f"{prefix}{k}: {vs}")
    return out


with report("sample_landing_data") as r:
    r.heading("Sample S3 data shapes for new index.html")

    for key in KEYS:
        r.section(key)
        try:
            obj = s3.get_object(Bucket=BUCKET, Key=key)
            sz = obj["ContentLength"]
            mod = obj["LastModified"].strftime("%Y-%m-%d %H:%M")
            r.log(f"  size={sz}B  mod={mod}")
            data = json.loads(obj["Body"].read())
            for line in summarize(data, "  ", max_depth=2)[:50]:
                r.log(line)
        except Exception as e:
            r.warn(f"  err: {e}")

    r.log("Done")
