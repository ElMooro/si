#!/usr/bin/env python3
"""
Step 193 — Sample unsurfaced S3 data to inform new pages.

Step 192's CloudWatch log scan was narrow (most Lambdas don't print
S3 keys). Better approach: sample S3 keys directly and figure out
which surfaced data is highest-value to build pages for.

Targets (identified as gaps):
  - _health/dashboard.json + _health/last_alerted.json
    → System Health page
  - investor-analysis/ contents
    → Watchlist Debate / Legendary Investor output
  - data/cftc-all-cache.json (used by positioning?)
  - data/khalid-metrics.json (used by /khalid/)
  - reports/scorecard.json (used by /reports.html?)
  - sentiment/, secretary/, learning/ contents
"""
import json
from ops_report import report
import boto3

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"

s3 = boto3.client("s3", region_name=REGION)

KEYS_TO_SAMPLE = [
    "_health/dashboard.json",
    "_health/last_alerted.json",
    "data/khalid-metrics.json",
    "data/khalid-config.json",
    "data/khalid-analysis.json",
    "reports/scorecard.json",
    "sentiment/news.json",
    "secretary/findings.json",
    "learning/prompt_templates.json",
    "learning/improvement_log.json",
]

PREFIXES_TO_LIST = [
    "investor-analysis/",
    "calibration/",
    "_audit/",
    "deploy/",
    "stock-ai/",
    "stock-analysis/",
    "khalid/",
    "secretary/",
    "sentiment/",
    "telegram/",
]


def summarize(obj, prefix="", max_depth=2, depth=0):
    out = []
    if isinstance(obj, dict):
        for k in list(obj.keys())[:25]:
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
                    sample = json.dumps(v[:3])[:80] if v else "[]"
                    out.append(f"{prefix}{k}: list[{len(v)}] {sample}")
            else:
                vs = json.dumps(v)[:80] if v is not None else "null"
                out.append(f"{prefix}{k}: {vs}")
    return out


with report("sample_unsurfaced_data") as r:
    r.heading("Sample unsurfaced S3 data")

    # ─── A. Sample known files ─────────────────────────────────────────
    r.section("A. Direct file samples")
    for key in KEYS_TO_SAMPLE:
        r.section(f"📄 {key}")
        try:
            obj = s3.get_object(Bucket=BUCKET, Key=key)
            sz = obj["ContentLength"]
            mod = obj["LastModified"].strftime("%Y-%m-%d %H:%M")
            r.log(f"  size={sz}B  mod={mod}")
            data = json.loads(obj["Body"].read())
            for line in summarize(data, "  ", max_depth=2)[:30]:
                r.log(line)
        except s3.exceptions.NoSuchKey:
            r.warn(f"  ⚠ does not exist")
        except Exception as e:
            r.warn(f"  err: {e}")

    # ─── B. List + sample contents of prefixes ─────────────────────────
    r.section("B. Prefix contents")
    for prefix in PREFIXES_TO_LIST:
        r.section(f"📁 {prefix}")
        try:
            resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix, MaxKeys=20)
            objs = resp.get("Contents", [])
            r.log(f"  {len(objs)} objects")
            for o in sorted(objs, key=lambda x: x["LastModified"], reverse=True)[:10]:
                from datetime import datetime, timezone
                age_h = (datetime.now(timezone.utc) - o["LastModified"]).total_seconds() / 3600
                r.log(f"    {o['Key']:55} {o['Size']:>8}B  {age_h:6.1f}h")
            # Sample first/most-recent json
            jsons = [o for o in objs if o["Key"].endswith(".json")]
            if jsons:
                latest = sorted(jsons, key=lambda x: x["LastModified"], reverse=True)[0]
                try:
                    obj = s3.get_object(Bucket=BUCKET, Key=latest["Key"])
                    data = json.loads(obj["Body"].read())
                    r.log(f"\n  Sample: {latest['Key']}")
                    for line in summarize(data, "    ", max_depth=2)[:15]:
                        r.log(line)
                except Exception as e:
                    r.log(f"    sample err: {e}")
        except Exception as e:
            r.warn(f"  err: {e}")

    r.log("Done")
