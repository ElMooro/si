#!/usr/bin/env python3
"""
Step 198 — Probe every gap Lambda. Determine: zombie or real?

For each of 14 gap Lambdas + 5 likely-broken:
  A. Get function config (last modified, runtime, memory)
  B. Get last 24h CloudWatch invocations + error count
  C. Find any S3 keys associated (search by Lambda name pattern
     in S3 inventory + check known suspected paths)
  D. Test invoke with empty event to see what it does

Output: per-Lambda verdict —
  ALIVE-PRODUCES-DATA → build a page
  ALIVE-NO-OUTPUT     → fix or archive
  ZOMBIE              → archive
"""
import io, json, time, zipfile
from datetime import datetime, timezone, timedelta
from ops_report import report
import boto3
from botocore.exceptions import ClientError

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
lam = boto3.client("lambda", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)
cw = boto3.client("cloudwatch", region_name=REGION)


GAP_LAMBDAS = [
    # likely-broken (per memory)
    "justhodl-daily-macro-report",
    "justhodl-financial-secretary",
    "justhodl-news-sentiment",
    "justhodl-repo-monitor",
    "macro-financial-intelligence",
    # high-value
    "volatility-monitor-agent",
    "dollar-strength-agent",
    "fmp-stock-picks-agent",
    "bond-indices-agent",
    # medium-value
    "bea-economic-agent",
    "manufacturing-global-agent",
    "securities-banking-agent",
    "google-trends-agent",
    "news-sentiment-agent",
]

# Suspected S3 paths per Lambda (best guess based on naming)
SUSPECTED_S3_PATHS = {
    "justhodl-daily-macro-report":   ["macro-report.json", "data/macro-report.json", "reports/daily-macro.json"],
    "justhodl-financial-secretary":  ["secretary/findings.json", "secretary/latest.json", "data/secretary.json"],
    "justhodl-news-sentiment":       ["sentiment/news.json", "sentiment/latest.json", "news-sentiment.json"],
    "justhodl-repo-monitor":         ["repo-data.json", "repo/status.json"],
    "macro-financial-intelligence":  ["macro-intel.json", "data/macro-intel.json"],
    "volatility-monitor-agent":      ["volatility.json", "data/volatility.json", "vol/current.json"],
    "dollar-strength-agent":         ["dxy.json", "data/dxy.json", "dollar.json"],
    "fmp-stock-picks-agent":         ["stock-picks.json", "data/stock-picks.json", "picks/latest.json"],
    "bond-indices-agent":            ["bond-indices.json", "data/bond-indices.json", "bonds.json"],
    "bea-economic-agent":            ["bea.json", "data/bea.json"],
    "manufacturing-global-agent":    ["manufacturing.json", "data/manufacturing.json", "pmi.json"],
    "securities-banking-agent":      ["banking.json", "data/banking.json"],
    "google-trends-agent":           ["trends.json", "data/trends.json", "google-trends.json"],
    "news-sentiment-agent":          ["news-sentiment.json", "sentiment.json"],
}


with report("probe_gap_lambdas") as r:
    r.heading("Probe all 14 gap Lambdas — alive or zombie?")

    # ─── Get full S3 inventory ──────────────────────────────────────────
    r.section("A. Build S3 key index")
    s3_keys = {}
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET):
        for o in page.get("Contents", []):
            s3_keys[o["Key"]] = {
                "size": o["Size"], "mod": o["LastModified"],
                "age_h": (datetime.now(timezone.utc) - o["LastModified"]).total_seconds() / 3600,
            }
    r.log(f"  {len(s3_keys)} S3 keys total")

    # ─── Per-Lambda probe ───────────────────────────────────────────────
    verdicts = {}
    for name in GAP_LAMBDAS:
        r.section(f"🔍 {name}")
        verdict = {}

        # Function config
        try:
            cfg = lam.get_function_configuration(FunctionName=name)
            verdict["last_modified"] = cfg["LastModified"][:10]
            verdict["runtime"] = cfg.get("Runtime", "?")
            verdict["state"] = cfg.get("State", "?")
            r.log(f"  config: runtime={verdict['runtime']} mod={verdict['last_modified']} state={verdict['state']}")
        except ClientError as e:
            r.warn(f"  config fail: {e}")
            verdicts[name] = {"verdict": "MISSING", "reason": str(e)}
            continue

        # CloudWatch invocations last 24h
        try:
            now = datetime.now(timezone.utc)
            metrics = cw.get_metric_statistics(
                Namespace="AWS/Lambda",
                MetricName="Invocations",
                Dimensions=[{"Name":"FunctionName","Value":name}],
                StartTime=now - timedelta(days=2),
                EndTime=now,
                Period=86400,
                Statistics=["Sum"],
            )
            invocations_2d = sum(d.get("Sum",0) for d in metrics.get("Datapoints",[]))
            verdict["invocations_2d"] = int(invocations_2d)

            errors = cw.get_metric_statistics(
                Namespace="AWS/Lambda",
                MetricName="Errors",
                Dimensions=[{"Name":"FunctionName","Value":name}],
                StartTime=now - timedelta(days=2),
                EndTime=now,
                Period=86400,
                Statistics=["Sum"],
            )
            errors_2d = sum(d.get("Sum",0) for d in errors.get("Datapoints",[]))
            verdict["errors_2d"] = int(errors_2d)
            r.log(f"  invocations_2d: {int(invocations_2d)}  errors_2d: {int(errors_2d)}")
        except Exception as e:
            r.log(f"  metrics err: {e}")

        # Check suspected S3 paths
        suspected = SUSPECTED_S3_PATHS.get(name, [])
        found_keys = []
        for path in suspected:
            if path in s3_keys:
                found_keys.append((path, s3_keys[path]))

        # Also search for any keys containing the Lambda's distinctive token
        token = name.replace("justhodl-", "").replace("-", "_")
        token_alt = name.replace("justhodl-", "").split("-")[0]
        for k in s3_keys:
            kl = k.lower()
            if (token.lower() in kl or token_alt.lower() in kl) and not any(k == fk[0] for fk in found_keys):
                found_keys.append((k, s3_keys[k]))
                if len(found_keys) >= 5: break

        if found_keys:
            r.log(f"  S3 outputs found:")
            for k, m in sorted(found_keys, key=lambda x: x[1]["mod"], reverse=True)[:5]:
                mark = "🟢" if m["age_h"] < 24 else "🟡" if m["age_h"] < 168 else "🔴"
                r.log(f"    {mark} {k:50} {m['size']:>9}B  {m['age_h']:>6.1f}h ago")
            verdict["s3_keys"] = [(k, m["age_h"]) for k, m in found_keys[:5]]
        else:
            r.log(f"  ⚠ no S3 outputs found")
            verdict["s3_keys"] = []

        # Final verdict
        if verdict.get("invocations_2d", 0) == 0:
            verdict["verdict"] = "ZOMBIE"
            verdict["reason"] = "0 invocations in 2 days"
        elif verdict.get("errors_2d", 0) > 0 and not found_keys:
            verdict["verdict"] = "BROKEN"
            verdict["reason"] = f"{verdict.get('errors_2d')} errors in 2d, no S3 output"
        elif found_keys and any(m[1]["age_h"] < 48 for m in found_keys):
            verdict["verdict"] = "ALIVE-PRODUCES-DATA"
            verdict["reason"] = "fresh S3 output found"
        elif verdict.get("invocations_2d", 0) > 0:
            verdict["verdict"] = "ALIVE-NO-OUTPUT"
            verdict["reason"] = "running but no fresh S3 output"
        else:
            verdict["verdict"] = "UNCLEAR"
            verdict["reason"] = "needs manual investigation"

        r.log(f"  ▸ {verdict['verdict']}: {verdict['reason']}")
        verdicts[name] = verdict

    # ─── Summary ────────────────────────────────────────────────────────
    r.section("FINAL VERDICTS")
    by_verdict = {}
    for name, v in verdicts.items():
        by_verdict.setdefault(v["verdict"], []).append(name)
    for verdict in ["ALIVE-PRODUCES-DATA", "ALIVE-NO-OUTPUT", "BROKEN", "ZOMBIE", "MISSING", "UNCLEAR"]:
        names = by_verdict.get(verdict, [])
        if not names: continue
        r.log(f"\n  {verdict} ({len(names)}):")
        for n in sorted(names):
            v = verdicts[n]
            r.log(f"    {n:42} inv2d={v.get('invocations_2d','?')} err2d={v.get('errors_2d','?')}")
            if v.get("s3_keys"):
                for k, age_h in v["s3_keys"][:2]:
                    r.log(f"      └ {k} ({age_h:.1f}h)")

    r.log("Done")
