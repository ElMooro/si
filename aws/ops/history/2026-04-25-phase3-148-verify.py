#!/usr/bin/env python3
"""
Step 148 — Re-invoke risk-sizer after the cluster KeyError fix from
the previous push (e583aee). The CI deploy-lambdas.yml workflow
auto-deploys aws/lambdas/*/source/** on push, so by the time this
script runs, the fixed code should be live.

Verifies:
  1. Lambda invokes cleanly without FunctionError
  2. Output reads sane on production
  3. EventBridge schedule still wired
"""
import json
import os
import time
from pathlib import Path

from ops_report import report
import boto3

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
lam = boto3.client("lambda", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)
events = boto3.client("events", region_name=REGION)

FNAME = "justhodl-risk-sizer"


with report("verify_risk_sizer_fix") as r:
    r.heading("Verify Phase 3 — risk-sizer post-fix")

    # ─── 1. Confirm CI deployed the fix ─────────────────────────────────
    r.section("1. Confirm Lambda is updated")
    cfg = lam.get_function_configuration(FunctionName=FNAME)
    last_modified = cfg.get("LastModified", "")
    r.log(f"  LastModified: {last_modified}")
    r.log(f"  CodeSha256: {cfg.get('CodeSha256','?')[:16]}...")

    # ─── 2. Invoke ──────────────────────────────────────────────────────
    r.section("2. Test invoke")
    invoke_start = time.time()
    resp = lam.invoke(FunctionName=FNAME, InvocationType="RequestResponse")
    elapsed = time.time() - invoke_start
    payload = resp.get("Payload").read().decode()
    if resp.get("FunctionError"):
        r.fail(f"  FunctionError ({elapsed:.1f}s): {payload[:1000]}")
        raise SystemExit(1)
    r.ok(f"  Invoked in {elapsed:.1f}s")

    outer = json.loads(payload)
    body = json.loads(outer.get("body", "{}"))
    r.log(f"\n  Response body:")
    for k, v in body.items():
        r.log(f"    {k:25} {v}")

    # ─── 3. Read full output ────────────────────────────────────────────
    r.section("3. Read risk/recommendations.json — full report")
    obj = s3.get_object(Bucket=BUCKET, Key="risk/recommendations.json")
    snap = json.loads(obj["Body"].read().decode("utf-8"))

    r.log(f"  Regime: {snap.get('regime')} (strength {snap.get('regime_strength')})")
    r.log(f"  Max gross exposure: {snap.get('max_gross_exposure_pct')}%")

    dd = snap.get("drawdown_status", {})
    r.log(f"  Drawdown: {dd.get('current_dd_pct')}% (peak: {dd.get('peak_date')})")
    r.log(f"  DD multiplier: ×{dd.get('size_multiplier')}  ({dd.get('active_trigger')})")

    s = snap.get("summary", {})
    r.log(f"  Candidate ideas: {s.get('n_candidate_ideas')}")
    r.log(f"  Clusters: {s.get('n_clusters')}")
    r.log(f"  Pre-cap signal sum: {s.get('total_pre_caps_pct')}%")
    r.log(f"  Total recommended (after caps): {s.get('total_recommended_size_pct')}%")

    if snap.get("warnings"):
        r.log(f"\n  Warnings ({len(snap['warnings'])}):")
        for w in snap["warnings"]:
            r.log(f"    [{w['level']:6}] {w['message']}")

    r.log(f"\n  Top 10 sized recommendations:")
    for rec in snap.get("sized_recommendations", [])[:10]:
        r.log(f"    {rec['symbol']:6} {rec.get('sector','?')[:14]:14} "
              f"size={rec.get('recommended_size_pct'):>5}%  "
              f"conv={rec.get('raw_conviction'):.3f}  "
              f"cluster={rec.get('cluster','?')[:18]:18}")
        if rec.get('reasoning'):
            r.log(f"      → {rec['reasoning'][:120]}")

    r.log(f"\n  Cluster summary (top 8 by size):")
    for c in snap.get("clusters", [])[:8]:
        r.log(f"    {c['id'][:25]:25} size={c['size']} avg_corr={c['avg_correlation']}")

    # ─── 4. Verify schedule is in place ─────────────────────────────────
    r.section("4. Verify EventBridge schedule")
    rule_name = "justhodl-risk-sizer-daily"
    try:
        rule = events.describe_rule(Name=rule_name)
        r.ok(f"  Rule: {rule['State']} {rule['ScheduleExpression']}")
    except Exception as e:
        r.warn(f"  Rule check: {e}")

    r.kv(
        regime=body.get("regime"),
        n_ideas=body.get("n_ideas"),
        n_clusters=body.get("n_clusters"),
        total_size_pct=body.get("total_size_pct"),
        n_warnings=body.get("n_warnings"),
        invoke_s=f"{elapsed:.1f}",
    )
    r.log("Done")
