#!/usr/bin/env python3
"""Step 316 — Comprehensive end-state audit for the session.

Checks every Lambda + EB rule + S3 output for the 4 new systems:
  1. justhodl-divergence-engine-v2  (Sprint patch — 71 pairs, 100% cov)
  2. justhodl-divergence-interpreter (Phase B — Claude regime synthesis)
  3. justhodl-sector-tilt           (Sprint 5 — regime → 11 SPDR tilts)
  4. justhodl-pairs-scanner         (Sprint 6 — 37 stat-arb pairs)
  5. justhodl-morning-brief-tg     (Phase C+D — Telegram digest)

For each:
  - Lambda exists + state
  - Last modified (recent = post-patch)
  - EB rule exists + schedule
  - S3 output age (fresh = system actively producing)
  - Recent CloudWatch invocations from Lambda metrics
"""
import json
import os
from datetime import datetime, timezone, timedelta

import boto3
from botocore.exceptions import ClientError

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
REPORT = "aws/ops/reports/316_end_state_audit.json"

lam = boto3.client("lambda", region_name=REGION)
events = boto3.client("events", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)
cw = boto3.client("cloudwatch", region_name=REGION)


# Spec: (lambda_name, expected_eb_rule, s3_output_key, expected_max_age_h)
SYSTEMS = [
    ("justhodl-divergence-engine-v2", "divergence-v2-2hourly",
     "data/divergence-v2.json", 2.5),
    ("justhodl-divergence-interpreter", "divergence-interpreter-4hourly",
     "data/divergence-interpreted.json", 4.5),
    ("justhodl-sector-tilt", "sector-tilt-4hourly",
     "data/sector-tilt.json", 4.5),
    ("justhodl-pairs-scanner", "pairs-scanner-6hourly",
     "data/pairs-scanner.json", 6.5),
    ("justhodl-morning-brief-tg", None,  # daily ET, schedule varies
     None, None),
]


def check_lambda(name):
    try:
        cfg = lam.get_function_configuration(FunctionName=name)
        return {
            "exists": True,
            "state": cfg.get("State"),
            "last_modified": cfg.get("LastModified"),
            "runtime": cfg.get("Runtime"),
            "memory_mb": cfg.get("MemorySize"),
            "timeout_s": cfg.get("Timeout"),
        }
    except ClientError:
        return {"exists": False}


def check_rules_for_lambda(fname):
    arn = f"arn:aws:lambda:{REGION}:857687956942:function:{fname}"
    try:
        names = events.list_rule_names_by_target(TargetArn=arn).get("RuleNames", [])
        out = []
        for n in names:
            r = events.describe_rule(Name=n)
            out.append({
                "name": n,
                "schedule": r.get("ScheduleExpression"),
                "state": r.get("State"),
            })
        return out
    except Exception as e:
        return [{"err": str(e)[:200]}]


def check_s3(key):
    if not key:
        return None
    try:
        obj = s3.head_object(Bucket=BUCKET, Key=key)
        age_h = (datetime.now(timezone.utc) - obj["LastModified"]).total_seconds() / 3600
        return {
            "exists": True,
            "size_kb": round(obj["ContentLength"] / 1024, 1),
            "age_hours": round(age_h, 2),
            "last_modified": obj["LastModified"].isoformat(),
        }
    except ClientError:
        return {"exists": False}


def check_invocations_24h(fname):
    """Get number of invocations in last 24h from CloudWatch metric."""
    try:
        end = datetime.now(timezone.utc)
        start = end - timedelta(hours=24)
        resp = cw.get_metric_statistics(
            Namespace="AWS/Lambda",
            MetricName="Invocations",
            Dimensions=[{"Name": "FunctionName", "Value": fname}],
            StartTime=start, EndTime=end,
            Period=3600,
            Statistics=["Sum"],
        )
        points = resp.get("Datapoints", [])
        total = sum(p.get("Sum", 0) for p in points)
        return {
            "invocations_24h": int(total),
            "n_data_points": len(points),
        }
    except Exception as e:
        return {"err": str(e)[:200]}


def main():
    out = {"started": datetime.now(timezone.utc).isoformat(), "systems": {}}

    for fname, eb_rule, s3_key, max_age_h in SYSTEMS:
        sys_out = {
            "lambda": check_lambda(fname),
            "rules": check_rules_for_lambda(fname),
            "s3_output": check_s3(s3_key),
            "invocations_24h": check_invocations_24h(fname),
            "expected_max_age_h": max_age_h,
        }
        # Verdict
        verdict = []
        if not sys_out["lambda"].get("exists"):
            verdict.append("LAMBDA_MISSING")
        if eb_rule and not any(r.get("name") == eb_rule for r in sys_out["rules"]):
            verdict.append(f"RULE_MISSING_{eb_rule}")
        if s3_key:
            if not sys_out["s3_output"].get("exists"):
                verdict.append("OUTPUT_MISSING")
            elif max_age_h and sys_out["s3_output"].get("age_hours", 999) > max_age_h * 1.5:
                verdict.append(f"OUTPUT_STALE_{sys_out['s3_output'].get('age_hours')}h")
        if not verdict:
            verdict = ["HEALTHY"]
        sys_out["verdict"] = verdict
        out["systems"][fname] = sys_out

    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f:
        json.dump(out, f, indent=2, default=str)

    # Pretty summary
    print()
    print("═" * 80)
    print("  END-STATE AUDIT — All 5 production systems")
    print("═" * 80)
    for fname, info in out["systems"].items():
        l = info["lambda"]
        rules = info["rules"]
        s3o = info["s3_output"]
        inv = info["invocations_24h"]
        verdict = info["verdict"]
        verdict_icon = "✅" if verdict == ["HEALTHY"] else "⚠️"
        print()
        print(f"  {verdict_icon} {fname}")
        print(f"     Lambda:    state={l.get('state','?')}  mem={l.get('memory_mb','?')}MB  timeout={l.get('timeout_s','?')}s")
        print(f"     Modified:  {l.get('last_modified','?')[:19] if l.get('last_modified') else '?'}")
        for r in rules:
            if r.get("name"):
                print(f"     Rule:      {r['name']:<32s} {r.get('schedule','?'):<22s} {r.get('state','?')}")
        if s3o:
            if s3o.get("exists"):
                age = s3o.get("age_hours")
                expected = info.get("expected_max_age_h")
                age_icon = "✅" if (expected and age and age <= expected) else "⚠️"
                print(f"     S3 output: {age_icon} {s3o.get('size_kb','?')}KB  age={age}h (expected ≤{expected}h)")
        print(f"     CloudWatch: {inv.get('invocations_24h','?')} invocations in last 24h")
        print(f"     Verdict:   {verdict}")

    # Aggregate health
    healthy = sum(1 for s in out["systems"].values() if s["verdict"] == ["HEALTHY"])
    total = len(out["systems"])
    print()
    print("═" * 80)
    print(f"  OVERALL: {healthy}/{total} systems HEALTHY")
    print("═" * 80)


if __name__ == "__main__":
    main()
