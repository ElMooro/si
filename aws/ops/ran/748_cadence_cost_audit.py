"""ops/748 — refresh-cadence & cost audit (READ-ONLY, no changes).

Lists every EventBridge schedule rule + target Lambda, pulls real
30-day CloudWatch invocations / duration / memory, flags Anthropic
callers, and estimates monthly Lambda cost. Feeds the cadence
right-sizing plan.
"""
import json, os, re
from datetime import datetime, timezone, timedelta
import boto3
from botocore.config import Config

cfg = Config(retries={"max_attempts": 4})
events = boto3.client("events", region_name="us-east-1", config=cfg)
cw = boto3.client("cloudwatch", region_name="us-east-1", config=cfg)
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)

report = {"ops": 748, "ts": datetime.now(timezone.utc).isoformat(),
          "subject": "refresh-cadence & cost audit (read-only)"}

REQ_COST = 0.20 / 1_000_000          # $ per request
GBS_COST = 0.0000166667              # $ per GB-second
now = datetime.now(timezone.utc)
start = now - timedelta(days=30)


def metric(fn, name, stat):
    try:
        r = cw.get_metric_statistics(
            Namespace="AWS/Lambda", MetricName=name,
            Dimensions=[{"Name": "FunctionName", "Value": fn}],
            StartTime=start, EndTime=now, Period=2592000, Statistics=[stat])
        dp = r.get("Datapoints") or []
        return dp[0][stat] if dp else 0
    except Exception:
        return None


# ── 1. all EventBridge schedule rules + targets ──
rules = []
paginator = events.get_paginator("list_rules")
for page in paginator.paginate():
    for r in page.get("Rules", []):
        sched = r.get("ScheduleExpression")
        if not sched:
            continue  # not a scheduled rule
        targets = []
        try:
            tr = events.list_targets_by_rule(Rule=r["Name"])
            for t in tr.get("Targets", []):
                arn = t.get("Arn", "")
                if ":function:" in arn:
                    targets.append(arn.split(":function:")[-1].split("/")[0])
        except Exception:
            pass
        rules.append({"rule": r["Name"], "schedule": sched,
                      "state": r.get("State"), "targets": targets})

# ── 2. per-Lambda real cost from CloudWatch ──
sched_lambdas = sorted({t for r in rules for t in r["targets"]})
lam_data = {}
for fn in sched_lambdas:
    inv = metric(fn, "Invocations", "Sum") or 0
    dur = metric(fn, "Duration", "Average") or 0
    mem = 128
    has_anthropic = False
    try:
        c = lam.get_function_configuration(FunctionName=fn)
        mem = c.get("MemorySize", 128)
        env = (c.get("Environment") or {}).get("Variables") or {}
        has_anthropic = any("ANTHROPIC" in k for k in env)
    except Exception:
        pass
    gb_s = inv * (dur / 1000.0) * (mem / 1024.0)
    cost = inv * REQ_COST + gb_s * GBS_COST
    lam_data[fn] = {
        "invocations_30d": int(inv),
        "fires_per_day": round(inv / 30.0, 1),
        "avg_duration_ms": round(dur, 1),
        "memory_mb": mem,
        "gb_seconds_30d": round(gb_s, 1),
        "est_monthly_cost_usd": round(cost, 4),
        "calls_anthropic": has_anthropic,
    }

# ── 3. assemble ──
for r in rules:
    r["lambda_detail"] = {t: lam_data.get(t) for t in r["targets"]}
    # actual daily fire rate from the busiest target
    rates = [lam_data[t]["fires_per_day"] for t in r["targets"] if t in lam_data]
    r["observed_fires_per_day"] = max(rates) if rates else None

rules.sort(key=lambda x: x.get("observed_fires_per_day") or 0, reverse=True)

total_cost = round(sum(v["est_monthly_cost_usd"] for v in lam_data.values()), 2)
total_inv = sum(v["invocations_30d"] for v in lam_data.values())
anthropic_lambdas = sorted(fn for fn, v in lam_data.items()
                           if v["calls_anthropic"])

# sub-hourly / high-frequency scheduled rules = the over-refresh suspects
high_freq = [{"rule": r["rule"], "schedule": r["schedule"],
              "targets": r["targets"],
              "fires_per_day": r["observed_fires_per_day"]}
             for r in rules
             if (r.get("observed_fires_per_day") or 0) >= 12]

report["fleet"] = {
    "scheduled_rules": len(rules),
    "scheduled_lambdas": len(sched_lambdas),
    "total_invocations_30d": total_inv,
    "est_total_monthly_lambda_cost_usd": total_cost,
    "anthropic_calling_lambdas": anthropic_lambdas,
}
report["high_frequency_rules"] = high_freq
report["all_rules"] = rules
report["lambda_cost_detail"] = dict(
    sorted(lam_data.items(),
           key=lambda kv: kv[1]["est_monthly_cost_usd"], reverse=True))

print(json.dumps(report, indent=2, default=str))
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/748_cadence_cost_audit.json", "w") as f:
    json.dump(report, f, indent=2, default=str)
print("[ok] wrote aws/ops/reports/748_cadence_cost_audit.json")
