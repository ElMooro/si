"""ops 2795 — FORENSICS: rank LLM engines by real CloudWatch invocations over the
burn window (7d) to find what drained Anthropic/Z.ai. Cross with per-run callsites
and max_tokens to estimate relative token cost.
"""
import os, json, subprocess
from datetime import datetime, timezone, timedelta
import boto3

REGION = "us-east-1"
cw = boto3.client("cloudwatch", region_name=REGION)
R = {"ops": 2795, "ts": datetime.now(timezone.utc).isoformat()}

# discover LLM engines + their per-run callsites & max_tokens from the repo
try:
    engines = subprocess.check_output(
        "grep -rlE 'import (anthropic_shim|llm_router)|api\\.anthropic\\.com|api\\.z\\.ai' "
        "aws/lambdas/*/source/*.py 2>/dev/null | awk -F/ '{print $3}' | sort -u", shell=True).decode().split()
except Exception:
    engines = []

def code_meta(d):
    try:
        src = subprocess.check_output("cat aws/lambdas/%s/source/*.py 2>/dev/null" % d, shell=True).decode()
    except Exception:
        return 1, 0, "?"
    calls = src.count("messages.create") + src.count("api.anthropic.com") + src.count("complete(")
    import re
    mt = [int(x) for x in re.findall(r"max_tokens[\"']?[: =]+(\d+)", src)]
    mx = max(mt) if mt else 0
    model = "sonnet" if ("sonnet" in src or "opus" in src or 'tier="critical' in src) else ("reason" if 'tier="reason' in src or "z.ai" in src else "haiku")
    return max(calls, 1), mx, model

now = datetime.now(timezone.utc)
def inv(fn, days):
    try:
        r = cw.get_metric_statistics(
            Namespace="AWS/Lambda", MetricName="Invocations",
            Dimensions=[{"Name": "FunctionName", "Value": fn}],
            StartTime=now - timedelta(days=days), EndTime=now, Period=86400 * days, Statistics=["Sum"])
        return int(sum(p["Sum"] for p in r.get("Datapoints", [])))
    except Exception:
        return -1

rows = []
for d in engines:
    i7 = inv(d, 7)
    if i7 <= 0:
        continue
    i1 = inv(d, 1)
    calls, mx, model = code_meta(d)
    price_out = {"sonnet": 15.0, "reason": 2.2, "haiku": 4.0}.get(model, 4.0)
    # rough per-week token cost estimate: invocations * callsites * max_tokens(out) * price
    est = i7 * calls * (mx or 800) / 1e6 * price_out
    rows.append({"engine": d, "inv_7d": i7, "inv_24h": i1, "calls_per_run": calls,
                 "max_tok": mx, "model": model, "est_7d_out_cost_usd": round(est, 2)})

rows.sort(key=lambda x: -x["est_7d_out_cost_usd"])
R["ranked_by_est_cost"] = rows[:20]
R["top_by_invocations"] = sorted(rows, key=lambda x: -x["inv_7d"])[:12]
R["total_llm_invocations_7d"] = sum(x["inv_7d"] for x in rows)
R["engines_active"] = len(rows)

print("active LLM engines (7d):", len(rows), "| total invocations:", R["total_llm_invocations_7d"])
print("\n== TOP BY ESTIMATED OUTPUT COST (7d) ==")
for x in rows[:12]:
    print("  $%-7s inv7d=%-6d inv24h=%-5d x%d @%s max_tok=%d  %s" % (
        x["est_7d_out_cost_usd"], x["inv_7d"], x["inv_24h"], x["calls_per_run"], x["model"], x["max_tok"], x["engine"]))
print("\n== TOP BY RAW INVOCATIONS (7d) ==")
for x in R["top_by_invocations"]:
    print("  inv7d=%-6d inv24h=%-5d %s (%s)" % (x["inv_7d"], x["inv_24h"], x["engine"], x["model"]))

os.makedirs("aws/ops/reports", exist_ok=True)
json.dump(R, open("aws/ops/reports/2795_burn_forensics.json", "w"), indent=1, default=str)
print("\nOPS 2795 COMPLETE")
