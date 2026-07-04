"""ops 2802 — FULL-FLEET LLM CREDIT-BURN AUDIT.

For every LLM engine, cross-reference:
  - real CloudWatch invocations (24h + 7d) and errors (retry-storm signal)
  - code: LLM callsites, max_tokens, model/tier, per-item LOOP detection, retry loops,
    large-input hint (json.dumps of feeds into the prompt)
  - config: schedule frequency, on-demand Function URL
  - current per-engine daily cap (SSM)
Then estimate daily LLM calls / tokens / USD, flag risk categories, and RANK by burn.
Writes data/llm-cost-audit.json for the dashboard + prints the ranked report.
"""
import os, io, json, subprocess, re
from datetime import datetime, timezone, timedelta
import boto3

REGION = "us-east-1"
cw = boto3.client("cloudwatch", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION)
ssm = boto3.client("ssm", region_name=REGION)
now = datetime.now(timezone.utc)
R = {"ops": 2802, "ts": now.isoformat()}

PRICE = {"sonnet": (3.0, 15.0), "opus": (5.0, 25.0), "haiku": (0.8, 4.0), "reason": (0.6, 2.2)}

try:
    engines = subprocess.check_output(
        "grep -rlE 'import (anthropic_shim|llm_router)|api\\.anthropic\\.com|api\\.z\\.ai' "
        "aws/lambdas/*/source/*.py 2>/dev/null | awk -F/ '{print $3}' | sort -u", shell=True).decode().split()
except Exception:
    engines = []

try:
    caps = json.loads(ssm.get_parameter(Name="/justhodl/llm/engine-daily-cap")["Parameter"]["Value"])
except Exception:
    caps = {}

def cw_sum(fn, metric, days):
    try:
        r = cw.get_metric_statistics(Namespace="AWS/Lambda", MetricName=metric,
            Dimensions=[{"Name": "FunctionName", "Value": fn}],
            StartTime=now - timedelta(days=days), EndTime=now, Period=86400 * days, Statistics=["Sum"])
        return int(sum(p["Sum"] for p in r.get("Datapoints", [])))
    except Exception:
        return 0

def code_meta(d):
    try:
        src = subprocess.check_output("cat aws/lambdas/%s/source/*.py 2>/dev/null" % d, shell=True).decode()
    except Exception:
        return {}
    m = {}
    m["callsites"] = src.count("messages.create") + src.count("api.anthropic.com") + src.count(".complete(") + src.count("complete(")
    m["callsites"] = max(1, m["callsites"] // 2 or 1)
    mt = [int(x) for x in re.findall(r"max_tokens[\"']?[: =]+(\d+)", src)]
    m["max_tokens"] = max(mt) if mt else 800
    if "opus" in src:
        m["model"] = "opus"
    elif "sonnet" in src or 'tier="critical' in src or "tier='critical" in src:
        m["model"] = "sonnet"
    elif "z.ai" in src or 'tier="reason' in src or "tier='reason" in src or "glm-" in src:
        m["model"] = "reason"
    else:
        m["model"] = "haiku"
    # per-item loop: an LLM call textually inside a for-loop block
    m["loop"] = bool(re.search(r"for .+ in .+:(?:[^\n]*\n)+?\s+.*(messages\.create|\.complete\(|api\.anthropic)", src))
    m["retry"] = bool(re.search(r"for .*attempt.* in range|while .*(retry|attempt)", src))
    m["big_input"] = src.count("json.dumps") >= 3 or "read().decode" in src
    return m

def runs_per_day(d):
    cfg = "aws/lambdas/%s/config.json" % d
    try:
        c = json.load(open(cfg))
    except Exception:
        return None, False
    sched = (c.get("schedule") or {}).get("cron")
    onurl = bool(c.get("function_url") or c.get("url_config") or c.get("enable_url"))
    if not sched:
        return None, onurl
    try:
        if sched.startswith("rate("):
            n, u = re.findall(r"rate\((\d+) (\w+)", sched)[0]; n = int(n)
            return (1440 / n if "minute" in u else 24 / n if "hour" in u else 1 / n), onurl
        p = sched[5:-1].split(); mi, h = p[0], p[1]
        if mi.startswith("*/"): return 1440 / int(mi[2:]), onurl
        if h == "*": return 24.0, onurl
        if "/" in h: return 24 / int(h.split("/")[1]), onurl
        if "," in h: return float(len(h.split(","))), onurl
        return 1.0, onurl
    except Exception:
        return 1.0, onurl

rows = []
for d in engines:
    inv24 = cw_sum(d, "Invocations", 1)
    inv7 = cw_sum(d, "Invocations", 7)
    err7 = cw_sum(d, "Errors", 7)
    m = code_meta(d)
    rpd, onurl = runs_per_day(d)
    # daily LLM-call estimate: prefer real 24h invocations, else 7d/7, else schedule
    base = inv24 or (inv7 / 7.0 if inv7 else (rpd or 0))
    loop_mult = 8 if m.get("loop") else 1  # unknown loop size -> conservative x8
    daily_calls = base * m.get("callsites", 1) * loop_mult
    pin, pout = PRICE.get(m.get("model", "haiku"), (0.8, 4.0))
    in_est = 8000 if m.get("big_input") else 1500       # rough input tokens/call
    out_est = min(m.get("max_tokens", 800), 4000)        # assume avg output ~ up to cap/4000
    daily_cost = daily_calls * (in_est / 1e6 * pin + out_est / 1e6 * pout)
    flags = []
    if base >= 50: flags.append("HIGH_FREQ")
    if m.get("max_tokens", 0) > 6000: flags.append("BIG_TOKENS")
    if m.get("model") in ("sonnet", "opus"): flags.append(m["model"].upper())
    if onurl and d not in caps: flags.append("ONDEMAND_UNCAPPED")
    if m.get("loop"): flags.append("LOOP")
    if m.get("retry") or err7 > 200: flags.append("RETRY_RISK")
    if d not in caps: flags.append("UNCAPPED")
    rows.append({"engine": d, "inv_24h": inv24, "inv_7d": inv7, "err_7d": err7,
                 "callsites": m.get("callsites"), "max_tokens": m.get("max_tokens"),
                 "model": m.get("model"), "loop": m.get("loop"), "on_demand": onurl,
                 "runs_per_day_sched": rpd, "capped": caps.get(d),
                 "est_daily_calls": round(daily_calls, 1), "est_daily_cost_usd": round(daily_cost, 2),
                 "risk_flags": flags})

rows.sort(key=lambda x: -x["est_daily_cost_usd"])
for i, r in enumerate(rows):
    r["rank"] = i + 1
R["engines_audited"] = len(rows)
R["top_burn_risk"] = rows[:20]
R["est_total_daily_cost_uncapped"] = round(sum(r["est_daily_cost_usd"] for r in rows), 2)
R["uncapped_high_risk"] = [r["engine"] for r in rows if r["est_daily_cost_usd"] >= 1.0 and not r["capped"]]
R["generated_at"] = now.isoformat()

# write for the dashboard page
try:
    boto3.client("s3", region_name=REGION).put_object(
        Bucket="justhodl-dashboard-live", Key="data/llm-cost-audit.json",
        Body=json.dumps({"generated_at": now.isoformat(), "engines": rows,
                         "est_total_daily_cost_uncapped": R["est_total_daily_cost_uncapped"],
                         "caps": caps}, default=str).encode(),
        ContentType="application/json", CacheControl="max-age=300")
    R["dashboard_json"] = "data/llm-cost-audit.json written"
except Exception as e:
    R["dashboard_json"] = "ERR " + str(e)[:80]

print("=== TOP 15 BURN-RISK ENGINES ($/day est, uncapped assumption) ===")
for r in rows[:15]:
    print("  #%-2d $%-6s calls/d=%-7s inv24=%-5d x%d @%-6s tok=%-5d %s %s" % (
        r["rank"], r["est_daily_cost_usd"], r["est_daily_calls"], r["inv_24h"], r["callsites"],
        r["model"], r["max_tokens"], r["engine"], ",".join(r["risk_flags"])))
print("\nuncapped high-risk (>=$1/day):", R["uncapped_high_risk"])
print("est total daily (uncapped):", R["est_total_daily_cost_uncapped"], "| hard cap in place: $15")
os.makedirs("aws/ops/reports", exist_ok=True)
json.dump(R, open("aws/ops/reports/2802_burn_audit.json", "w"), indent=1, default=str)
print("OPS 2802 COMPLETE")
