"""ops 2802 (v2) — FULL-FLEET LLM CREDIT-BURN AUDIT (safe line-based loop detection)."""
import os, json, subprocess, re
from datetime import datetime, timezone, timedelta
import boto3

REGION = "us-east-1"
cw = boto3.client("cloudwatch", region_name=REGION)
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

def detect_loop(lines):
    for i, ln in enumerate(lines):
        s = ln.strip()
        if s.startswith("for ") and " in " in s and s.endswith(":"):
            indent = len(ln) - len(ln.lstrip())
            for j in range(i + 1, min(i + 45, len(lines))):
                bl = lines[j]
                if not bl.strip():
                    continue
                if (len(bl) - len(bl.lstrip())) <= indent:
                    break
                if "messages.create" in bl or ".complete(" in bl or "api.anthropic" in bl:
                    return True
    return False

def code_meta(d):
    try:
        src = subprocess.check_output("cat aws/lambdas/%s/source/*.py 2>/dev/null" % d, shell=True).decode()
    except Exception:
        return {"callsites": 1, "max_tokens": 800, "model": "haiku", "loop": False, "retry": False, "big_input": False}
    lines = src.split("\n")
    calls = src.count("messages.create") + src.count("api.anthropic.com") + src.count(".complete(")
    mt = [int(x) for x in re.findall(r"max_tokens[\"']?[: =]+(\d+)", src)]
    if "opus" in src:
        model = "opus"
    elif "sonnet" in src or 'tier="critical' in src or "tier='critical" in src:
        model = "sonnet"
    elif "z.ai" in src or 'tier="reason' in src or "tier='reason" in src or "glm-" in src:
        model = "reason"
    else:
        model = "haiku"
    return {"callsites": max(1, calls), "max_tokens": (max(mt) if mt else 800), "model": model,
            "loop": detect_loop(lines), "retry": ("for attempt in range" in src or "while retry" in src),
            "big_input": (src.count("json.dumps") >= 3)}

def runs_per_day(d):
    try:
        c = json.load(open("aws/lambdas/%s/config.json" % d))
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
    try:
        inv24 = cw_sum(d, "Invocations", 1)
        inv7 = cw_sum(d, "Invocations", 7)
        err7 = cw_sum(d, "Errors", 7)
        m = code_meta(d)
        rpd, onurl = runs_per_day(d)
        base = inv24 or (inv7 / 7.0 if inv7 else (rpd or 0))
        loop_mult = 8 if m["loop"] else 1
        daily_calls = base * m["callsites"] * loop_mult
        pin, pout = PRICE.get(m["model"], (0.8, 4.0))
        in_est = 8000 if m["big_input"] else 1500
        out_est = min(m["max_tokens"], 4000)
        daily_cost = daily_calls * (in_est / 1e6 * pin + out_est / 1e6 * pout)
        flags = []
        if base >= 50: flags.append("HIGH_FREQ")
        if m["max_tokens"] > 6000: flags.append("BIG_TOKENS")
        if m["model"] in ("sonnet", "opus"): flags.append(m["model"].upper())
        if onurl and d not in caps: flags.append("ONDEMAND_UNCAPPED")
        if m["loop"]: flags.append("LOOP")
        if m["retry"] or err7 > 200: flags.append("RETRY_RISK")
        if d not in caps: flags.append("UNCAPPED")
        rows.append({"engine": d, "inv_24h": inv24, "inv_7d": inv7, "err_7d": err7,
                     "callsites": m["callsites"], "max_tokens": m["max_tokens"], "model": m["model"],
                     "loop": m["loop"], "on_demand": onurl, "runs_per_day_sched": rpd, "capped": caps.get(d),
                     "est_daily_calls": round(daily_calls, 1), "est_daily_cost_usd": round(daily_cost, 2),
                     "risk_flags": flags})
    except Exception as e:
        rows.append({"engine": d, "err": str(e)[:80], "est_daily_cost_usd": 0, "risk_flags": ["AUDIT_ERR"]})

rows.sort(key=lambda x: -x.get("est_daily_cost_usd", 0))
for i, r in enumerate(rows):
    r["rank"] = i + 1
R["engines_audited"] = len(rows)
R["top_burn_risk"] = rows[:20]
R["est_total_daily_cost_uncapped"] = round(sum(r.get("est_daily_cost_usd", 0) for r in rows), 2)
R["uncapped_high_risk"] = [r["engine"] for r in rows if r.get("est_daily_cost_usd", 0) >= 1.0 and not r.get("capped")]
try:
    boto3.client("s3", region_name=REGION).put_object(
        Bucket="justhodl-dashboard-live", Key="data/llm-cost-audit.json",
        Body=json.dumps({"generated_at": now.isoformat(), "engines": rows,
                         "est_total_daily_cost_uncapped": R["est_total_daily_cost_uncapped"], "caps": caps}, default=str).encode(),
        ContentType="application/json", CacheControl="max-age=300")
    R["dashboard_json"] = "written"
except Exception as e:
    R["dashboard_json"] = "ERR " + str(e)[:80]
print("=== TOP 15 BURN-RISK ===")
for r in rows[:15]:
    print("  #%-2d $%-6s calls/d=%-7s inv24=%-5d x%s @%-6s tok=%-5s %s %s" % (
        r["rank"], r.get("est_daily_cost_usd"), r.get("est_daily_calls"), r.get("inv_24h", 0),
        r.get("callsites"), r.get("model"), r.get("max_tokens"), r["engine"], ",".join(r.get("risk_flags", []))))
print("\nuncapped high-risk:", R["uncapped_high_risk"])
os.makedirs("aws/ops/reports", exist_ok=True)
json.dump(R, open("aws/ops/reports/2802_burn_audit.json", "w"), indent=1, default=str)
print("OPS 2802 COMPLETE")
