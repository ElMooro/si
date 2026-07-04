"""ops 2800 — verify per-engine daily caps live end-to-end."""
import os, sys, json, time
from datetime import datetime, timezone
import boto3
sys.path.insert(0, "aws/shared")
R = {"ops": 2800, "ts": datetime.now(timezone.utc).isoformat()}
lam = boto3.client("lambda", region_name="us-east-1")
ssm = boto3.client("ssm", region_name="us-east-1")
events = boto3.client("events", region_name="us-east-1")
now = time.time()
# 1) redeploy freshness of the 4 capped engines
fresh = {}
for fn in ["justhodl-equity-research", "justhodl-ticker-deep-research",
           "justhodl-research-critique", "justhodl-news-wire"]:
    try:
        lm = lam.get_function_configuration(FunctionName=fn)["LastModified"]
        t = datetime.strptime(lm.split(".")[0], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc).timestamp()
        fresh[fn] = round((now - t) / 60, 1)
    except Exception as e:
        fresh[fn] = "err"
R["redeploy_age_min"] = fresh
# 2) SSM cap param
R["engine_caps"] = json.loads(ssm.get_parameter(Name="/justhodl/llm/engine-daily-cap")["Parameter"]["Value"])
# 3) news-wire schedule now daily?
try:
    rule = events.describe_rule(Name="justhodl-news-wire-schedule")
    R["news_wire_cron"] = rule.get("ScheduleExpression")
except Exception:
    for rn in ["justhodl-news-wire", "justhodl-news-wire-daily", "justhodl-news-wire-hourly"]:
        try:
            R["news_wire_cron"] = events.describe_rule(Name=rn).get("ScheduleExpression"); break
        except Exception:
            continue
# 4) exercise within_daily_cap against real config
import llm_cost as lc
lc._cfg["t"] = 0.0  # force refresh from SSM
cfg = lc._config()
R["config_engine_caps"] = cfg.get("engine_caps")
# simulate: equity-research with 0 vs 1 calls today
lc._cap_cache = {"t": 9e18, "n": {"justhodl-equity-research": 0}}
under = lc.within_daily_cap("justhodl-equity-research")
lc._cap_cache["n"]["justhodl-equity-research"] = 1
over = lc.within_daily_cap("justhodl-equity-research")
R["cap_logic"] = {"at_0_calls_allowed": under, "at_1_call_blocked": (not over)}
recent = [v for v in fresh.values() if isinstance(v, (int, float)) and v < 40]
R["status"] = "CAPS LIVE" if (len(recent) >= 3 and R["engine_caps"].get("justhodl-equity-research") == 1
                              and under and not over) else "CHECK"
print("redeploy ages:", json.dumps(fresh))
print("engine_caps (SSM):", json.dumps(R["engine_caps"]))
print("config picked up caps:", json.dumps(R["config_engine_caps"]))
print("news_wire_cron:", R.get("news_wire_cron"))
print("cap_logic:", json.dumps(R["cap_logic"]))
print("STATUS:", R["status"])
os.makedirs("aws/ops/reports", exist_ok=True)
json.dump(R, open("aws/ops/reports/2800_caps_verify.json", "w"), indent=1, default=str)
print("OPS 2800 COMPLETE")
