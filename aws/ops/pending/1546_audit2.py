# ops 1546 — verify external audit v2 claims against live AWS
import json, time, boto3
from datetime import datetime, timezone
from botocore.config import Config
cfg = Config(read_timeout=300, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
logs = boto3.client("logs", region_name="us-east-1", config=cfg)
ev = boto3.client("events", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)
B = "justhodl-dashboard-live"
out = {"ops": 1546}
now = datetime.now(timezone.utc)


def age_h(key):
    try:
        h = s3.head_object(Bucket=B, Key=key)
        return round((now - h["LastModified"]).total_seconds() / 3600, 1)
    except Exception:
        return None

CHECK = ["data/carry-surface.json", "data/carry.json", "data/vol-surface.json", "data/volatility.json",
         "data/sector-rotation.json", "data/best-setups.json", "data/_skill/frontrun-skill-index.json",
         "data/alert-backtests.json", "data/auction-crisis.json", "data/eurodollar-stress.json",
         "data/historical-analogs.json", "data/backtest-summary.json"]
out["ages_h"] = {k: age_h(k) for k in CHECK}

# briefs sweep — every key containing 'brief'
briefs = []
for page in s3.get_paginator("list_objects_v2").paginate(Bucket=B, Prefix="data/"):
    for o in page.get("Contents", []):
        if "brief" in o["Key"].lower():
            briefs.append((o["Key"], round((now - o["LastModified"]).total_seconds() / 3600, 1)))
briefs.sort(key=lambda x: -x[1])
out["briefs_stalest_15"] = briefs[:15]
out["briefs_n"] = len(briefs)

# best-setups bond_vol_regime type (the [object Object] suspect)
try:
    bsj = json.loads(s3.get_object(Bucket=B, Key="data/best-setups.json")["Body"].read())
    bvr = bsj.get("bond_vol_regime")
    out["bond_vol_regime"] = {"type": type(bvr).__name__, "value": (bvr if not isinstance(bvr, dict) else {k: bvr[k] for k in list(bvr)[:4]})}
except Exception as e:
    out["bond_vol_regime"] = str(e)[:80]

# skill index sample
try:
    sk = json.loads(s3.get_object(Bucket=B, Key="data/_skill/frontrun-skill-index.json")["Body"].read())
    engines = sk.get("engines") or sk.get("per_engine") or {}
    items = list(engines.items())[:5] if isinstance(engines, dict) else engines[:5]
    out["skill_sample"] = [{"k": (k if isinstance(engines, dict) else v.get('engine')),
                            "hit": (v.get("hit_rate") if isinstance(v, dict) else None),
                            "n": (v.get("n") or v.get("n_graded") if isinstance(v, dict) else None)}
                           for k, v in (items if isinstance(engines, dict) else [(None, x) for x in items])]
except Exception as e:
    out["skill_sample"] = str(e)[:90]

# rule states via lambda resource policy
def rules_for(fn):
    res = []
    try:
        pol = json.loads(lam.get_policy(FunctionName=fn)["Policy"])
        for st in pol.get("Statement", []):
            arn = (((st.get("Condition") or {}).get("ArnLike") or {}).get("AWS:SourceArn")) or ""
            if ":rule/" in arn:
                rn = arn.split(":rule/")[-1]
                try:
                    d = ev.describe_rule(Name=rn)
                    res.append({"rule": rn, "state": d.get("State"), "sched": d.get("ScheduleExpression")})
                except Exception as e2:
                    res.append({"rule": rn, "state": f"DESCRIBE_FAIL {str(e2)[:40]}"})
    except Exception as e:
        res.append({"err": str(e)[:60]})
    return res

for fn in ("justhodl-sector-rotation", "justhodl-ai-brief-router", "justhodl-ai-brief",
           "justhodl-alpha-daily-brief", "justhodl-auction-crisis-detector",
           "justhodl-carry-surface", "justhodl-vol-surface", "justhodl-fleet-freshness-monitor"):
    out.setdefault("rules", {})[fn] = rules_for(fn)

# freshness monitor env presence + manifest
try:
    c = lam.get_function_configuration(FunctionName="justhodl-fleet-freshness-monitor")
    env = (c.get("Environment") or {}).get("Variables", {})
    out["monitor_env"] = {k: ("SET" if env.get(k) else "MISSING") for k in
                          ("TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID", "SNS_ARN", "DEFAULT_MAX_AGE_H", "ALERT_RATIO")}
except Exception as e:
    out["monitor_env"] = str(e)[:80]
for mk in ("data/_admin/freshness-manifest.json", "data/_freshness/manifest.json", "config/freshness-manifest.json"):
    try:
        m = json.loads(s3.get_object(Bucket=B, Key=mk)["Body"].read())
        out["manifest"] = {"key": mk, "rules": m.get("rules"), "n_overrides": len(m.get("key_overrides", {}) or {}),
                           "sample_overrides": dict(list((m.get("key_overrides") or {}).items())[:5])}
        break
    except Exception:
        continue
out.setdefault("manifest", "NOT FOUND at known keys")

# CW: last START + ERROR counts (48h)
start48 = int((time.time() - 48 * 3600) * 1000)
def cw(fn):
    g = f"/aws/lambda/{fn}"
    try:
        st = logs.filter_log_events(logGroupName=g, startTime=start48, filterPattern='"START RequestId"', limit=50)
        starts = [e["timestamp"] for e in st.get("events", [])]
        er = logs.filter_log_events(logGroupName=g, startTime=start48, filterPattern='?ERROR ?"Task timed out"', limit=20)
        return {"n_starts_48h": len(starts),
                "last_start": datetime.fromtimestamp(max(starts)/1000, tz=timezone.utc).isoformat() if starts else None,
                "n_err_48h": len(er.get("events", [])),
                "err_sample": [e["message"][:110].strip() for e in er.get("events", [])[:2]]}
    except Exception as e:
        return str(e)[:80]
for fn in ("justhodl-sector-rotation", "justhodl-ai-brief-router", "justhodl-carry-surface",
           "justhodl-vol-surface", "justhodl-fleet-freshness-monitor"):
    out.setdefault("cw", {})[fn] = cw(fn)

# monitor's own last verdict lines
try:
    r = logs.filter_log_events(logGroupName="/aws/lambda/justhodl-fleet-freshness-monitor",
                               startTime=start48, filterPattern='?STALE ?stale ?alert', limit=12)
    out["monitor_lines"] = [e["message"][:130].strip() for e in r.get("events", [])][-8:]
except Exception as e:
    out["monitor_lines"] = str(e)[:80]

# kick sector-rotation now
try:
    r = lam.invoke(FunctionName="justhodl-sector-rotation", InvocationType="RequestResponse", Payload=b"{}")
    out["sector_invoke"] = {"err": r.get("FunctionError", "NONE")}
    time.sleep(2)
    out["sector_invoke"]["age_after_h"] = age_h("data/sector-rotation.json")
except Exception as e:
    out["sector_invoke"] = str(e)[:100]

open("aws/ops/reports/1546_audit2.json", "w").write(json.dumps(out, indent=2, default=str))
print(json.dumps({"ages": out["ages_h"], "bvr": out["bond_vol_regime"],
                  "mon_env": out["monitor_env"], "sector": out["sector_invoke"]}, default=str)[:800])
