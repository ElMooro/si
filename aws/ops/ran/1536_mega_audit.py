# ops 1536 — institutional mega-audit: every past promise vs deployed reality + health sweep
import json, time, urllib.request, ssl, boto3
from datetime import datetime, timezone, timedelta
from botocore.config import Config
cfg = Config(read_timeout=120, retries={"max_attempts": 2})
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)
ev = boto3.client("events", region_name="us-east-1", config=cfg)
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
cw = boto3.client("cloudwatch", region_name="us-east-1", config=cfg)
ssm = boto3.client("ssm", region_name="us-east-1", config=cfg)
dd = boto3.client("dynamodb", region_name="us-east-1", config=cfg)
B = "justhodl-dashboard-live"
now = datetime.now(timezone.utc)
out = {"ops": 1536, "ts": now.isoformat()}
_ctx = ssl.create_default_context()

def rd(key, n=300_000):
    try:
        o = s3.get_object(Bucket=B, Key=key); b = o["Body"].read()
        return json.loads(b if len(b) < n else b[:n])
    except Exception as e:
        return {"_err": str(e)[:70]}

# ── 1. full top-level brief freshness ──
keys = {}
tok = None
while True:
    kw = {"Bucket": B, "Prefix": "data/", "MaxKeys": 1000}
    if tok: kw["ContinuationToken"] = tok
    r = s3.list_objects_v2(**kw)
    for o in r.get("Contents", []):
        k = o["Key"]
        if k.count("/") == 1 and k.endswith(".json"):
            keys[k] = round((now - o["LastModified"]).total_seconds() / 3600, 1)
    if not r.get("IsTruncated"): break
    tok = r.get("NextContinuationToken")
out["n_briefs"] = len(keys)
out["stale_72h"] = sorted([(k.split("/")[1], v) for k, v in keys.items() if v > 72], key=lambda x: -x[1])[:30]

def age(name): return keys.get(f"data/{name}", None)

# ── 2. promise checklist ──
chk = {}
# A realistic backtest
bt = {k.split("/")[1]: v for k, v in keys.items() if "backtest" in k or "walk" in k}
chk["backtest_briefs"] = bt
wf = rd("data/walk-forward-backtest.json") if "data/walk-forward-backtest.json" in keys else rd("data/backtest-results.json")
if isinstance(wf, dict) and not wf.get("_err"):
    chk["backtest_sample"] = {k: wf.get(k) for k in list(wf.keys())[:14]}
# B analogs
ha = rd("data/historical-analogs.json")
chk["historical_analogs"] = {"age_h": age("historical-analogs.json"),
                             "keys": list(ha.keys())[:14] if isinstance(ha, dict) and not ha.get("_err") else ha}
if isinstance(ha, dict):
    for f in ("method", "methodology", "n_analogs", "state_vector", "current_state", "analogs", "matches"):
        if f in ha:
            v = ha[f]
            chk["historical_analogs"][f] = (v[:2] if isinstance(v, list) else (str(v)[:200] if not isinstance(v, dict) else {kk: v[kk] for kk in list(v)[:6]}))
# C alerts
al = rd("data/alert-history.json")
chk["alert_history"] = {"age_h": age("alert-history.json"), "keys": list(al.keys())[:12] if isinstance(al, dict) and not al.get("_err") else al}
ar = {k.split("/")[1]: v for k, v in keys.items() if "alert" in k}
chk["alert_briefs"] = ar
# D pnl attribution + vintage
chk["pnl_attribution_age_h"] = age("pnl-attribution.json")
chk["desk_returns_age_h"] = age("desk-returns.json")
vint = [k for k in keys if "vintage" in k or "alfred" in k]
try:
    vr = s3.list_objects_v2(Bucket=B, Prefix="data/vintage", MaxKeys=5)
    vint += [o["Key"] for o in vr.get("Contents", [])]
    vr2 = s3.list_objects_v2(Bucket=B, Prefix="data/alfred", MaxKeys=5)
    vint += [o["Key"] for o in vr2.get("Contents", [])]
except Exception: pass
chk["vintage_keys"] = sorted(set(vint))[:8]
# E vol stack
chk["vol_surface_age_h"] = age("vol-surface.json")
chk["opex_age_h"] = age("opex-calendar.json")
chk["vix_curve_age_h"] = age("vix-curve.json")
# F scorecard regime-conditioning
sc = rd("data/signal-scorecard.json")
if isinstance(sc, dict) and not sc.get("_err"):
    rows = sc.get("scorecard") or []
    chk["scorecard"] = {"age_h": age("signal-scorecard.json"), "n_rows": len(rows),
                        "has_by_regime": any("by_regime" in r or "regime" in json.dumps(list(r.keys())) for r in rows[:5]),
                        "apex_in_scorecard": any("apex" in str(r.get("signal_type", "")).lower() for r in rows)}
# G kill switch
try:
    p = ssm.get_parameter(Name="/justhodl/kill-switch")
    chk["kill_switch"] = p["Parameter"]["Value"]
except Exception as e:
    chk["kill_switch"] = f"MISSING ({type(e).__name__})"
# H apex closed-loop: outcomes appearing?
try:
    sc1 = dd.scan(TableName="justhodl-signals", Limit=60,
                  FilterExpression="#s = :v", ExpressionAttributeNames={"#s": "source"},
                  ExpressionAttributeValues={":v": {"S": "apex-fusion"}})
    items = sc1.get("Items", [])
    chk["apex_ddb"] = {"n_seen": len(items),
                       "with_outcomes": sum(1 for i in items if "outcomes" in i or "accuracy_scores" in i),
                       "sample_id": items[0]["signal_id"]["S"] if items else None}
except Exception as e:
    chk["apex_ddb"] = str(e)[:90]
# I todays engines
chk["global_tide_age_h"] = age("global-tide.json")
chk["apex_fusion_age_h"] = age("apex-fusion.json")
sw = rd("data/smart-wake.json")
chk["smart_wake"] = {"age_h": age("smart-wake.json"), "mode": sw.get("mode"), "woken": sw.get("woken_rules"), "stress": sw.get("stress")}
chk["ecb_derived_age_h"] = age("ecb-derived.json")
chk["cascade_validation_age_h"] = age("cascade-validation-log.json")
chk["opportunity_calibration_age_h"] = age("opportunity-calibration.json")
out["checklist"] = chk

# ── 3. EB state ──
rules = []
tok = None
while True:
    kw = {"Limit": 100}
    if tok: kw["NextToken"] = tok
    r = ev.list_rules(**kw)
    rules += r["Rules"]; tok = r.get("NextToken")
    if not tok: break
dis = [r["Name"] for r in rules if r["State"] == "DISABLED"]
out["eb"] = {"total": len(rules), "disabled": len(dis)}

# ── 4. CW errors 48h on critical fns ──
errs = {}
for fn in ("justhodl-daily-report-v3", "justhodl-ai-chat", "justhodl-ecb-derived", "justhodl-apex-fusion",
           "justhodl-global-tide", "justhodl-smart-wake", "justhodl-signal-board", "justhodl-master-ranker",
           "justhodl-pnl-attribution", "justhodl-historical-analogs", "justhodl-alert-router", "justhodl-backtest-engine"):
    try:
        m = cw.get_metric_statistics(Namespace="AWS/Lambda", MetricName="Errors",
                                     Dimensions=[{"Name": "FunctionName", "Value": fn}],
                                     StartTime=now - timedelta(hours=48), EndTime=now, Period=172800, Statistics=["Sum"])
        s_ = sum(p["Sum"] for p in m["Datapoints"])
        inv = cw.get_metric_statistics(Namespace="AWS/Lambda", MetricName="Invocations",
                                       Dimensions=[{"Name": "FunctionName", "Value": fn}],
                                       StartTime=now - timedelta(hours=48), EndTime=now, Period=172800, Statistics=["Sum"])
        i_ = sum(p["Sum"] for p in inv["Datapoints"])
        errs[fn] = {"err": int(s_), "inv": int(i_)}
    except Exception as e:
        errs[fn] = str(e)[:60]
out["cw_48h"] = errs

# ── 5. page HTTP from runner ──
pages = {}
for pg in ("", "ecb.html", "apex.html", "global-tide.html", "signal-board.html", "master-board.html",
           "risk-desk.html", "squeeze.html", "retail-edges.html", "best-ideas.html", "skill.html",
           "alerts.html", "analogs.html", "backtest.html", "pnl-attribution.html", "fundamentals.html"):
    try:
        r = urllib.request.urlopen(f"https://justhodl.ai/{pg}", timeout=15, context=_ctx)
        pages[pg or "index"] = r.status
    except Exception as e:
        pages[pg or "index"] = str(e)[:40]
out["pages"] = pages

# ── 6. SPY depth probe (for analog/alert backtests) ──
try:
    u = ("https://api.polygon.io/v2/aggs/ticker/SPY/range/1/day/1999-01-01/1999-03-01"
         "?adjusted=true&sort=asc&limit=50&apiKey=zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d")
    j = json.loads(urllib.request.urlopen(u, timeout=20, context=_ctx).read())
    out["spy_1999"] = {"n": j.get("resultsCount"), "first_ts": (j.get("results") or [{}])[0].get("t")}
except Exception as e:
    out["spy_1999"] = str(e)[:90]

open("aws/ops/reports/1536_mega.json", "w").write(json.dumps(out, indent=2, default=str))
print(json.dumps({"briefs": out["n_briefs"], "stale72": len(out["stale_72h"]), "eb_dis": out["eb"]["disabled"],
                  "analogs_age": chk["historical_analogs"].get("age_h"), "alerts_age": chk["alert_history"].get("age_h"),
                  "kill": chk["kill_switch"], "apex_ddb": chk["apex_ddb"]}, default=str))
