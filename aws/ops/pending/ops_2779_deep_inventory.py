"""ops 2779 — DEEP options data inventory for hub enhancement + visuals.
Dumps full nested structures (by_strike profiles, vanna/charm, skew, dix history,
opex calendar, per-name boards) so the enhanced page renders real fields.
Read-only. Report: 2779_deep_inventory.json.
"""
import os, json
from datetime import datetime, timezone
import boto3
from botocore.exceptions import ClientError
s3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
R = {"ops": 2779, "ts": datetime.now(timezone.utc).isoformat(), "dump": {}}
def rd(k):
    return json.loads(s3.get_object(Bucket=BUCKET, Key=k)["Body"].read())
def keys(o, n=20): return list(o)[:n] if isinstance(o, dict) else "list[%d]" % len(o)

out = R["dump"]
# 1) dealer-gex full underlying + composite + squeeze + by_strike schema
try:
    d = rd("data/dealer-gex.json")
    u = (d.get("underlyings") or {})
    spy = u.get("SPY") or (list(u.values())[0] if u else {})
    o = {"underlying_all_keys": list(spy)}
    for k, v in spy.items():
        if isinstance(v, list) and v and isinstance(v[0], dict):
            o["arr_%s_schema" % k] = list(v[0]); o["arr_%s_len" % k] = len(v); o["arr_%s_sample" % k] = v[:3]
        elif isinstance(v, dict):
            o["obj_%s" % k] = {kk: str(vv)[:30] for kk, vv in list(v.items())[:12]}
        else:
            o.setdefault("scalars", {})[k] = str(v)[:40]
    o["market_composite"] = d.get("market_composite")
    sc = d.get("squeeze_candidates") or []
    o["squeeze_candidates_schema"] = list(sc[0]) if sc and isinstance(sc[0], dict) else sc[:3]
    o["squeeze_candidates_n"] = len(sc)
    out["dealer-gex"] = o
    print("dealer-gex SPY keys:", list(spy))
    print("  by_strike schema:", o.get("arr_by_strike_schema"), "len:", o.get("arr_by_strike_len"))
    print("  by_expiry schema:", o.get("arr_by_expiry_schema"))
    print("  market_composite keys:", keys(d.get("market_composite") or {}))
    print("  squeeze_candidates:", o["squeeze_candidates_n"], o["squeeze_candidates_schema"])
except Exception as e:
    out["dealer-gex"] = {"err": str(e)[:120]}; print("dealer-gex err", str(e)[:100])

# 2) options-gamma by_strike (the GEX profile) + by_expiry
try:
    g = rd("data/options-gamma.json")
    bs = g.get("by_strike"); be = g.get("by_expiry")
    o = {"top": list(g), "spot": g.get("spot"), "zero_gamma_strike": g.get("zero_gamma_strike"),
         "regime": g.get("regime"), "total_gex": g.get("total_gex")}
    if isinstance(bs, dict):
        it = list(bs.items()); o["by_strike_type"] = "dict"; o["by_strike_n"] = len(it)
        o["by_strike_item_schema"] = list(it[0][1]) if it and isinstance(it[0][1], dict) else "scalar"
        o["by_strike_sample"] = dict(it[:4])
    elif isinstance(bs, list):
        o["by_strike_type"] = "list"; o["by_strike_n"] = len(bs); o["by_strike_schema"] = list(bs[0]) if bs and isinstance(bs[0], dict) else "?"; o["by_strike_sample"] = bs[:4]
    if isinstance(be, (list, dict)):
        o["by_expiry_type"] = type(be).__name__; o["by_expiry_sample"] = (be[:4] if isinstance(be, list) else dict(list(be.items())[:4]))
    out["options-gamma"] = o
    print("options-gamma by_strike:", o.get("by_strike_type"), "n=", o.get("by_strike_n"), "schema=", o.get("by_strike_item_schema") or o.get("by_strike_schema"))
except Exception as e:
    out["options-gamma"] = {"err": str(e)[:120]}; print("options-gamma err", str(e)[:100])

# 3) options-analytics board (per-name rich)
try:
    a = rd("data/options-analytics.json")
    o = {"top": list(a)}
    for bk in ("board", "top_picks", "squeeze_setups", "most_unusual"):
        v = a.get(bk)
        if isinstance(v, list) and v and isinstance(v[0], dict):
            o["%s_schema" % bk] = list(v[0]); o["%s_n" % bk] = len(v); o["%s_sample" % bk] = v[0]
    out["options-analytics"] = o
    print("options-analytics board schema:", o.get("board_schema"), "n:", o.get("board_n"))
except Exception as e:
    out["options-analytics"] = {"err": str(e)[:120]}; print("options-analytics err", str(e)[:100])

# 4) dix full + history
try:
    dx = rd("data/dix.json")
    o = {"top": list(dx), "current": dx.get("current"), "moving_averages": dx.get("moving_averages"),
         "statistics": dx.get("statistics"), "day_over_day": dx.get("day_over_day"),
         "sustained_signals": dx.get("sustained_signals"),
         "dix_regime": dx.get("dix_regime"), "gex_regime": dx.get("gex_regime"), "combined_regime": dx.get("combined_regime")}
    out["dix"] = o
    print("dix current:", dx.get("current"), "| MAs:", keys(dx.get("moving_averages") or {}))
    try:
        h = rd("data/dix-history.json")
        arr = h if isinstance(h, list) else (h.get("history") or h.get("series") or list(h.values())[0] if isinstance(h, dict) else [])
        o["history_type"] = type(h).__name__; o["history_n"] = len(arr) if hasattr(arr, "__len__") else "?"
        o["history_sample"] = arr[:2] if isinstance(arr, list) else dict(list(h.items())[:2])
        print("  dix-history:", o["history_type"], "n=", o.get("history_n"))
    except Exception as e:
        o["history_err"] = str(e)[:60]
except Exception as e:
    out["dix"] = {"err": str(e)[:120]}; print("dix err", str(e)[:100])

# 5) opex detail
try:
    op = rd("data/opex-calendar.json")
    out["opex"] = {"top": list(op), "forward_expectations": op.get("forward_expectations"),
                   "current_readings": op.get("current_readings"), "recommended_trade": op.get("recommended_trade"),
                   "historical_backtest": (list(op.get("historical_backtest").keys()) if isinstance(op.get("historical_backtest"), dict) else op.get("historical_backtest")),
                   "calendar_sample": (op.get("calendar")[:3] if isinstance(op.get("calendar"), list) else op.get("calendar")),
                   "trigger_conditions": op.get("trigger_conditions")}
    print("opex forward_expectations:", keys(op.get("forward_expectations") or {}), "| current_readings:", keys(op.get("current_readings") or {}))
except Exception as e:
    out["opex"] = {"err": str(e)[:120]}; print("opex err", str(e)[:100])

# 6) dealer-gex history for GEX time-series
try:
    gh = rd("data/dealer-gex-history.json")
    arr = gh if isinstance(gh, list) else (gh.get("history") or gh.get("snapshots") or [])
    out["dealer-gex-history"] = {"type": type(gh).__name__, "n": len(arr) if hasattr(arr, "__len__") else "?",
                                 "sample": (arr[:1] if isinstance(arr, list) else dict(list(gh.items())[:2]))}
    print("dealer-gex-history:", out["dealer-gex-history"]["type"], "n=", out["dealer-gex-history"]["n"])
except Exception as e:
    out["dealer-gex-history"] = {"err": str(e)[:80]}

# 7) extra options-derived feeds
for f in ("catalyst-skew-premove", "earnings-iv-crush", "volatility-squeeze"):
    try:
        d = rd("data/%s.json" % f)
        o = {"top": list(d) if isinstance(d, dict) else "list[%d]" % len(d), "generated_at": (d.get("generated_at") if isinstance(d, dict) else None)}
        if isinstance(d, dict):
            for k, v in d.items():
                if isinstance(v, list) and v and isinstance(v[0], dict):
                    o["arr_%s_schema" % k] = list(v[0]); o["arr_%s_n" % k] = len(v); o["arr_%s_sample" % k] = v[0]; break
        out[f] = o
        print("%s top:" % f, o["top"])
    except Exception as e:
        out[f] = {"err": str(e)[:80]}; print("%s err %s" % (f, str(e)[:60]))

os.makedirs("aws/ops/reports", exist_ok=True)
json.dump(R, open("aws/ops/reports/2779_deep_inventory.json", "w"), indent=1, default=str)
print("OPS 2779 COMPLETE")
