"""
justhodl-alpha-decay  ·  v1.0  —  THE OUT-OF-SAMPLE WATCHDOG
================================================================================
Only 3 of ~67 graded signals are ALPHA_PROVEN. This engine guards exactly those
— and catches any engine whose edge is fading BEFORE it costs money. Three jobs:

  1. PROVEN-ENGINE GUARD — are the 3 ALPHA_PROVEN engines STILL beating SPY, and
     do they hold up in the CURRENT regime (by_regime), or is the edge thinning?
  2. DECAY TREND — snapshot each signal's live hit-rate/excess every run; compare
     today to the earliest snapshot >=10 days old. Edge falling on both axes =
     DECAYING. Builds a baseline over the first ~10 days (honest, no faked trend).
  3. BACKTEST-vs-LIVE GAP — backtest-harness says these archetypes are deployable
     (OOS deflated-Sharpe PASS). Are the live signals they map to delivering?

Self-contained: maintains its own history (no DynamoDB scan). Writes
data/alpha-decay.json. Telegram alert disabled (bot token compromised).
"""
import json, time
from datetime import datetime, timezone, timedelta
import boto3

VERSION = "1.0"
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/alpha-decay.json"
HIST_KEY = "data/alpha-decay-history.json"
s3 = boto3.client("s3", "us-east-1")

MIN_AGE_DAYS = 10
EXCESS_DROP = 1.0
HIT_DROP = 0.05
MAX_SNAPSHOTS = 180


def _read(key, default=None):
    try: return json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception: return default

def _write(key, obj, cache="public, max-age=900"):
    s3.put_object(Bucket=BUCKET, Key=key, Body=json.dumps(obj, default=str).encode(),
                  ContentType="application/json", CacheControl=cache)

def _regime_excess(v):
    if isinstance(v, dict):
        return v.get("excess_mean") or v.get("mean_excess") or v.get("excess")
    return None


def lambda_handler(event, context):
    t0 = time.time()
    now = datetime.now(timezone.utc)
    today = now.strftime("%Y-%m-%d")

    sc = _read("data/signal-scorecard.json") or {}
    rows = sc.get("scorecard") or sc.get("rows") or []
    cur = {}
    for r in rows:
        st = r.get("signal_type")
        if not st: continue
        cur[st] = {"excess": r.get("alpha_mean_excess_pct"), "hit": r.get("hit_rate"),
                   "alpha_status": r.get("alpha_status"), "grade": r.get("grade"),
                   "n": r.get("alpha_n") or r.get("n_scored"), "by_regime": r.get("by_regime") or {}}

    rr = _read("data/risk-regime.json") or {}
    regime = rr.get("regime") or rr.get("regime_label") or rr.get("label")

    # 1. PROVEN-ENGINE GUARD
    proven_watch = []
    for st, c in cur.items():
        if c["alpha_status"] != "ALPHA_PROVEN":
            continue
        reg_ex = [(rg, _regime_excess(v)) for rg, v in (c["by_regime"] or {}).items() if _regime_excess(v) is not None]
        worst = min(reg_ex, key=lambda x: x[1]) if reg_ex else None
        cur_reg_excess = _regime_excess((c["by_regime"] or {}).get(regime)) if regime else None
        alert = (c["excess"] is not None and c["excess"] <= 0) or (worst is not None and worst[1] < -0.5)
        proven_watch.append({"signal": st, "live_excess_vs_spy_pct": c["excess"], "hit_rate": c["hit"], "n": c["n"],
                             "current_regime": regime, "current_regime_excess_pct": cur_reg_excess,
                             "worst_regime": (worst[0] if worst else None),
                             "worst_regime_excess_pct": (worst[1] if worst else None),
                             "status": "WATCH — edge thin or regime-fragile" if alert else "HEALTHY — still beating SPY",
                             "alert": bool(alert)})

    # 2. DECAY TREND
    hist = _read(HIST_KEY) or {"snapshots": []}
    snaps = [s for s in hist.get("snapshots", []) if s.get("date") != today]
    snaps.append({"date": today, "signals": {st: {"excess": c["excess"], "hit": c["hit"]} for st, c in cur.items()}})
    snaps.sort(key=lambda s: s["date"])
    snaps = snaps[-MAX_SNAPSHOTS:]
    cutoff = (now - timedelta(days=MIN_AGE_DAYS)).strftime("%Y-%m-%d")
    baseline = next((s for s in snaps if s["date"] <= cutoff), None)
    decaying, improved, holding = [], [], []
    history_depth_days = 0
    if len(snaps) >= 2:
        try:
            history_depth_days = (datetime.strptime(snaps[-1]["date"], "%Y-%m-%d") -
                                  datetime.strptime(snaps[0]["date"], "%Y-%m-%d")).days
        except Exception: pass
    if baseline:
        for st, c in cur.items():
            b = (baseline.get("signals") or {}).get(st)
            if not b or c["excess"] is None or b.get("excess") is None or c["hit"] is None or b.get("hit") is None:
                continue
            d_excess = round(c["excess"] - b["excess"], 2)
            d_hit = round((c["hit"] - b["hit"]) * 100, 1)
            rec = {"signal": st, "alpha_status": c["alpha_status"], "n": c["n"],
                   "excess_now": c["excess"], "excess_then": b["excess"], "delta_excess_pp": d_excess,
                   "hit_now_pct": round(c["hit"] * 100, 1), "delta_hit_pp": d_hit, "baseline_date": baseline["date"]}
            if d_excess <= -EXCESS_DROP and d_hit <= -HIT_DROP * 100:
                rec["verdict"] = "DECAYING"; decaying.append(rec)
            elif d_excess >= EXCESS_DROP and d_hit >= HIT_DROP * 100:
                rec["verdict"] = "IMPROVED"; improved.append(rec)
            else:
                rec["verdict"] = "STABLE"; holding.append(rec)
        decaying.sort(key=lambda r: r["delta_excess_pp"])
        for r in decaying:
            if r["alpha_status"] == "ALPHA_PROVEN": r["PROVEN_DECAY_ALERT"] = True

    # 3. BACKTEST-vs-LIVE GAP
    bh = _read("data/backtest-harness.json") or {}
    live_map = bh.get("live_signal_types") or {}
    gap = []
    for rule in (bh.get("rules") or []):
        if not rule.get("PASS"):
            continue
        name = rule.get("rule")
        mapped = live_map.get(name) if isinstance(live_map, dict) else None
        live_sig = cur.get(mapped) if mapped else None
        gap.append({"archetype": name, "family": rule.get("family"),
                    "backtest_gate_sharpe": rule.get("deflated_gate_sr"), "backtest_says": "DEPLOYABLE",
                    "mapped_live_signal": mapped,
                    "live_status": (live_sig["alpha_status"] if live_sig else "no live mapping"),
                    "live_excess_pct": (live_sig["excess"] if live_sig else None),
                    "verdict": ("CAPTURED LIVE" if live_sig and live_sig.get("alpha_status") == "ALPHA_PROVEN"
                                else "NOT SHOWING UP LIVE" if live_sig and (live_sig.get("excess") or 0) <= 0
                                else "live-building / unmapped")})

    _write(HIST_KEY, {"snapshots": snaps, "updated": now.isoformat()}, cache="no-cache")
    n_proven_alert = sum(1 for p in proven_watch if p["alert"])
    n_proven_decay = sum(1 for r in decaying if r.get("PROVEN_DECAY_ALERT"))
    out = {"engine": "alpha-decay", "version": VERSION, "generated_at": now.isoformat(),
           "duration_s": round(time.time() - t0, 1),
           "thesis": ("Out-of-sample watchdog: guards the 3 proven engines, flags any engine whose live edge is "
                      "decaying vs its own recent baseline, and checks whether backtested-deployable archetypes deliver live."),
           "current_regime": regime, "history_depth_days": history_depth_days, "baseline_ready": bool(baseline),
           "stats": {"proven_engines": len(proven_watch), "proven_alerts": n_proven_alert, "decaying": len(decaying),
                     "improved": len(improved), "stable": len(holding), "proven_decay_alerts": n_proven_decay,
                     "backtest_pass_archetypes": len(gap)},
           "proven_engine_guard": proven_watch, "decaying": decaying[:25], "improved": improved[:15],
           "backtest_vs_live": gap[:20],
           "note": ("Decay trend compares today to the earliest snapshot >=%d days old; until then it is BUILDING a "
                    "baseline. Telegram alerting disabled (bot token compromised)." % MIN_AGE_DAYS)}
    _write(OUT_KEY, out)
    print("[alpha-decay v%s] proven=%d (alerts %d) decaying=%d improved=%d stable=%d backtest_pass=%d hist=%dd baseline=%s" % (
        VERSION, len(proven_watch), n_proven_alert, len(decaying), len(improved), len(holding), len(gap),
        history_depth_days, bool(baseline)))
    for p in proven_watch:
        print("  PROVEN %s: excess=%s%% status=%s" % (p["signal"], p["live_excess_vs_spy_pct"], p["status"]))
    return {"statusCode": 200, "body": json.dumps(out["stats"])}
