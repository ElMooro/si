"""
justhodl-signal-board — Unified Cross-Asset Signal Board

═══════════════════════════════════════════════════════════════════════
WHY THIS EXISTS
───────────────
The platform grew to ~268 engines but the newest ones — fundamentals,
construction-housing, crypto-narratives, short-pressure, mean-reversion,
pm-decision, cross-asset-rv — were not fused into any synthesis layer.
Data computed, never read.

A hedge fund solves this with a SIGNAL STORE: every model writes its
current read into one board, and the desk reads cross-asset posture
from a single place — instead of point-to-point spaghetti into every
scoring engine (which would destabilise scores the desk already trusts).

This engine reads each engine's sidecar, normalises its headline into a
5-state signal (-2 strong risk-off … +2 strong risk-on), aggregates a
composite posture + per-category sub-postures, and flags any stale feed.

OUTPUT: data/signal-board.json   SCHEDULE: every 3h
═══════════════════════════════════════════════════════════════════════
"""
import json
import os
import time
from datetime import datetime, timezone, timedelta

import boto3

S3_BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
OUT_KEY = "data/signal-board.json"
STALE_HOURS = 40

s3 = boto3.client("s3", region_name="us-east-1")

SIG_LABEL = {-2: "STRONG RISK-OFF", -1: "RISK-OFF", 0: "NEUTRAL",
             1: "RISK-ON", 2: "STRONG RISK-ON"}


def read_json(key):
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        return json.loads(obj["Body"].read()), obj["LastModified"]
    except Exception:
        return None, None


def clamp(v):
    return max(-2, min(2, int(round(v))))


# ── per-engine normalisers — each returns (signal:-2..2, read:str) ──
def n_pm_decision(d):
    pw = (d.get("posture_word") or "").upper()
    m = {"AGGRESSIVE": 2, "CONSTRUCTIVE": 1, "NEUTRAL": 0,
         "CAUTIOUS": -1, "DEFENSIVE": -2}
    return m.get(pw, 0), f"Desk posture {pw or 'n/a'}"


def n_cross_asset_rv(d):
    st = (d.get("rv_state") or "").upper()
    m = {"ALIGNED": 1, "STRETCHED": 0, "DISLOCATION_PRESENT": -1}
    return m.get(st, 0), f"RV {st.replace('_', ' ').lower() or 'n/a'}"


def n_fundamentals(d):
    s = d.get("summary") or {}
    uv, ov = s.get("n_undervalued") or 0, s.get("n_overvalued") or 0
    sig = 1 if uv > ov * 1.5 else -1 if ov > uv * 1.5 else 0
    return sig, f"{uv} undervalued vs {ov} overvalued (DCF)"


def n_construction_housing(d):
    rg = (d.get("regime") or "").upper()
    m = {"EXPANSION": 2, "RECOVERY": 1, "SLOWING": -1, "CONTRACTION": -2}
    return m.get(rg, 0), f"Housing cycle {rg or 'n/a'}"


def n_crypto_narratives(d):
    st = (d.get("stance") or "").upper()
    m = {"RISK-ON ROTATION": 2, "SELECTIVE": 0, "RISK-OFF": -2}
    br = d.get("narrative_breadth_pct")
    return m.get(st, 0), f"Crypto {st or 'n/a'} ({br}% breadth)"


def n_short_pressure(d):
    b = d.get("n_pressure_building") or 0
    c = d.get("n_shorts_covering") or 0
    sig = 1 if c > b * 1.5 else -1 if b > c * 1.5 else 0
    return sig, f"{b} building short pressure, {c} covering"


def n_mean_reversion(d):
    ch = d.get("n_cheap_vs_history") or 0
    ri = d.get("n_rich_vs_history") or 0
    sig = 1 if ch > ri * 1.3 else -1 if ri > ch * 1.3 else 0
    return sig, f"{ch} cheap vs {ri} rich on own multiple history"


def n_canary_grid(d):
    band = (d.get("band") or "").upper()
    m = {"CALM": 1, "WATCH": 0, "ELEVATED": -1, "WARNING": -2, "CRITICAL": -2}
    lvl = d.get("early_warning_level")
    return m.get(band, 0), f"Global early-warning {band or 'n/a'} ({lvl}/100)"


def n_dollar_radar(d):
    # dollar_pressure -100 (DUMP) .. +100 (PUMP). A dollar PUMP (squeeze) is
    # risk-off; a dollar DUMP (a liquidity flood) is risk-on.
    p = d.get("dollar_pressure")
    if not isinstance(p, (int, float)):
        return 0, "Dollar pressure n/a"
    reg = d.get("regime") or "n/a"
    sig = (-2 if p >= 50 else -1 if p >= 20 else
           2 if p <= -50 else 1 if p <= -20 else 0)
    return sig, f"Dollar {reg} (pressure {p:+.0f})"


def n_global_stress(d):
    # global_stress_index 0-100; high = world equity/bond stress = risk-off.
    gsi = d.get("global_stress_index")
    lvl = d.get("global_stress_level") or "n/a"
    if not isinstance(gsi, (int, float)):
        return 0, "Global stress n/a"
    sig = -2 if gsi >= 75 else -1 if gsi >= 55 else 1 if gsi < 32 else 0
    return sig, f"Global market stress {lvl} ({gsi}/100)"


# (engine, category, s3_key, normaliser)
FEEDS = [
    ("PM Decision",        "positioning",      "data/pm-decision.json",        n_pm_decision),
    ("Cross-Asset RV",     "relative value",   "data/cross-asset-rv.json",     n_cross_asset_rv),
    ("Fundamentals X-Ray", "equity valuation", "data/fundamentals.json",       n_fundamentals),
    ("Housing Cycle",      "macro",            "data/construction-housing.json", n_construction_housing),
    ("Crypto Narratives",  "crypto",           "data/crypto-narratives.json",  n_crypto_narratives),
    ("Short Pressure",     "positioning",      "data/short-pressure.json",     n_short_pressure),
    ("Mean Reversion",     "equity valuation", "screener/mean-reversion.json", n_mean_reversion),
    ("Canary Grid",        "macro",            "data/canary-grid.json",        n_canary_grid),
    ("Dollar Radar",       "macro",            "data/dollar-radar.json",       n_dollar_radar),
    ("Global Stress",      "macro",            "data/global-stress.json",      n_global_stress),
]


def lambda_handler(event, context):
    t0 = time.time()
    now = datetime.now(timezone.utc)
    engines, stale = [], 0

    for name, cat, key, fn in FEEDS:
        data, last_mod = read_json(key)
        if data is None:
            engines.append({"engine": name, "category": cat, "signal": None,
                            "signal_label": "NO DATA", "read": "sidecar missing",
                            "as_of": None, "stale": True})
            stale += 1
            continue
        try:
            sig, read = fn(data)
            sig = clamp(sig)
        except Exception as e:
            sig, read = None, f"parse error: {str(e)[:80]}"
        as_of = data.get("generated_at") or (
            last_mod.isoformat() if last_mod else None)
        is_stale = False
        if last_mod and (now - last_mod) > timedelta(hours=STALE_HOURS):
            is_stale = True
            stale += 1
        engines.append({
            "engine": name, "category": cat, "signal": sig,
            "signal_label": SIG_LABEL.get(sig, "—") if sig is not None else "—",
            "read": read, "as_of": as_of, "stale": is_stale})

    live = [e for e in engines if e["signal"] is not None and not e["stale"]]
    composite = round(sum(e["signal"] for e in live) / len(live), 2) if live else None

    # per-category sub-posture
    cats = {}
    for e in live:
        cats.setdefault(e["category"], []).append(e["signal"])
    categories = {c: {"signal": round(sum(v) / len(v), 2), "n": len(v)}
                  for c, v in cats.items()}

    if composite is None:
        posture = "NO SIGNAL"
    elif composite >= 1.0:
        posture = "RISK-ON"
    elif composite >= 0.25:
        posture = "MILDLY RISK-ON"
    elif composite > -0.25:
        posture = "NEUTRAL / MIXED"
    elif composite > -1.0:
        posture = "MILDLY RISK-OFF"
    else:
        posture = "RISK-OFF"

    out = {
        "schema_version": "1.0",
        "method": "cross_asset_signal_aggregation",
        "generated_at": now.isoformat(),
        "elapsed_s": round(time.time() - t0, 2),
        "composite_signal": composite,
        "composite_posture": posture,
        "n_engines": len(engines),
        "n_live": len(live),
        "n_stale": stale,
        "categories": categories,
        "engines": engines,
        "note": ("Unified signal store — each engine's headline read "
                 "normalised to a 5-state signal and aggregated into one "
                 "cross-asset posture. Stale feeds (sidecar older than "
                 f"{STALE_HOURS}h) are flagged and excluded from the "
                 "composite. A synthesis view, not advice."),
    }
    s3.put_object(Bucket=S3_BUCKET, Key=OUT_KEY,
                  Body=json.dumps(out, default=str).encode("utf-8"),
                  ContentType="application/json",
                  CacheControl="public, max-age=1800")
    print(f"[signal-board] posture={posture} composite={composite} "
          f"{len(live)}/{len(engines)} live, {stale} stale, {out['elapsed_s']}s")
    return {"statusCode": 200, "body": json.dumps({
        "ok": True, "composite_posture": posture,
        "composite_signal": composite, "n_live": len(live),
        "n_stale": stale})}
