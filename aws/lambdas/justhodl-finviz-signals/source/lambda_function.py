"""justhodl-finviz-signals — whole-market technical signal engine, computed ENTIRELY
from the Finviz universe snapshot (zero extra API calls). Detects:
  - price x SMA200 / SMA50 crossovers (up/down)
  - golden / death cross (SMA50 vs SMA200, derived from the MA-distance columns)
  - momentum leaders (multi-window perf blend + RSI + 52w-high proximity)
  - unusual relative volume
  - RSI overbought / oversold extremes
  - 52-week-high breakout proximity

Crossovers are detected day-over-day by diffing against data/finviz-signals-state.json
(the prior snapshot's MA distances). First run establishes the baseline (0 crossovers);
they populate from the next snapshot onward.

Output: data/finviz-signals.json   State: data/finviz-signals-state.json
"""
import json
from datetime import datetime, timezone
import boto3
import finviz as FV

s3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
STATE_KEY = "data/finviz-signals-state.json"
OUT_KEY = "data/finviz-signals.json"


def _load(key):
    try:
        return json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception:
        return {}


def _tradeable(r):
    return (r.get("price") or 0) >= 2 and (r.get("avg_volume") or 0) >= 300000 \
        and (r.get("market_cap") or 0) >= 1e8


def lambda_handler(event=None, context=None):
    uni = FV.load_universe()
    prev = _load(STATE_KEY).get("by_ticker", {})
    now = datetime.now(timezone.utc).isoformat()
    state = {}
    cross_up200, cross_dn200, golden, death = [], [], [], []
    momentum, unusual_vol, overbought, oversold, near_high = [], [], [], [], []

    for tk, r in uni.items():
        s200, s50 = r.get("sma200_pct"), r.get("sma50_pct")
        if s200 is not None and s50 is not None:
            state[tk] = {"s200": s200, "s50": s50}
        if not _tradeable(r):
            continue
        px = r.get("price")
        base = {"ticker": tk, "name": r.get("company") or tk, "sector": r.get("sector"),
                "price": px, "rsi": r.get("rsi"), "perf_m": r.get("perf_m"),
                "perf_q": r.get("perf_q"), "sma200_pct": s200, "rel_volume": r.get("rel_volume")}
        pw, pm, pq = r.get("perf_w"), r.get("perf_m"), r.get("perf_q")
        rsi, relv, offhigh = r.get("rsi"), r.get("rel_volume"), r.get("off_52w_high_pct")
        p = prev.get(tk) or {}
        ps200, ps50 = p.get("s200"), p.get("s50")

        # price x SMA200 crossover (sma200_pct = price's % distance from its 200-day MA)
        if s200 is not None and ps200 is not None:
            if ps200 < 0 <= s200:
                cross_up200.append(base)
            elif ps200 > 0 >= s200:
                cross_dn200.append(base)
        # golden / death cross: SMA50 above SMA200  <=>  sma50_pct < sma200_pct
        if None not in (s50, s200, ps50, ps200):
            cur, prv = s50 - s200, ps50 - ps200   # < 0 => 50 above 200 (bullish)
            if prv >= 0 > cur:
                golden.append(base)
            elif prv <= 0 < cur:
                death.append(base)
        # momentum composite
        if None not in (pm, pq, rsi):
            mom = 0.4 * pq + 0.3 * pm + 0.2 * (pw or 0) + 0.1 * (rsi - 50)
            momentum.append({**base, "mom_score": round(mom, 2), "off_52w_high": offhigh})
        # unusual relative volume
        if relv is not None and relv >= 2:
            unusual_vol.append({**base, "change_pct": r.get("change_pct")})
        # RSI extremes
        if rsi is not None:
            if rsi >= 75:
                overbought.append(base)
            elif rsi <= 25:
                oversold.append(base)
        # near 52-week high (within 3%)
        if offhigh is not None and -3 <= offhigh <= 0:
            near_high.append({**base, "off_52w_high": offhigh})

    momentum.sort(key=lambda x: x["mom_score"], reverse=True)
    unusual_vol.sort(key=lambda x: (x.get("rel_volume") or 0), reverse=True)
    overbought.sort(key=lambda x: (x.get("rsi") or 0), reverse=True)
    oversold.sort(key=lambda x: (x.get("rsi") or 0))
    near_high.sort(key=lambda x: (x.get("off_52w_high") or -99), reverse=True)

    payload = {
        "generated_at": now, "universe_n": len(uni),
        "ma_crosses": {
            "price_cross_sma200_up": cross_up200, "price_cross_sma200_down": cross_dn200,
            "golden_cross": golden, "death_cross": death,
        },
        "momentum_leaders": momentum[:50],
        "unusual_volume": unusual_vol[:50],
        "rsi_overbought": overbought[:40], "rsi_oversold": oversold[:40],
        "near_52w_high": near_high[:50],
    }
    s3.put_object(Bucket=BUCKET, Key=OUT_KEY,
                  Body=json.dumps(payload, separators=(",", ":"), default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=900")
    s3.put_object(Bucket=BUCKET, Key=STATE_KEY,
                  Body=json.dumps({"generated_at": now, "by_ticker": state}, separators=(",", ":")).encode(),
                  ContentType="application/json")
    print("[finviz-signals] crossUp200=%d crossDn200=%d golden=%d death=%d mom=%d unusualVol=%d ob=%d os=%d nearHigh=%d"
          % (len(cross_up200), len(cross_dn200), len(golden), len(death), len(momentum),
             len(unusual_vol), len(overbought), len(oversold), len(near_high)))
    return {"statusCode": 200, "body": json.dumps(
        {"crossUp200": len(cross_up200), "crossDn200": len(cross_dn200),
         "golden": len(golden), "death": len(death), "momentum": len(momentum),
         "unusualVol": len(unusual_vol), "had_prev": bool(prev)})}
