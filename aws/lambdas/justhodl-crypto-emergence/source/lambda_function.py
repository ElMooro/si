"""
justhodl-crypto-emergence — EARLY-INNINGS BULL DETECTION (crypto)
═══════════════════════════════════════════════════════════════════════════════════════════════
The crypto-specific version of the sector emergence detector. Crypto trades as its own complex with
its own internal benchmark — BITCOIN — so the framework adapts:
  • RS-vs-BTC (the alt-season tell): an altcoin's coin/BTC ratio turning up = risk appetite rotating
    down the curve. BTC itself is scored on absolute trend.
  • The 200-DAY RECLAIM is the canonical crypto trend trigger (far more decisive than in equities).
  • BREADTH across the coin universe (% above 50d / 200d) separates a real broad bull from a BTC-only bounce.
  • CONTEXT from the crypto-cycle-risk and funding feeds: washed-out funding + low cycle-risk = early/fuel;
    frothy funding + high cycle-risk = late/top-risk.

It outputs a COMPLEX read (is crypto in a bear, basing, EMERGING early-bull, confirmed, or extended/top-risk),
per-coin stages with RS-vs-BTC, an alt-season gauge, and — most useful when crypto is down — the concrete
TRIGGER LEVELS that would confirm the early innings and the INVALIDATION level that would kill it.
"""
import json
import os
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone, timedelta

import boto3

REGION = "us-east-1"
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
POLY = os.environ.get("POLYGON_KEY", "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d")
S3 = boto3.client("s3", REGION)
OUT_KEY = "data/crypto-emergence.json"

COINS = {
    "BTCUSD": ("Bitcoin", "Large-cap"), "ETHUSD": ("Ethereum", "Large-cap"),
    "SOLUSD": ("Solana", "L1 Alts"), "ADAUSD": ("Cardano", "L1 Alts"),
    "AVAXUSD": ("Avalanche", "L1 Alts"), "DOTUSD": ("Polkadot", "L1 Alts"),
    "XRPUSD": ("XRP", "Payments"), "LTCUSD": ("Litecoin", "Payments"), "BCHUSD": ("Bitcoin Cash", "Payments"),
    "LINKUSD": ("Chainlink", "DeFi/Oracle"), "DOGEUSD": ("Dogecoin", "Meme"),
}


def fetch(t):
    frm = (datetime.now(timezone.utc) - timedelta(days=430)).strftime("%Y-%m-%d")
    to = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    url = f"https://api.polygon.io/v2/aggs/ticker/X:{t}/range/1/day/{frm}/{to}?adjusted=true&sort=asc&limit=500&apiKey={POLY}"
    try:
        r = json.loads(urllib.request.urlopen(urllib.request.Request(url, headers={"User-Agent": "jh"}), timeout=20).read())
        return t, [x["c"] for x in r.get("results", []) if x.get("c")]
    except Exception:
        return t, []


def ma(a, n):
    return sum(a[-n:]) / n if len(a) >= n else (sum(a) / len(a) if a else 0)


def clamp(v, lo=0, hi=100):
    return max(lo, min(hi, v))


def lambda_handler(event=None, context=None):
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=12) as ex:
        px = dict(ex.map(fetch, list(COINS)))
    btc = px.get("BTCUSD", [])
    if len(btc) < 210:
        return {"statusCode": 500, "body": "no BTC"}

    def _read(k):
        try:
            return json.loads(S3.get_object(Bucket=BUCKET, Key=k)["Body"].read())
        except Exception:
            return {}
    cyc = _read("data/crypto-cycle-risk.json")
    fund = _read("data/crypto-funding.json")
    cyc_risk = cyc.get("dump_risk_score") if isinstance(cyc, dict) else None
    cyc_risk = float(cyc_risk) if isinstance(cyc_risk, (int, float)) else None
    risk_level = cyc.get("risk_level") if isinstance(cyc, dict) else None
    try:
        mvrv = float(((cyc.get("factors") or {}).get("mvrv_extension") or {}).get("mvrv"))
    except Exception:
        mvrv = None
    mc = (fund.get("market_composite") or {}) if isinstance(fund, dict) else {}
    market_funding = mc.get("vw_funding_annualized_pct")
    market_funding = float(market_funding) if isinstance(market_funding, (int, float)) else None
    coin_funding = {}
    for cn, cv in ((fund.get("by_coin") or {}) if isinstance(fund, dict) else {}).items():
        if isinstance(cv, dict) and isinstance(cv.get("annualized_pct"), (int, float)):
            coin_funding[cn] = float(cv["annualized_pct"])

    # ── complex-level breadth & BTC trend ──
    above50 = above200 = ncoins = 0
    for t, c in px.items():
        if len(c) < 210:
            continue
        ncoins += 1
        if c[-1] > ma(c, 50):
            above50 += 1
        if c[-1] > ma(c, 200):
            above200 += 1
    breadth50 = round(100 * above50 / ncoins) if ncoins else 0
    breadth200 = round(100 * above200 / ncoins) if ncoins else 0
    btc_ma200, btc_ma200_prev = ma(btc, 200), ma(btc[:-21], 200)
    btc_ma50 = ma(btc, 50)
    btc_above_200 = btc[-1] > btc_ma200
    btc_200_up = btc_ma200 > btc_ma200_prev
    eth = px.get("ETHUSD", [])
    ethbtc_trend = None
    if len(eth) > 63 and len(btc) > 63:
        L = min(len(eth), len(btc))
        eb = [eth[-L + i] / btc[-L + i] for i in range(L)]
        ethbtc_trend = round((eb[-1] / eb[-42] - 1) * 100, 1)   # ETH/BTC 2-month trend = alt-season proxy

    # ── per-coin scoring ──
    out = []
    for t, (name, sect) in COINS.items():
        c = px.get(t, [])
        if len(c) < 210:
            continue
        price = c[-1]
        m50, m150, m200 = ma(c, 50), ma(c, 150), ma(c, 200)
        m200_prev = ma(c[:-21], 200)
        reclaim200 = price > m200
        m200_up = m200 > m200_prev
        base_break = price >= max(c[-55:-5]) * 0.97
        below_falling = price < m200 and m200 < m200_prev
        trend_score = clamp(50 + (20 if reclaim200 else -16) + (14 if m200_up else -10)
                            + (10 if price > m50 else -8) + (8 if base_break else 0) + (-18 if below_falling else 0))
        # RS vs BTC (alts); BTC uses absolute as its own RS
        if t == "BTCUSD":
            rs_score = clamp(50 + (20 if reclaim200 else -20) + (16 if m200_up else -12))
            rs_slope = round((btc[-1] / btc[-50] - 1) * 100, 1)
            rs_new_low = btc[-1] <= min(btc[-63:]) * 1.01
        else:
            L = min(len(c), len(btc))
            rs = [c[-L + i] / btc[-L + i] for i in range(L)]
            rs_ma50 = sum(rs[-50:]) / 50
            rs_slope = round((rs[-1] / rs[-50] - 1) * 100, 1)
            rs_new_low = rs[-1] <= min(rs[-63:]) * 1.01
            rs_hl = rs[-1] > min(rs[-126:-42]) if len(rs) > 126 else True
            rs_score = clamp(50 + (18 if rs[-1] > rs_ma50 else -10) + (16 if rs_slope > 0 else -16)
                             + (10 if rs_hl else 0) + (-30 if rs_new_low else 8))
        m3 = c[-1] / c[-63] - 1
        dist_low = c[-1] / min(c[-252:]) - 1 if len(c) >= 252 else c[-1] / min(c) - 1
        if m3 <= 0:
            mom_score = 35
        elif dist_low < 0.50:
            mom_score = 88
        elif dist_low < 1.0:
            mom_score = 60
        else:
            mom_score = 38
        ctx = 50.0
        if cyc_risk is not None:
            ctx += -15 if cyc_risk >= 70 else (12 if cyc_risk <= 35 else 0)   # high dump-risk = late
        if mvrv is not None:
            ctx += 12 if mvrv < 1.0 else (-12 if mvrv > 3.0 else 0)            # undervalued vs euphoric
        cf = coin_funding.get(t.replace("USD", ""))
        if cf is not None:
            ctx += 6 if cf < 0 else (-10 if cf > 30 else 0)                    # negative funding = washed-out fuel; very hot = crowded
        ctx = clamp(ctx + (8 if breadth50 > 50 else -6))

        emergence = round(0.25 * rs_score + 0.30 * trend_score + 0.18 * (breadth50 * 0.6 + breadth200 * 0.4)
                          + 0.15 * mom_score + 0.12 * ctx, 1)
        if below_falling and rs_new_low:
            stage = "DECLINING"
        elif not reclaim200 and abs(m3) < 0.12 and not rs_new_low:
            stage = "BASING"
        elif reclaim200 and m200_up and rs_slope > 0 and dist_low < 1.2 and not rs_new_low:
            stage = "EMERGING"
        elif reclaim200 and m200_up and price > m50 and m3 > 0.05:
            stage = "EXTENDED" if (cyc_risk is not None and cyc_risk >= 75) or dist_low > 2.0 else "CONFIRMED"
        elif rs_new_low or below_falling:
            stage = "DECLINING"
        else:
            stage = "TRANSITION"
        falsifier = f"loses {m200:,.0f} (200-day, {(price/m200-1)*100:+.0f}% away)" if t == "BTCUSD" \
            else f"RS-vs-BTC makes a new 3-month low / price loses {m200:,.4g} (200-day)"
        sigs = []
        if reclaim200 and m200_up:
            sigs.append("above rising 200d")
        if t != "BTCUSD" and rs_slope > 0 and not rs_new_low:
            sigs.append("gaining vs BTC")
        if base_break:
            sigs.append("base breakout")
        if m3 > 0 and dist_low < 0.6:
            sigs.append("early momentum")
        if cf is not None and cf < -5:
            sigs.append("funding negative (squeeze fuel)")
        out.append({"ticker": t.replace("USD", ""), "name": name, "sector": sect, "emergence_score": emergence,
                    "stage": stage, "rs_score": round(rs_score), "trend_score": round(trend_score),
                    "rs_vs_btc_slope_pct": rs_slope, "ret_3m_pct": round(m3 * 100, 1),
                    "funding_annualized_pct": round(cf, 1) if cf is not None else None,
                    "dist_from_low_pct": round(dist_low * 100), "price": round(price, 4),
                    "ma200": round(m200, 4), "signals": sigs, "falsifier": falsifier})
    out.sort(key=lambda x: -x["emergence_score"])

    # ── COMPLEX read + trigger levels ──
    if not btc_above_200 and breadth200 < 25:
        complex_stage = "BEAR / DECLINING"
        complex_read = ("Crypto is in a downtrend — BTC below a falling 200-day and breadth washed out. "
                        "This is where you build a watch-list and wait for the trigger, not chase.")
    elif not btc_above_200 and breadth50 >= 40:
        complex_stage = "BASING"
        complex_read = "Crypto is basing — selling exhausted, BTC reclaiming shorter MAs but not yet the 200-day. Early but unconfirmed."
    elif btc_above_200 and btc_200_up and breadth50 < 60:
        complex_stage = "EMERGING"
        complex_read = "Early-innings: BTC has reclaimed a rising 200-day and breadth is building. The turn the watch-list was waiting for."
    elif btc_above_200 and breadth50 >= 60:
        complex_stage = "EXTENDED / TOP-RISK" if (cyc_risk is not None and cyc_risk >= 75) else "CONFIRMED BULL"
        complex_read = ("Broad crypto bull underway." if complex_stage == "CONFIRMED BULL"
                        else "Bull is extended and cycle-risk is elevated — late innings, manage risk.")
    else:
        complex_stage = "TRANSITION"
        complex_read = "Mixed — no clean complex regime; trade coins individually on their own stage."

    # accumulation-vs-euphoria context — distinguishes a bottoming bear from a euphoric top
    acc = []
    if mvrv is not None and mvrv < 1.0:
        acc.append(f"MVRV {mvrv:.2f} (undervalued)")
    if cyc_risk is not None and cyc_risk <= 40:
        acc.append(f"dump-risk {cyc_risk:.0f} ({(risk_level or 'low').lower()})")
    if market_funding is not None and market_funding < 0:
        acc.append(f"funding {market_funding:+.1f}% ann. (negative = washed out)")
    euph = []
    if mvrv is not None and mvrv > 3.0:
        euph.append(f"MVRV {mvrv:.2f} (euphoric)")
    if cyc_risk is not None and cyc_risk >= 70:
        euph.append(f"dump-risk {cyc_risk:.0f} (high)")
    if market_funding is not None and market_funding > 25:
        euph.append(f"funding {market_funding:+.0f}% ann. (frothy)")
    if complex_stage.startswith("BEAR") and len(acc) >= 2:
        complex_read += (" But this reads as an ACCUMULATION-zone bear, not a euphoric top — "
                         + ", ".join(acc) + ". That's the setup that precedes early innings; watch the trigger.")
    elif euph and complex_stage in ("CONFIRMED BULL",):
        complex_read += " ⚠ Froth building: " + ", ".join(euph) + "."

    triggers = {
        "early_bull_confirms_when": f"BTC reclaims its 200-day (~{btc_ma200:,.0f}) and HOLDS, breadth >50% of coins above 50d "
                                    f"(now {breadth50}%), and ETH/BTC turns up (alts joining).",
        "btc_200d_level": round(btc_ma200), "btc_price": round(btc[-1]),
        "btc_pct_to_200d": round((btc[-1] / btc_ma200 - 1) * 100, 1),
        "invalidation": "fresh lows in BTC with breadth staying <25% = the bear isn't done.",
        "alt_season": ("alts gaining on BTC (ETH/BTC up)" if (ethbtc_trend or 0) > 1 else
                       "BTC-dominant / alts still weak vs BTC" if (ethbtc_trend or 0) < -1 else "neutral"),
    }

    top_picks = [{"ticker": o["ticker"], "direction": "UP", "conviction": o["emergence_score"],
                  "thesis": f"{o['stage']} — " + ", ".join(o["signals"][:3])}
                 for o in out if o["stage"] in ("EMERGING",)][:5]

    payload = {
        "engine": "justhodl-crypto-emergence", "version": "1.0.0", "ok": True,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "wl_research": __import__("wl_fusion").block(('CRYPTO',)),
        "thesis": "Early-innings bull detection for crypto — RS-vs-BTC, the 200-day reclaim, coin-universe breadth, "
                  "and cycle/funding context, with concrete trigger & invalidation levels.",
        "complex_stage": complex_stage, "complex_read": complex_read,
        "breadth_pct_above_50d": breadth50, "breadth_pct_above_200d": breadth200,
        "btc_above_200d": btc_above_200, "btc_200d_rising": btc_200_up,
        "ethbtc_2m_trend_pct": ethbtc_trend, "cycle_risk": cyc_risk, "risk_level": risk_level,
        "mvrv": mvrv, "market_funding_annualized_pct": market_funding,
        "accumulation_context": acc, "euphoria_context": euph,
        "triggers": triggers, "coins": out, "top_picks": top_picks,
        "legend": {"DECLINING": "downtrend — avoid", "BASING": "bottoming — watch", "EMERGING": "early Stage-2 — the target",
                   "CONFIRMED": "uptrend underway", "EXTENDED": "late / top-risk", "TRANSITION": "mixed"},
        "note": "Crypto trades vs BTC internally; alts gaining on BTC = risk rotating down-curve. The 200-day reclaim is "
                "the decisive trend trigger. EMERGING coins are logged and graded forward. Research, not advice.",
        "elapsed_s": round(time.time() - t0, 1),
    }
    S3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(payload, default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=1800")
    print(f"[crypto-emergence] complex={complex_stage} | breadth50={breadth50}% breadth200={breadth200}% "
          f"btc>200d={btc_above_200} ethbtc2m={ethbtc_trend} | EMERGING={[o['ticker'] for o in out if o['stage']=='EMERGING']} | {payload['elapsed_s']}s")
    return {"statusCode": 200, "body": json.dumps({"ok": True, "complex_stage": complex_stage, "breadth50": breadth50,
            "emerging": [o["ticker"] for o in out if o["stage"] == "EMERGING"]})}
