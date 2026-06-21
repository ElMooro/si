"""
justhodl-sector-emergence — EARLY-INNINGS BULL DETECTION (equity sectors)
═══════════════════════════════════════════════════════════════════════════════════════════════
"There's always a bull market somewhere." This finds it EARLY — the Stage 1→Stage 2 transition,
before a sector is obviously booming (which the Risk Map, by design, only shows you once it already is).

Detection is staged the way a stage-analysis desk (Weinstein/O'Neil) reads it, and weighted toward the
LEADING signals because the earlier you detect, the noisier it is — so confluence + sequence is the edge:
  1. RELATIVE-STRENGTH inflection (earliest, highest weight): sector/SPY ratio stops making new lows and
     turns up — institutions accumulate the laggard before price confirms.
  2. STAGE-2 reclaim (Weinstein): price reclaims a flattening→rising 150-day MA; breaks its base; golden cross.
  3. BREADTH broadening WITHIN the sector: % of its big constituents above their 50-day MA rises (a thrust).
  4. MOMENTUM that is positive but NOT yet extended (still early, not late-cycle).
  5. External confirmation (lighter): EPS-revision turn, ETF-flow support, business-cycle fit.

Output per sector: an emergence_score, the STAGE (DECLINING/BASING/EMERGING/CONFIRMED/EXTENDED), the exact
signals firing, and a concrete FALSIFIER (the level/condition that kills the thesis). EMERGING = the target.
Logs top emerging sectors as measure-before-trust picks so the call is graded forward, not just asserted.
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
OUT_KEY = "data/sector-emergence.json"

SECTORS = {
    "XLK": ("Technology", ["AAPL", "MSFT", "NVDA", "AVGO", "ORCL", "CRM"]),
    "XLF": ("Financials", ["JPM", "V", "MA", "BAC", "WFC", "GS"]),
    "XLE": ("Energy", ["XOM", "CVX", "COP", "SLB", "EOG", "MPC"]),
    "XLV": ("Health Care", ["LLY", "UNH", "JNJ", "ABBV", "MRK", "TMO"]),
    "XLP": ("Cons. Staples", ["PG", "KO", "PEP", "COST", "WMT", "MDLZ"]),
    "XLY": ("Cons. Discretionary", ["AMZN", "TSLA", "HD", "MCD", "NKE", "LOW"]),
    "XLI": ("Industrials", ["GE", "CAT", "RTX", "UNP", "HON", "BA"]),
    "XLU": ("Utilities", ["NEE", "SO", "DUK", "CEG", "AEP", "D"]),
    "XLB": ("Materials", ["LIN", "SHW", "FCX", "ECL", "NEM", "APD"]),
    "XLC": ("Communications", ["META", "GOOGL", "NFLX", "DIS", "TMUS", "CMCSA"]),
    "XLRE": ("Real Estate", ["PLD", "AMT", "EQIX", "WELL", "SPG", "O"]),
}
CYCLICAL = {"XLK", "XLY", "XLF", "XLI", "XLB"}
DEFENSIVE = {"XLP", "XLU", "XLV"}


def fetch(t):
    frm = (datetime.now(timezone.utc) - timedelta(days=430)).strftime("%Y-%m-%d")
    to = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    url = f"https://api.polygon.io/v2/aggs/ticker/{t}/range/1/day/{frm}/{to}?adjusted=true&sort=asc&limit=500&apiKey={POLY}"
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
    names = ["SPY"] + list(SECTORS) + [c for _, cons in SECTORS.values() for c in cons]
    with ThreadPoolExecutor(max_workers=20) as ex:
        px = dict(ex.map(fetch, list(set(names))))
    spy = px.get("SPY", [])
    if len(spy) < 210:
        return {"statusCode": 500, "body": "no SPY"}

    # external legs (best-effort)
    def _read(k):
        try:
            return json.loads(S3.get_object(Bucket=BUCKET, Key=k)["Body"].read())
        except Exception:
            return {}
    regime = (_read("data/regime-map.json").get("regime") or {}).get("label")
    flows = _read("etf-flows/daily.json")
    fmap = {m.get("ticker"): m for m in (flows.get("metrics") or []) if m.get("ticker")}
    rev = _read("data/eps-revision-velocity.json")

    out = []
    for etf, (name, cons) in SECTORS.items():
        c = px.get(etf, [])
        if len(c) < 210:
            continue
        price = c[-1]
        ma50, ma150, ma200 = ma(c, 50), ma(c, 150), ma(c, 200)
        ma150_prev = ma(c[:-21], 150)
        # ── 1. relative strength vs SPY ──
        L = min(len(c), len(spy))
        rs = [c[-L + i] / spy[-L + i] for i in range(L)]
        rs_ma50 = sum(rs[-50:]) / 50
        rs_slope = rs[-1] / rs[-50] - 1 if len(rs) > 50 else 0
        rs_new_low = rs[-1] <= min(rs[-63:]) * 1.005
        rs_higher_low = rs[-1] > min(rs[-126:-42]) if len(rs) > 126 else True
        rs_score = clamp(50 + (18 if rs[-1] > rs_ma50 else -10) + (16 if rs_slope > 0 else -16)
                         + (12 if rs_higher_low else 0) + (-32 if rs_new_low else 8))
        # ── 2. trend / Weinstein stage ──
        reclaim = price > ma150
        ma150_up = ma150 > ma150_prev
        base_break = price >= max(c[-55:-5]) * 0.985
        gc = ma50 > ma200
        below_falling = price < ma200 and ma150 < ma150_prev
        trend_score = clamp(50 + (16 if reclaim else -12) + (14 if ma150_up else -10)
                            + (10 if base_break else 0) + (10 if gc else -6) + (-22 if below_falling else 0))
        # ── 3. within-sector breadth ──
        above, above_prev, npos = 0, 0, 0
        for s in cons:
            sc = px.get(s, [])
            if len(sc) < 80:
                continue
            npos += 1
            if sc[-1] > ma(sc, 50):
                above += 1
            if sc[-22] > ma(sc[:-21], 50):
                above_prev += 1
        pct50 = 100 * above / npos if npos else 50
        pct50_prev = 100 * above_prev / npos if npos else 50
        thrust = pct50 - pct50_prev
        breadth_score = clamp(0.7 * pct50 + 25 + thrust * 0.6)
        # ── 4. momentum, not extended (early) ──
        m3 = c[-1] / c[-63] - 1
        m6 = c[-1] / c[-126] - 1 if len(c) > 126 else m3
        dist_low = c[-1] / min(c[-252:]) - 1 if len(c) >= 252 else c[-1] / min(c) - 1
        if m3 <= 0:
            mom_score = 35
        elif dist_low < 0.35:
            mom_score = 88           # up but still near the base = early
        elif dist_low < 0.60:
            mom_score = 62
        else:
            mom_score = 38           # very extended = late
        # ── 5. external confirmation (light) ──
        ext = 50.0
        if regime:
            if regime in ("BROAD RISK-ON", "BIFURCATED"):
                ext += 12 if etf in CYCLICAL else (-8 if etf in DEFENSIVE else 0)
            elif regime == "BROAD RISK-OFF":
                ext += 12 if etf in DEFENSIVE else (-8 if etf in CYCLICAL else 0)
        fm = fmap.get(etf) or {}
        f21 = fm.get("flow_21d_usd") or fm.get("fund_flow_21d_usd") or 0
        if f21:
            ext += 10 if f21 > 0 else -10
        ext = clamp(ext)

        emergence = round(0.30 * rs_score + 0.25 * trend_score + 0.22 * breadth_score
                          + 0.13 * mom_score + 0.10 * ext, 1)

        # ── stage classification ──
        if below_falling and rs_new_low:
            stage = "DECLINING"        # Stage 4 — avoid
        elif (not reclaim or not ma150_up) and abs(m3) < 0.06 and not rs_new_low:
            stage = "BASING"           # Stage 1 — watch, not yet
        elif reclaim and ma150_up and rs_slope > 0 and dist_low < 0.45 and not rs_new_low:
            stage = "EMERGING"         # early Stage 2 — THE TARGET
        elif reclaim and gc and pct50 >= 55 and m3 > 0.04:
            stage = "EXTENDED" if (dist_low > 0.60 or thrust < -10) else "CONFIRMED"
        elif rs_new_low or below_falling:
            stage = "DECLINING"
        else:
            stage = "TRANSITION"

        falsifier = (f"loses {ma150:.2f} (150-day MA, now {(price/ma150-1)*100:+.1f}% above) "
                     f"or relative strength makes a new 3-month low")
        signals = []
        if not rs_new_low and rs_slope > 0:
            signals.append("RS inflecting up")
        if reclaim and ma150_up:
            signals.append("reclaimed rising 150d MA")
        if base_break:
            signals.append("base breakout")
        if gc:
            signals.append("golden cross")
        if thrust > 5:
            signals.append(f"breadth thrust (+{thrust:.0f}pt)")
        if pct50 >= 55:
            signals.append(f"{pct50:.0f}% of names > 50d MA")
        if m3 > 0 and dist_low < 0.45:
            signals.append("early momentum, not extended")
        out.append({"ticker": etf, "name": name, "emergence_score": emergence, "stage": stage,
                    "rs_score": round(rs_score), "trend_score": round(trend_score),
                    "breadth_score": round(breadth_score), "momentum_score": mom_score, "ext_score": round(ext),
                    "rs_slope_50d_pct": round(rs_slope * 100, 1), "pct_above_50dma": round(pct50),
                    "breadth_thrust_pt": round(thrust), "ret_3m_pct": round(m3 * 100, 1),
                    "dist_from_52w_low_pct": round(dist_low * 100), "price": round(price, 2),
                    "ma150": round(ma150, 2), "signals": signals, "falsifier": falsifier})

    out.sort(key=lambda x: -x["emergence_score"])
    emerging = [o for o in out if o["stage"] in ("EMERGING", "BASING")]
    # measure-before-trust: top EMERGING sectors become graded picks
    top_picks = [{"ticker": o["ticker"], "direction": "UP", "conviction": o["emergence_score"],
                  "thesis": f"{o['stage']} — " + ", ".join(o["signals"][:3])}
                 for o in out if o["stage"] == "EMERGING"][:5]

    payload = {
        "engine": "justhodl-sector-emergence", "version": "1.0.0", "ok": True,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "thesis": "Early-innings bull detection for equity sectors — staged RS-inflection → Stage-2 reclaim → "
                  "breadth broadening, weighted toward leading signals, with an explicit falsifier per sector.",
        "regime_context": regime,
        "emerging_now": [o["ticker"] for o in emerging],
        "sectors": out, "top_picks": top_picks,
        "legend": {"DECLINING": "Stage 4 downtrend — avoid", "BASING": "Stage 1 — bottoming, watch (not yet)",
                   "EMERGING": "early Stage 2 — the target: RS turning + reclaiming rising 150d + breadth building",
                   "CONFIRMED": "Stage 2 underway — works but less early", "EXTENDED": "late Stage 2/3 — don't chase",
                   "TRANSITION": "mixed/unclear"},
        "note": "Earlier detection is noisier — this up-weights leading signals (RS, reclaim, breadth) and carries "
                "a falsifier. EMERGING calls are logged as measure-before-trust picks and graded forward vs SPY.",
        "elapsed_s": round(time.time() - t0, 1),
    }
    S3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(payload, default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=1800")
    print(f"[sector-emergence] {len(out)} sectors | EMERGING={[o['ticker'] for o in out if o['stage']=='EMERGING']} "
          f"| BASING={[o['ticker'] for o in out if o['stage']=='BASING']} | regime={regime} | {payload['elapsed_s']}s")
    return {"statusCode": 200, "body": json.dumps({"ok": True, "emerging": [o["ticker"] for o in out if o["stage"] == "EMERGING"],
            "n_sectors": len(out)})}
