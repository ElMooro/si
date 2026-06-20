"""
justhodl-options-analytics — PER-NAME OPTIONS ANALYTICS (SpotGamma / Unusual-Whales class)
═════════════════════════════════════════════════════════════════════════════════════════
Built on the Polygon options entitlement (confirmed ops 2011-2012: /v3/snapshot/options/
{ticker} returns per-contract greeks + implied_volatility + open_interest + day volume for
ANY name). Prior engines (options-gamma = indices only; rv-iv-scanner = VIX proxy;
polygon-options-flow = dormant) could not do per-name greeks. This is the authoritative
single-name engine.

PER NAME (liquid options universe), computes from 3 monthly expiries (3rd Fridays) × a
±30% strike window:

  GAMMA
    • net GEX ($ per 1% move) = Σ_calls γ·OI·100·S²·0.01  −  Σ_puts γ·OI·100·S²·0.01
    • gamma regime: SHORT (net<0, dealers amplify moves → squeeze-prone) vs
                    LONG  (net>0, dealers dampen → pinning/mean-revert)
    • gamma flip strike (where cumulative GEX-by-strike crosses zero)
    • call wall / put wall (largest positive/negative gamma strikes = resistance/support)

  IMPLIED VOL
    • ATM IV per expiry → term structure slope (M3−M1): BACKWARDATION = event/stress
    • 25-delta skew (front monthly): put25_IV − call25_IV (downside fear)
    • IV vs 20d realized (VRP): positive = options rich
    • IV rank (accrues from daily ATM-IV history snapshots; warms up)

  FLOW / UNUSUAL ACTIVITY
    • put/call volume + OI ratios, net dollar premium (call$ − put$)
    • unusual contracts (volume/OI > 2 and volume material) = fresh positioning

  COMPOSITE SIGNAL per name: GAMMA_SQUEEZE_SETUP / SQUEEZE_PRIMED / DOWNSIDE_HEDGING /
  PINNED / NEUTRAL.  top_picks (bullish short-gamma + call-heavy flow + price confirm) →
  signal-harvester (eng:options-analytics) → MEASURE-BEFORE-TRUST forward-excess-vs-SPY
  grading; NOT wired into decision engines until alpha-proven.
"""
import json
import os
import time
import math
import calendar
import urllib.request
import urllib.error
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta, timezone

import boto3

REGION = "us-east-1"
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
OUT_KEY = "data/options-analytics.json"
HIST_KEY = "data/options-analytics-iv-history.json"   # {ticker: [[date, atm_iv], ...]}
POLY = os.environ.get("POLYGON_KEY", "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d")
S3 = boto3.client("s3", region_name=REGION)
UA = {"User-Agent": "JustHodl-options-analytics/1.0"}
BASE = "https://api.polygon.io"

# Liquid single-name options universe (mega-cap + high-options + squeeze/high-vol)
UNIVERSE = [
    "AAPL", "MSFT", "NVDA", "AMZN", "META", "GOOGL", "TSLA", "AMD", "AVGO", "NFLX",
    "MU", "INTC", "SMCI", "ARM", "PLTR", "CRM", "ORCL", "ADBE", "QCOM", "MRVL",
    "SPY", "QQQ", "IWM",
    "GME", "AMC", "COIN", "MARA", "RIOT", "MSTR", "HOOD", "SOFI", "RIVN", "LCID",
    "CVNA", "UPST", "AFRM", "RBLX", "SNAP", "DKNG", "CHWY", "ASTS", "RGTI", "IONQ",
    "BA", "DIS", "BAC", "JPM", "XOM", "CVX", "UNH", "LLY", "WMT", "F", "GM",
    "PYPL", "UBER", "NKE", "DELL",
]


def _http(url, timeout=20):
    req = urllib.request.Request(url, headers=UA)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8", "replace"))


def _safe_http(url, timeout=20):
    try:
        return _http(url, timeout)
    except Exception:
        return None


def third_fridays(n=3):
    """Next n monthly option expiries (3rd Friday of the month) >= today."""
    out = []
    today = date.today()
    y, m = today.year, today.month
    while len(out) < n + 1:
        cal = calendar.monthcalendar(y, m)
        fris = [w[calendar.FRIDAY] for w in cal if w[calendar.FRIDAY] != 0]
        tf = date(y, m, fris[2])
        if tf >= today:
            out.append(tf.isoformat())
        m += 1
        if m > 12:
            m = 1
            y += 1
    return out[:n]


def stock_history(ticker):
    """40 daily closes → spot (latest), 20d realized vol (annualized), 20d MA, 52w-ish high proxy."""
    end = date.today()
    start = end - timedelta(days=70)
    j = _safe_http(f"{BASE}/v2/aggs/ticker/{ticker}/range/1/day/{start.isoformat()}/{end.isoformat()}?adjusted=true&sort=asc&limit=60&apiKey={POLY}")
    res = (j or {}).get("results") or []
    closes = [r.get("c") for r in res if r.get("c")]
    if len(closes) < 21:
        return None
    spot = closes[-1]
    ma20 = sum(closes[-20:]) / 20.0
    recent_high = max(closes[-40:]) if len(closes) >= 40 else max(closes)
    rets = [math.log(closes[i] / closes[i - 1]) for i in range(len(closes) - 20, len(closes)) if closes[i - 1]]
    if len(rets) >= 2:
        mean = sum(rets) / len(rets)
        var = sum((x - mean) ** 2 for x in rets) / (len(rets) - 1)
        hv20 = math.sqrt(var) * math.sqrt(252)
    else:
        hv20 = None
    return {"spot": spot, "ma20": ma20, "recent_high": recent_high, "hv20": hv20}


def fetch_expiry_chain(ticker, spot, exp):
    """One snapshot page for a single expiration within a ±30% strike window."""
    lo, hi = round(spot * 0.70, 2), round(spot * 1.30, 2)
    url = (f"{BASE}/v3/snapshot/options/{ticker}?expiration_date={exp}"
           f"&strike_price.gte={lo}&strike_price.lte={hi}&limit=250&apiKey={POLY}")
    j = _safe_http(url, timeout=25)
    return (j or {}).get("results") or []


def _atm_iv(contracts, spot):
    """Average call+put IV at the strike nearest spot."""
    by_strike = {}
    for c in contracts:
        iv = c.get("implied_volatility")
        st = (c.get("details") or {}).get("strike_price")
        if iv is None or st is None:
            continue
        by_strike.setdefault(st, []).append(iv)
    if not by_strike:
        return None
    nearest = min(by_strike.keys(), key=lambda s: abs(s - spot))
    ivs = by_strike[nearest]
    return sum(ivs) / len(ivs)


def _skew_25d(contracts):
    """25-delta skew: IV of ~-0.25-delta put minus IV of ~+0.25-delta call."""
    put_c = call_c = None
    put_d = call_d = 1e9
    for c in contracts:
        g = c.get("greeks") or {}
        d = g.get("delta")
        iv = c.get("implied_volatility")
        typ = (c.get("details") or {}).get("contract_type")
        if d is None or iv is None:
            continue
        if typ == "put" and abs(abs(d) - 0.25) < put_d:
            put_d = abs(abs(d) - 0.25); put_c = iv
        elif typ == "call" and abs(d - 0.25) < call_d:
            call_d = abs(d - 0.25); call_c = iv
    if put_c is not None and call_c is not None:
        return round(put_c - call_c, 4)
    return None


def analyze(ticker, hist, iv_hist):
    spot = hist["spot"]
    exps = third_fridays(3)
    chains = {}
    for e in exps:
        ch = fetch_expiry_chain(ticker, spot, e)
        if ch:
            chains[e] = ch
    if not chains:
        return None
    all_c = [c for ch in chains.values() for c in ch]

    # ── GAMMA ──
    gex_by_strike = {}
    net_gex = 0.0
    for c in all_c:
        g = c.get("greeks") or {}
        gam = g.get("gamma")
        oi = c.get("open_interest")
        st = (c.get("details") or {}).get("strike_price")
        typ = (c.get("details") or {}).get("contract_type")
        if gam is None or not oi or st is None:
            continue
        notional = gam * oi * 100 * spot * spot * 0.01  # $ per 1% move
        signed = notional if typ == "call" else -notional
        net_gex += signed
        gex_by_strike[st] = gex_by_strike.get(st, 0.0) + signed

    gamma_regime = "SHORT_GAMMA" if net_gex < 0 else "LONG_GAMMA"
    # flip strike: cumulative GEX across strikes ascending crosses zero
    flip = None
    strikes_sorted = sorted(gex_by_strike.keys())
    cum = 0.0
    prev_s = None
    for s in strikes_sorted:
        nc = cum + gex_by_strike[s]
        if prev_s is not None and ((cum < 0 <= nc) or (cum > 0 >= nc)):
            flip = round((prev_s + s) / 2, 2)
            break
        cum = nc
        prev_s = s
    # walls
    call_wall = max((s for s in gex_by_strike if gex_by_strike[s] > 0 and s >= spot),
                    key=lambda s: gex_by_strike[s], default=None)
    put_wall = min((s for s in gex_by_strike if gex_by_strike[s] < 0 and s <= spot),
                   key=lambda s: gex_by_strike[s], default=None)

    # ── IV term structure + skew ──
    atm_by_exp = {e: _atm_iv(ch, spot) for e, ch in chains.items()}
    atm_front = atm_by_exp.get(exps[0]) if exps else None
    atm_back = next((atm_by_exp.get(e) for e in reversed(exps) if atm_by_exp.get(e)), None)
    term_slope = round(atm_back - atm_front, 4) if (atm_front and atm_back) else None
    if term_slope is None:
        term_label = "n/a"
    elif term_slope < -0.02:
        term_label = "BACKWARDATION"
    elif term_slope > 0.02:
        term_label = "CONTANGO"
    else:
        term_label = "FLAT"
    skew = _skew_25d(chains.get(exps[0], []))
    # IV vs realized
    hv20 = hist.get("hv20")
    vrp = round(atm_front - hv20, 4) if (atm_front and hv20) else None
    # IV rank from accrued history
    series = [v for _, v in (iv_hist.get(ticker) or []) if v]
    iv_rank = None
    if atm_front and len(series) >= 20:
        below = sum(1 for v in series if v <= atm_front)
        iv_rank = round(below / len(series) * 100, 1)

    # ── flow / unusual ──
    call_vol = put_vol = call_oi = put_oi = 0
    call_prem = put_prem = 0.0
    unusual = []
    for c in all_c:
        typ = (c.get("details") or {}).get("contract_type")
        day = c.get("day") or {}
        vol = day.get("volume") or 0
        vwap = day.get("vwap") or day.get("close") or 0
        oi = c.get("open_interest") or 0
        prem = vol * vwap * 100
        if typ == "call":
            call_vol += vol; call_oi += oi; call_prem += prem
        elif typ == "put":
            put_vol += vol; put_oi += oi; put_prem += prem
        if vol >= 500 and oi > 0 and vol / oi >= 2.0:
            unusual.append({"contract": (c.get("details") or {}).get("ticker"),
                            "type": typ, "strike": (c.get("details") or {}).get("strike_price"),
                            "exp": (c.get("details") or {}).get("expiration_date"),
                            "vol": int(vol), "oi": int(oi), "vol_oi": round(vol / oi, 1)})
    pcr_vol = round(put_vol / call_vol, 3) if call_vol else None
    pcr_oi = round(put_oi / call_oi, 3) if call_oi else None
    net_premium = round(call_prem - put_prem)
    unusual.sort(key=lambda x: x["vol_oi"], reverse=True)

    # ── composite signal + score ──
    price_confirm = spot > hist["ma20"] or spot >= 0.90 * hist["recent_high"]
    bullish_flow = (pcr_vol is not None and pcr_vol < 0.8) or net_premium > 0
    bearish_flow = (pcr_vol is not None and pcr_vol > 1.3) and net_premium < 0
    iv_expanding = (vrp is not None and vrp > 0) or (iv_rank is not None and iv_rank > 60)
    reasons = []
    if gamma_regime == "SHORT_GAMMA" and bullish_flow and (iv_expanding or len(unusual) >= 2):
        signal = "GAMMA_SQUEEZE_SETUP"
        reasons.append("dealers short gamma (amplify up-moves)")
        if bullish_flow: reasons.append("call-heavy flow")
        if iv_expanding: reasons.append("IV expanding")
    elif gamma_regime == "SHORT_GAMMA" and call_wall and spot >= 0.97 * call_wall:
        signal = "SQUEEZE_PRIMED"; reasons.append(f"short gamma, spot near call wall {call_wall}")
    elif (skew is not None and skew > 0.06) and bearish_flow:
        signal = "DOWNSIDE_HEDGING"; reasons.append(f"steep put skew {round(skew*100,1)}pts + put-heavy flow")
    elif gamma_regime == "LONG_GAMMA" and (vrp is not None and vrp < 0.02):
        signal = "PINNED"; reasons.append("dealers long gamma (vol suppressed)")
    else:
        signal = "NEUTRAL"

    # 0-100 bullish "options ignition" score
    score = 0.0
    if net_gex < 0:
        score += 30 * min(1.0, abs(net_gex) / 5e7)
    if pcr_vol is not None:
        score += 30 * max(0.0, min(1.0, (1.0 - pcr_vol) / 0.6)) if pcr_vol < 1 else 0
    if net_premium > 0:
        score += 10 * min(1.0, net_premium / 5e7)
    if vrp is not None and vrp > 0:
        score += 15 * min(1.0, vrp / 0.15)
    score += 15 * min(1.0, len(unusual) / 5.0)
    score = round(min(100.0, score), 1)

    return {
        "ticker": ticker, "spot": round(spot, 2),
        "signal": signal, "score": score, "price_confirm": price_confirm,
        "gamma_regime": gamma_regime,
        "net_gex_musd_per_1pct": round(net_gex / 1e6, 2),
        "gamma_flip_strike": flip, "call_wall": call_wall, "put_wall": put_wall,
        "atm_iv_front": round(atm_front, 4) if atm_front else None,
        "term_slope": term_slope, "term_structure": term_label,
        "skew_25d": skew, "hv20": round(hv20, 4) if hv20 else None,
        "vrp": vrp, "iv_rank": iv_rank,
        "pcr_vol": pcr_vol, "pcr_oi": pcr_oi,
        "call_vol": int(call_vol), "put_vol": int(put_vol),
        "net_premium_usd": net_premium,
        "n_unusual": len(unusual), "unusual": unusual[:6],
        "expiries": list(chains.keys()),
        "reasons": reasons,
        "_atm_front": atm_front,  # for history write (stripped before output)
    }


def lambda_handler(event=None, context=None):
    t0 = time.time()
    try:
        iv_hist = json.loads(S3.get_object(Bucket=BUCKET, Key=HIST_KEY)["Body"].read())
    except Exception:
        iv_hist = {}

    # 1) stock history (spot/HV/MA) for the universe, threaded
    hists = {}
    with ThreadPoolExecutor(max_workers=10) as ex:
        futs = {ex.submit(stock_history, t): t for t in UNIVERSE}
        for f in as_completed(futs):
            h = f.result()
            if h:
                hists[futs[f]] = h

    # 2) analyze options per name, threaded
    rows = []
    with ThreadPoolExecutor(max_workers=8) as ex:
        futs = {ex.submit(analyze, t, hists[t], iv_hist): t for t in UNIVERSE if t in hists}
        for f in as_completed(futs):
            r = f.result()
            if r:
                rows.append(r)

    # 3) accrue ATM-IV history (cap ~260 trading days)
    today = date.today().isoformat()
    for r in rows:
        atm = r.pop("_atm_front", None)
        if atm:
            ser = iv_hist.get(r["ticker"]) or []
            if not ser or ser[-1][0] != today:
                ser.append([today, round(atm, 4)])
            iv_hist[r["ticker"]] = ser[-260:]
    try:
        S3.put_object(Bucket=BUCKET, Key=HIST_KEY,
                      Body=json.dumps(iv_hist, default=str).encode(), ContentType="application/json")
    except Exception as e:
        print(f"[options-analytics] iv-history write fail: {e}")

    rows.sort(key=lambda r: r["score"], reverse=True)

    short_gamma = [r for r in rows if r["gamma_regime"] == "SHORT_GAMMA"]
    squeeze_setups = [r for r in rows if r["signal"] in ("GAMMA_SQUEEZE_SETUP", "SQUEEZE_PRIMED")]
    hedging = [r for r in rows if r["signal"] == "DOWNSIDE_HEDGING"]
    most_unusual = sorted(rows, key=lambda r: r["n_unusual"], reverse=True)
    rich_iv = sorted([r for r in rows if r.get("vrp") is not None],
                     key=lambda r: r["vrp"], reverse=True)

    top_picks = [{"ticker": r["ticker"], "score": r["score"], "direction": "long",
                  "signal": r["signal"], "gamma_regime": r["gamma_regime"],
                  "net_gex_musd": r["net_gex_musd_per_1pct"], "pcr_vol": r["pcr_vol"],
                  "price_confirm": r["price_confirm"], "reasons": r["reasons"]}
                 for r in rows
                 if r["signal"] in ("GAMMA_SQUEEZE_SETUP", "SQUEEZE_PRIMED")][:20]

    payload = {
        "engine": "justhodl-options-analytics", "version": "1.0.0", "ok": True,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "thesis": ("Per-name options analytics on the Polygon options entitlement: dealer "
                   "gamma exposure (GEX/flip/walls), IV term structure + 25-delta skew + "
                   "IV-vs-realized, and unusual options activity. Gamma regime tells you "
                   "whether dealers amplify (short gamma → squeeze-prone) or dampen (long "
                   "gamma → pinning) moves."),
        "universe_size": len(UNIVERSE), "n_analyzed": len(rows),
        "distribution": {
            "short_gamma": len(short_gamma), "long_gamma": len(rows) - len(short_gamma),
            "squeeze_setups": len(squeeze_setups), "downside_hedging": len(hedging),
        },
        "board": rows,
        "top_picks": top_picks,
        "squeeze_setups": squeeze_setups[:15],
        "most_unusual": [{"ticker": r["ticker"], "n_unusual": r["n_unusual"],
                          "pcr_vol": r["pcr_vol"], "net_premium_usd": r["net_premium_usd"],
                          "unusual": r["unusual"][:3]} for r in most_unusual[:15] if r["n_unusual"]],
        "richest_iv_vrp": [{"ticker": r["ticker"], "vrp": r["vrp"], "atm_iv": r["atm_iv_front"],
                            "hv20": r["hv20"]} for r in rich_iv[:12]],
        "data_source": "Polygon /v3/snapshot/options (greeks+IV+OI+volume) + /v2/aggs (spot/HV)",
        "caveats": [
            "Discovery engine — MEASURE-BEFORE-TRUST. top_picks logged to signal-harvester "
            "(eng:options-analytics) for forward excess-vs-SPY grading; NOT wired into "
            "best-setups/master-ranker until the scorecard proves net-of-cost alpha.",
            "GEX from 3 monthly expiries × ±30% strikes (where dealer gamma concentrates); "
            "OI is EOD/T+1 settled, greeks/IV/volume current. No real-time NBBO (tick tier gated).",
            "IV rank warms up as daily ATM-IV history accrues; VRP (IV vs 20d realized) is the "
            "day-1 rich/cheap gauge.",
        ],
        "elapsed_s": round(time.time() - t0, 1),
    }
    S3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(payload, default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=1800")
    print(f"[options-analytics] analyzed={len(rows)} short_gamma={len(short_gamma)} "
          f"setups={len(squeeze_setups)} picks={len(top_picks)} in {payload['elapsed_s']}s")
    return {"statusCode": 200, "body": json.dumps({
        "ok": True, "n_analyzed": len(rows), "n_picks": len(top_picks),
        "distribution": payload["distribution"],
        "top": [(r["ticker"], r["score"], r["signal"], r["net_gex_musd_per_1pct"]) for r in rows[:8]]})}
