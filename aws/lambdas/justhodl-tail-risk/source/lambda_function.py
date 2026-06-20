"""
justhodl-tail-risk — OPTION-IMPLIED CRASH PROBABILITY & TAIL INDEX (cross-index)
═════════════════════════════════════════════════════════════════════════════════
The market's risk-neutral return distribution is fat-tailed and left-skewed; a
log-normal model (what justhodl-implied-prob fell back to when options looked 403)
misses the crash wing entirely. With the Polygon options entitlement we reconstruct
the REAL risk-neutral density from the OTM IV smile and read the tail directly —
exactly how vol desks price crash protection.

PER INDEX (SPY, QQQ, IWM), front ~30d + ~60d monthly expiries:
  • Wing IVs: 10Δ / 25Δ put, ATM, 25Δ / 10Δ call
  • Put-skew slope (put10Δ IV − ATM IV)  ·  Risk reversal 25Δ & 10Δ (call IV − put IV)
  • Breeden-Litzenberger risk-neutral density from the BS-priced IV smile →
        F(K) = e^{rT}·∂Put/∂K  ⇒  implied P(drop ≥ 10%) and P(drop ≥ 20%) by expiry
  • Risk-neutral skewness/kurtosis (from the density) → SKEW-style index = 100 − 10·skew
  • Risk-reversal TERM structure (30d vs 60d): rising tail bid = hedging demand building

SYSTEM TAIL GAUGE: SPY-weighted composite 0-100 (tail stress), regime
  CALM / WATCH / ELEVATED / STRESSED, plus a tail-VALUATION read (protection
  CHEAP vs EXPENSIVE vs its own accrued history) — the timing input for tail-hedge.

This is a RISK/REGIME signal (not a stock pick): it feeds the risk stack
(tail-hedge timing, risk-regime tail block, crisis composites) and upgrades
implied-prob's SPY crash probabilities from log-normal to the true fat-tailed density.
"""
import json
import os
import time
import math
import calendar
import urllib.request
import urllib.error
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timezone

import boto3

REGION = "us-east-1"
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
OUT_KEY = "data/tail-risk.json"
HIST_KEY = "data/tail-risk-history.json"
POLY = os.environ.get("POLYGON_KEY", "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d")
S3 = boto3.client("s3", region_name=REGION)
UA = {"User-Agent": "JustHodl-tail-risk/1.0"}
BASE = "https://api.polygon.io"
R_FREE = 0.043   # short-horizon risk-free; immaterial at ~30-60d (e^{rT}≈1.003)

INDICES = [("SPY", 0.55), ("QQQ", 0.30), ("IWM", 0.15)]   # ticker, system weight


def _http(url, timeout=25):
    try:
        with urllib.request.urlopen(urllib.request.Request(url, headers=UA), timeout=timeout) as r:
            return json.loads(r.read().decode("utf-8", "replace"))
    except Exception:
        return None


def _ncdf(x):
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def bs_put(S, K, T, sigma, r=R_FREE):
    if sigma <= 0 or T <= 0:
        return max(K - S, 0.0)
    d1 = (math.log(S / K) + (r + 0.5 * sigma * sigma) * T) / (sigma * math.sqrt(T))
    d2 = d1 - sigma * math.sqrt(T)
    return K * math.exp(-r * T) * _ncdf(-d2) - S * _ncdf(-d1)


def third_fridays(n=4):
    out = []
    t = date.today(); y, m = t.year, t.month
    while len(out) < n + 1:
        cal = calendar.monthcalendar(y, m)
        fr = [w[calendar.FRIDAY] for w in cal if w[calendar.FRIDAY]]
        d = date(y, m, fr[2])
        if d >= t:
            out.append(d.isoformat())
        m += 1
        if m > 12:
            m = 1; y += 1
    return out


def fetch_wing(ticker, spot, exp):
    lo, hi = round(spot * 0.82, 2), round(spot * 1.12, 2)
    j = _http(f"{BASE}/v3/snapshot/options/{ticker}?expiration_date={exp}"
              f"&strike_price.gte={lo}&strike_price.lte={hi}&limit=250&apiKey={POLY}")
    return (j or {}).get("results") or []


def smile_points(contracts, spot):
    """OTM IV smile: puts for K<=spot, calls for K>spot. Returns sorted [(K, iv)]."""
    pts = {}
    for c in contracts:
        iv = c.get("implied_volatility")
        det = c.get("details") or {}
        K = det.get("strike_price"); typ = det.get("contract_type")
        if iv is None or K is None or iv <= 0.01:
            continue
        otm = (typ == "put" and K <= spot) or (typ == "call" and K > spot)
        if otm:
            pts[K] = iv
    return sorted(pts.items())


def interp_iv(smile, K):
    if not smile:
        return None
    if K <= smile[0][0]:
        return smile[0][1]
    if K >= smile[-1][0]:
        return smile[-1][1]
    for i in range(1, len(smile)):
        k0, v0 = smile[i - 1]; k1, v1 = smile[i]
        if k0 <= K <= k1:
            w = (K - k0) / (k1 - k0) if k1 > k0 else 0
            return v0 + w * (v1 - v0)
    return smile[-1][1]


def near_delta(contracts, typ, td):
    best = None; bd = 9
    for c in contracts:
        g = c.get("greeks") or {}; d = g.get("delta"); iv = c.get("implied_volatility")
        if d is None or iv is None or (c.get("details") or {}).get("contract_type") != typ:
            continue
        if abs(abs(d) - abs(td)) < bd:
            bd = abs(abs(d) - abs(td)); best = iv
    return best


def density_and_tail(smile, spot, T):
    """Breeden-Litzenberger from BS-priced IV smile → CDF, crash probs, RN skew/kurt."""
    if len(smile) < 5:
        return None
    lo, hi = 0.70 * spot, 1.25 * spot
    nstep = 240
    dK = (hi - lo) / nstep
    Ks = [lo + i * dK for i in range(nstep + 1)]
    P = []
    for K in Ks:
        sig = interp_iv(smile, K)
        P.append(bs_put(spot, K, T, sig))
    # CDF F(K) = e^{rT} dP/dK (central diff)
    erT = math.exp(R_FREE * T)
    F = [None] * len(Ks)
    for i in range(1, len(Ks) - 1):
        F[i] = min(1.0, max(0.0, erT * (P[i + 1] - P[i - 1]) / (2 * dK)))
    # density f = dF/dK
    f = [0.0] * len(Ks)
    for i in range(2, len(Ks) - 2):
        if F[i + 1] is not None and F[i - 1] is not None:
            f[i] = max(0.0, (F[i + 1] - F[i - 1]) / (2 * dK))
    area = sum(f[i] * dK for i in range(len(Ks)))
    if area <= 0:
        return None
    f = [x / area for x in f]
    # crash probabilities (interp F at strike levels)
    def Fat(level):
        Kt = level * spot
        for i in range(1, len(Ks)):
            if Ks[i] >= Kt:
                a, b = F[i - 1], F[i]
                if a is None or b is None:
                    return None
                w = (Kt - Ks[i - 1]) / dK
                return min(1.0, max(0.0, a + w * (b - a)))
        return None
    p10 = Fat(0.90); p20 = Fat(0.80); p5 = Fat(0.95)
    # RN moments in log-return space
    mu = sum(math.log(Ks[i] / spot) * f[i] * dK for i in range(len(Ks)) if Ks[i] > 0)
    var = sum((math.log(Ks[i] / spot) - mu) ** 2 * f[i] * dK for i in range(len(Ks)) if Ks[i] > 0)
    sd = math.sqrt(var) if var > 0 else None
    skew = kurt = None
    if sd and sd > 0:
        skew = sum((math.log(Ks[i] / spot) - mu) ** 3 * f[i] * dK for i in range(len(Ks)) if Ks[i] > 0) / sd ** 3
        kurt = sum((math.log(Ks[i] / spot) - mu) ** 4 * f[i] * dK for i in range(len(Ks)) if Ks[i] > 0) / sd ** 4
    return {"p_drop_5": p5, "p_drop_10": p10, "p_drop_20": p20,
            "rn_skew": round(skew, 3) if skew is not None else None,
            "rn_kurt": round(kurt, 2) if kurt is not None else None}


def analyze_index(ticker, spot):
    exps = third_fridays(4)
    front = next((e for e in exps if (date.fromisoformat(e) - date.today()).days >= 25), exps[0])
    back = next((e for e in exps if (date.fromisoformat(e) - date.today()).days >= 50), exps[-1])
    cf = fetch_wing(ticker, spot, front)
    cb = fetch_wing(ticker, spot, back) if back != front else cf
    if not cf:
        return None
    Tf = max((date.fromisoformat(front) - date.today()).days, 1) / 365.0
    sm = smile_points(cf, spot)
    atm = near_delta(cf, "put", 0.50) or interp_iv(sm, spot)
    p10iv = near_delta(cf, "put", 0.10); p25iv = near_delta(cf, "put", 0.25)
    c25iv = near_delta(cf, "call", 0.25); c10iv = near_delta(cf, "call", 0.10)
    put_skew_slope = round((p10iv - atm), 4) if (p10iv and atm) else None
    rr25 = round((c25iv - p25iv), 4) if (c25iv and p25iv) else None
    rr10 = round((c10iv - p10iv), 4) if (c10iv and p10iv) else None
    # back-month RR for term structure
    rr25_back = None
    if cb:
        p25b = near_delta(cb, "put", 0.25); c25b = near_delta(cb, "call", 0.25)
        if p25b and c25b:
            rr25_back = round(c25b - p25b, 4)
    rr_term = round(rr25_back - rr25, 4) if (rr25_back is not None and rr25 is not None) else None
    dens = density_and_tail(sm, spot, Tf) or {}
    skew_index = round(100 - 10 * dens["rn_skew"], 1) if dens.get("rn_skew") is not None else None
    return {
        "ticker": ticker, "spot": round(spot, 2), "front_exp": front, "back_exp": back,
        "atm_iv": round(atm, 4) if atm else None,
        "put10_iv": round(p10iv, 4) if p10iv else None, "put25_iv": round(p25iv, 4) if p25iv else None,
        "call25_iv": round(c25iv, 4) if c25iv else None,
        "put_skew_slope": put_skew_slope, "risk_reversal_25": rr25, "risk_reversal_10": rr10,
        "rr_term_slope": rr_term,
        "p_drop_5": round(dens["p_drop_5"], 4) if dens.get("p_drop_5") is not None else None,
        "p_drop_10": round(dens["p_drop_10"], 4) if dens.get("p_drop_10") is not None else None,
        "p_drop_20": round(dens["p_drop_20"], 4) if dens.get("p_drop_20") is not None else None,
        "rn_skew": dens.get("rn_skew"), "rn_kurt": dens.get("rn_kurt"),
        "skew_index": skew_index,
    }


def pct_rank(series, val):
    if not series or val is None:
        return None
    below = sum(1 for v in series if v is not None and v <= val)
    return round(below / len([v for v in series if v is not None]) * 100, 1)


def tail_stress(idx):
    """0-100 tail-stress score from skew steepness, risk-reversal, crash prob, skew index."""
    s = 0.0; w = 0.0
    if idx.get("put_skew_slope") is not None:
        s += 25 * min(1.0, max(0.0, idx["put_skew_slope"] / 0.10)); w += 25
    if idx.get("risk_reversal_25") is not None:
        s += 25 * min(1.0, max(0.0, -idx["risk_reversal_25"] / 0.06)); w += 25
    if idx.get("p_drop_10") is not None:
        s += 30 * min(1.0, idx["p_drop_10"] / 0.20); w += 30
    if idx.get("skew_index") is not None:
        s += 20 * min(1.0, max(0.0, (idx["skew_index"] - 100) / 50)); w += 20
    return round(s / w * 100, 1) if w else None


def lambda_handler(event=None, context=None):
    t0 = time.time()
    try:
        hist = json.loads(S3.get_object(Bucket=BUCKET, Key=HIST_KEY)["Body"].read())
    except Exception:
        hist = {}

    spots = {}
    with ThreadPoolExecutor(max_workers=4) as ex:
        futs = {ex.submit(_http, f"{BASE}/v2/aggs/ticker/{t}/prev?adjusted=true&apiKey={POLY}"): t for t, _ in INDICES}
        for f in as_completed(futs):
            j = f.result(); res = (j or {}).get("results") or []
            if res:
                spots[futs[f]] = res[0].get("c")

    rows = []
    with ThreadPoolExecutor(max_workers=3) as ex:
        futs = {ex.submit(analyze_index, t, spots[t]): t for t, _ in INDICES if spots.get(t)}
        for f in as_completed(futs):
            r = f.result()
            if r:
                r["tail_stress"] = tail_stress(r)
                rows.append(r)
    rows_by = {r["ticker"]: r for r in rows}

    today = date.today().isoformat()
    # accrue history + percentile ranks
    for r in rows:
        ser = hist.get(r["ticker"]) or []
        if not ser or ser[-1].get("d") != today:
            ser.append({"d": today, "skew_slope": r.get("put_skew_slope"),
                        "rr25": r.get("risk_reversal_25"), "p10": r.get("p_drop_10"),
                        "skew_index": r.get("skew_index")})
        hist[r["ticker"]] = ser[-260:]
        s_ser = [x.get("skew_slope") for x in hist[r["ticker"]]]
        r["skew_slope_pctile"] = pct_rank(s_ser, r.get("put_skew_slope"))
        p_ser = [x.get("p10") for x in hist[r["ticker"]]]
        r["crash_prob_pctile"] = pct_rank(p_ser, r.get("p_drop_10"))
    try:
        S3.put_object(Bucket=BUCKET, Key=HIST_KEY, Body=json.dumps(hist, default=str).encode(),
                      ContentType="application/json")
    except Exception as e:
        print(f"[tail-risk] hist write fail: {e}")

    # system gauge (SPY-weighted)
    tw = sw = 0.0
    for t, wt in INDICES:
        r = rows_by.get(t)
        if r and r.get("tail_stress") is not None:
            tw += wt * r["tail_stress"]; sw += wt
    system_tail = round(tw / sw, 1) if sw else None
    if system_tail is None:
        regime = "n/a"
    elif system_tail >= 70:
        regime = "STRESSED"
    elif system_tail >= 50:
        regime = "ELEVATED"
    elif system_tail >= 30:
        regime = "WATCH"
    else:
        regime = "CALM"

    # tail valuation (cheap/expensive) from SPY crash-prob percentile
    spy = rows_by.get("SPY") or {}
    cp_pct = spy.get("crash_prob_pctile")
    spy_hist_n = len(hist.get("SPY") or [])
    if cp_pct is None or spy_hist_n < 15:
        valuation = "WARMING"     # percentile needs ~15+ daily points to be meaningful
    elif cp_pct <= 30:
        valuation = "CHEAP"        # crash protection underpriced vs own history → good to hedge
    elif cp_pct >= 70:
        valuation = "EXPENSIVE"    # tail already bid → fade / don't chase
    else:
        valuation = "FAIR"

    payload = {
        "engine": "justhodl-tail-risk", "version": "1.0.0", "ok": bool(rows),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "thesis": ("Option-implied crash probability and tail index from the real risk-neutral "
                   "density (Breeden-Litzenberger on the OTM IV smile) — fat-tailed, unlike a "
                   "log-normal model. Put-skew slope + risk-reversal term structure + implied "
                   "P(drop) read how aggressively the market is pricing a left-tail event."),
        "system_tail_gauge": system_tail, "tail_regime": regime, "tail_valuation": valuation,
        "indices": rows,
        "interpretation": {
            "system_tail_gauge": "0-100 SPY-weighted tail stress (skew + risk-reversal + crash prob + skew index)",
            "tail_valuation": "CHEAP = crash protection underpriced vs own history (favourable to add hedges); EXPENSIVE = tail already bid",
            "risk_reversal_25": "call25 IV − put25 IV; negative = puts bid (downside fear)",
            "rr_term_slope": "back-month minus front RR; falling (more negative back) = tail bid building further out",
            "p_drop_10": "risk-neutral implied probability of a >=10% decline by the front expiry",
        },
        "data_source": "Polygon /v3/snapshot/options OTM IV smile (greeks+IV) + Breeden-Litzenberger",
        "caveats": [
            "Risk-neutral (not real-world) probabilities — they embed the variance risk premium, "
            "so implied P(drop) overstates physical odds; use for relative/timing reads.",
            "Reconstructed from EOD smile (no live NBBO); 30d front + 60d back monthly expiries.",
            "Feeds the risk stack (tail-hedge timing, risk-regime, crisis composites) and upgrades "
            "implied-prob's SPY crash probs from log-normal to this fat-tailed density.",
        ],
        "elapsed_s": round(time.time() - t0, 1),
    }
    S3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(payload, default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=1800")
    print(f"[tail-risk] system={system_tail} regime={regime} val={valuation} "
          f"idx={len(rows)} in {payload['elapsed_s']}s")
    return {"statusCode": 200, "body": json.dumps({
        "ok": bool(rows), "system_tail_gauge": system_tail, "regime": regime, "valuation": valuation,
        "indices": [(r["ticker"], r["tail_stress"], r.get("p_drop_10"), r.get("skew_index"),
                     r.get("risk_reversal_25")) for r in rows]})}
