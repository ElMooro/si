"""
justhodl-regime-map — THE RISK MAP: what's booming (risk-on) vs what's getting destroyed (risk-off)
══════════════════════════════════════════════════════════════════════════════════════════════════
A single risk-on/off number hides the most important thing about a tape: DISPERSION. In Oct'25→Jun'26
the cap-weighted S&P rose +11.7% while crypto fell 50-60% — two regimes at once. A multi-strat desk
never reads one number; it reads the cross-asset MAP: which sleeves are ripping, which are bleeding,
and whether the rally is BROAD, NARROW (megacap-led), or BIFURCATED (equities up / crypto down).

This engine decomposes the market into institutional sleeves (equity breadth/size/style, the 11 GICS
sectors, factors, crypto, rates & credit, commodities & havens, international), scores each on a
−100..+100 risk-on scale from multi-window momentum + trend, ranks the whole board booming→destroyed,
and classifies the REGIME via a dispersion read (cap-vs-equal-weight concentration spread, equity
breadth %, equity-vs-crypto split). Output: data/regime-map.json → regime-map.html + main-page ribbon.
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

# institutional sleeves — liquid, Polygon-covered ETFs + crypto
UNIVERSE = {
    "Equity · Breadth & Size": [
        ("SPY", "S&P 500 (cap-weight)"), ("RSP", "S&P 500 (equal-weight)"), ("IWM", "Small caps"),
        ("MDY", "Mid caps"), ("MAGS", "Magnificent 7"), ("IWF", "Large Growth"), ("IWD", "Large Value")],
    "Equity · Sectors": [
        ("XLK", "Technology"), ("XLC", "Communications"), ("XLY", "Cons. Discretionary"),
        ("XLF", "Financials"), ("XLI", "Industrials"), ("XLE", "Energy"), ("XLB", "Materials"),
        ("XLV", "Health Care"), ("XLP", "Cons. Staples"), ("XLU", "Utilities"), ("XLRE", "Real Estate")],
    "Equity · Factors": [
        ("MTUM", "Momentum"), ("QUAL", "Quality"), ("USMV", "Low Volatility"), ("VLUE", "Value")],
    "Crypto": [("BTCUSD", "Bitcoin"), ("ETHUSD", "Ethereum"), ("SOLUSD", "Solana")],
    "Rates & Credit": [
        ("TLT", "Long Treasuries 20y+"), ("IEF", "Treasuries 7-10y"), ("LQD", "IG Credit"),
        ("HYG", "High-Yield Credit"), ("TIP", "TIPS / Inflation")],
    "Commodities & Havens": [
        ("GLD", "Gold"), ("SLV", "Silver"), ("USO", "Oil"), ("DBC", "Broad Commodities"), ("UUP", "US Dollar")],
    "International": [("EFA", "Developed ex-US"), ("EEM", "Emerging Mkts"), ("FXI", "China")],
}
CRYPTO = {"BTCUSD", "ETHUSD", "SOLUSD"}


def fetch(ticker):
    pre = "X:" if ticker in CRYPTO else ""
    frm = (datetime.now(timezone.utc) - timedelta(days=320)).strftime("%Y-%m-%d")
    to = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    url = (f"https://api.polygon.io/v2/aggs/ticker/{pre}{ticker}/range/1/day/{frm}/{to}"
           f"?adjusted=true&sort=asc&limit=400&apiKey={POLY}")
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "jh-regime"})
        r = json.loads(urllib.request.urlopen(req, timeout=25).read())
        return ticker, [x["c"] for x in r.get("results", []) if x.get("c")]
    except Exception:
        return ticker, []


def clamp(v, lo, hi):
    return max(lo, min(hi, v))


def score(closes):
    if len(closes) < 130:
        return None
    last = closes[-1]
    def ret(n):
        return (last / closes[-1 - n] - 1) if len(closes) > n else 0.0
    r21, r63, r126 = ret(21), ret(63), ret(126)
    ma50 = sum(closes[-50:]) / 50
    ma200 = sum(closes[-200:]) / 200 if len(closes) >= 200 else sum(closes) / len(closes)
    pts = (1 if last > ma50 else 0) + (1 if last > ma200 else 0) + (1 if ma50 > ma200 else 0)
    trend = (pts - 1.5) / 1.5  # -1..+1
    mom = 0.45 * r63 + 0.30 * r126 + 0.25 * r21  # decimal
    risk_on = round(clamp(mom * 100 * 2.2 + trend * 25, -100, 100))
    return {"risk_on": risk_on, "r1m": round(r21 * 100, 1), "r3m": round(r63 * 100, 1),
            "r6m": round(r126 * 100, 1), "above_ma200": last > ma200}


def state_of(ro):
    if ro >= 45:
        return "BOOMING"
    if ro >= 15:
        return "RISK-ON"
    if ro > -15:
        return "NEUTRAL"
    if ro > -45:
        return "RISK-OFF"
    return "DESTROYED"


def lambda_handler(event=None, context=None):
    t0 = time.time()
    # prior snapshot for transition detection
    try:
        prev = json.loads(S3.get_object(Bucket=BUCKET, Key="data/regime-map-prev.json")["Body"].read())
    except Exception:
        prev = None
    all_tk = [(t, nm, sleeve) for sleeve, lst in UNIVERSE.items() for (t, nm) in lst]
    with ThreadPoolExecutor(max_workers=20) as ex:
        closes = dict(ex.map(fetch, [t for t, _, _ in all_tk]))

    sleeves = {s: [] for s in UNIVERSE}
    ranked = []
    lookup = {}
    for t, nm, sleeve in all_tk:
        sc = score(closes.get(t, []))
        if not sc:
            continue
        item = {"ticker": t.replace("USD", "") if t in CRYPTO else t, "name": nm, "sleeve": sleeve,
                "risk_on": sc["risk_on"], "state": state_of(sc["risk_on"]),
                "r1m": sc["r1m"], "r3m": sc["r3m"], "r6m": sc["r6m"]}
        sleeves[sleeve].append(item)
        ranked.append(item)
        lookup[t] = item
    for s in sleeves:
        sleeves[s].sort(key=lambda x: -x["risk_on"])
    ranked.sort(key=lambda x: -x["risk_on"])

    # dispersion / regime read
    def avg(keys):
        vals = [lookup[k]["risk_on"] for k in keys if k in lookup]
        return round(sum(vals) / len(vals), 1) if vals else 0.0
    eq_keys = [t for t, _, s in all_tk if s.startswith("Equity")]
    eq_avg = avg(eq_keys)
    crypto_avg = avg(["BTCUSD", "ETHUSD", "SOLUSD"])
    rates_avg = avg(["TLT", "IEF", "LQD", "HYG", "TIP"])
    commod_avg = avg(["GLD", "SLV", "USO", "DBC", "UUP"])
    intl_avg = avg(["EFA", "EEM", "FXI"])
    eq_breadth_pct = round(100 * sum(1 for k in eq_keys if k in lookup and lookup[k]["risk_on"] > 0)
                           / max(1, sum(1 for k in eq_keys if k in lookup)))
    spy = lookup.get("SPY", {}).get("r3m", 0.0)
    rsp = lookup.get("RSP", {}).get("r3m", 0.0)
    conc = round(spy - rsp, 1)  # cap-weight minus equal-weight (3m) — concentration tell

    if eq_avg > 8 and crypto_avg < -15:
        label = "BIFURCATED"
        summary = (f"Two regimes at once: equities broadly risk-on (avg {eq_avg:+}) while crypto is risk-off "
                   f"(avg {crypto_avg:+}). The index strength is real and broad — it is NOT masking weak breadth.")
    elif eq_avg > 8 and eq_breadth_pct >= 62 and conc < 4:
        label = "BROAD RISK-ON"
        summary = (f"Broad-based equity strength: {eq_breadth_pct}% of equity sleeves risk-on, equal-weight "
                   f"keeping pace (concentration spread only {conc:+}pts). Healthy participation, not a narrow melt-up.")
    elif eq_avg > 8 and (eq_breadth_pct < 45 or conc >= 5):
        label = "NARROW / CONCENTRATED"
        summary = (f"Index up but breadth thin — concentration spread {conc:+}pts, only {eq_breadth_pct}% of "
                   f"equity sleeves risk-on. A megacap-led tape sitting over a weak median stock.")
    elif eq_avg < -8:
        label = "BROAD RISK-OFF"
        summary = (f"Broad equity weakness (avg {eq_avg:+}, breadth {eq_breadth_pct}%). Risk-off across the board.")
    else:
        label = "MIXED / ROTATIONAL"
        summary = (f"No dominant regime — equities mixed (avg {eq_avg:+}, breadth {eq_breadth_pct}%), "
                   f"crypto {crypto_avg:+}, rates {rates_avg:+}. A rotational, stock-picker's tape.")

    # ── TRANSITIONS: where the tape is TURNING (the alpha is in the turn, not the trend) ──
    RANK = {"DESTROYED": 0, "RISK-OFF": 1, "NEUTRAL": 2, "RISK-ON": 3, "BOOMING": 4}
    transitions = []
    if prev and prev.get("states"):
        ps = prev["states"]
        for it in ranked:
            tk = it["ticker"]
            p = ps.get(tk)
            if not p:
                continue
            d_ro = it["risk_on"] - p.get("risk_on", 0)
            if it["state"] == p.get("state") and abs(d_ro) < 18:
                continue
            pr, cr = RANK.get(p.get("state"), 2), RANK[it["state"]]
            if cr > pr and cr >= 2 and pr <= 1:
                kind = "BOTTOMING / RECOVERING"
            elif cr >= 4 and pr < 4:
                kind = "ACCELERATING"
            elif cr < pr and pr >= 3 and cr <= 2:
                kind = "ROLLING OVER"
            elif cr == 0 and pr > 0:
                kind = "CAPITULATING"
            elif p.get("risk_on", 0) <= 0 < it["risk_on"]:
                kind = "FLIPPED RISK-ON"
            elif p.get("risk_on", 0) >= 0 > it["risk_on"]:
                kind = "FLIPPED RISK-OFF"
            elif d_ro >= 18:
                kind = "STRENGTHENING"
            else:
                kind = "WEAKENING"
            transitions.append({"ticker": tk, "name": it["name"], "sleeve": it["sleeve"],
                                "from": p.get("state"), "to": it["state"], "delta": d_ro, "kind": kind})
        transitions.sort(key=lambda x: -abs(x["delta"]))
    prev_label = (prev or {}).get("label")
    regime_changed = bool(prev_label and prev_label != label)

    # persist current snapshot for next run's diff
    try:
        S3.put_object(Bucket=BUCKET, Key="data/regime-map-prev.json",
                      Body=json.dumps({"generated_at": datetime.now(timezone.utc).isoformat(), "label": label,
                                       "states": {it["ticker"]: {"risk_on": it["risk_on"], "state": it["state"]}
                                                  for it in ranked}}).encode(), ContentType="application/json")
    except Exception:
        pass

    payload = {
        "engine": "justhodl-regime-map", "version": "1.0.0", "ok": True,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "thesis": "Cross-asset dispersion map — what's booming (risk-on) vs getting destroyed (risk-off), "
                  "with a regime classifier that tells broad from narrow from bifurcated.",
        "regime": {"label": label, "summary": summary, "concentration_spread_3m": conc,
                   "equity_breadth_pct": eq_breadth_pct, "equity_avg": eq_avg, "crypto_avg": crypto_avg,
                   "rates_avg": rates_avg, "commodities_avg": commod_avg, "intl_avg": intl_avg},
        "regime_changed": regime_changed,
        "prev_regime": prev_label,
        "transitions": transitions[:14],
        "n_transitions": len(transitions),
        "booming": ranked[:6],
        "destroyed": list(reversed(ranked[-6:])),
        "sleeves": sleeves,
        "ranked": ranked,
        "n_instruments": len(ranked),
        "windows": "risk_on = multi-window momentum (1m/3m/6m) + trend (vs 50/200d MA), −100..+100",
        "elapsed_s": round(time.time() - t0, 1),
    }
    S3.put_object(Bucket=BUCKET, Key="data/regime-map.json", Body=json.dumps(payload).encode(),
                  ContentType="application/json", CacheControl="public, max-age=900")
    print(f"[regime-map] {len(ranked)} instruments | regime={label} | eq {eq_avg:+} crypto {crypto_avg:+} "
          f"breadth {eq_breadth_pct}% conc {conc:+} | transitions={len(transitions)} regime_changed={regime_changed} "
          f"| {payload['elapsed_s']}s")
    return {"statusCode": 200, "body": json.dumps({"ok": True, "regime": label, "n": len(ranked),
            "equity_avg": eq_avg, "crypto_avg": crypto_avg})}
