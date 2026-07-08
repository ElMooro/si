"""justhodl-industry-rotation -- Market regime -> industry-ETF leadership
-> strongest stocks inside the strongest ETFs.

Doctrine (Khalid, 2026-07-07): industry momentum subsumes most individual
stock momentum (Moskowitz-Grinblatt 1999; AQR industry-momentum work).
Find the strongest army first, then the strongest soldiers. The premium
signal is DIVERGENCE UNDER WEAKNESS: SPY below its short MAs while an
industry ETF holds its 50/100/200 ladder with a RISING ETF/SPY ratio --
that is institutional absorption, not luck.

Layers (all real data, zero LLM):
  1. Market regime: SPY vs SMA20/50 -> STRONG / NEUTRAL / WEAK, enriched
     with risk-regime score (failure-isolated).
  2. 33-ETF ladder (11 SPDR sectors + 22 industries): SMA50/100/200
     state, ETF/SPY ratio vs its own 50d MA, 20d ratio slope, 3m
     relative-momentum percentile -> leadership_score 0-100.
  3. Tags: ABSORPTION (weak tape, strong ladder, rising ratio),
     BREAKDOWN (strong tape, broken ladder, falling ratio).
  4. Self-accruing score history -> rank_delta_20d (honest WARMING
     until 21 sessions accrue).
  5. Soldiers: top-5 leader ETFs get holdings via FMP /stable/etf-holder
     (graceful skip) + resilient-name join from data/resilience.json.
  6. by_sector_name map so best-setups can join on harvested sector
     strings.

Out: data/industry-rotation.json (CacheControl 900s).
"""
import json
import time
import urllib.request
from datetime import datetime, timedelta, timezone

import boto3

S3 = boto3.client("s3")
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/industry-rotation.json"

import os
POLY = os.environ.get("POLYGON_API_KEY", "")
FMP = os.environ.get("FMP_API_KEY", "")

SECTORS = {
    "XLK": "Technology", "XLF": "Financials", "XLE": "Energy",
    "XLV": "Health Care", "XLI": "Industrials",
    "XLY": "Consumer Discretionary", "XLP": "Consumer Staples",
    "XLU": "Utilities", "XLB": "Materials",
    "XLC": "Communication Services", "XLRE": "Real Estate"}
INDUSTRIES = {
    "SMH": ("Semiconductors", "Technology"),
    "IGV": ("Software", "Technology"),
    "FDN": ("Internet", "Communication Services"),
    "CIBR": ("Cybersecurity", "Technology"),
    "XBI": ("Biotech", "Health Care"),
    "IBB": ("Biotech Large", "Health Care"),
    "ITB": ("Homebuilders", "Consumer Discretionary"),
    "KRE": ("Regional Banks", "Financials"),
    "KBE": ("Banks", "Financials"),
    "XOP": ("Oil E&P", "Energy"),
    "OIH": ("Oil Services", "Energy"),
    "GDX": ("Gold Miners", "Materials"),
    "XME": ("Metals & Mining", "Materials"),
    "XRT": ("Retail", "Consumer Discretionary"),
    "IYT": ("Transports", "Industrials"),
    "ITA": ("Aerospace & Defense", "Industrials"),
    "JETS": ("Airlines", "Industrials"),
    "TAN": ("Solar", "Energy"),
    "URA": ("Uranium", "Energy"),
    "KWEB": ("China Internet", "Communication Services"),
    "LIT": ("Lithium & Battery", "Materials"),
    "PAVE": ("Infrastructure", "Industrials")}
UNIVERSE = list(SECTORS) + list(INDUSTRIES)


def _http(url, timeout=25, tries=2):
    for a in range(tries):
        try:
            req = urllib.request.Request(
                url, headers={"User-Agent": "justhodl-industry-rotation"})
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return json.loads(r.read().decode())
        except Exception:
            time.sleep(1.0 + a)
    return None


def polygon_daily(tkr, days=520):
    to = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    frm = (datetime.now(timezone.utc) - timedelta(days=days)
           ).strftime("%Y-%m-%d")
    d = _http("https://api.polygon.io/v2/aggs/ticker/%s/range/1/day/"
              "%s/%s?adjusted=true&sort=asc&limit=5000&apiKey=%s"
              % (tkr, frm, to, POLY))
    return [float(b["c"]) for b in (d or {}).get("results") or []]


def s3_json(key):
    try:
        return json.loads(
            S3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception:
        return None


def sma(v, n):
    return sum(v[-n:]) / n if len(v) >= n else None


def pct_rank(x, pop):
    if x is None or not pop:
        return None
    return round(100.0 * sum(1 for p in pop if p <= x) / len(pop), 1)


def regime_of(spy):
    px = spy[-1]
    s20, s50 = sma(spy, 20), sma(spy, 50)
    if s20 and s50:
        if px > s20 and px > s50:
            return "STRONG"
        if px < s20 and px < s50:
            return "WEAK"
    return "NEUTRAL"


def ladder_row(tkr, closes, spy, regime, mom3_pop):
    px = closes[-1]
    s50, s100, s200 = sma(closes, 50), sma(closes, 100), sma(closes, 200)
    ab50 = bool(s50 and px > s50)
    ab100 = bool(s100 and px > s100)
    ab200 = bool(s200 and px > s200)
    n = min(len(closes), len(spy))
    ratio = [closes[-n + i] / spy[-n + i] for i in range(n)
             if spy[-n + i]]
    r50 = sma(ratio, 50)
    ratio_above = bool(r50 and ratio[-1] > r50)
    slope = (round((ratio[-1] / ratio[-21] - 1.0) * 100, 2)
             if len(ratio) >= 21 and ratio[-21] else None)
    mom3 = (round((closes[-1] / closes[-64] - 1.0)
                  * 100, 2) if len(closes) >= 64 else None)
    rel3 = (round(mom3 - ((spy[-1] / spy[-64] - 1.0) * 100), 2)
            if mom3 is not None and len(spy) >= 64 else None)
    score = 0
    score += 20 if ab50 else 0
    score += 15 if ab100 else 0
    score += 15 if ab200 else 0
    score += 15 if ratio_above else 0
    score += 15 if (slope or 0) > 0 else 0
    pr = pct_rank(rel3, mom3_pop)
    score += round((pr or 0) / 5.0)          # 0-20
    score = min(100, score)
    tag = None
    if regime == "WEAK" and ab50 and (slope or 0) > 0 and score >= 65:
        tag = "ABSORPTION"
    elif regime != "WEAK" and not ab50 and (slope or 0) < 0 \
            and score <= 25:
        tag = "BREAKDOWN"
    name, parent = (SECTORS.get(tkr, ""), None) if tkr in SECTORS \
        else INDUSTRIES[tkr]
    return {"etf": tkr, "name": name or SECTORS.get(tkr),
            "kind": "SECTOR" if tkr in SECTORS else "INDUSTRY",
            "parent_sector": parent or SECTORS.get(tkr),
            "price": round(px, 2),
            "above_sma50": ab50, "above_sma100": ab100,
            "above_sma200": ab200, "ratio_above_50d": ratio_above,
            "ratio_slope_20d_pct": slope,
            "rel_mom_3m_pp": rel3, "rel_mom_pctile": pr,
            "leadership_score": score, "tag": tag}


def fmp_holdings(tkr):
    if not FMP:
        return None
    d = _http("https://financialmodelingprep.com/stable/etf-holder"
              "?symbol=%s&apikey=%s" % (tkr, FMP))
    if not isinstance(d, list) or not d:
        return None
    rows = sorted(
        (r for r in d if r.get("asset")),
        key=lambda r: -(r.get("weightPercentage") or 0))[:10]
    return [{"ticker": r.get("asset"),
             "weight_pct": round(r.get("weightPercentage") or 0, 2)}
            for r in rows] or None


def resilient_join(doc, etf, sector_name):
    """Rows from data/resilience.json whose best-fit sector ETF or
    sector string matches. Defensive to field naming."""
    if not doc:
        return []
    rows = None
    for k in ("resilient", "names", "rows", "items", "assets"):
        if isinstance(doc.get(k), list):
            rows = doc[k]
            break
    if rows is None:
        rows = next((v for v in doc.values()
                     if isinstance(v, list) and v
                     and isinstance(v[0], dict)
                     and ("ticker" in v[0] or "symbol" in v[0])), [])
    hits = []
    for r in rows:
        blob = json.dumps(r)
        if ('"%s"' % etf) in blob or (
                sector_name and sector_name in blob):
            hits.append({"ticker": r.get("ticker") or r.get("symbol"),
                         "state": r.get("state") or r.get("status"),
                         "score": r.get("score")
                         or r.get("resilience_score")})
        if len(hits) >= 8:
            break
    return hits


def lambda_handler(event=None, context=None):
    warns = []
    spy = polygon_daily("SPY")
    if len(spy) < 210:
        raise RuntimeError("SPY history short: %d" % len(spy))
    regime = regime_of(spy)
    rr = s3_json("data/risk-regime.json") or {}

    closes = {}
    for t in UNIVERSE:
        closes[t] = polygon_daily(t)
        if len(closes[t]) < 210:
            warns.append("%s: %d bars" % (t, len(closes[t])))
        time.sleep(0.14)

    mom3_pop = []
    for t, c in closes.items():
        if len(c) >= 64 and len(spy) >= 64:
            mom3_pop.append((c[-1] / c[-64] - 1.0) * 100
                            - (spy[-1] / spy[-64] - 1.0) * 100)

    rows = [ladder_row(t, c, spy, regime, mom3_pop)
            for t, c in closes.items() if len(c) >= 210]
    rows.sort(key=lambda r: -r["leadership_score"])

    # self-accruing history -> 20d rank delta
    prev = s3_json(OUT_KEY) or {}
    hist = prev.get("score_history") or {}
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    hist[today] = {r["etf"]: r["leadership_score"] for r in rows}
    hist = dict(sorted(hist.items())[-45:])
    dates = sorted(hist)
    rank_note = None
    if len(dates) >= 21:
        old = hist[dates[-21]]
        old_rank = {e: i for i, (e, _) in enumerate(
            sorted(old.items(), key=lambda kv: -kv[1]))}
        for i, r in enumerate(rows):
            if r["etf"] in old_rank:
                r["rank_delta_20d"] = old_rank[r["etf"]] - i
    else:
        rank_note = ("WARMING_UP: rank_delta_20d activates after 21 "
                     "sessions (%d/21 accrued)" % len(dates))

    # soldiers for the top-5 leaders
    res_doc = s3_json("data/resilience.json")
    leaders = []
    for r in rows[:5]:
        holds = None
        try:
            holds = fmp_holdings(r["etf"])
        except Exception:
            pass
        if holds is None:
            warns.append("holdings skip %s" % r["etf"])
        leaders.append(dict(
            r, holdings_top=holds,
            resilient_names=resilient_join(
                res_doc, r["etf"], r["parent_sector"])))

    fv = s3_json("data/finviz-groups.json") or {}
    by_sector = {}
    for r in rows:
        if r["kind"] == "SECTOR":
            by_sector[r["name"]] = {
                "etf": r["etf"],
                "leadership_score": r["leadership_score"],
                "tag": r["tag"]}

    out = {
        "engine": "justhodl-industry-rotation", "version": "1.0",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "doctrine": "regime -> industry leadership -> strongest "
                    "soldiers; divergence under weakness = absorption "
                    "(industry momentum subsumes stock momentum: "
                    "Moskowitz-Grinblatt 1999, AQR)",
        "market_regime": {
            "state": regime,
            "spy": round(spy[-1], 2),
            "spy_above_sma20": spy[-1] > (sma(spy, 20) or 9e9),
            "spy_above_sma50": spy[-1] > (sma(spy, 50) or 9e9),
            "risk_regime_score": rr.get("risk_regime_score"),
            "risk_regime": rr.get("risk_regime")},
        "method": {
            "leadership_score": "SMA50 +20, SMA100 +15, SMA200 +15, "
                                "ratio>50dMA +15, ratio 20d slope>0 "
                                "+15, 3m rel-mom percentile /5 (0-20)",
            "absorption": "regime WEAK + above SMA50 + rising ratio + "
                          "score>=65", "universe_n": len(rows)},
        "ladder": rows,
        "leaders": leaders,
        "absorption_watch": [r["etf"] for r in rows
                             if r["tag"] == "ABSORPTION"],
        "breakdown_watch": [r["etf"] for r in rows
                            if r["tag"] == "BREAKDOWN"],
        "by_sector_name": by_sector,
        "finviz_sector_perf_attached": bool(fv),
        "score_history": hist,
        "rank_note": rank_note,
        "warns": warns[:20]}
    S3.put_object(Bucket=BUCKET, Key=OUT_KEY,
                  Body=json.dumps(out, separators=(",", ":")).encode(),
                  ContentType="application/json",
                  CacheControl="public, max-age=900")
    return {"statusCode": 200,
            "body": json.dumps({"ok": True, "regime": regime,
                                "top": rows[0]["etf"] if rows else None,
                                "absorption": len(out["absorption_watch"]),
                                "warns": len(warns)})}
