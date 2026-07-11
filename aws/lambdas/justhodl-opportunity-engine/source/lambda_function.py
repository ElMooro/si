"""
justhodl-opportunity-engine v2 — Retail Edge

WHAT IT DOES
Turns the platform's institutional data into ONE plain verdict per S&P 500
stock so a non-professional can spot a real opportunity (and its risks) in
seconds. It does not recompute fundamentals — it SYNTHESISES existing
sidecars and adds the judgement layer a hedge-fund analyst would apply.

v2 UPGRADES
  1. 3-METHOD valuation triangulation — DCF + historical-multiple reversion
     + analyst consensus target. Median anchor, outlier rejection. Three
     independent opinions are far harder to fool than one.
  2. CYCLE AWARENESS — for cyclical sectors (energy, materials, industrials,
     etc.) a low P/E next to surging revenue / peak margins is a PEAK-
     EARNINGS TRAP, not a bargain; depressed earnings with a falling
     forward P/E is an EARLY-CYCLE turn. The DCF is down-weighted when it
     is extrapolating an unsustainable cyclical surge.
  3. INDUSTRY-RELATIVE SCORECARD — every key metric (P/E, forward P/E,
     P/S, growth, margins, ROIC, debt, FCF yield) compared to the stock's
     own sector median, in plain words ("cheaper than peers").
  4. GURU / BLOOMBERG METRICS — PEG, FCF yield, moat tier, forward-vs-
     trailing P/E earnings-direction read, fair-value entry zone.
  5. PLAIN-ENGLISH BOTTOM LINE — one normie sentence per stock.
  6. WHAT-CHANGED-TODAY diff — new opportunities, new risks, verdict flips.

SAFEGUARDS (why this is not a naive "buy" screen)
  • A fair-value RANGE, never false-precision.
  • Confidence rating — HIGH only when methods agree.
  • VALUE-TRAP GUARD — cheap + distressed can never be an "opportunity".
  • CYCLE GUARD — a late-cycle peak-earnings stock can never be STRONG.
  • CAP GUARD — an estimate pegged at the +/-50% cap is flagged uncertain
    and can never be rated STRONG OPPORTUNITY.

Research / education only — not financial advice.
OUTPUT: data/opportunities.json   SCHEDULE: daily 14:00 UTC
"""
import json
import os
import time
import urllib.request
from datetime import datetime, timezone

import boto3

S3_BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
OUT_KEY = "data/opportunities.json"
s3 = boto3.client("s3", region_name="us-east-1")
ssm = boto3.client("ssm", region_name="us-east-1")

# baseline factor weights — the prior. The opportunity-calibrator may
# replace these in SSM once enough forward-return data has matured;
# until then (or on any error) the engine falls back to this prior.
BASE_WEIGHTS = {"value": 0.40, "quality": 0.30, "growth": 0.20,
                "momentum": 0.10}
WEIGHTS_PARAM = "/justhodl/opportunity/weights"


def get_weights():
    try:
        raw = ssm.get_parameter(Name=WEIGHTS_PARAM)["Parameter"]["Value"]
        w = json.loads(raw)
        tot = sum(float(w.get(k, 0)) for k in BASE_WEIGHTS)
        if tot > 0 and all(k in w for k in BASE_WEIGHTS):
            return {k: round(float(w[k]) / tot, 4) for k in BASE_WEIGHTS}
    except Exception as e:
        print(f"[opp] weights: using baseline prior ({e})")
    return dict(BASE_WEIGHTS)

# sector cyclicality — drives the cycle-aware valuation logic
CYCLICAL = {"Energy", "Basic Materials", "Materials", "Industrials",
            "Consumer Cyclical", "Consumer Discretionary", "Real Estate",
            "Financial Services", "Financials"}
DEFENSIVE = {"Utilities", "Consumer Defensive", "Consumer Staples",
             "Healthcare"}

CAP = 50.0  # max |under/over-valued %| the engine will report


# ───────────────────────────── helpers ──────────────────────────────
def load(key):
    try:
        return json.loads(s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read())
    except Exception as e:
        print(f"[opp] WARN could not read {key}: {e}")
        return None


def num(v):
    try:
        f = float(v)
        return f if f == f else None  # drop NaN
    except (TypeError, ValueError):
        return None


def clamp(v, lo=0, hi=100):
    return max(lo, min(hi, v))


def median(xs):
    xs = sorted(x for x in xs if x is not None)
    n = len(xs)
    if n == 0:
        return None
    return xs[n // 2] if n % 2 else (xs[n // 2 - 1] + xs[n // 2]) / 2.0


# ───────────────────── sector benchmark medians ─────────────────────
BENCH = {  # field: (sane_lo, sane_hi)
    "peRatio": (0.5, 150), "forwardPE": (0.5, 150), "psRatio": (0.05, 60),
    "pbRatio": (0.1, 60), "evEbitda": (1, 80),
    "revenueGrowth": (-60, 250), "epsGrowth": (-95, 400),
    "grossMargin": (-20, 100), "operatingMargin": (-60, 80),
    "netMargin": (-70, 70), "roic": (-40, 90), "roe": (-80, 160),
    "debtToEquity": (0, 10), "fcfYieldCalc": (-25, 35),
}


def build_benchmarks(universe):
    by_sector = {}
    for s in universe:
        by_sector.setdefault(s.get("sector") or "Unknown", []).append(s)
    out = {}
    for sec, members in by_sector.items():
        if len(members) < 5:
            continue
        meds = {}
        for fld, (lo, hi) in BENCH.items():
            vals = [num(x.get(fld)) for x in members]
            vals = [v for v in vals if v is not None and lo <= v <= hi]
            if len(vals) >= 5:
                meds[fld] = round(median(vals), 2)
        out[sec] = {"n": len(members), "medians": meds}
    return out


# ── INDUSTRY benchmarks (finer than sector) + growth aggregates ──
def build_industry_benchmarks(universe, fwd_growth):
    """Per-industry medians + INDUSTRY GROWTH (trailing) + EXPECTED INDUSTRY
    GROWTH (forward analyst). Falls back to sector for thin industries."""
    by_ind = {}
    for s in universe:
        key = s.get("industry") or s.get("sector") or "Unknown"
        by_ind.setdefault(key, []).append(s)
    out = {}
    for ind, members in by_ind.items():
        if len(members) < 4:
            continue
        meds = {}
        for fld, (lo, hi) in BENCH.items():
            vals = [num(x.get(fld)) for x in members if num(x.get(fld)) is not None and lo <= num(x.get(fld)) <= hi]
            if len(vals) >= 4:
                meds[fld] = round(median(vals), 2)
        # trailing industry growth (median revenue growth of the cohort)
        trail = [num(x.get("revenueGrowth")) for x in members]
        trail = [v*100 if (v is not None and abs(v) < 3) else v for v in trail]
        trail = [v for v in trail if v is not None and -95 <= v <= 300]
        ind_growth = round(median(trail), 1) if len(trail) >= 4 else None
        # expected industry growth (median forward revenue growth from analyst est)
        fwd = [fwd_growth.get((x.get("symbol") or x.get("ticker")), {}).get("fwd_rev_growth") for x in members]
        fwd = [v for v in fwd if v is not None and -95 <= v <= 300]
        ind_fwd_growth = round(median(fwd), 1) if len(fwd) >= 3 else None
        out[ind] = {"n": len(members), "medians": meds,
                    "industry_growth_pct": ind_growth,
                    "expected_industry_growth_pct": ind_fwd_growth}
    return out


def fetch_forward_growth(universe, max_n=2600):
    """Concurrent FMP analyst-estimates → expected (forward) revenue & EPS growth
    per ticker. Computes EXPECTED COMPANY GROWTH from current vs next-year est."""
    import concurrent.futures as cf
    syms = [(s.get("symbol") or s.get("ticker")) for s in universe][:max_n]
    syms = [s for s in syms if s]
    fmp_key = os.environ.get("FMP_KEY", "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb")
    base = "https://financialmodelingprep.com/stable"

    def one(sym):
        try:
            est = http_get_json(f"{base}/analyst-estimates?symbol={sym}&period=annual&limit=3&apikey={fmp_key}")
            if not isinstance(est, list) or len(est) < 2:
                return sym, None
            # sort by year ascending
            rows = sorted([e for e in est if e.get("date")], key=lambda e: e.get("date"))
            def rev(e): return num(e.get("revenueAvg") or e.get("estimatedRevenueAvg"))
            def eps(e): return num(e.get("epsAvg") or e.get("estimatedEpsAvg"))
            r0, r1 = rev(rows[0]), rev(rows[-1])
            e0, e1 = eps(rows[0]), eps(rows[-1])
            yrs = max(1, len(rows) - 1)
            fwd_rev_growth = None
            if r0 and r1 and r0 > 0:
                fwd_rev_growth = round(((r1 / r0) ** (1 / yrs) - 1) * 100, 1)
            fwd_eps_growth = None
            if e0 and e1 and e0 > 0:
                fwd_eps_growth = round(((e1 / e0) ** (1 / yrs) - 1) * 100, 1)
            return sym, {"fwd_rev_growth": fwd_rev_growth, "fwd_eps_growth": fwd_eps_growth,
                          "next_year_revenue": r1}
        except Exception:
            return sym, None

    result = {}
    with cf.ThreadPoolExecutor(max_workers=32) as ex:
        for fut in cf.as_completed([ex.submit(one, s) for s in syms]):
            sym, v = fut.result()
            if v:
                result[sym] = v
    return result


# AI-infrastructure / capex-beneficiary sectors: rising capex here signals the
# AI/power buildout (demand for the picks-and-shovels), so capex GROWTH is read
# bullishly. Elsewhere capex is more neutral/margin-dilutive.
CAPEX_BULL_SECTORS = {"Technology", "Communication Services", "Industrials", "Energy", "Utilities"}

def fetch_capex_buyback(universe, max_n=2600):
    """Per-ticker buyback yield + capex growth from FMP cash-flow statements.
       buyback_yield  = trailing stock repurchases / market cap  (price support)
       capex_growth   = capex YoY %  (capacity/demand buildout; AI-buildout proxy)
       capex_intensity= capex / revenue
    Concurrent; capped at max_n to bound the run."""
    import concurrent.futures as _cf
    fmp_key = os.environ.get("FMP_KEY", "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb")
    base = "https://financialmodelingprep.com/stable"
    syms = [s.get("symbol") for s in universe if s.get("symbol")][:max_n]
    result = {}
    def one(sym):
        try:
            cf = http_get_json(f"{base}/cash-flow-statement?symbol={sym}&period=annual&limit=2&apikey={fmp_key}")
            if not isinstance(cf, list) or not cf:
                return sym, None
            cur = cf[0]
            mcap = None
            q = http_get_json(f"{base}/quote?symbol={sym}&apikey={fmp_key}")
            if isinstance(q, list) and q:
                mcap = q[0].get("marketCap")
            buyback = abs(cur.get("commonStockRepurchased") or cur.get("stockRepurchased") or 0)
            capex = abs(cur.get("capitalExpenditure") or 0)
            rev = cur.get("revenue") or (q[0].get("revenue") if isinstance(q, list) and q else None)
            out = {}
            if mcap and mcap > 0:
                out["buyback_yield_pct"] = round(buyback / mcap * 100, 2)
            if len(cf) >= 2:
                prev_capex = abs(cf[1].get("capitalExpenditure") or 0)
                if prev_capex > 0:
                    out["capex_growth_pct"] = round((capex / prev_capex - 1) * 100, 1)
            if rev and rev > 0:
                out["capex_intensity_pct"] = round(capex / rev * 100, 1)
            return sym, (out or None)
        except Exception:
            return sym, None
    with _cf.ThreadPoolExecutor(max_workers=24) as ex:
        futs = [ex.submit(one, s) for s in syms]
        for fut in _cf.as_completed(futs):
            sym, v = fut.result()
            if v:
                result[sym] = v
    return result


def http_get_json(url, timeout=12):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "JustHodl/1.0"})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read().decode())
    except Exception:
        return None


def extract_backlog(ticker, fmp_key):
    """Scan the latest earnings-call transcript for backlog / RPO / bookings + $."""
    import re
    base = "https://financialmodelingprep.com/stable"
    tr = (http_get_json(f"{base}/earning-call-transcript-latest?symbol={ticker}&apikey={fmp_key}")
          or http_get_json(f"{base}/earning-call-transcript?symbol={ticker}&limit=1&apikey={fmp_key}"))
    if not tr:
        return None
    content = ""
    if isinstance(tr, list) and tr:
        content = tr[0].get("content") or tr[0].get("transcript") or ""
    elif isinstance(tr, dict):
        content = tr.get("content") or tr.get("transcript") or ""
    if not content:
        return None
    low = content.lower()
    for kw in ["backlog", "remaining performance obligation", "committed", "contracted revenue", "bookings"]:
        m = low.find(kw)
        if m >= 0:
            seg = content[max(0, m - 80): m + 140]
            dollar = re.search(r"\$\s?[\d.,]+\s?(billion|million|bn|mn|b|m)?", seg, re.I)
            if dollar:
                return {"term": kw, "figure": dollar.group(0),
                        "snippet": seg.strip().replace("\n", " ")[:180]}
    return None


# ──────────────────── 3-method valuation triangulation ──────────────
def triangulate(price, methods, dcf_trust=1.0):
    """methods: list of (name, fair_value). dcf_trust down-weights an
    extrapolated DCF on a cyclical peak. Returns a dict."""
    pts = [(n, fv) for n, fv in methods if fv and fv > 0 and price and price > 0]
    blank = {"mid": None, "low": None, "high": None, "under_pct": None,
             "confidence": "n/a", "capped": False, "n_methods": 0, "methods": {}}
    if not pts:
        return blank

    # cyclical-peak DCF down-weight: if DCF is the high outlier and trust
    # is low, drop it from the anchor (keep it visible in methods{}).
    anchor_pts = list(pts)
    if dcf_trust < 0.7 and len(pts) >= 2:
        dcf = next((fv for n, fv in pts if n == "DCF"), None)
        others = [fv for n, fv in pts if n != "DCF"]
        if dcf and others and dcf > max(others) * 1.25:
            anchor_pts = [(n, fv) for n, fv in pts if n != "DCF"]

    fvs = [fv for _, fv in anchor_pts]
    # outlier rejection when 3+ survive
    if len(fvs) >= 3:
        med = median(fvs)
        kept = [(n, fv) for n, fv in anchor_pts if 0.42 * med <= fv <= 2.4 * med]
        if len(kept) >= 2:
            anchor_pts, fvs = kept, [fv for _, fv in kept]

    anchor = median(fvs)
    lo, hi = min(fvs), max(fvs)
    spread = (hi / lo - 1.0) if lo > 0 else 9.0
    raw = (anchor / price - 1) * 100
    capped = abs(raw) > CAP
    under = max(-CAP, min(CAP, raw))

    if len(anchor_pts) >= 2 and spread <= 0.25:
        conf = "high"
    elif len(anchor_pts) >= 2 and spread <= 0.55:
        conf = "moderate"
    elif len(anchor_pts) == 1:
        conf = "single-source"
    else:
        conf = "low"
    if capped and conf in ("high", "moderate"):
        conf = "capped"

    return {"mid": round(anchor, 2), "low": round(lo, 2), "high": round(hi, 2),
            "under_pct": round(under, 1), "confidence": conf, "capped": capped,
            "n_methods": len(pts),
            "methods": {n: round(fv, 2) for n, fv in pts}}


# ─────────────────────────── cycle read ─────────────────────────────
def cycle_read(s, sec_med):
    """Returns (tag, note, dcf_trust)."""
    sec = s.get("sector") or ""
    if sec not in CYCLICAL:
        return "—", None, 1.0
    pe = num(s.get("peRatio"))
    fpe = num(s.get("forwardPE"))
    rev_g = num(s.get("revenueGrowth"))
    op_m = num(s.get("operatingMargin"))
    sec_opm = (sec_med or {}).get("operatingMargin")

    earn_falling = bool(pe and fpe and fpe > pe * 1.18)
    earn_rising = bool(pe and fpe and fpe < pe * 0.85)
    peak_margin = (op_m is not None and sec_opm is not None
                   and op_m > sec_opm * 1.5 and op_m > 18)
    surge = rev_g is not None and rev_g > 35
    cheap_pe = pe is not None and 0 < pe < 16

    if cheap_pe and (surge or peak_margin or earn_falling):
        return ("LATE CYCLE",
                "Earnings look near a cyclical peak — a low P/E here can be a "
                "trap, because peak profits rarely last. Treat 'cheap' with "
                "caution.", 0.45)
    depressed = (rev_g is not None and rev_g < 4) or (pe is not None and pe > 45)
    if (depressed or earn_rising) and earn_rising:
        return ("EARLY CYCLE",
                "Earnings look depressed but the forward outlook is improving — "
                "the classic point where cyclical stocks look pricey just "
                "before they turn up.", 0.85)
    return "MID CYCLE", None, 1.0


# ───────────────────── industry-relative scorecard ──────────────────
SCORECARD = [
    ("peRatio", "P/E", "low"), ("forwardPE", "Forward P/E", "low"),
    ("psRatio", "Price / Sales", "low"),
    ("revenueGrowth", "Revenue growth", "high"),
    ("operatingMargin", "Operating margin", "high"),
    ("netMargin", "Net margin", "high"),
    ("roic", "Return on capital", "high"),
    ("debtToEquity", "Debt / Equity", "low"),
    ("fcfYieldCalc", "Free-cash-flow yield", "high"),
]


def vs_industry(s, meds):
    rows = []
    for fld, label, direction in SCORECARD:
        v = num(s.get(fld))
        med = (meds or {}).get(fld)
        if v is None or med is None or med == 0:
            continue
        if direction == "low":
            if v < med * 0.8:
                tag, good = "cheaper than peers", True
            elif v > med * 1.25:
                tag, good = "pricier than peers", False
            else:
                tag, good = "in line with peers", None
        else:
            if v > med * 1.2:
                tag, good = "better than peers", True
            elif v < med * 0.8:
                tag, good = "weaker than peers", False
            else:
                tag, good = "in line with peers", None
        rows.append({"metric": label, "value": round(v, 2),
                     "sector_median": med, "tag": tag, "good": good})
    return rows


# ───────────────────────── guru / bloomberg ─────────────────────────
def guru_metrics(s):
    pe = num(s.get("peRatio"))
    eg = num(s.get("epsGrowth"))
    rg = num(s.get("revenueGrowth"))
    g = eg if (eg and eg > 0) else (rg if (rg and rg > 0) else None)
    peg = round(pe / g, 2) if (pe and pe > 0 and g) else None

    gm = num(s.get("grossMargin"))
    om = num(s.get("operatingMargin"))
    roic = num(s.get("roic"))
    sc = 0
    if gm is not None and gm > 45:
        sc += 1
    if om is not None and om > 20:
        sc += 1
    if roic is not None and roic > 15:
        sc += 1
    if roic is not None and roic > 25:
        sc += 1
    moat = ["No moat", "Narrow moat", "Narrow moat", "Wide moat", "Wide moat"][min(sc, 4)]
    return {"peg": peg, "fcf_yield": num(s.get("fcfYieldCalc")), "moat": moat}


# ─────────────────────────── score a stock ──────────────────────────
def score_stock(s, mr, fund, short_state, bench, weights, sflow=None):
    sym = s.get("symbol") or s.get("ticker")
    price = num(s.get("price"))
    if not sym or not price:
        return None

    sec = s.get("sector") or "Unknown"
    sec_med = (bench.get(sec) or {}).get("medians") or {}
    cyc_tag, cyc_note, dcf_trust = cycle_read(s, sec_med)

    # ── 3-method valuation ──
    methods = [
        ("DCF", num(s.get("dcfFairValue"))),
        ("Reversion", num((mr or {}).get("mr_price"))),
        ("Analyst", num(s.get("priceTargetMean")) or num(s.get("priceTargetMedian"))),
    ]
    val = triangulate(price, methods, dcf_trust)
    under = val["under_pct"]
    conf = val["confidence"]

    # ── raw fields ──
    rev_g = num(s.get("revenueGrowth"))
    fwd_rev_g = num(s.get("forwardRevenueGrowth"))
    op_m = num(s.get("operatingMargin"))
    roic = num(s.get("roic"))
    d2e = num(s.get("debtToEquity"))
    pe = num(s.get("peRatio"))
    fpe = num(s.get("forwardPE"))
    chg6 = num(s.get("chg6m"))
    cross = s.get("crossSignal")
    beats = num(s.get("beatStreak"))
    fcfy = num(s.get("fcfYieldCalc"))
    grades = s.get("gradesConsensus")
    altman = num((fund or {}).get("altman_z")) or num(s.get("altmanZ"))
    piotroski = num((fund or {}).get("piotroski")) or num(s.get("piotroski"))

    # ── sub-scores (0-100) ──
    # value is CONFIDENCE-DISCOUNTED: a trustworthy +25% (three methods
    # agreeing) outranks an uncertain capped +50%, so the cleanest
    # opportunities surface at the top of the list, not the most extreme.
    CONF_MULT = {"high": 1.0, "moderate": 0.85, "single-source": 0.6,
                 "low": 0.58, "capped": 0.5, "n/a": 0.0}
    cmult = CONF_MULT.get(conf, 0.7)
    value_score = clamp(42 + (under if under is not None else 0) * 1.05 * cmult)

    q = 50
    if altman is not None:
        q += 22 if altman >= 3 else (-32 if altman < 1.8 else 0)
    if piotroski is not None:
        q += 15 if piotroski >= 7 else (-15 if piotroski <= 3 else 0)
    if d2e is not None:
        q += -15 if d2e > 2 else (10 if d2e < 0.5 else 0)
    if op_m is not None:
        q += 10 if op_m > 20 else (-15 if op_m < 0 else 0)
    if roic is not None and roic > 15:
        q += 8
    if fcfy is not None:
        q += 8 if fcfy > 5 else (-8 if fcfy < 0 else 0)
    # ── capital-return / dilution leg (share-flows join, Khalid) ──
    sf = sflow or {}
    sf_yoy = num(sf.get("sh_yoy_pct"))
    sf_read = sf.get("read")
    if sf_read == "EXTREME_DILUTION" or sf.get("extreme"):
        q -= 35
    elif sf_yoy is not None:
        if sf_yoy >= 5:
            q -= 15
        elif sf_yoy >= 2:
            q -= 8
        elif sf_yoy <= -1:
            q += 8
    if num(sf.get("buyback_net_yield_pct")) and \
            num(sf.get("buyback_net_yield_pct")) >= 2:
        q += 6
    quality_score = clamp(q)

    g = 50
    if rev_g is not None:
        g += 22 if rev_g > 20 else (12 if rev_g > 8 else (-22 if rev_g < 0 else 0))
    if fwd_rev_g is not None and rev_g is not None and fwd_rev_g > rev_g + 3:
        g += 8
    if beats is not None and beats >= 4:
        g += 12
    growth_score = clamp(g)

    m = 50
    if chg6 is not None:
        m += 18 if chg6 > 20 else (-18 if chg6 < -20 else 0)
    if cross == "GOLDEN":
        m += 15
    elif cross == "DEATH":
        m -= 15
    momentum_score = clamp(m)

    opp = round(weights["value"] * value_score
                + weights["quality"] * quality_score
                + weights["growth"] * growth_score
                + weights["momentum"] * momentum_score, 1)

    guru = guru_metrics(s)
    scard = vs_industry(s, sec_med)

    # peer-relative valuation bucket — isolated from the bundled verdict so the
    # backtest can measure "cheap vs industry peers" as its OWN axis. Net of the
    # valuation metrics that read cheaper vs pricier than the industry median.
    _VAL_METRICS = {"P/E", "Forward P/E", "Price / Sales"}
    _net = _cov = 0
    for _c in scard:
        if _c.get("metric") in _VAL_METRICS and _c.get("tag") in ("cheaper than peers", "pricier than peers"):
            _cov += 1
            _net += 1 if _c["tag"] == "cheaper than peers" else -1
    peer_val = (None if _cov == 0 else "cheap" if _net >= 1 else "rich" if _net <= -1 else "fair")

    # ── risk flags & opportunity highlights (plain English) ──
    distress = altman is not None and altman < 1.8
    risks, ops = [], []
    if distress:
        risks.append("Elevated bankruptcy risk (weak Altman-Z score)")
    if cyc_tag == "LATE CYCLE":
        risks.append("Late in its industry cycle — earnings may be near a peak")
    if cross == "DEATH":
        days = s.get("crossDaysAgo")
        risks.append(f"In a downtrend — death cross{f' {days}d ago' if days else ''}")
    if rev_g is not None and rev_g < 0:
        risks.append(f"Revenue is shrinking ({rev_g:.0f}% YoY)")
    if d2e is not None and d2e > 2:
        risks.append("Carries heavy debt relative to equity")
    if pe is not None and pe < 0:
        risks.append("Not currently profitable")
    if sf_read == "EXTREME_DILUTION" or sf.get("extreme"):
        risks.append("Death-spiral issuance — share count exploding, "
                     "your stake is being printed away")
    elif sf_yoy is not None and sf_yoy >= 5:
        risks.append(f"Heavy dilution — share count +{sf_yoy:.0f}% YoY")
    if sf_yoy is not None and sf_yoy <= -3 and \
            num(sf.get("buyback_net_yield_pct") or 0) >= 2:
        ops.append(f"Shrinking float — net buyback "
                   f"{sf.get('buyback_net_yield_pct')}%/yr with share "
                   f"count {sf_yoy}% YoY")
    if fpe is not None and pe is not None and pe > 0 and fpe > pe * 1.2:
        risks.append("Analysts expect earnings to fall next year")
    if short_state == "PRESSURE BUILDING":
        risks.append("Short sellers are building pressure")
    if val["capped"]:
        risks.append("Valuation gap is very large — often a sign of an "
                     "unreliable estimate; treat with caution")
    elif conf in ("low", "single-source"):
        risks.append("Valuation estimate is uncertain — the methods disagree")

    if under is not None and under >= 12 and not val["capped"]:
        ops.append(f"Trading ~{under:.0f}% below estimated fair value")
    if conf == "high" and under is not None and under > 0:
        ops.append("Three valuation methods agree it looks cheap")
    if cyc_tag == "EARLY CYCLE":
        ops.append("Looks like an early-cycle turn — earnings outlook improving")
    if rev_g is not None and rev_g > 10:
        ops.append(f"Revenue growing ~{rev_g:.0f}% a year")
    if guru["moat"] == "Wide moat":
        ops.append("Wide-moat business — durable competitive advantage")
    if altman is not None and altman >= 3:
        ops.append("Rock-solid balance sheet")
    if guru["peg"] is not None and 0 < guru["peg"] < 1:
        ops.append(f"Cheap for its growth (PEG {guru['peg']})")
    if fcfy is not None and fcfy > 6:
        ops.append(f"Strong free-cash-flow yield (~{fcfy:.0f}%)")
    if cross == "GOLDEN":
        ops.append("Recently entered an uptrend (golden cross)")
    if beats is not None and beats >= 3:
        ops.append(f"Beaten earnings {int(beats)} quarters running")

    # ── verdict (value-trap + cycle + cap guards) ──
    value_trap = (under is not None and under >= 12
                  and (distress or (rev_g is not None and rev_g < -3)))
    hi_conf = conf == "high"
    strong_ok = (hi_conf and not val["capped"] and cyc_tag != "LATE CYCLE")

    if distress or value_trap:
        verdict, vcolor = "HIGH RISK", "red"
    elif under is not None and under <= -15:
        verdict, vcolor = "EXPENSIVE", "orange"
    elif strong_ok and opp >= 70 and (under or 0) > 12:
        verdict, vcolor = "STRONG OPPORTUNITY", "green"
    elif opp >= 56 and (under or 0) > 5:
        verdict, vcolor = "OPPORTUNITY", "cyan"
    elif under is not None and -15 < under < 8:
        verdict, vcolor = "FAIR VALUE", "yellow"
    else:
        verdict, vcolor = "HOLD / NEUTRAL", "dim"

    # ── entry zone (actionable price level) ──
    entry = None
    if val["low"] and verdict in ("STRONG OPPORTUNITY", "OPPORTUNITY", "FAIR VALUE"):
        entry = round(min(price, val["low"] * 0.97), 2)

    # ── plain-English bottom line ──
    bottom = build_bottom_line(sym, verdict, under, conf, cyc_tag, guru,
                               distress, rev_g, val["capped"])

    return {
        "ticker": sym,
        "company": s.get("companyName") or s.get("name") or sym,
        "sector": sec, "industry": s.get("industry"),
        "peer_val": peer_val,
        "price": price,
        "fair_value_low": val["low"], "fair_value_mid": val["mid"],
        "fair_value_high": val["high"],
        "undervalued_pct": under, "confidence": conf,
        "valuation_methods": val["methods"], "n_methods": val["n_methods"],
        "verdict": verdict, "verdict_color": vcolor,
        "bottom_line": bottom,
        "opportunity_score": opp,
        "scores": {"value": round(value_score), "quality": round(quality_score),
                   "growth": round(growth_score), "momentum": round(momentum_score)},
        "cycle": {"tag": cyc_tag, "note": cyc_note},
        "guru": {"peg": guru["peg"], "fcf_yield": guru["fcf_yield"],
                 "moat": guru["moat"],
                 "analyst_consensus": grades,
                 "fwd_pe": fpe, "trailing_pe": pe},
        "market_cap": (num(s.get("marketCap")) or num(s.get("market_cap"))
                       or sf.get("market_cap")),
        "capital_return": ({k2: sf.get(k2) for k2 in
                            ("sh_yoy_pct", "buyback_net_yield_pct",
                             "read", "pe_ttm", "ps_ttm", "peg",
                             "fcf_yield_pct", "extreme")
                            if sf.get(k2) is not None} or None),
        "entry_zone_below": entry,
        "vs_industry": scard,
        "opportunities": ops[:4],
        "risks": risks[:4],
    }


def build_bottom_line(sym, verdict, under, conf, cyc, guru, distress,
                      rev_g, capped):
    moat = guru["moat"]
    if verdict == "STRONG OPPORTUNITY":
        return (f"{sym} looks meaningfully undervalued (~{under:.0f}% below "
                f"fair value) with multiple methods in agreement — a "
                f"{moat.lower()} business worth a close look.")
    if verdict == "OPPORTUNITY":
        base = f"{sym} looks somewhat undervalued (~{under:.0f}% upside to fair value)"
        if cyc == "EARLY CYCLE":
            return base + " and may be turning up early in its cycle — but confirm the trend."
        if conf in ("low", "single-source") or capped:
            return base + ", though the valuation estimate is uncertain — do your own check."
        return base + " with a reasonable risk/reward."
    if verdict == "EXPENSIVE":
        return (f"{sym} trades above what the models say it's worth "
                f"(~{abs(under):.0f}% overvalued) — paying up here lowers your margin of safety.")
    if verdict == "HIGH RISK":
        if distress:
            return (f"{sym} is cheap, but for a reason — the balance sheet shows "
                    f"distress risk. Cheap-and-broken is not a bargain.")
        return (f"{sym} is flagged high risk — it may look cheap, but shrinking "
                f"revenue makes this a likely value trap.")
    if verdict == "FAIR VALUE":
        return f"{sym} looks roughly fairly priced — no clear edge either way right now."
    return f"{sym} is neutral right now — no strong opportunity or obvious risk."


# ────────────────────────── what changed ────────────────────────────
def compute_changes(prev, rows):
    if not prev:
        return {"baseline": True, "newly_opportunity": [], "newly_high_risk": [],
                "verdict_flips": []}
    old = {r["ticker"]: r.get("verdict") for r in prev.get("all", [])}
    OPP = ("STRONG OPPORTUNITY", "OPPORTUNITY")
    new_opp, new_risk, flips = [], [], []
    for r in rows:
        t, v = r["ticker"], r["verdict"]
        ov = old.get(t)
        if ov is None:
            continue
        if v in OPP and ov not in OPP:
            new_opp.append({"ticker": t, "from": ov, "to": v})
        if v == "HIGH RISK" and ov != "HIGH RISK":
            new_risk.append({"ticker": t, "from": ov, "to": v})
        if v != ov:
            flips.append({"ticker": t, "from": ov, "to": v})
    return {"baseline": False,
            "newly_opportunity": new_opp[:20],
            "newly_high_risk": new_risk[:20],
            "verdict_flips": flips[:60]}


# ────────────────────────────  handler  ─────────────────────────────
def fetch_tail_metrics(tail):
    """Deep-fetch full-universe tail (all caps) → screener-shaped records so
    they flow through the same scoring. key-metrics-ttm + ratios-ttm + quote."""
    import concurrent.futures as cf
    fmp_key = os.environ.get("FMP_KEY", "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb")
    base = "https://financialmodelingprep.com/stable"

    def cap_bucket(m):
        if not m: return None
        if m < 50e6: return "nano"
        if m < 300e6: return "micro"
        if m < 2e9: return "small"
        if m < 10e9: return "mid"
        if m < 200e9: return "large"
        return "mega"

    def one(stock):
        sym = (stock.get("symbol") or "").upper()
        try:
            km = http_get_json(f"{base}/key-metrics-ttm?symbol={sym}&apikey={fmp_key}")
            rt = http_get_json(f"{base}/ratios-ttm?symbol={sym}&apikey={fmp_key}")
            qt = http_get_json(f"{base}/quote?symbol={sym}&apikey={fmp_key}")
            est = http_get_json(f"{base}/analyst-estimates?symbol={sym}&period=annual&limit=3&apikey={fmp_key}")
            inc = http_get_json(f"{base}/income-statement?symbol={sym}&period=annual&limit=2&apikey={fmp_key}")
            km = (km[0] if isinstance(km, list) and km else km) or {}
            rt = (rt[0] if isinstance(rt, list) and rt else rt) or {}
            qt = (qt[0] if isinstance(qt, list) and qt else qt) or {}
            def kp(*keys):
                for k in keys:
                    if rt.get(k) is not None: return num(rt[k])
                    if km.get(k) is not None: return num(km[k])
                return None
            def asp(x):
                if x is None: return None
                return x*100 if abs(x) < 3 else x
            mcap = num(stock.get("market_cap")) or num(qt.get("marketCap"))
            price = num(qt.get("price"))
            yh, yl = num(qt.get("yearHigh")), num(qt.get("yearLow"))
            # creative: 52-week range position (0=at low, 1=at high)
            rng_pos = ((price - yl) / (yh - yl)) if (price and yh and yl and yh > yl) else None
            # forward (expected) growth from analyst estimates — same pass
            fwd_rev = None
            if isinstance(est, list) and len(est) >= 2:
                erows = sorted([e for e in est if e.get("date")], key=lambda e: e.get("date"))
                r0 = num(erows[0].get("revenueAvg") or erows[0].get("estimatedRevenueAvg"))
                r1 = num(erows[-1].get("revenueAvg") or erows[-1].get("estimatedRevenueAvg"))
                yrs = max(1, len(erows) - 1)
                if r0 and r1 and r0 > 0:
                    fwd_rev = round(((r1 / r0) ** (1 / yrs) - 1) * 100, 1)
            # trailing revenue growth (YoY) from the last two annual income statements
            rev_growth = None
            if isinstance(inc, list) and len(inc) >= 2:
                r_new = num(inc[0].get("revenue")); r_old = num(inc[1].get("revenue"))
                if r_new and r_old and r_old > 0:
                    rev_growth = round((r_new / r_old - 1) * 100, 1)
            return {
                "symbol": sym, "name": stock.get("name") or sym,
                "sector": stock.get("sector") or "", "industry": stock.get("industry") or "",
                "cap_bucket": stock.get("cap_bucket") or cap_bucket(mcap),
                "marketCap": mcap, "price": price,
                "peRatio": kp("priceToEarningsRatioTTM", "peRatioTTM"),
                "psRatio": kp("priceToSalesRatioTTM", "priceSalesRatioTTM"),
                "evEbitda": kp("enterpriseValueOverEBITDATTM", "evToEBITDATTM"),
                "roic": asp(kp("returnOnInvestedCapitalTTM", "roicTTM")),
                "roe": asp(kp("returnOnEquityTTM", "roeTTM")),
                "grossMargin": asp(kp("grossProfitMarginTTM")),
                "operatingMargin": asp(kp("operatingProfitMarginTTM")),
                "netMargin": asp(kp("netProfitMarginTTM")),
                "debtToEquity": kp("debtToEquityRatioTTM", "debtEquityRatioTTM"),
                "currentRatio": kp("currentRatioTTM"),
                "interestCoverage": kp("interestCoverageRatioTTM"),
                "revenueGrowth": rev_growth,  # trailing YoY from income-statement (was None)
                "fcfYieldCalc": kp("freeCashFlowYieldTTM"),
                "yearHigh": yh, "yearLow": yl, "range_position_52w": round(rng_pos, 2) if rng_pos is not None else None,
                "changesPct": num(qt.get("changePercentage") or qt.get("changesPercentage")),
                "_fwd_rev_growth": fwd_rev,
                "_tail": True,
            }
        except Exception:
            return None

    out = []
    with cf.ThreadPoolExecutor(max_workers=24) as ex:
        for fut in cf.as_completed([ex.submit(one, s) for s in tail]):
            r = fut.result()
            if r and (r.get("peRatio") is not None or r.get("psRatio") is not None):
                out.append(r)
    return out


def lambda_handler(event, context):
    t0 = time.time()
    screener = load("screener/data.json") or {}
    mr = {x.get("symbol"): x for x in
          (load("screener/mean-reversion.json") or {}).get("stocks", [])}
    fund = {x.get("ticker"): x for x in
            (load("data/fundamentals.json") or {}).get("companies", [])}
    shorts = {x.get("ticker"): x.get("state") for x in
              (load("data/short-pressure.json") or {}).get("names", [])}
    sfl = (load("data/share-flows.json") or {}).get("tickers") or {}
    print(f"[opp] share-flows join: {len(sfl)} names")
    prev = load(OUT_KEY)

    universe = screener.get("stocks", [])
    screener_syms = {(s.get("symbol") or s.get("ticker")) for s in universe}

    # ── FULL-UNIVERSE COVERAGE — deep-fetch the names not in the screener
    # (all caps: nano/micro/small/mid/large) so every stock gets scored. ──
    uni = load("data/universe.json") or {}
    tail = [s for s in (uni.get("stocks") or [])
            if (s.get("symbol") or "").upper() not in screener_syms
            and num(s.get("market_cap")) and num(s.get("market_cap")) >= 30e6]
    tail = tail[:1500]
    print(f"[opp] full-universe: deep-fetching {len(tail)} tail names (all caps)")
    tail_rows = fetch_tail_metrics(tail)
    universe = universe + tail_rows
    print(f"[opp] total universe now {len(universe)} (was {len(screener_syms)} screener)")

    bench = build_benchmarks(universe)
    # NEW: expected (forward) growth — tail names already fetched theirs in the
    # same pass; only the screener-500 need a forward fetch now (bounded, fast).
    print("[opp] fetching forward growth for screener names…")
    fwd_growth = fetch_forward_growth([s for s in universe if not s.get("_tail")])
    for tr in tail_rows:
        if tr.get("_fwd_rev_growth") is not None:
            fwd_growth[tr["symbol"]] = {"fwd_rev_growth": tr["_fwd_rev_growth"]}
    print(f"[opp] forward growth for {len(fwd_growth)} names (full universe)")
    print("[opp] fetching capex + buyback (price-support & buildout signals)…")
    capex_bb = fetch_capex_buyback(universe)
    print(f"[opp] capex/buyback for {len(capex_bb)} names")
    industry_bench = build_industry_benchmarks(universe, fwd_growth)
    weights = get_weights()
    print(f"[opp] factor weights: {weights}")

    rows = []
    for s in universe:
        sym = s.get("symbol") or s.get("ticker")
        r = score_stock(s, mr.get(sym), fund.get(sym), shorts.get(sym),
                         bench, weights, sfl.get(sym))
        if r:
            # ── NEW: growth-vs-industry intelligence ──
            ind_key = s.get("industry") or s.get("sector") or "Unknown"
            ib = industry_bench.get(ind_key) or {}
            fg = fwd_growth.get(sym) or {}
            cg = num(s.get("revenueGrowth"))
            if cg is not None and abs(cg) < 3: cg *= 100
            pe = num(s.get("peRatio"))
            ind_pe = (ib.get("medians") or {}).get("peRatio")
            exp_co = fg.get("fwd_rev_growth")
            exp_ind = ib.get("expected_industry_growth_pct")
            ind_g = ib.get("industry_growth_pct")
            gi = {
                "industry": ind_key,
                "company_rev_growth_pct": round(cg, 1) if cg is not None else None,
                "industry_growth_pct": ind_g,
                "expected_company_growth_pct": exp_co,
                "expected_industry_growth_pct": exp_ind,
                "expected_eps_growth_pct": fg.get("fwd_eps_growth"),
                "pe": round(pe, 1) if pe is not None else None,
                "industry_pe": ind_pe,
                "pe_vs_industry_pct": (round((pe / ind_pe - 1) * 100) if (pe and ind_pe and ind_pe > 0) else None),
                "outgrowing_industry": (cg is not None and ind_g is not None and cg > ind_g),
                "expected_to_outgrow_industry": (exp_co is not None and exp_ind is not None and exp_co > exp_ind),
                "peg_forward": (round(pe / exp_co, 2) if (pe and exp_co and exp_co > 0) else None),
                "range_position_52w": s.get("range_position_52w"),
            }
            # ── REVERSE-DCF IMPLIED GROWTH ──
            # The growth rate the market is pricing in. Simplified reverse-DCF:
            # solve the perpetual-growth FCF model for g given price/FCF and a
            # discount rate. implied_g = r - (FCF/Price) on a no-growth base,
            # extended with a 10y fade. We approximate via earnings yield:
            #   a P/E of X at discount r implies growth g s.t. the GGM holds.
            # implied_g ≈ r − (1/PE)  (Gordon, payout=1) — a clean first-order proxy.
            disc = 0.09  # 9% cost of equity
            implied_g = None
            if pe and pe > 0:
                implied_g = round((disc - (1.0 / pe)) * 100, 1)
            gi["implied_growth_pct"] = implied_g
            # Mispricing: market implies far LESS growth than analysts expect → cheap
            if implied_g is not None and exp_co is not None:
                gi["growth_gap_pct"] = round(exp_co - implied_g, 1)
                gi["reverse_dcf_mispriced"] = (exp_co - implied_g) > 8  # expected >> implied
            else:
                gi["growth_gap_pct"] = None
                gi["reverse_dcf_mispriced"] = False
            # Growth-Opportunity score (0-100): rewards growing faster than the
            # industry AND being cheap vs it, with forward confirmation + quality.
            go = 50.0
            if gi["outgrowing_industry"]: go += 12
            if gi["expected_to_outgrow_industry"]: go += 14
            if gi["pe_vs_industry_pct"] is not None and gi["pe_vs_industry_pct"] < -10: go += 12   # cheaper P/E than industry
            elif gi["pe_vs_industry_pct"] is not None and gi["pe_vs_industry_pct"] > 30: go -= 10
            if gi["peg_forward"] is not None:
                if gi["peg_forward"] < 1: go += 14
                elif gi["peg_forward"] < 1.5: go += 6
                elif gi["peg_forward"] > 3: go -= 8
            if exp_co is not None and exp_co > 20: go += 6
            if r.get("fund") and num((fund.get(sym) or {}).get("gross_margin", None)) and num((fund.get(sym) or {}).get("gross_margin")) > 50: go += 4

            # ── CAPEX + BUYBACK (price-support & buildout drivers of who pumps) ──
            cb = capex_bb.get(sym) or {}
            bby = cb.get("buyback_yield_pct")
            cgr = cb.get("capex_growth_pct")
            sec = s.get("sector") or ""
            gi["buyback_yield_pct"] = bby
            gi["capex_growth_pct"] = cgr
            gi["capex_intensity_pct"] = cb.get("capex_intensity_pct")
            # Buybacks: a genuine, well-documented support — shrinking share count
            # mechanically lifts EPS/price. Weight meaningfully but cap it.
            if bby is not None:
                if bby >= 8:   go += 12; gi["buyback_signal"] = "aggressive (>8% yield)"
                elif bby >= 4: go += 8;  gi["buyback_signal"] = "strong (4-8% yield)"
                elif bby >= 1.5: go += 4; gi["buyback_signal"] = "moderate"
                else: gi["buyback_signal"] = "minimal"
            # Capex GROWTH: bullish where it signals the AI/power buildout (capex-
            # beneficiary sectors); modest/neutral elsewhere (it can dilute margins).
            if cgr is not None:
                in_bull = sec in CAPEX_BULL_SECTORS
                if cgr >= 40 and in_bull:   go += 10; gi["capex_signal"] = f"surging capex (+{cgr}%) in a buildout sector"
                elif cgr >= 20 and in_bull: go += 6;  gi["capex_signal"] = f"rising capex (+{cgr}%) — capacity/demand buildout"
                elif cgr >= 25 and not in_bull: go += 2; gi["capex_signal"] = f"rising capex (+{cgr}%)"
                elif cgr <= -25: go -= 2; gi["capex_signal"] = f"capex cut ({cgr}%) — retrenchment"
                else: gi["capex_signal"] = "steady capex"

            r["growth_intel"] = gi
            r["growth_opportunity_score"] = max(0, min(100, round(go, 1)))
            # ── MOMENTUM / REGIME GATE: "cheap AND inflecting" ──
            # The backtest showed cheap value lagged momentum. Use the engine's
            # own momentum_score + 52w range position to flag whether a name is
            # actually inflecting. Cheap names that are still falling get a
            # 'falling knife' tag + score haircut; cheap + rising get a boost.
            mom = r.get("scores", {}).get("momentum")
            rng = (r.get("growth_intel") or {}).get("range_position_52w")
            chg = num(s.get("changesPct"))
            inflecting = None
            if mom is not None:
                inflecting = (mom >= 55) or (rng is not None and rng >= 0.5) or (chg is not None and chg > 0)
                cheap_value = (r.get("scores", {}).get("value") or 0) >= 60
                if cheap_value and inflecting:
                    r["growth_opportunity_score"] = min(100, r["growth_opportunity_score"] + 8)
                    r["cheap_and_inflecting"] = True
                elif cheap_value and rng is not None and rng < 0.25 and (mom or 50) < 40:
                    r["growth_opportunity_score"] = max(0, r["growth_opportunity_score"] - 10)
                    r["cheap_and_inflecting"] = False
                    r.setdefault("flags", []).append("falling knife — cheap but not inflecting")
                else:
                    r["cheap_and_inflecting"] = bool(inflecting)
            # cap bucket (from universe or derived from market cap)
            mcap_v = num(s.get("marketCap"))
            r["cap_bucket"] = s.get("cap_bucket") or (
                "nano" if (mcap_v and mcap_v < 50e6) else "micro" if (mcap_v and mcap_v < 300e6)
                else "small" if (mcap_v and mcap_v < 2e9) else "mid" if (mcap_v and mcap_v < 10e9)
                else "large" if (mcap_v and mcap_v < 200e9) else "mega" if mcap_v else None)
            # ── CREATIVE: Compounder composite — durable high-quality GROWTH ──
            growth_for_comp = exp_co if (exp_co is not None) else cg   # forward, else trailing
            roic_v = num(s.get("roic")); roic_v = roic_v*100 if (roic_v is not None and abs(roic_v) < 3) else roic_v
            gm_v = num(s.get("grossMargin")); gm_v = gm_v*100 if (gm_v is not None and abs(gm_v) < 3) else gm_v
            de_v = num(s.get("debtToEquity"))
            if growth_for_comp is not None and growth_for_comp > 5:
                comp = 0.0; parts = 0.0
                comp += min(1.0, max(0, growth_for_comp/30.0)) * 1.5; parts += 1.5
                if roic_v is not None: comp += min(1.0, max(0, roic_v/25.0)); parts += 1
                if gm_v is not None: comp += min(1.0, max(0, gm_v/70.0)); parts += 1
                if de_v is not None: comp += (1.0 if de_v < 0.5 else 0.5 if de_v < 1.0 else 0.0); parts += 1
                if gi["peg_forward"] is not None: comp += (1.0 if gi["peg_forward"] < 1.5 else 0.4 if gi["peg_forward"] < 2.5 else 0.0); parts += 1
                r["compounder_score"] = round((comp/parts*100), 1) if parts >= 3.5 else None
            else:
                r["compounder_score"] = None
            dy = num(s.get("dividendYield")); dy = dy*100 if (dy is not None and abs(dy) < 1) else dy
            gr_for_lynch = exp_co if exp_co is not None else cg
            if pe and pe > 0 and gr_for_lynch is not None:
                r["lynch_ratio"] = round((gr_for_lynch + (dy or 0)) / pe, 2)
            rows.append(r)

    rows.sort(key=lambda r: r["opportunity_score"], reverse=True)

    # ── NEW: backlog/RPO from earnings transcripts for the top names ──
    print("[opp] extracting backlog for top names…")
    fmp_key = os.environ.get("FMP_KEY", "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb")
    for r in rows[:40]:
        bl = extract_backlog(r["ticker"], fmp_key)
        if bl:
            r["backlog"] = bl
            # backlog is a durability bonus to the growth-opportunity score
            r["growth_opportunity_score"] = min(100, (r.get("growth_opportunity_score") or 50) + 6)

    # ── estimate-revision momentum (v2: dated baselines, multi-week lookback) ──
    # Revisions show up over WEEKS, not intraday. The v1 logic compared today
    # vs a file it overwrote every run, so prior≈current → everything collapsed
    # to FLAT/None and the signal was never measurable. v2 persists an IMMUTABLE
    # dated baseline once per day and compares today's forward-growth estimate
    # against one ~REV_LOOKBACK_DAYS old, so analysts walking estimates up/down
    # is actually detectable. Writes data/estimate-revisions/{date}.json (per-day,
    # not clobbered intraday) plus -latest.json for back-compat.
    REV_LOOKBACK_DAYS = 15   # compare vs ~3 trading weeks ago
    REV_THRESH_PP = 1.5      # +/- pp change in fwd revenue-growth rate = a real revision
    try:
        from datetime import date as _date
        today_iso = datetime.now(timezone.utc).date().isoformat()
        est_snap = {sym: (fwd_growth.get(sym) or {}).get("fwd_rev_growth")
                    for sym in [(s.get("symbol") or s.get("ticker")) for s in universe]
                    if (fwd_growth.get(sym) or {}).get("fwd_rev_growth") is not None}

        # 1) persist today's dated baseline ONCE (don't clobber intraday re-runs)
        dated_key = f"data/estimate-revisions/{today_iso}.json"
        try:
            s3.head_object(Bucket=S3_BUCKET, Key=dated_key)
            baseline_exists = True
        except Exception:
            baseline_exists = False
        if not baseline_exists:
            s3.put_object(Bucket=S3_BUCKET, Key=dated_key,
                          Body=json.dumps({"date": today_iso, "fwd_rev_growth": est_snap}, default=str).encode(),
                          ContentType="application/json")

        # 2) choose comparison baseline: newest dated file that is >= LOOKBACK old;
        #    while warming up (no file that old yet), fall back to OLDEST available.
        objs = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix="data/estimate-revisions/").get("Contents", [])
        dated = []
        for o in objs:
            nm = o["Key"].split("/")[-1].replace(".json", "")
            try:
                age = (_date.fromisoformat(today_iso) - _date.fromisoformat(nm)).days
                if age > 0:
                    dated.append((age, nm, o["Key"]))
            except Exception:
                continue
        dated.sort()  # age ascending
        aged = [d for d in dated if d[0] >= REV_LOOKBACK_DAYS]
        chosen = aged[0] if aged else (dated[-1] if dated else None)
        warming = not aged
        prev_snap, base_date, base_age = {}, None, None
        if chosen:
            base_age, base_date, base_k = chosen
            prev_snap = (load(base_k) or {}).get("fwd_rev_growth", {})

        # 3) patch revision momentum onto rows
        n_up = n_dn = n_flat = 0
        for r in rows:
            cur, old = est_snap.get(r["ticker"]), prev_snap.get(r["ticker"])
            if cur is not None and old is not None:
                delta = round(cur - old, 1)
                direction = "UP" if delta > REV_THRESH_PP else "DOWN" if delta < -REV_THRESH_PP else "FLAT"
                r["estimate_revision"] = {"prior": old, "current": cur, "delta_pp": delta,
                                           "direction": direction, "baseline_date": base_date,
                                           "baseline_age_days": base_age, "warming_up": warming}
                if direction == "UP":   n_up += 1
                elif direction == "DOWN": n_dn += 1
                else:                    n_flat += 1
                if delta > 2.0:
                    r["growth_opportunity_score"] = min(100, (r.get("growth_opportunity_score") or 50) + 8)
        print(f"[opp] revision-momentum: baseline={base_date} age={base_age}d warming={warming} "
              f"UP={n_up} DOWN={n_dn} FLAT={n_flat} covered={len(est_snap)}")

        # 4) keep -latest.json for back-compat
        s3.put_object(Bucket=S3_BUCKET, Key="data/estimate-revisions-latest.json",
                      Body=json.dumps({"date": today_iso, "fwd_rev_growth": est_snap}, default=str).encode(),
                      ContentType="application/json")
    except Exception as e:
        print(f"[opp] revision momentum err: {e}")

    by_verdict = {}
    for r in rows:
        by_verdict[r["verdict"]] = by_verdict.get(r["verdict"], 0) + 1

    top = [r for r in rows if r["verdict"] in
           ("STRONG OPPORTUNITY", "OPPORTUNITY")][:30]
    avoid = sorted([r for r in rows if r["verdict"] == "HIGH RISK"],
                   key=lambda r: r["opportunity_score"])[:12]
    changes = compute_changes(prev, rows)

    sector_bench = {sec: {"n": b["n"], "medians": b["medians"]}
                    for sec, b in bench.items()}

    out = {
        "schema_version": "2.0",
        "method": "retail_edge_multi_method_synthesis",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "elapsed_s": round(time.time() - t0, 2),
        "n_covered": len(rows),
        "factor_weights": weights,
        "verdict_counts": by_verdict,
        "changes": changes,
        "top_opportunities": top,
        "avoid_list": avoid,
        "all": rows,
        "sector_benchmarks": sector_bench,
        "industry_benchmarks": {ind: {"n": b["n"], "industry_growth_pct": b.get("industry_growth_pct"),
                                       "expected_industry_growth_pct": b.get("expected_industry_growth_pct"),
                                       "median_pe": (b.get("medians") or {}).get("peRatio")}
                                 for ind, b in industry_bench.items()},
        "disclaimer": ("Research and education only — not financial advice. "
                       "Every stock carries risk of loss. Fair value is an "
                       "estimate from models that can be wrong; always do your "
                       "own diligence before investing."),
        "note": ("Each verdict triangulates three independent fair-value "
                 "methods (DCF, historical-multiple reversion, analyst "
                 "consensus) with quality, growth, momentum, industry-"
                 "relative position and cycle stage. Value-trap, cycle and "
                 "cap guards prevent cheap-but-dangerous stocks from being "
                 "rated an opportunity."),
    }
    s3.put_object(Bucket=S3_BUCKET, Key=OUT_KEY,
                  Body=json.dumps(out, default=str).encode("utf-8"),
                  ContentType="application/json",
                  CacheControl="public, max-age=3600")

    # ── append-only daily snapshot — the track-record ledger ──
    # Records what we said, when, at what price. justhodl-track-record
    # later computes forward returns / alpha by verdict tier from these.
    try:
        day = datetime.now(timezone.utc).date().isoformat()
        snap = {"date": day, "generated_at": out["generated_at"],
                "schema": "1.1", "n": len(rows),
                "picks": {r["ticker"]: {"v": r["verdict"], "p": r["price"],
                                        "fv": r["fair_value_mid"],
                                        "s": r["opportunity_score"],
                                        "c": r["confidence"],
                                        "go": r.get("growth_opportunity_score"),
                                        "comp": r.get("compounder_score"),
                                        "rev": (r.get("estimate_revision") or {}).get("direction"),
                                        "cap": r.get("cap_bucket"),
                                        "pv": r.get("peer_val"),
                                        "cyc": (r.get("cycle") or {}).get("tag"),
                                        "ss": [r["scores"]["value"],
                                               r["scores"]["quality"],
                                               r["scores"]["growth"],
                                               r["scores"]["momentum"]]}
                          for r in rows}}
        s3.put_object(Bucket=S3_BUCKET,
                      Key=f"data/track-record/snapshots/{day}.json",
                      Body=json.dumps(snap, default=str).encode("utf-8"),
                      ContentType="application/json")
        print(f"[opp] snapshot logged: {day} ({len(rows)} picks)")
    except Exception as e:
        print(f"[opp] WARN snapshot write failed: {e}")

    print(f"[opp] v2 · {len(rows)} covered · {len(top)} opportunities · "
          f"{len(avoid)} high-risk · {len(changes.get('newly_opportunity', []))} "
          f"new · {out['elapsed_s']}s")
    return {"statusCode": 200, "body": json.dumps({
        "ok": True, "schema": "2.0", "n_covered": len(rows),
        "n_opportunities": len(top), "verdict_counts": by_verdict,
        "n_new_opportunities": len(changes.get("newly_opportunity", []))})}
