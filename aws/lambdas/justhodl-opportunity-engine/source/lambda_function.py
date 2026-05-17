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
def score_stock(s, mr, fund, short_state, bench, weights):
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
def lambda_handler(event, context):
    t0 = time.time()
    screener = load("screener/data.json") or {}
    mr = {x.get("symbol"): x for x in
          (load("screener/mean-reversion.json") or {}).get("stocks", [])}
    fund = {x.get("ticker"): x for x in
            (load("data/fundamentals.json") or {}).get("companies", [])}
    shorts = {x.get("ticker"): x.get("state") for x in
              (load("data/short-pressure.json") or {}).get("names", [])}
    prev = load(OUT_KEY)

    universe = screener.get("stocks", [])
    bench = build_benchmarks(universe)
    weights = get_weights()
    print(f"[opp] factor weights: {weights}")

    rows = []
    for s in universe:
        sym = s.get("symbol") or s.get("ticker")
        r = score_stock(s, mr.get(sym), fund.get(sym), shorts.get(sym),
                         bench, weights)
        if r:
            rows.append(r)

    rows.sort(key=lambda r: r["opportunity_score"], reverse=True)
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
                "schema": "1.0", "n": len(rows),
                "picks": {r["ticker"]: {"v": r["verdict"], "p": r["price"],
                                        "fv": r["fair_value_mid"],
                                        "s": r["opportunity_score"],
                                        "c": r["confidence"],
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
