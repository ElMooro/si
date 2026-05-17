"""
justhodl-opportunity-engine v2.0 - Retail Opportunity & Risk Verdict

WHAT IT DOES
------------
Turns the platform's scattered institutional signals into ONE plain-English
verdict per S&P 500 stock so a non-professional can spot a real opportunity
(and its risks) in seconds. It does NOT recompute fundamentals - it reads
existing sidecars and fuses them.

v2.0 ADDS (the "true edge" layer)
---------------------------------
  * PEER BENCHMARKING - every metric scored vs the stock's INDUSTRY median
    (P/E, forward P/E, growth, margins, ROIC, debt, cash-flow yield, P/B).
  * CYCLE RADAR - detects early- vs late-cycle. For cyclical sectors a low
    P/E at PEAK margins is flagged as a trap; low margins + rising forward
    earnings is flagged as genuine early-cycle value.
  * STATEMENT SCORECARD - income statement / balance sheet / cash flow each
    graded against the industry.
  * ACCURACY FIXES - capped fair-value estimates are flagged honestly, a
    STRONG rating is never given on a capped/low-confidence number, and the
    DCF is discounted when growth is an unsustainable spike.
  * PLAIN-ENGLISH - a one-line bottom_line per stock + a glossary.

INSTITUTIONAL SAFEGUARDS
  1. TWO independent fair-value methods (DCF + multiple-reversion); a RANGE,
     never a single false-precision number.
  2. Confidence rating - HIGH only when both methods agree on magnitude.
  3. VALUE-TRAP GUARD - cheap + distressed, or cheap cyclical at peak
     earnings, can never be rated an opportunity.
  4. Risks surfaced with the same prominence as opportunities.

Research / education tooling - NOT financial advice.
OUTPUT: data/opportunities.json   SCHEDULE: daily 14:00 UTC
"""
import json
import os
import time
import statistics
from datetime import datetime, timezone

import boto3

S3_BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
OUT_KEY = "data/opportunities.json"
s3 = boto3.client("s3", region_name="us-east-1")

# honest ceiling on the displayed under/over-valued % - past this a model
# estimate is not credible enough to quote a precise number.
PCT_CAP = 45.0

CYCLICAL_SECTORS = {"Energy", "Basic Materials", "Materials",
                    "Consumer Cyclical", "Industrials", "Real Estate"}
CYCLICAL_HINTS = ("semiconduc", "oil", "gas ", "mining", "steel", "copper",
                  "aluminum", "coal", "auto", "homebuild", "construction",
                  "chemical", "airline", "shipping", "metals", "lumber")

GLOSSARY = {
    "pe": "Price-to-Earnings (P/E): dollars you pay for $1 of yearly profit. "
          "Lower often means cheaper - but not always.",
    "forward_pe": "Forward P/E: same idea using NEXT year's expected profit. "
                  "Lower than today's P/E means the market expects profit to grow.",
    "revenue_growth": "Revenue growth: how fast yearly sales are rising.",
    "operating_margin": "Operating margin: profit left from each sales dollar "
                        "after running costs. Higher = more efficient.",
    "net_margin": "Net margin: final profit kept from each sales dollar.",
    "roic": "Return on Invested Capital: profit squeezed from the money put "
            "into the business. Above ~15% is excellent.",
    "debt_to_equity": "Debt-to-Equity: debt carried vs the company's own "
                      "funds. Lower is safer.",
    "fcf_yield": "Free-Cash-Flow yield: real cash the business throws off "
                 "each year as a % of its price. Higher is better.",
    "pb": "Price-to-Book: price vs the accounting value of assets. Useful "
          "for cyclical industries where P/E lies.",
    "altman_z": "Altman-Z: bankruptcy-risk score. Above 3 is safe, below "
                "1.8 is a danger zone.",
    "piotroski": "Piotroski F-Score (0-9): a financial-health checklist. "
                 "7+ is strong, 3 or less is weak.",
    "cycle": "Where the company sits in its boom-bust cycle. Cyclical "
             "industries look 'cheap' on P/E exactly when profits are about "
             "to fall - the classic trap.",
    "fair_value": "Fair value: what our two models estimate the business is "
                  "worth. An estimate - models can be wrong.",
}


# --------------------------------------------------------------------------
# helpers
# --------------------------------------------------------------------------
def load(key):
    try:
        return json.loads(s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read())
    except Exception as e:
        print(f"[opportunity] WARN could not read {key}: {e}")
        return None


def num(v):
    try:
        f = float(v)
        return f if f == f else None  # drop NaN
    except (TypeError, ValueError):
        return None


def clamp(v, lo=0, hi=100):
    return max(lo, min(hi, v))


def metrics_of(s):
    """Normalised numeric metric bundle for one screener record."""
    mc = num(s.get("marketCap"))
    fcf = num(s.get("freeCashFlow"))
    return {
        "pe": num(s.get("peRatio")),
        "forward_pe": num(s.get("forwardPE")),
        "revenue_growth": num(s.get("revenueGrowth")),
        "eps_growth": num(s.get("epsGrowth")),
        "gross_margin": num(s.get("grossMargin")),
        "operating_margin": num(s.get("operatingMargin")),
        "net_margin": num(s.get("netMargin")),
        "roic": num(s.get("roic")),
        "roe": num(s.get("roe")),
        "debt_to_equity": num(s.get("debtToEquity")),
        "pb": num(s.get("pbRatio")),
        "fcf_yield": (fcf / mc * 100.0) if (fcf is not None and mc and mc > 0)
        else None,
    }


# metrics where a HIGHER value is better (for peer "read" wording)
HIGHER_BETTER = {"revenue_growth", "eps_growth", "gross_margin",
                 "operating_margin", "net_margin", "roic", "roe", "fcf_yield"}
# sane bands so a garbage value never poisons an industry median
SANE_BAND = {
    "pe": (0.5, 200), "forward_pe": (0.5, 200), "pb": (0.05, 60),
    "debt_to_equity": (0, 15), "revenue_growth": (-90, 300),
    "eps_growth": (-95, 400), "gross_margin": (-50, 100),
    "operating_margin": (-80, 80), "net_margin": (-90, 70),
    "roic": (-50, 120), "roe": (-100, 150), "fcf_yield": (-30, 40),
}


def in_band(metric, v):
    lo, hi = SANE_BAND.get(metric, (-1e9, 1e9))
    return v is not None and lo <= v <= hi


# --------------------------------------------------------------------------
# peer groups - industry medians (fallback to sector)
# --------------------------------------------------------------------------
def build_peer_groups(universe):
    """Returns peer_median(stock, metric) -> (median, group_label)."""
    ind_members, sec_members = {}, {}
    for s in universe:
        ind = (s.get("industry") or "").strip()
        sec = (s.get("sector") or "").strip() or "Unknown"
        m = metrics_of(s)
        if ind:
            ind_members.setdefault(ind, []).append(m)
        sec_members.setdefault(sec, []).append(m)

    def medians(groups):
        out = {}
        for g, members in groups.items():
            mm = {}
            for metric in SANE_BAND:
                vals = [x[metric] for x in members if in_band(metric, x[metric])]
                if len(vals) >= 3:
                    mm[metric] = round(statistics.median(vals), 3)
            out[g] = mm
        return out

    ind_med = medians(ind_members)
    sec_med = medians(sec_members)

    def peer_median(stock):
        ind = (stock.get("industry") or "").strip()
        sec = (stock.get("sector") or "").strip() or "Unknown"
        # industry group only if it has enough members for a stable median
        if ind and len(ind_members.get(ind, [])) >= 4 and ind_med.get(ind):
            return ind_med[ind], f"{ind} industry"
        return sec_med.get(sec, {}), f"{sec} sector"

    return peer_median, ind_med, sec_med


# --------------------------------------------------------------------------
# fair value - two methods, outlier-rejected, capped + flagged
# --------------------------------------------------------------------------
def fair_value(price, dcf_fv, mr_fv, growth_spike):
    """Returns dict: low/mid/high/under_pct/confidence/capped.

    growth_spike (bool) - revenue growing so fast the DCF is probably
    extrapolating an unsustainable boom; we down-weight the DCF then.
    """
    blank = {"low": None, "mid": None, "high": None, "under_pct": None,
             "confidence": "n/a", "capped": False}
    if not price or price <= 0:
        return blank
    fvs = [v for v in (dcf_fv, mr_fv) if v and v > 0]
    if not fvs:
        return blank

    if len(fvs) == 2:
        lo, hi = min(fvs), max(fvs)
        agree_dir = (dcf_fv > price) == (mr_fv > price)
        spread = hi / lo - 1.0
        extreme = max(abs(dcf_fv / price - 1), abs(mr_fv / price - 1)) > 0.55
        if growth_spike and dcf_fv > mr_fv:
            # unsustainable-growth DCF -> lean on the reversion estimate
            anchor, conf = mr_fv * 0.65 + dcf_fv * 0.35, "low"
        elif agree_dir and spread <= 0.40 and not extreme:
            anchor, conf = sum(fvs) / 2.0, "high"
        elif agree_dir:
            anchor = min(fvs, key=lambda v: abs(v - price))  # conservative
            conf = "low"
        else:
            anchor, conf = sum(fvs) / 2.0, "mixed"
    else:
        anchor = fvs[0]
        lo, hi = anchor * 0.88, anchor * 1.12
        conf = "single" if abs(anchor / price - 1) <= 0.55 else "single-weak"

    raw = (anchor / price - 1) * 100.0
    capped = abs(raw) > PCT_CAP
    under = max(-PCT_CAP, min(PCT_CAP, raw))
    if capped and conf == "high":
        conf = "low"  # a capped gap is never high-confidence
    return {"low": round(lo, 2), "mid": round(anchor, 2), "high": round(hi, 2),
            "under_pct": round(under, 1), "confidence": conf, "capped": capped}


# --------------------------------------------------------------------------
# peer comparison + statement scorecard
# --------------------------------------------------------------------------
PEER_LABELS = {
    "pe": "P/E", "forward_pe": "Forward P/E", "revenue_growth": "Revenue growth",
    "operating_margin": "Operating margin", "net_margin": "Net margin",
    "roic": "ROIC", "debt_to_equity": "Debt / Equity", "fcf_yield": "FCF yield",
    "pb": "Price / Book",
}


def peer_comparison(m, peer_med):
    """Per-metric value vs industry median, with a plain read."""
    rows = []
    for key in PEER_LABELS:
        v, pm = m.get(key), peer_med.get(key)
        if v is None or pm is None or pm == 0:
            continue
        delta = (v / pm - 1.0) * 100.0
        higher_better = key in HIGHER_BETTER
        if abs(delta) < 8:
            read = "in line with peers"
        elif (delta > 0) == higher_better:
            read = "better than peers" if higher_better else "richer than peers"
        else:
            read = "weaker than peers" if higher_better else "cheaper than peers"
        rows.append({"metric": PEER_LABELS[key], "key": key,
                     "value": round(v, 2), "industry_median": round(pm, 2),
                     "delta_pct": round(delta, 1), "read": read})
    return rows


def statement_scorecard(m, fund, peer_med):
    """Income statement / balance sheet / cash flow, each 0-100 vs industry."""
    def rel(metric, higher_better=True):
        v, pm = m.get(metric), peer_med.get(metric)
        if v is None or pm is None:
            return 0
        if higher_better:
            return clamp((v - pm), -25, 25)
        return clamp((pm - v), -25, 25)

    income = clamp(50 + rel("revenue_growth") * 0.5
                   + rel("operating_margin") * 0.6 + rel("net_margin") * 0.4)
    altman = num((fund or {}).get("altman_z"))
    bs = 50 + rel("debt_to_equity", higher_better=False) * 0.9
    if altman is not None:
        bs += 18 if altman >= 3 else (-22 if altman < 1.8 else 0)
    balance = clamp(bs)
    cash = clamp(50 + rel("fcf_yield") * 1.4)

    def grade(x):
        return ("stronger than its industry" if x >= 62
                else "weaker than its industry" if x <= 40
                else "in line with its industry")
    return {
        "income_statement": {"score": round(income), "read": grade(income)},
        "balance_sheet": {"score": round(balance), "read": grade(balance)},
        "cash_flow": {"score": round(cash), "read": grade(cash)},
    }


# --------------------------------------------------------------------------
# cycle radar
# --------------------------------------------------------------------------
def cycle_position(s, m, peer_med):
    """Detects early- vs late-cycle. Returns dict with label + plain text."""
    sector = (s.get("sector") or "").strip()
    industry = (s.get("industry") or "").lower()
    cyclical = (sector in CYCLICAL_SECTORS
                or any(h in industry for h in CYCLICAL_HINTS))

    pe, fpe = m.get("pe"), m.get("forward_pe")
    op, op_med = m.get("operating_margin"), peer_med.get("operating_margin")

    # earnings direction implied by forward vs trailing P/E
    earn_dir = None
    if pe and fpe and pe > 0 and fpe > 0:
        r = fpe / pe - 1.0
        earn_dir = "falling" if r > 0.15 else ("rising" if r < -0.15 else "flat")
    # margin position vs industry
    margin_pos = None
    if op is not None and op_med is not None:
        if op > op_med * 1.25:
            margin_pos = "peak"
        elif op < op_med * 0.75:
            margin_pos = "trough"
        else:
            margin_pos = "mid"

    if not cyclical:
        return {"cyclical": False, "label": "NOT CYCLICAL", "color": "dim",
                "text": "Not a cyclical business - its P/E is a reliable gauge."}

    late = (margin_pos == "peak") or (earn_dir == "falling")
    early = (margin_pos == "trough") or (earn_dir == "rising")
    if late and not early:
        return {"cyclical": True, "label": "LATE CYCLE - PEAK RISK",
                "color": "red",
                "text": "Cyclical running at peak earnings - a low P/E here "
                        "is misleading and profits often fall from this point."}
    if early and not late:
        return {"cyclical": True, "label": "EARLY CYCLE - RECOVERY UPSIDE",
                "color": "green",
                "text": "Cyclical with earnings near a trough - historically "
                        "the rewarding part of the cycle to look early."}
    return {"cyclical": True, "label": "MID CYCLE", "color": "yellow",
            "text": "Cyclical business mid-cycle - no clear cycle edge "
                    "either way right now."}


# --------------------------------------------------------------------------
# per-stock scoring
# --------------------------------------------------------------------------
def score_stock(s, mr, fund, short_state, peer_median_fn):
    sym = s.get("symbol") or s.get("ticker")
    price = num(s.get("price"))
    if not sym or not price:
        return None
    m = metrics_of(s)
    peer_med, peer_group = peer_median_fn(s)

    rev_g = m["revenue_growth"]
    growth_spike = rev_g is not None and rev_g > 40
    dcf_fv = num(s.get("dcfFairValue"))
    mr_fv = num((mr or {}).get("mr_price"))
    fv = fair_value(price, dcf_fv, mr_fv, growth_spike)
    under = fv["under_pct"]
    conf = fv["confidence"]

    altman = num((fund or {}).get("altman_z"))
    piotroski = num((fund or {}).get("piotroski"))
    d2e = m["debt_to_equity"]
    op_m = m["operating_margin"]
    roic = m["roic"]
    pe = m["pe"]
    chg6 = num(s.get("chg6m"))
    cross = s.get("crossSignal")
    beats = num(s.get("beatStreak"))

    peers = peer_comparison(m, peer_med)
    scorecard = statement_scorecard(m, fund, peer_med)
    cyc = cycle_position(s, m, peer_med)

    # -- sub-scores (0-100) --
    value_score = clamp(40 + (under if under is not None else 0) * 1.0)
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
    quality_score = clamp(q)
    g = 50
    if rev_g is not None:
        g += 22 if rev_g > 20 else (12 if rev_g > 8 else (-22 if rev_g < 0 else 0))
    if beats is not None and beats >= 4:
        g += 12
    growth_score = clamp(g)
    mo = 50
    if chg6 is not None:
        mo += 18 if chg6 > 20 else (-18 if chg6 < -20 else 0)
    if cross == "GOLDEN":
        mo += 15
    elif cross == "DEATH":
        mo -= 15
    momentum_score = clamp(mo)
    # peer-relative bonus folds into value
    cheaper_peers = sum(1 for p in peers if p["key"] in ("pe", "forward_pe", "pb")
                        and p["read"] == "cheaper than peers")
    value_score = clamp(value_score + cheaper_peers * 4)

    opp = round(0.38 * value_score + 0.30 * quality_score
                + 0.20 * growth_score + 0.12 * momentum_score, 1)

    # -- risk flags & opportunity highlights (plain English) --
    distress = altman is not None and altman < 1.8
    risks, ops = [], []
    if distress:
        risks.append("Elevated bankruptcy risk (weak Altman-Z score)")
    if cyc["label"] == "LATE CYCLE - PEAK RISK":
        risks.append("Cyclical near peak earnings - the low P/E is misleading")
    if cross == "DEATH":
        days = s.get("crossDaysAgo")
        risks.append(f"In a downtrend - death cross{f' {days}d ago' if days else ''}")
    if rev_g is not None and rev_g < 0:
        risks.append(f"Revenue is shrinking ({rev_g:.0f}% YoY)")
    if d2e is not None and d2e > 2:
        risks.append("Carries heavy debt relative to equity")
    if pe is not None and pe < 0:
        risks.append("Not currently profitable")
    if short_state == "PRESSURE BUILDING":
        risks.append("Short sellers are building pressure")
    if fv["capped"]:
        risks.append("Valuation gap is too large to quote precisely - "
                     "treat the % as a rough flag, not a target")
    elif conf in ("mixed", "low", "single-weak"):
        risks.append("Valuation estimate is uncertain - the methods disagree")

    if cyc["label"] == "EARLY CYCLE - RECOVERY UPSIDE":
        ops.append("Looks early in its cycle - profits likely near a trough")
    if under is not None and under >= 15 and not fv["capped"]:
        ops.append(f"Trading ~{under:.0f}% below estimated fair value")
    if conf == "high" and under is not None and under > 0:
        ops.append("Two independent valuation methods agree it looks cheap")
    if cheaper_peers >= 2:
        ops.append("Cheaper than its industry peers on several measures")
    if rev_g is not None and rev_g > 10:
        ops.append(f"Revenue growing ~{rev_g:.0f}% a year")
    if altman is not None and altman >= 3:
        ops.append("Rock-solid balance sheet")
    if cross == "GOLDEN":
        ops.append("Recently entered an uptrend (golden cross)")
    if beats is not None and beats >= 3:
        ops.append(f"Beaten earnings {int(beats)} quarters running")
    if op_m is not None and op_m > 20:
        ops.append("Highly profitable business")

    # -- verdict (value-trap + cycle + confidence gated) --
    value_trap = (under is not None and under >= 15
                  and (distress or (rev_g is not None and rev_g < -3)))
    cycle_trap = (cyc["label"] == "LATE CYCLE - PEAK RISK"
                  and under is not None and under > 0)
    hi_conf = conf == "high"
    if distress or value_trap or cycle_trap:
        verdict, vcolor = "HIGH RISK", "red"
    elif under is not None and under <= -15:
        verdict, vcolor = "EXPENSIVE", "orange"
    elif (hi_conf and opp >= 70 and under is not None
          and 12 < under <= PCT_CAP and not fv["capped"]):
        verdict, vcolor = "STRONG OPPORTUNITY", "green"
    elif opp >= 56 and under is not None and under > 5:
        verdict, vcolor = "OPPORTUNITY", "cyan"
    elif under is not None and -15 < under < 8:
        verdict, vcolor = "FAIR VALUE", "yellow"
    else:
        verdict, vcolor = "HOLD / NEUTRAL", "dim"

    bottom_line = build_bottom_line(verdict, under, conf, fv["capped"],
                                    scorecard, cyc, cheaper_peers)

    return {
        "ticker": sym,
        "company": s.get("companyName") or s.get("name") or sym,
        "sector": s.get("sector"),
        "industry": s.get("industry"),
        "peer_group": peer_group,
        "price": price,
        "fair_value_low": fv["low"], "fair_value_mid": fv["mid"],
        "fair_value_high": fv["high"],
        "undervalued_pct": under, "valuation_capped": fv["capped"],
        "confidence": conf,
        "verdict": verdict, "verdict_color": vcolor,
        "opportunity_score": opp,
        "scores": {"value": round(value_score), "quality": round(quality_score),
                   "growth": round(growth_score), "momentum": round(momentum_score)},
        "cycle": cyc,
        "peer_comparison": peers,
        "statement_scorecard": scorecard,
        "bottom_line": bottom_line,
        "opportunities": ops[:4],
        "risks": risks[:4],
    }


def build_bottom_line(verdict, under, conf, capped, scorecard, cyc, cheaper):
    """One plain-English sentence a non-investor can act on."""
    if under is None:
        val = "Valuation is unclear from the data"
    elif capped:
        val = ("Screens as deeply mispriced, but the gap is too wide to "
               "trust as a precise number")
    elif under >= 15:
        val = f"Looks roughly {under:.0f}% undervalued"
    elif under >= 8:
        val = f"Looks modestly undervalued (~{under:.0f}%)"
    elif under > -8:
        val = "Sits close to fair value"
    elif under > -15:
        val = f"Looks modestly expensive (~{abs(under):.0f}%)"
    else:
        val = f"Looks roughly {abs(under):.0f}% overvalued"

    bs = scorecard["balance_sheet"]["score"]
    quality = (" with a strong balance sheet" if bs >= 62
               else " though the balance sheet is shaky" if bs <= 40 else "")
    peer = (" and it is cheaper than its industry" if cheaper >= 2 else "")
    cycle = ""
    if cyc["label"] == "LATE CYCLE - PEAK RISK":
        cycle = " - but it is a cyclical near peak earnings, so the low P/E can mislead"
    elif cyc["label"] == "EARLY CYCLE - RECOVERY UPSIDE":
        cycle = " - and it looks early in its cycle, often the rewarding time to look"

    tail = {
        "STRONG OPPORTUNITY": " Our models rate it a strong opportunity.",
        "OPPORTUNITY": " Worth a closer look.",
        "FAIR VALUE": " Fairly priced for now.",
        "EXPENSIVE": " The price already bakes in a lot.",
        "HIGH RISK": " Treat with caution - the risks outweigh the discount.",
        "HOLD / NEUTRAL": " Nothing decisive either way.",
    }.get(verdict, "")
    return (val + quality + peer + cycle + "." + tail).strip()


# --------------------------------------------------------------------------
# handler
# --------------------------------------------------------------------------
def lambda_handler(event, context):
    t0 = time.time()
    screener = load("screener/data.json") or {}
    mr = {x.get("symbol"): x for x in
          (load("screener/mean-reversion.json") or {}).get("stocks", [])}
    fund = {x.get("ticker"): x for x in
            (load("data/fundamentals.json") or {}).get("companies", [])}
    shorts = {x.get("ticker"): x.get("state") for x in
              (load("data/short-pressure.json") or {}).get("names", [])}

    universe = screener.get("stocks", [])
    peer_median_fn, ind_med, sec_med = build_peer_groups(universe)

    rows = []
    for s in universe:
        sym = s.get("symbol") or s.get("ticker")
        r = score_stock(s, mr.get(sym), fund.get(sym), shorts.get(sym),
                        peer_median_fn)
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
    early_cycle = [r for r in rows if r["cycle"].get("label")
                   == "EARLY CYCLE - RECOVERY UPSIDE"][:12]
    # single hero pick - best STRONG, else best OPPORTUNITY
    strong = [r for r in rows if r["verdict"] == "STRONG OPPORTUNITY"]
    top_pick = (strong or [r for r in rows if r["verdict"] == "OPPORTUNITY"]
                or [None])[0]

    out = {
        "schema_version": "2.0",
        "method": "peer_relative_cycle_aware_opportunity_synthesis",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "elapsed_s": round(time.time() - t0, 2),
        "n_covered": len(rows),
        "verdict_counts": by_verdict,
        "top_pick": top_pick,
        "top_opportunities": top,
        "early_cycle": early_cycle,
        "avoid_list": avoid,
        "all": rows,
        "glossary": GLOSSARY,
        "disclaimer": ("Research and education only - not financial advice. "
                       "Every stock carries risk of loss. Fair value is an "
                       "estimate from models that can be wrong; always do your "
                       "own diligence before investing."),
        "note": ("v2.0 fuses two fair-value methods with quality, growth, "
                 "momentum, INDUSTRY-PEER benchmarking and a cycle radar. "
                 "Value-trap and cycle-trap guards stop cheap-but-dangerous "
                 "stocks from ever being rated an opportunity."),
    }
    s3.put_object(Bucket=S3_BUCKET, Key=OUT_KEY,
                  Body=json.dumps(out, default=str).encode("utf-8"),
                  ContentType="application/json",
                  CacheControl="public, max-age=3600")
    print(f"[opportunity] v2.0 - {len(rows)} covered, {len(top)} opportunities, "
          f"{len(avoid)} high-risk, {len(early_cycle)} early-cycle, "
          f"{out['elapsed_s']}s")
    return {"statusCode": 200, "body": json.dumps({
        "ok": True, "schema": "2.0", "n_covered": len(rows),
        "n_opportunities": len(top), "n_early_cycle": len(early_cycle),
        "top_pick": top_pick["ticker"] if top_pick else None,
        "verdict_counts": by_verdict})}
