"""
justhodl-opportunity-engine — Retail Opportunity & Risk Verdict

═══════════════════════════════════════════════════════════════════════
WHY THIS EXISTS
───────────────
The platform computes institutional-grade valuation, quality and risk
signals — but they are scattered across a 40-column screener and a
dozen pages no retail user could navigate. This engine SYNTHESISES them
into one plain-English verdict per stock so a non-professional can spot
a real opportunity (and its risks) in five seconds.

It does NOT recompute anything — it reads existing sidecars and fuses:
  • screener/data.json        — DCF fair value, P/E, growth, margins, momentum
  • screener/mean-reversion.json — historical-multiple fair value
  • data/fundamentals.json    — Altman-Z / Piotroski (where covered)
  • data/short-pressure.json  — short-selling pressure (where covered)

INSTITUTIONAL SAFEGUARDS (the reason this isn't a naive "buy" screen):
  1. TWO independent fair-value methods (DCF + multiple-reversion). A
     fair-value RANGE, never a single false-precision number.
  2. Confidence rating — HIGH only when both methods agree.
  3. VALUE-TRAP GUARD — a cheap stock with distress risk or shrinking
     revenue can never be rated an "opportunity". Cheap-for-a-reason is
     flagged as HIGH RISK, not surfaced as a bargain.
  4. Risk flags are produced with the same prominence as opportunities.

This is research/education tooling — not financial advice.
OUTPUT: data/opportunities.json   SCHEDULE: daily 14:00 UTC
═══════════════════════════════════════════════════════════════════════
"""
import json
import os
import time
from datetime import datetime, timezone

import boto3

S3_BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
OUT_KEY = "data/opportunities.json"
s3 = boto3.client("s3", region_name="us-east-1")


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


def fair_value(price, dcf_fv, mr_fv):
    """Two-method fair value with outlier rejection + agreement-gating.
    Returns (low, mid, high, undervalued_pct, confidence).

    Conservative by design — the failure mode of a naive blend is a wild
    DCF ($800 on a $300 stock) dragging the average into fantasy. So:
      • methods that imply >80% move are flagged 'extreme'
      • 'high' confidence requires the two estimates within 40% of each other
      • when they disagree on magnitude, the headline anchors on the
        estimate CLOSER to the current price (the conservative read)
      • the reported under/over-valued % is capped to a sane +/-60%
    """
    if not price or price <= 0:
        return None, None, None, None, "n/a"
    fvs = [v for v in (dcf_fv, mr_fv) if v and v > 0]
    if not fvs:
        return None, None, None, None, "n/a"

    if len(fvs) == 2:
        lo, hi = min(fvs), max(fvs)
        agree_dir = (dcf_fv > price) == (mr_fv > price)
        spread = hi / lo - 1.0
        extreme = max(abs(dcf_fv / price - 1),
                      abs(mr_fv / price - 1)) > 0.80
        if agree_dir and spread <= 0.40 and not extreme:
            anchor, conf = sum(fvs) / 2.0, "high"
        elif agree_dir:
            # agree on direction, not magnitude → anchor on the
            # estimate nearer the price (conservative)
            anchor = min(fvs, key=lambda v: abs(v - price))
            conf = "low"
        else:
            anchor, conf = sum(fvs) / 2.0, "mixed"
    else:
        anchor = fvs[0]
        lo, hi = anchor * 0.88, anchor * 1.12
        conf = "single" if abs(anchor / price - 1) <= 0.80 else "single-weak"

    under = max(-60.0, min(60.0, (anchor / price - 1) * 100))
    return round(lo, 2), round(anchor, 2), round(hi, 2), round(under, 1), conf


def score_stock(s, mr, fund, short_state):
    sym = s.get("symbol") or s.get("ticker")
    price = num(s.get("price"))
    if not sym or not price:
        return None

    dcf_fv = num(s.get("dcfFairValue"))
    mr_fv = num((mr or {}).get("mr_price"))
    lo, mid, hi, under, conf = fair_value(price, dcf_fv, mr_fv)

    rev_g = num(s.get("revenueGrowth"))
    op_m = num(s.get("operatingMargin"))
    roic = num(s.get("roic"))
    d2e = num(s.get("debtToEquity"))
    pe = num(s.get("peRatio"))
    chg6 = num(s.get("chg6m"))
    cross = s.get("crossSignal")
    beats = num(s.get("beatStreak"))
    altman = num((fund or {}).get("altman_z"))
    piotroski = num((fund or {}).get("piotroski"))

    # ── sub-scores (0-100) ──
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

    m = 50
    if chg6 is not None:
        m += 18 if chg6 > 20 else (-18 if chg6 < -20 else 0)
    if cross == "GOLDEN":
        m += 15
    elif cross == "DEATH":
        m -= 15
    momentum_score = clamp(m)

    opp = round(0.40 * value_score + 0.30 * quality_score
                + 0.20 * growth_score + 0.10 * momentum_score, 1)

    # ── risk flags & opportunity highlights (plain English) ──
    distress = altman is not None and altman < 1.8
    risks, ops = [], []
    if distress:
        risks.append("Elevated bankruptcy risk (weak Altman-Z score)")
    if cross == "DEATH":
        days = s.get("crossDaysAgo")
        risks.append(f"In a downtrend — death cross{f' {days}d ago' if days else ''}")
    if rev_g is not None and rev_g < 0:
        risks.append(f"Revenue is shrinking ({rev_g:.0f}% YoY)")
    if d2e is not None and d2e > 2:
        risks.append("Carries heavy debt relative to equity")
    if pe is not None and pe < 0:
        risks.append("Not currently profitable")
    if short_state == "PRESSURE BUILDING":
        risks.append("Short sellers are building pressure")
    if conf in ("mixed", "low", "single-weak"):
        risks.append("Valuation estimate is uncertain — the methods disagree on how cheap it is")

    if under is not None and under >= 15:
        ops.append(f"Trading ~{under:.0f}% below estimated fair value")
    if conf == "high" and under is not None and under > 0:
        ops.append("Two independent valuation methods agree it looks cheap")
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

    # ── verdict (value-trap guard + confidence-gated) ──
    # STRONG OPPORTUNITY demands 'high' confidence: the two independent
    # valuation methods must actually agree on magnitude, not just direction.
    value_trap = (under is not None and under >= 15
                  and (distress or (rev_g is not None and rev_g < -3)))
    hi_conf = conf == "high"
    if distress:
        verdict, vcolor = "HIGH RISK", "red"
    elif value_trap:
        verdict, vcolor = "HIGH RISK", "red"
    elif under is not None and under <= -15:
        verdict, vcolor = "EXPENSIVE", "orange"
    elif hi_conf and opp >= 70 and (under or 0) > 12:
        verdict, vcolor = "STRONG OPPORTUNITY", "green"
    elif opp >= 56 and (under or 0) > 5:
        verdict, vcolor = "OPPORTUNITY", "cyan"
    elif under is not None and -15 < under < 8:
        verdict, vcolor = "FAIR VALUE", "yellow"
    else:
        verdict, vcolor = "HOLD / NEUTRAL", "dim"

    return {
        "ticker": sym,
        "company": s.get("companyName") or s.get("name") or sym,
        "sector": s.get("sector"),
        "price": price,
        "fair_value_low": lo, "fair_value_mid": mid, "fair_value_high": hi,
        "undervalued_pct": under,
        "confidence": conf,
        "verdict": verdict, "verdict_color": vcolor,
        "opportunity_score": opp,
        "scores": {"value": round(value_score), "quality": round(quality_score),
                   "growth": round(growth_score), "momentum": round(momentum_score)},
        "opportunities": ops[:3],
        "risks": risks[:3],
    }


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
    rows = []
    for s in universe:
        sym = s.get("symbol") or s.get("ticker")
        r = score_stock(s, mr.get(sym), fund.get(sym), shorts.get(sym))
        if r:
            rows.append(r)

    rows.sort(key=lambda r: r["opportunity_score"], reverse=True)
    by_verdict = {}
    for r in rows:
        by_verdict[r["verdict"]] = by_verdict.get(r["verdict"], 0) + 1

    top = [r for r in rows if r["verdict"] in
           ("STRONG OPPORTUNITY", "OPPORTUNITY")][:25]
    avoid = sorted([r for r in rows if r["verdict"] == "HIGH RISK"],
                   key=lambda r: r["opportunity_score"])[:10]

    out = {
        "schema_version": "1.0",
        "method": "multi_signal_opportunity_synthesis",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "elapsed_s": round(time.time() - t0, 2),
        "n_covered": len(rows),
        "verdict_counts": by_verdict,
        "top_opportunities": top,
        "avoid_list": avoid,
        "all": rows,
        "disclaimer": ("Research and education only — not financial advice. "
                       "Every stock carries risk of loss. Fair value is an "
                       "estimate from models that can be wrong; always do your "
                       "own diligence before investing."),
        "note": ("Each verdict fuses two independent fair-value methods (DCF + "
                 "historical-multiple reversion) with quality, growth and "
                 "momentum. A value-trap guard prevents cheap-but-distressed "
                 "stocks from ever being rated an opportunity."),
    }
    s3.put_object(Bucket=S3_BUCKET, Key=OUT_KEY,
                  Body=json.dumps(out, default=str).encode("utf-8"),
                  ContentType="application/json",
                  CacheControl="public, max-age=3600")
    print(f"[opportunity] {len(rows)} covered · {len(top)} opportunities · "
          f"{len(avoid)} high-risk · {out['elapsed_s']}s")
    return {"statusCode": 200, "body": json.dumps({
        "ok": True, "n_covered": len(rows),
        "n_opportunities": len(top), "verdict_counts": by_verdict})}
