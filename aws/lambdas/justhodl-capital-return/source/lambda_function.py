"""
justhodl-capital-return - the Cannibal screen.

A company that steadily buys back its own stock shrinks the share count, so
each remaining share owns more of the business: earnings per share rise
mechanically, and the effect compounds. Pabrai called the best of these
"cannibals". It is one of the most durable documented equity factors -
shareholder yield.

But the naive buyback screen is a trap, so this engine gates hard:
  - Buybacks at a nosebleed valuation DESTROY value (overpaying for your
    own stock). A real cannibal repurchases when the stock is reasonably
    priced. We cap the P/E.
  - Buybacks funded by DEBT are financial engineering, not capital return.
    We require free cash flow to largely fund the payout and screen out
    over-levered balance sheets.
  - A high gross buyback that only offsets stock-comp dilution is not
    shrinking the float. We use NET buyback yield (repurchases net of
    issuance) so the signal reflects a genuinely shrinking share count.
  - A company propping up EPS while the business rots is not a cannibal.
    We gate on a non-deteriorating business (positive margin, revenue not
    collapsing, no distress).

Each name carries plain-English reasoning, the funding check, and a price
target. Pure synthesis of screener/data.json - no new API calls.

OUTPUT data/capital-return.json     SCHEDULE daily 13:45 UTC
Real data only. Research, not advice.
"""
import json
import time
from datetime import datetime, timezone

import boto3

s3 = boto3.client("s3")
S3_BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/capital-return.json"


def num(v):
    try:
        f = float(v)
        return f if f == f else None
    except (TypeError, ValueError):
        return None


def clamp(v, lo, hi):
    return max(lo, min(hi, v))


def pct(v):
    """Normalise a yield-type field to percent (FMP stores fractions)."""
    if v is None:
        return None
    return v * 100.0 if abs(v) <= 1.5 else v


def lambda_handler(event, context):
    t0 = time.time()
    now = datetime.now(timezone.utc)

    try:
        sc = json.loads(s3.get_object(
            Bucket=S3_BUCKET, Key="screener/data.json")["Body"].read())
    except Exception as e:
        return {"statusCode": 500, "body": f"screener read failed: {e}"}

    rows = sc.get("stocks")
    if not isinstance(rows, list):
        bs = sc.get("by_symbol") or {}
        rows = list(bs.values()) if isinstance(bs, dict) else []

    cannibals = []
    n_eval = 0
    for r in rows:
        if not isinstance(r, dict):
            continue
        sy = (r.get("symbol") or "").upper()
        if not sy:
            continue
        n_eval += 1

        buyback = pct(num(r.get("buybackYield")))
        div = pct(num(r.get("dividendYield"))) or 0.0
        fcf_yield = pct(num(r.get("fcfYieldCalc")))
        pe = num(r.get("peRatio"))
        d2e = num(r.get("debtToEquity"))
        icov = num(r.get("interestCoverage"))
        rev_g = pct(num(r.get("revenueGrowth")))
        net_margin = pct(num(r.get("netMargin")))
        altman = num(r.get("altmanZ"))
        roic = pct(num(r.get("roic")))
        price = num(r.get("price"))
        mcap = num(r.get("marketCap"))

        # ── must be a real buyback story ──
        if buyback is None or buyback < 1.5:
            continue
        shareholder_yield = buyback + div

        # ── FUNDING gate: FCF must largely fund the payout, not debt ──
        if fcf_yield is None or fcf_yield <= 0:
            continue
        if fcf_yield < shareholder_yield * 0.6:
            continue                       # payout outrunning cash generation
        # over-levered balance sheet → debt-funded buybacks, exclude
        if d2e is not None and d2e > 2.5 and not (icov and icov > 6):
            continue
        if icov is not None and icov < 2 and (d2e is None or d2e > 1):
            continue

        # ── VALUATION gate: don't reward overpaying for your own stock ──
        if pe is not None and (pe <= 0 or pe > 32):
            continue

        # ── BUSINESS-HEALTH gate: no propping up a rotting business ──
        if net_margin is not None and net_margin <= 0:
            continue
        if rev_g is not None and rev_g < -12:
            continue
        if altman is not None and altman < 1.8:
            continue

        # ── score ──
        sy_c = clamp((shareholder_yield - 2) / 10.0, 0, 1)       # payout size
        fund_c = clamp(fcf_yield / max(shareholder_yield, 1) / 1.6, 0, 1)
        val_c = clamp((26 - (pe if pe else 20)) / 22.0, 0, 1)    # cheaper=better
        biz_c = (clamp((roic or 8) / 25.0, 0, 1) * 0.5
                 + clamp(((rev_g if rev_g is not None else 4) + 5) / 25.0,
                         0, 1) * 0.5)
        score = round(clamp(100 * (0.34 * sy_c + 0.26 * fund_c
                                   + 0.22 * val_c + 0.18 * biz_c), 0, 100), 1)

        # ── price target (analyst median / DCF, sane band) ──
        tgt, basis = None, None
        if price and price > 0:
            cands = []
            ptm = num(r.get("priceTargetMedian")) or num(
                r.get("priceTargetMean"))
            if ptm and 0.5 * price <= ptm <= 2.2 * price:
                cands.append(ptm)
            dcf = num(r.get("dcfFairValue"))
            if dcf and 0.55 * price <= dcf <= 2.0 * price:
                cands.append(dcf)
            if cands:
                tgt = round(sum(cands) / len(cands), 2)
                basis = ("analyst consensus + DCF" if len(cands) == 2
                         else "analyst consensus / DCF")
        upside = (round((tgt / price - 1) * 100, 1)
                  if tgt and price else None)

        fund_note = (f"funded by free cash flow (FCF yield "
                     f"{fcf_yield:.1f}% vs {shareholder_yield:.1f}% paid "
                     f"out)" + (f", balance sheet sound (debt/equity "
                                f"{d2e:.1f})" if d2e is not None else ""))
        why = (
            f"{sy} returned about {shareholder_yield:.1f}% of its market "
            f"value to shareholders - {buyback:.1f}% in net buybacks"
            + (f" plus a {div:.1f}% dividend" if div > 0.2 else "")
            + f". It is {fund_note}, not debt. "
            + (f"At {pe:.0f}x earnings " if pe else "")
            + "those buybacks are accretive: a shrinking share count lifts "
            "earnings per share mechanically and compounds over time. "
            + (f"The business is healthy - {rev_g:+.0f}% revenue growth, "
               f"{net_margin:.0f}% net margin"
               + (f", {roic:.0f}% ROIC" if roic is not None else "")
               + ". " if rev_g is not None and net_margin is not None
               else "")
            + (f"Fair value ~${tgt:.2f} ({upside:+.0f}%)."
               if tgt and upside is not None else ""))

        cannibals.append({
            "symbol": sy, "name": r.get("name") or sy,
            "sector": r.get("sector"), "market_cap": mcap, "price": price,
            "cannibal_score": score,
            "buyback_yield_pct": round(buyback, 2),
            "dividend_yield_pct": round(div, 2),
            "shareholder_yield_pct": round(shareholder_yield, 2),
            "fcf_yield_pct": round(fcf_yield, 2),
            "pe_ratio": round(pe, 1) if pe else None,
            "debt_to_equity": round(d2e, 2) if d2e is not None else None,
            "revenue_growth_pct": round(rev_g, 1)
            if rev_g is not None else None,
            "price_target": tgt, "upside_pct": upside,
            "target_basis": basis, "why": why,
            "risk_flags": ([] if (pe is None or pe < 25)
                           else ["valuation getting full - buybacks less "
                                 "accretive the higher the multiple"]),
        })

    cannibals.sort(key=lambda x: x["cannibal_score"], reverse=True)

    out = {
        "schema_version": "1.0",
        "method": "shareholder_yield_cannibal_screen",
        "generated_at": now.isoformat(),
        "elapsed_s": round(time.time() - t0, 2),
        "ok": len(cannibals) >= 3,
        "headline": (
            f"{len(cannibals)} capital-return cannibals - companies "
            "shrinking their share count with free cash flow, lifting EPS "
            "for every remaining shareholder."),
        "how_to_read": (
            "Each name returns real cash to shareholders through net "
            "buybacks (plus dividends) FUNDED BY free cash flow, not debt, "
            "while trading at a reasonable multiple and running a healthy "
            "business. A shrinking share count compounds earnings per "
            "share quietly year after year. The score blends payout size, "
            "how well cash flow funds it, valuation and business quality. "
            "Research, not advice."),
        "n_evaluated": n_eval,
        "n_cannibals": len(cannibals),
        "cannibals": cannibals[:60],
        "source": "stock-screener",
    }
    s3.put_object(Bucket=S3_BUCKET, Key=OUT_KEY,
                  Body=json.dumps(out, default=str).encode("utf-8"),
                  ContentType="application/json",
                  CacheControl="public, max-age=3600")
    print(f"[capital-return] {len(cannibals)} cannibals from {n_eval} "
          f"evaluated, {out['elapsed_s']}s")
    return {"statusCode": 200, "body": json.dumps({
        "ok": out["ok"], "n_cannibals": len(cannibals),
        "n_evaluated": n_eval})}
