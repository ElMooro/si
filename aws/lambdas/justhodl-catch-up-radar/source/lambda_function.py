"""
justhodl-catch-up-radar — Relative-Value Catch-Up Radar
=======================================================
A synthesis engine. It does NOT scan for absolute hypergrowth (that is
boom-radar / Boom Board). It hunts the *relative-value* trade Khalid
described: names that should run AFTER the leaders already have.

Two sections, both built off engines that already exist:

  1. BETA-LAGGARDS — for every market-leading theme (theme-rotation
     already ranks hot themes and carries each constituent's 20-day
     return + weight), find members that have LAGGED the cohort despite
     a high beta. The signal is a *beta-adjusted shortfall*: a name with
     beta 1.4 inside a theme up 25% "should" be up ~35%; if it is only
     up 4% the beta says ~31% of catch-up room is unspent. A fundamental
     gate keeps coiled laggards in and value traps out.

  2. ETF HOLDINGS CATCH-UP — for each hot theme-ETF, the weighted return
     of its constituents vs the ETF's own trailing return. When the
     basket runs hotter than the fund, the fund's forward return tends
     to track the basket. Honest caveat baked into every thesis: liquid
     ETFs track NAV intraday via AP arbitrage, so this is a holdings-
     MOMENTUM read, not a discount to arbitrage.

Reads   : data/theme-rotation.json, data/sector-rotation.json,
          screener/data.json
Writes  : screener/catch-up-radar.json
Schedule: daily 14:30 UTC (after theme/sector engines refresh)

Design notes — false-alarm discipline:
  * A theme must be genuinely hot (momentum + positive 60d RS + a real
    20d move) before any of its members are even considered.
  * A laggard needs a meaningful beta-adjusted shortfall, not just "down
    a bit". Convergence factor < 1 — catch-ups rarely fully close.
  * If nothing clean qualifies the engine says so. It never forces a
    list.
"""

import json
import time
from datetime import datetime, timezone

import boto3

s3 = boto3.client("s3")
S3_BUCKET = "justhodl-dashboard-live"
OUT_KEY = "screener/catch-up-radar.json"

THEME_KEY = "data/theme-rotation.json"
SECTOR_KEY = "data/sector-rotation.json"
SCREENER_KEY = "screener/data.json"

# ───────────────────────── tunables ─────────────────────────
HOT_MOMENTUM_MIN = 55.0      # theme momentum_score floor (0-100)
HOT_RET20_MIN = 2.0          # theme must actually be up over 20d (%)
LAG_GAP_MIN = 4.0            # member must trail the cohort by >= this (pp)
SHORTFALL_MIN = 6.0          # beta-adjusted shortfall floor (%)
GAIN_CEIL = 50.0             # credible 1-3mo catch-up ceiling (%)
ETF_GAP_MIN = 2.5            # basket-vs-fund gap floor (pp)
DEFAULT_BETA = 1.10          # used when a name has no published beta


# ───────────────────────── helpers ─────────────────────────
def read_json(key):
    try:
        return json.loads(s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read())
    except Exception:
        return None


def num(v):
    try:
        f = float(v)
        return f if f == f else None
    except (TypeError, ValueError):
        return None


def clamp(v, lo, hi):
    return max(lo, min(hi, v))


def sym_of(d):
    if not isinstance(d, dict):
        return None
    for k in ("symbol", "ticker", "sym", "Symbol"):
        v = d.get(k)
        if v:
            return str(v).upper().strip()
    return None


def fmt_pct(v, digits=1):
    return "n/a" if v is None else f"{v:+.{digits}f}%"


def build_screener_index(screener):
    """screener/data.json -> {SYMBOL: record}. Tolerant of container shape."""
    rows = []
    if isinstance(screener, list):
        rows = screener
    elif isinstance(screener, dict):
        for k in ("stocks", "data", "results", "rows", "all", "universe",
                  "candidates"):
            v = screener.get(k)
            if isinstance(v, list):
                rows = v
                break
        if not rows:                       # maybe {SYM: {...}} already
            vals = [v for v in screener.values() if isinstance(v, dict)]
            if vals and sym_of(vals[0]):
                rows = vals
    idx = {}
    for r in rows:
        sy = sym_of(r)
        if sy and sy not in idx:
            idx[sy] = r
    return idx


def rev_growth_pct(rec):
    """Revenue growth as a percentage, whatever field the screener used."""
    for k in ("revenueGrowth", "forwardRevenueGrowth", "revenueGrowthTTM",
              "revGrowth", "salesGrowth"):
        g = num(rec.get(k))
        if g is not None:
            return g * 100.0 if abs(g) <= 5 else g
    return None


def fundamental_health(rec):
    """0-100 health score + a value-trap flag. The point is not precision
    but to keep obviously broken names out of a catch-up list."""
    if not rec:
        return None, False, "fundamentals unverified (outside screener universe)"
    score, parts = 50.0, []
    rg = rev_growth_pct(rec)
    if rg is not None:
        if rg >= 15:
            score += 18; parts.append(f"revenue +{rg:.0f}%")
        elif rg >= 3:
            score += 9; parts.append(f"revenue +{rg:.0f}%")
        elif rg < -5:
            score -= 20; parts.append(f"revenue {rg:.0f}% (shrinking)")
        else:
            parts.append(f"revenue {rg:+.0f}%")
    roic = num(rec.get("roic"))
    roe = num(rec.get("roe"))
    prof = roic if roic is not None else roe
    if prof is not None:
        pv = prof * 100.0 if abs(prof) <= 5 else prof
        if pv >= 12:
            score += 16; parts.append(f"ROIC/ROE {pv:.0f}%")
        elif pv >= 4:
            score += 7
        elif pv < 0:
            score -= 22; parts.append(f"ROIC/ROE {pv:.0f}% (negative)")
    pe = num(rec.get("peRatio"))
    if pe is not None:
        if pe < 0:
            score -= 8; parts.append("loss-making (negative P/E)")
        elif pe > 70:
            score -= 6
    score = clamp(score, 0, 100)
    trap = score < 35
    note = "; ".join(parts) if parts else "fundamentals thin"
    return round(score, 1), trap, note


# ───────────────────────── core ─────────────────────────
def find_hot_themes(theme_json):
    """Themes that are genuinely leading: momentum + positive 60d RS + a
    real 20d move + breadth data available to scan members."""
    out = []
    themes = (theme_json or {}).get("all_themes") or []
    breadth = (theme_json or {}).get("breadth_details") or {}
    for t in themes:
        tk = t.get("ticker")
        ms = num(t.get("momentum_score"))
        r20 = num(t.get("ret_20d"))
        rs60 = num(t.get("rs_60d"))
        if not tk or ms is None or r20 is None:
            continue
        if ms < HOT_MOMENTUM_MIN or r20 < HOT_RET20_MIN:
            continue
        if rs60 is not None and rs60 <= 0:
            continue
        bd = breadth.get(tk) or {}
        cons = bd.get("constituents_perf") or []
        if len(cons) < 4:                   # need a cohort to compare against
            continue
        bpct = (bd.get("breadth") or {}).get("breadth_outperform_pct")
        out.append({
            "ticker": tk, "name": t.get("name") or tk,
            "category": t.get("category") or "",
            "momentum_score": ms, "ret_20d": r20,
            "rs_20d": num(t.get("rs_20d")), "rs_60d": rs60,
            "today_close": num(t.get("today_close")),
            "breadth_pct": num(bpct),
            "constituents": cons,
        })
    return sorted(out, key=lambda x: x["momentum_score"], reverse=True)


def scan_laggards(hot, scr_idx):
    """For every hot theme, surface beta-adjusted laggards."""
    cands, seen = [], {}
    for th in hot:
        cons = th["constituents"]
        rets = [num(c.get("ret_20d")) for c in cons]
        rets = [r for r in rets if r is not None]
        if len(rets) < 4:
            continue
        # cohort reference = weighted constituent return, fall back to mean
        wsum = num(sum(num(c.get("weight")) or 0 for c in cons)) or 0
        if wsum > 0:
            cohort = sum((num(c.get("weight")) or 0) * (num(c.get("ret_20d")) or 0)
                         for c in cons) / wsum
        else:
            cohort = sum(rets) / len(rets)
        cohort = max(cohort, th["ret_20d"] * 0.5)   # don't let it go soft
        ts = th["momentum_score"]
        bpct = th["breadth_pct"]

        for c in cons:
            sy = sym_of(c)
            r20 = num(c.get("ret_20d"))
            if not sy or r20 is None:
                continue
            gap = cohort - r20
            if gap < LAG_GAP_MIN:           # not actually a laggard
                continue
            rec = scr_idx.get(sy) or {}
            beta = num(rec.get("beta")) or DEFAULT_BETA
            beta = clamp(beta, 0.5, 3.0)
            beta_expected = beta * cohort
            shortfall = beta_expected - r20
            if shortfall < SHORTFALL_MIN:
                continue
            price = num(rec.get("price")) or num(c.get("today_close"))
            health, trap, fnote = fundamental_health(rec)
            low_conf = rec == {} or rec is None or not rec

            # theme-strength component (a strong engine pulls harder)
            th_str = clamp((ts - HOT_MOMENTUM_MIN) / 45.0, 0, 1)
            if bpct is not None:
                th_str = clamp(0.5 * th_str + 0.5 * (bpct / 100.0), 0, 1)
            # shortfall component
            sf_c = clamp(shortfall / 35.0, 0, 1)
            # fundamental component
            fh = (health if health is not None else 45.0) / 100.0

            score = 45 * sf_c + 30 * th_str + 25 * fh
            if trap:
                score -= 28
            if low_conf:
                score -= 6
            score = round(clamp(score, 0, 100), 1)

            # convergence factor — healthy name in a strong theme closes
            # more of the gap; a trap-flagged name barely converges
            conv = 0.40 + 0.20 * th_str + 0.15 * fh
            if trap:
                conv = min(conv, 0.30)
            conv = clamp(conv, 0.25, 0.72)
            gain = shortfall * conv
            capped = gain > GAIN_CEIL
            if capped:
                gain = GAIN_CEIL
            target = round(price * (1 + gain / 100.0), 2) if price else None
            upside = round(gain, 1) if price else None

            flags = []
            if trap:
                flags.append("value-trap risk — lagging on weak fundamentals, "
                              "not just neglect; treat with caution")
            if low_conf:
                flags.append("outside the screener universe — fundamentals "
                              "unverified, lower confidence")
            if beta >= 1.6:
                flags.append(f"high beta ({beta:.1f}) — amplifies both ways, "
                             "size the position smaller")
            if capped:
                flags.append("catch-up gain capped at the credible 1-3mo "
                             "ceiling — methods imply more but low-confidence")

            bp = (f" with {bpct:.0f}% of members beating the market"
                  if bpct is not None else "")
            thesis = (
                f"{th['name']} ({th['ticker']}) is one of the market's "
                f"leading themes — up {th['ret_20d']:+.0f}% over the last 20 "
                f"sessions{bp}. {sy} has returned only {r20:+.0f}% over the "
                f"same window despite a beta of {beta:.1f}, so on its own "
                f"sensitivity it 'should' be up around {beta_expected:+.0f}%. "
                f"That leaves a beta-adjusted shortfall of ~{shortfall:.0f}%. "
            )
            if health is not None and not trap:
                thesis += (f"Fundamentals back it up ({fnote}) — this reads "
                           f"as a coiled laggard the theme has not lifted "
                           f"yet, not a broken story. ")
            elif trap:
                thesis += (f"But the fundamentals are weak ({fnote}) — it may "
                           f"be lagging for a reason; lower-conviction. ")
            else:
                thesis += f"Fundamentals are unverified — treat as a lower-confidence read. "
            if target:
                thesis += (f"If the theme stays in favour and the gap "
                           f"partially closes, a fair catch-up target is "
                           f"~${target:.2f} ({upside:+.0f}% from ${price:.2f}) "
                           f"over roughly 1-3 months. ")
            thesis += ("Relative-value trade — laggards lag for reasons, and "
                       "it works only while the theme keeps leading.")

            cand = {
                "symbol": sy,
                "name": rec.get("name") or rec.get("companyName") or sy,
                "sector": rec.get("sector") or "",
                "theme": th["ticker"], "theme_name": th["name"],
                "price": price, "beta": round(beta, 2),
                "ret_20d": round(r20, 1),
                "cohort_ret_20d": round(cohort, 1),
                "beta_expected_20d": round(beta_expected, 1),
                "shortfall_pct": round(shortfall, 1),
                "catch_up_score": score,
                "fundamental_health": health,
                "value_trap_flag": trap,
                "low_confidence": bool(low_conf),
                "price_target": target, "upside_pct": upside,
                "target_horizon": "1-3 month catch-up",
                "convergence_factor": round(conv, 2),
                "flags": flags, "thesis": thesis,
            }
            # keep the strongest read per symbol (a name can sit in 2 themes)
            prev = seen.get(sy)
            if prev is None or score > prev["catch_up_score"]:
                seen[sy] = cand
    cands = sorted(seen.values(), key=lambda x: x["catch_up_score"],
                   reverse=True)
    return cands


def scan_etf_catchup(hot):
    """ETFs whose constituent basket has run hotter than the fund itself."""
    out = []
    for th in hot:
        cons = th["constituents"]
        wsum = sum(num(c.get("weight")) or 0 for c in cons)
        rets = [num(c.get("ret_20d")) for c in cons]
        rets = [r for r in rets if r is not None]
        if len(rets) < 4:
            continue
        if wsum > 0:
            basket = sum((num(c.get("weight")) or 0) * (num(c.get("ret_20d")) or 0)
                         for c in cons) / wsum
        else:
            basket = sum(rets) / len(rets)
        etf_ret = th["ret_20d"]
        gap = basket - etf_ret
        if gap < ETF_GAP_MIN:
            continue
        price = th["today_close"]
        bpct = th["breadth_pct"]
        conv = 0.60                       # ETFs converge faster than singles
        gain = min(gap * conv, 18.0)      # tight ceiling — funds track NAV
        target = round(price * (1 + gain / 100.0), 2) if price else None
        upside = round(gain, 1) if price else None
        bp = (f", and {bpct:.0f}% of holdings are beating the market"
              if bpct is not None else "")
        thesis = (
            f"{th['name']} ({th['ticker']}): its top holdings, weighted, are "
            f"up ~{basket:+.0f}% over 20 sessions while the fund itself has "
            f"returned {etf_ret:+.0f}% — a ~{gap:.0f}-point basket-vs-fund "
            f"gap{bp}. Liquid ETFs track NAV intraday through authorised-"
            f"participant arbitrage, so this is NOT a discount to grab — it "
            f"is a holdings-MOMENTUM read: when the underlying basket is "
            f"broadly stronger than the fund's trailing print, the fund's "
            f"forward return tends to follow the basket. "
        )
        if target:
            thesis += (f"A modest catch-up target is ~${target:.2f} "
                       f"({upside:+.0f}% from ${price:.2f}). ")
        thesis += "Lower-risk way to ride a hot theme than picking one name."
        out.append({
            "ticker": th["ticker"], "name": th["name"],
            "category": th["category"], "momentum_score": th["momentum_score"],
            "etf_price": price, "etf_ret_20d": round(etf_ret, 1),
            "basket_ret_20d": round(basket, 1),
            "holdings_gap_pct": round(gap, 1),
            "breadth_pct": bpct,
            "price_target": target, "upside_pct": upside,
            "target_horizon": "1-2 month catch-up",
            "thesis": thesis,
        })
    return sorted(out, key=lambda x: x["holdings_gap_pct"], reverse=True)


def lambda_handler(event, context):
    t0 = time.time()
    theme_json = read_json(THEME_KEY)
    screener = read_json(SCREENER_KEY)
    sector = read_json(SECTOR_KEY)

    if not theme_json or not theme_json.get("all_themes"):
        out = {
            "schema_version": "1.0", "method": "relative_value_catch_up",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "error": "theme-rotation feed unavailable — cannot build the radar",
            "hot_themes": [], "catch_up_candidates": [], "etf_catchup": [],
        }
        s3.put_object(Bucket=S3_BUCKET, Key=OUT_KEY,
                      Body=json.dumps(out).encode(), ContentType="application/json")
        return {"statusCode": 200, "body": json.dumps({"ok": False,
                "reason": "no theme-rotation feed"})}

    scr_idx = build_screener_index(screener)
    hot = find_hot_themes(theme_json)
    laggards = scan_laggards(hot, scr_idx)
    etfs = scan_etf_catchup(hot)

    # sector context — which sectors are confirmed in-favour
    sec_ctx = []
    if isinstance(sector, dict):
        for s in (sector.get("sectors") or [])[:11]:
            sc = num(s.get("rotation_score"))
            if sc is not None and sc >= 55:
                sec_ctx.append({"symbol": s.get("symbol"),
                                "rotation_score": sc})

    strong = [c for c in laggards if c["catch_up_score"] >= 55
              and not c["value_trap_flag"]]
    if strong:
        top = strong[0]
        headline = (
            f"Catch-Up Radar: {len(strong)} beta-laggards primed to follow "
            f"their leaders, plus {len(etfs)} ETFs whose basket is "
            f"outrunning the fund. Top: {top['symbol']} — "
            f"{top['shortfall_pct']:.0f}% beta-adjusted shortfall inside "
            f"{top['theme_name']}.")
    elif laggards:
        headline = (f"Catch-Up Radar: {len(laggards)} laggard set-ups found "
                    f"but none high-conviction — thin or trap-flagged. "
                    f"{len(etfs)} ETF basket-gaps flagged.")
    else:
        headline = ("Catch-Up Radar: no clean catch-up set-ups right now — "
                    "leaders and laggards are moving together. No forced list.")

    out = {
        "schema_version": "1.0",
        "method": "relative_value_catch_up_v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "elapsed_s": round(time.time() - t0, 2),
        "spy_ret_20d": num(theme_json.get("spy_ret_20d")),
        "headline": headline,
        "counts": {
            "hot_themes": len(hot),
            "catch_up_candidates": len(laggards),
            "high_conviction": len(strong),
            "etf_catchup": len(etfs),
        },
        "hot_themes": [{
            "ticker": h["ticker"], "name": h["name"], "category": h["category"],
            "momentum_score": h["momentum_score"], "ret_20d": h["ret_20d"],
            "rs_60d": h["rs_60d"], "breadth_pct": h["breadth_pct"],
            "n_members": len(h["constituents"]),
        } for h in hot],
        "sector_context": sec_ctx,
        "catch_up_candidates": laggards,
        "etf_catchup": etfs,
        "methodology": (
            "For each market-leading theme (theme-rotation: momentum_score "
            f">= {HOT_MOMENTUM_MIN:.0f}, positive 60-day RS, up >= "
            f"{HOT_RET20_MIN:.0f}% over 20d), every constituent is measured "
            "against the weighted cohort return. A laggard's beta-adjusted "
            "shortfall = beta x cohort return - the laggard's actual return. "
            "Candidates are gated on fundamentals so coiled laggards surface "
            "and value traps are flagged. Catch-up targets apply a "
            "convergence factor < 1 (catch-ups rarely fully close), scaled "
            "by theme strength and fundamental health. ETF section compares "
            "the weighted constituent return to the fund's own return."),
        "disclaimer": (
            "Research and education only — not financial advice. Relative-"
            "value catch-up is probabilistic: laggards often lag for real "
            "reasons, and the trade only works while the theme keeps "
            "leading. Liquid ETFs track NAV via AP arbitrage — the ETF "
            "section is a holdings-momentum read, not a discount."),
    }
    s3.put_object(Bucket=S3_BUCKET, Key=OUT_KEY,
                  Body=json.dumps(out, default=str).encode("utf-8"),
                  ContentType="application/json",
                  CacheControl="public, max-age=3600")
    print(f"[catch-up-radar] {len(hot)} hot themes · {len(laggards)} "
          f"laggards ({len(strong)} high-conviction) · {len(etfs)} ETF "
          f"gaps · {out['elapsed_s']}s")
    return {"statusCode": 200, "body": json.dumps({
        "ok": True, "hot_themes": len(hot),
        "catch_up_candidates": len(laggards),
        "high_conviction": len(strong), "etf_catchup": len(etfs)})}
