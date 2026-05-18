"""
justhodl-opportunity-screener — the Boom Board.

The platform already runs six excellent opportunity engines — bagger-engine
(100-bagger DNA across nano/micro/small caps), earnings-pead (serial
estimate-beat streaks + post-earnings drift), eps-revision-velocity
(analysts revising estimates up), revenue-acceleration (revenue growth
inflecting), momentum-breakout (early pumps) — plus commodity-curves and
construction-housing. But they live on six separate pages. A non-
professional has no single place that says, in plain English: BUY THIS,
here is WHY, here is the PRICE TARGET.

This engine is that place. It is pure synthesis — it reads the existing
engines' S3 outputs and the master screener, fuses them per symbol, and
produces ONE ranked board. A stock confirmed by several independent engines
is the highest-conviction setup: that cross-confirmation is the core score.

Every entry carries:
  • the signals that fired (and how many engines agree)
  • a plain-English thesis a normie can act on
  • a multi-method price target (analyst consensus + DCF + growth-justified
    P/E, blended) and the % upside
  • honest risk flags

Sections: boom_candidates · microcap_rockets · serial_beaters ·
hidden_growth (great growth the price hasn't repriced) · commodities_metals
· housing.

OUTPUT: screener/opportunity-screener.json   SCHEDULE: daily 14:00 UTC
Real data only — synthesis of platform engines. Not investment advice.
"""
import json
import time
from datetime import datetime, timezone

import boto3

s3 = boto3.client("s3")
S3_BUCKET = "justhodl-dashboard-live"
OUT_KEY = "screener/opportunity-screener.json"


# ───────────────────────── io ─────────────────────────
def read_json(key):
    try:
        return json.loads(s3.get_object(Bucket=S3_BUCKET,
                                        Key=key)["Body"].read())
    except Exception:
        return None


def sym_of(d):
    if not isinstance(d, dict):
        return None
    for k in ("symbol", "ticker", "sym", "Symbol"):
        v = d.get(k)
        if v:
            return str(v).upper().strip()
    return None


def extract_picks(jdata):
    """Pull every per-stock dict out of an engine's JSON, whatever the
    container shape (tiers map, tier_s/a/b arrays, all_qualifying...)."""
    out = []
    if not isinstance(jdata, dict):
        return out
    for k in ("tier_s", "tier_a", "tier_b", "tier_s_full", "all_qualifying",
              "microcap_picks", "candidates", "picks", "stocks", "results"):
        v = jdata.get(k)
        if isinstance(v, list):
            out += [x for x in v if isinstance(x, dict)]
    tiers = jdata.get("tiers")
    if isinstance(tiers, dict):
        for v in tiers.values():
            if isinstance(v, list):
                out += [x for x in v if isinstance(x, dict)]
    elif isinstance(tiers, list):
        out += [x for x in tiers if isinstance(x, dict)]
    # de-dup by symbol, keep first (highest-tier) occurrence
    seen, dedup = set(), []
    for d in out:
        sy = sym_of(d)
        if sy and sy not in seen:
            seen.add(sy)
            dedup.append(d)
    return dedup


def num(v):
    try:
        f = float(v)
        return f if f == f else None  # drop NaN
    except (TypeError, ValueError):
        return None


def clamp(v, lo, hi):
    return max(lo, min(hi, v))


def cap_tier(mcap):
    if mcap is None:
        return "unknown"
    if mcap < 50e6:
        return "nano"
    if mcap < 300e6:
        return "micro"
    if mcap < 2e9:
        return "small"
    if mcap < 10e9:
        return "mid"
    if mcap < 200e9:
        return "large"
    return "mega"


def fmt_b(v):
    if v is None:
        return "n/a"
    if v >= 1e9:
        return f"${v / 1e9:.1f}B"
    if v >= 1e6:
        return f"${v / 1e6:.0f}M"
    return f"${v:,.0f}"


# ───────────────────────── price target ─────────────────────────
def price_target(price, sd):
    """Multi-method fair value: analyst consensus, DCF intrinsic value and a
    growth-justified P/E (PEG ~ 1, fair multiple capped). Blended = median of
    whatever is available and sane. Returns (target, upside_pct, basis)."""
    if not price or price <= 0:
        return None, None, None
    cands = []
    band = (0.35 * price, 5.0 * price)

    ptm = num(sd.get("priceTargetMedian")) or num(sd.get("priceTargetMean"))
    if ptm and band[0] <= ptm <= band[1]:
        cands.append(("analyst consensus", ptm))

    dcf = num(sd.get("dcfFairValue"))
    if dcf and band[0] <= dcf <= band[1]:
        cands.append(("DCF intrinsic value", dcf))

    g = num(sd.get("epsGrowth"))
    if g is not None and abs(g) <= 5:          # epsGrowth stored as fraction
        g *= 100.0
    if g is None:
        gr = num(sd.get("forwardRevenueGrowth")) or num(
            sd.get("revenueGrowth"))
        if gr is not None:
            g = gr * 100.0 if abs(gr) <= 5 else gr
    fpe = num(sd.get("forwardPE")) or num(sd.get("peRatio"))
    if fpe and fpe > 0 and g and g > 0:
        fair_pe = clamp(g, 12.0, 45.0)         # PEG~1, multiple capped
        growth_t = price * fair_pe / fpe
        if band[0] <= growth_t <= band[1]:
            cands.append(("growth-justified P/E", growth_t))

    if not cands:
        return None, None, None
    vals = sorted(v for _, v in cands)
    n = len(vals)
    blended = vals[n // 2] if n % 2 else (vals[n // 2 - 1] + vals[n // 2]) / 2
    upside = round((blended / price - 1.0) * 100.0, 1)
    basis = "blend of " + ", ".join(name for name, _ in cands)
    return round(blended, 2), upside, basis


# ───────────────────────── handler ─────────────────────────
def lambda_handler(event, context):
    t0 = time.time()
    now = datetime.now(timezone.utc)
    errors = []

    bagger = read_json("data/bagger-engine.json") or {}
    pead = read_json("data/earnings-pead.json") or {}
    epsrev = read_json("data/eps-revision-velocity.json") or {}
    revacc = read_json("data/revenue-acceleration.json") or {}
    mom = read_json("data/momentum-breakout.json") or {}
    screener = read_json("screener/data.json") or {}
    commod = read_json("data/commodity-curves.json") or {}
    housing = read_json("data/construction-housing.json") or {}
    for nm, j in [("bagger", bagger), ("pead", pead), ("epsrev", epsrev),
                  ("revacc", revacc), ("momentum", mom),
                  ("screener", screener)]:
        if not j:
            errors.append(f"{nm}: sidecar missing")

    # screener master lookup (price / target inputs / fundamentals)
    by_sym = {}
    sc_stocks = screener.get("by_symbol")
    if isinstance(sc_stocks, dict):
        by_sym = {str(k).upper(): v for k, v in sc_stocks.items()
                  if isinstance(v, dict)}
    elif isinstance(screener.get("stocks"), list):
        for x in screener["stocks"]:
            sy = sym_of(x)
            if sy:
                by_sym[sy] = x

    # ── fuse every opportunity engine, per symbol ──
    rec = {}

    def touch(sy):
        if sy not in rec:
            rec[sy] = {"symbol": sy, "signals": [], "engines": [],
                       "_pts": 0.0, "name": None, "sector": None,
                       "market_cap": None, "cap_bucket": None}
        return rec[sy]

    # bagger-engine — 100-bagger DNA
    for d in extract_picks(bagger):
        sy = sym_of(d)
        if not sy:
            continue
        r = touch(sy)
        r["name"] = r["name"] or d.get("name")
        r["sector"] = r["sector"] or d.get("sector")
        r["market_cap"] = r["market_cap"] or num(d.get("market_cap"))
        r["cap_bucket"] = r["cap_bucket"] or d.get("cap_bucket")
        bs = num(d.get("bagger_score")) or num(d.get("score")) or 0
        rer = num(d.get("with_rerating_x")) or num(d.get("flat_multiple_x"))
        rcagr = num(d.get("revenue_cagr_pct"))
        roic = num(d.get("roic_pct"))
        r["_pts"] += clamp(bs / 100.0, 0, 1) * 26
        r["engines"].append("bagger")
        msg = "100-bagger DNA"
        if rcagr is not None and roic is not None:
            msg += f": {rcagr:.0f}% revenue CAGR at {roic:.0f}% ROIC"
        if rer:
            msg += f"; engine models ~{rer:.0f}x over the runway"
        r["signals"].append(msg)

    # earnings-pead — serial estimate-beat streak + drift
    for d in extract_picks(pead):
        sy = sym_of(d)
        if not sy:
            continue
        r = touch(sy)
        r["name"] = r["name"] or d.get("name")
        r["sector"] = r["sector"] or d.get("sector")
        r["market_cap"] = r["market_cap"] or num(d.get("market_cap"))
        streak = num(d.get("beat_streak")) or 0
        surp = num(d.get("latest_surprise_pct"))
        r["_pts"] += clamp(streak / 8.0, 0, 1) * 24
        r["engines"].append("pead")
        msg = (f"beaten earnings estimates {int(streak)} quarters running"
               if streak else "positive earnings-surprise pattern")
        if surp is not None:
            msg += f" (last surprise {surp:+.0f}%)"
        if d.get("drift_active"):
            msg += "; post-earnings drift still active"
        r["signals"].append(msg)

    # eps-revision-velocity — analysts revising up
    for d in extract_picks(epsrev):
        sy = sym_of(d)
        if not sy:
            continue
        r = touch(sy)
        r["name"] = r["name"] or d.get("company") or d.get("name")
        r["sector"] = r["sector"] or d.get("sector")
        r["market_cap"] = r["market_cap"] or num(d.get("market_cap"))
        up = num(d.get("upgrade_pct"))
        sc = num(d.get("score")) or 0
        r["_pts"] += clamp(sc / 100.0, 0, 1) * 20
        r["engines"].append("eps_revision")
        msg = "analysts revising EPS estimates upward"
        if up is not None:
            msg += f" ({up:.0f}% of recent revisions are upgrades)"
        r["signals"].append(msg)

    # revenue-acceleration — revenue growth inflecting
    for d in extract_picks(revacc):
        sy = sym_of(d)
        if not sy:
            continue
        r = touch(sy)
        r["name"] = r["name"] or d.get("name")
        r["sector"] = r["sector"] or d.get("sector")
        r["market_cap"] = r["market_cap"] or num(d.get("market_cap"))
        _ra_cb = d.get("cap_bucket")
        if not _ra_cb:
            _m = d.get("metrics") or {}
            if d.get("is_microcap") or _m.get("is_microcap"):
                _ra_cb = "micro"
            elif d.get("is_smallcap") or _m.get("is_smallcap"):
                _ra_cb = "small"
        r["cap_bucket"] = r["cap_bucket"] or _ra_cb
        yoy = num(d.get("latest_yoy_growth_pct"))
        consec = num(d.get("consec_accel_quarters")) or num(
            d.get("consec_accel")) or 0
        sc = num(d.get("score")) or 0
        r["_pts"] += clamp(sc / 100.0, 0, 1) * 20
        r["engines"].append("revenue_accel")
        msg = "revenue growth accelerating"
        if consec:
            msg += f" {int(consec)} quarters straight"
        if yoy is not None:
            msg += f", now {yoy:+.0f}% YoY"
        if d.get("eps_accelerating"):
            msg += "; earnings accelerating with it"
        r["signals"].append(msg)

    # momentum-breakout — early price/volume thrust
    for d in extract_picks(mom):
        sy = sym_of(d)
        if not sy:
            continue
        r = touch(sy)
        vr = num(d.get("vol_ratio")) or num(d.get("vol_ratio_today"))
        r["_pts"] += 11 if not d.get("is_parabolic") else 5
        r["engines"].append("momentum")
        msg = "early momentum breakout — volume thrust ahead of the crowd"
        if vr is not None:
            msg += f" (volume {vr:.1f}x normal)"
        if d.get("is_parabolic"):
            msg += " — already extended, treat as late"
        r["signals"].append(msg)

    # ── build the unified board ──
    board = []
    for sy, r in rec.items():
        engines = sorted(set(r["engines"]))
        n_eng = len(engines)
        sd = by_sym.get(sy, {})
        price = num(sd.get("price"))
        mcap = r["market_cap"] or num(sd.get("marketCap"))
        tier = (r.get("cap_bucket") or "").lower() or cap_tier(mcap)
        if tier not in ("nano", "micro", "small", "mid", "large",
                         "mega"):
            tier = cap_tier(mcap)
        # cross-confirmation is the heart of the score
        confirm_bonus = {1: 0, 2: 8, 3: 16, 4: 22, 5: 26}.get(min(n_eng, 5),
                                                              26)
        boom = round(clamp(r["_pts"] + confirm_bonus, 0, 100), 1)

        tgt, upside, basis = price_target(price, sd)

        # honest risk flags
        flags = []
        if tier in ("nano", "micro"):
            flags.append("micro/nano-cap — thin liquidity, size positions "
                         "small and use limit orders")
        az = num(sd.get("altmanZ"))
        if az is not None and az < 1.8:
            flags.append(f"Altman-Z {az:.1f} — financial-distress risk")
        fpe = num(sd.get("forwardPE"))
        if fpe and fpe > 60:
            flags.append(f"rich valuation (forward P/E {fpe:.0f}) — "
                         "growth must keep delivering")
        if any("parabolic" in s or "extended" in s for s in r["signals"]):
            flags.append("price already extended — wait for a pullback")

        # plain-English thesis
        growth_bits = []
        rg = num(sd.get("revenueGrowth"))
        if rg is not None:
            rg = rg * 100 if abs(rg) <= 5 else rg
            growth_bits.append(f"{rg:+.0f}% revenue growth")
        why = (f"Confirmed by {n_eng} independent engine"
               f"{'s' if n_eng != 1 else ''} ({', '.join(engines)}). "
               + "; ".join(r["signals"][:3]).capitalize() + ". ")
        if tier != "unknown":
            why += f"A {fmt_b(mcap)} {tier}-cap"
            if growth_bits:
                why += " growing " + " and ".join(growth_bits)
            if fpe and fpe > 0:
                why += f", still at {fpe:.0f}x forward earnings"
            why += ". "
        if tgt and upside is not None:
            why += (f"Multi-method fair value ~${tgt:.2f} ({upside:+.0f}% "
                    f"from ${price:.2f}) — {basis}.")
        elif price:
            why += f"Trading at ${price:.2f}; price target pending data."

        if upside is None:
            opp = boom
        else:
            uc = (clamp(upside, -30, 100) + 30) / 130.0 * 100.0
            opp = round(clamp(boom * 0.60 + uc * 0.40, 0, 100), 1)
        board.append({
            "opportunity_score": opp,
            "symbol": sy, "name": r["name"] or sy,
            "sector": r["sector"] or sd.get("sector"),
            "market_cap": mcap, "cap_tier": tier, "price": price,
            "boom_score": boom, "n_engines_confirming": n_eng,
            "signals_fired": engines, "signal_detail": r["signals"],
            "price_target": tgt, "upside_pct": upside,
            "target_basis": basis, "why": why, "risk_flags": flags,
        })

    board.sort(key=lambda x: (x["opportunity_score"], x["boom_score"]),
               reverse=True)

    # ── filtered views ──
    microcap_rockets = sorted(
        [b for b in board if b["cap_tier"] in ("nano", "micro")
         and b["boom_score"] >= 16],
        key=lambda x: x["opportunity_score"], reverse=True)[:40]
    serial_beaters = [b for b in board if "pead" in b["signals_fired"]
                      and b["boom_score"] >= 30][:40]
    # hidden growth — strong board score, real upside still on the table
    hidden_growth = sorted(
        [b for b in board if b["upside_pct"] is not None
         and b["upside_pct"] >= 25 and b["n_engines_confirming"] >= 2],
        key=lambda x: x["upside_pct"], reverse=True)[:40]

    # ── commodities & metals (read commodity-curves) ──
    commodities = []
    cm_items = (commod.get("fred_metrics") or []) + (
        commod.get("etf_metrics") or [])
    if not cm_items and isinstance(commod.get("ratios"), list):
        cm_items = commod["ratios"]
    for c in cm_items if isinstance(cm_items, list) else []:
        if not isinstance(c, dict):
            continue
        ytd = num(c.get("ret_ytd"))
        commodities.append({
            "name": c.get("name") or c.get("sym") or c.get("series_id"),
            "category": c.get("category"),
            "level": num(c.get("current")) or num(c.get("close"))
            or num(c.get("value")),
            "ytd_pct": ytd, "regime": c.get("regime")
            or commod.get("composite_regime"),
        })
    metals_note = (
        "Precious & industrial metals are a real-asset hedge and an "
        "inflation/growth read. The leverage play for a normie: when the "
        "metal trends up, the MINERS move 2-3x as hard — a gold miner earns "
        "the spread between a fixed cost base and a rising gold price, so "
        "its profit (and stock) is geared to the metal. Gold/silver "
        "exposure via GLD/SLV; miners via GDX (seniors) and GDXJ / SIL "
        "(juniors, highest beta); copper via COPX; broad via DBC. Miners "
        "lead and lag the metal — confirm the metal's trend first.")

    # ── housing (read construction-housing) ──
    house = {
        "cycle_score": num(housing.get("cycle_score")),
        "regime": housing.get("regime") or housing.get("label"),
        "read": housing.get("read") or housing.get("note"),
        "playbook": (
            "The housing cycle leads the economy and homebuilder stocks "
            "lead the cycle. When mortgage rates fall and permits/starts "
            "turn up, homebuilders (DHI, LEN, PHM, NVR) and the building-"
            "products chain (BLDR, masonry, HVAC, flooring) re-rate first; "
            "ETFs ITB and XHB give one-click exposure. When the cycle rolls "
            "over, they de-rate early — treat the cycle score as the "
            "regime gate before buying the group."),
    }

    out = {
        "schema_version": "1.0",
        "method": "multi_engine_opportunity_synthesis",
        "generated_at": now.isoformat(),
        "elapsed_s": round(time.time() - t0, 2),
        "ok": len(board) >= 5,
        "headline": (
            f"Boom Board: {len(board)} cross-confirmed opportunities — "
            f"{len(microcap_rockets)} micro-cap rockets, "
            f"{len(serial_beaters)} serial estimate-beaters, "
            f"{len(hidden_growth)} under-priced growers."),
        "how_to_read": (
            "Every name here is flagged by one or more of the platform's "
            "opportunity engines. The score rewards CROSS-CONFIRMATION — a "
            "stock independently flagged by three or four engines is a far "
            "stronger setup than one flagged by a single engine. Read the "
            "'why', check the price target and the risk flags, size "
            "positions sensibly, and never skip the micro-cap liquidity "
            "warning. Research, not financial advice."),
        "counts": {
            "boom_candidates": len(board),
            "microcap_rockets": len(microcap_rockets),
            "serial_beaters": len(serial_beaters),
            "hidden_growth": len(hidden_growth),
        },
        "boom_candidates": board[:120],
        "microcap_rockets": microcap_rockets,
        "serial_beaters": serial_beaters,
        "hidden_growth": hidden_growth,
        "commodities_metals": {
            "items": commodities,
            "composite_regime": commod.get("composite_regime")
            or commod.get("regime"),
            "playbook": metals_note,
        },
        "housing": house,
        "sources": ["bagger-engine", "earnings-pead",
                    "eps-revision-velocity", "revenue-acceleration",
                    "momentum-breakout", "stock-screener",
                    "commodity-curves", "construction-housing"],
        "errors": errors,
    }

    s3.put_object(Bucket=S3_BUCKET, Key=OUT_KEY,
                  Body=json.dumps(out, default=str).encode("utf-8"),
                  ContentType="application/json",
                  CacheControl="public, max-age=900")
    print(f"[opportunity-screener] {len(board)} boom candidates | "
          f"{len(microcap_rockets)} microcap | {len(serial_beaters)} "
          f"beaters | errors={len(errors)}")
    return {"statusCode": 200,
            "body": json.dumps({"ok": out["ok"],
                                "boom_candidates": len(board),
                                "errors": len(errors)})}
