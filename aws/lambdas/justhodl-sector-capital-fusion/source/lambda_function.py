"""
justhodl-sector-capital-fusion
==============================
Fuses every engine's per-sector capital-flow read into ONE verdict per GICS sector,
and draws the conclusion -- especially the price-vs-smart-money divergences that are
the actual edge (price rotating IN while tape / institutions / dark-pool flow OUT =
distribution; price lagging while money quietly enters = stealth accumulation).

Six independent flow lenses (kept independent to avoid double-counting a correlated
signal), each z-scored cross-sectionally across the 11 sectors so they are comparable:

  F1 rotation     price relative-strength trend          sector-rotation (rs_slope, rs_rank)
  F2 tape         grouped-daily signed dollar flow        money-flow-state.sectors
  F3 etf          SPDR-sector ETF creation + complex pump sector-rotation.etf_flow_5d + capital-flow-radar
  F4 institutional 13F net funds adding-trimming           money-flow-state.institutional_sector_tilt
  F5 darkpool     off-exchange accumulation-distribution   dark-pool.board rolled to sector

  backdrop        market-wide liquidity + RORO            liquidity-flow + risk-regime  (context, not a vote)

net_score = mean of available family z-scores. confluence = families voting the net
direction (|z|>=0.3). conclusion + posture from net + the price-vs-smartmoney split.
"""
import json, os, statistics
from datetime import datetime, timezone
import boto3

S3 = boto3.client("s3", "us-east-1")
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
OUT_KEY = os.environ.get("S3_KEY_OUT", "data/sector-capital-fusion.json")

CANON = ["Technology", "Financials", "Healthcare", "Consumer Discretionary",
         "Consumer Staples", "Energy", "Industrials", "Materials",
         "Utilities", "Real Estate", "Communication Services"]

_ALIAS = {
    "financial services": "Financials", "financials": "Financials", "financial": "Financials",
    "basic materials": "Materials", "materials": "Materials",
    "consumer cyclical": "Consumer Discretionary", "consumer discretionary": "Consumer Discretionary",
    "consumer defensive": "Consumer Staples", "consumer staples": "Consumer Staples",
    "information technology": "Technology", "technology": "Technology", "tech": "Technology",
    "health care": "Healthcare", "healthcare": "Healthcare",
    "communication services": "Communication Services", "communications": "Communication Services",
    "real estate": "Real Estate", "energy": "Energy", "industrials": "Industrials",
    "utilities": "Utilities",
}

def canon(name):
    if not name:
        return None
    return _ALIAS.get(str(name).strip().lower(), str(name).strip())

# capital-flow-radar complex name -> GICS sector (keyword match)
def complex_sector(cx):
    c = (cx or "").lower()
    pairs = [
        (("semiconductor", "software", "technology", "cloud", "internet", "hardware"), "Technology"),
        (("biotech", "pharma", "health", "medical", "life science", "drug"), "Healthcare"),
        (("aerospace", "defense", "airline", "industrial", "transport", "machinery", "rail"), "Industrials"),
        (("bank", "financial", "insurance", "broker", "capital market"), "Financials"),
        (("utilit", "power", "electric"), "Utilities"),
        (("energy", "oil", "gas", "exploration", "drilling"), "Energy"),
        (("real estate", "reit", "housing", "homebuild"), "Real Estate"),
        (("material", "metal", "mining", "gold", "copper", "steel", "chemical"), "Materials"),
        (("retail", "consumer disc", "auto", "leisure", "travel", "restaurant", "apparel"), "Consumer Discretionary"),
        (("staple", "consumer def", "food", "beverage", "tobacco", "household"), "Consumer Staples"),
        (("communication", "media", "telecom", "entertainment"), "Communication Services"),
    ]
    for kws, sec in pairs:
        if any(k in c for k in kws):
            return sec
    return None

def rj(key):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception as e:
        print(f"[warn] read {key}: {e}")
        return {}

def zmap(raw):
    """raw: {sector: value or None} -> {sector: zscore}; std=0 -> all 0."""
    vals = [v for v in raw.values() if isinstance(v, (int, float))]
    if len(vals) < 2:
        return {k: 0.0 for k in raw}
    mu = statistics.fmean(vals)
    sd = statistics.pstdev(vals) or 0.0
    if sd == 0:
        return {k: 0.0 for k in raw}
    return {k: ((v - mu) / sd if isinstance(v, (int, float)) else None) for k, v in raw.items()}

def sgn(x, thr=0.3):
    if x is None:
        return 0
    if x >= thr:
        return 1
    if x <= -thr:
        return -1
    return 0

def lambda_handler(event, context):
    t0 = datetime.now(timezone.utc)

    rot = rj("data/sector-rotation.json")
    mfs = rj("data/money-flow-state.json")
    cfr = rj("data/capital-flow-radar.json")
    dp = rj("data/dark-pool.json")
    liq = rj("data/liquidity-flow.json")
    rr = rj("data/risk-regime.json")
    uni = rj("data/universe.json")
    uni_list = uni if isinstance(uni, list) else (uni.get("stocks") or uni.get("universe") or [])
    tk_sector = {(u.get("symbol") or u.get("ticker")): canon(u.get("sector")) for u in uni_list if u.get("sector")}

    # ---- raw per-family values keyed by canonical sector ----
    f1_raw, f3_raw = {s: None for s in CANON}, {s: None for s in CANON}
    rot_meta = {}
    for r in (rot.get("sectors") or rot.get("rankings") or []):
        s = canon(r.get("name") or r.get("symbol"))
        if s not in f1_raw:
            continue
        # F1 = pure relative-strength trend: rs_slope + rs_rank (price-only)
        slope = r.get("rs_slope_21d_pct_per_day")
        rank = r.get("rs_pct_rank_1y")
        comp = []
        if isinstance(slope, (int, float)):
            comp.append(slope * 100.0)
        if isinstance(rank, (int, float)):
            comp.append((rank - 50) / 50.0)
        f1_raw[s] = statistics.fmean(comp) if comp else None
        # F3 = SPDR-sector ETF 5d creation flow
        f3_raw[s] = r.get("etf_flow_5d_usd")
        rot_meta[s] = {
            "rotation_score": r.get("rotation_score"),
            "rs_rank_1y": rank, "rs_slope": slope,
            "etf_flow_confirm": r.get("etf_flow_confirm"),
            "rotating_in": bool(r.get("rotating_in")), "rotating_out": bool(r.get("rotating_out")),
        }

    # F2 tape dollars
    f2_raw = {s: None for s in CANON}
    for r in (mfs.get("sectors") or []):
        s = canon(r.get("sector"))
        if s in f2_raw:
            f2_raw[s] = r.get("net_flow_usd")
    # F4 institutional 13F
    f4_raw = {s: None for s in CANON}
    for r in (mfs.get("institutional_sector_tilt") or []):
        s = canon(r.get("sector"))
        if s in f4_raw:
            f4_raw[s] = r.get("net_fund_actions")

    # F3 confirm: capital-flow-radar complexes -> sector (avg pump centered at 50, plus divergence flag)
    cx_pump, cx_div = {s: [] for s in CANON}, {s: 0 for s in CANON}
    for c in (cfr.get("complexes") or []):
        s = complex_sector(c.get("complex"))
        if s not in cx_pump:
            continue
        pp = c.get("pump_probability")
        if isinstance(pp, (int, float)):
            cx_pump[s].append(pp)
        if c.get("flow_price_divergence"):
            cx_div[s] += 1
    cx_pump_avg = {s: (statistics.fmean(v) if v else None) for s, v in cx_pump.items()}

    # F5 dark pool: roll board names to sector -> sum(score signed by state)
    dp_raw = {s: None for s in CANON}
    dp_acc, dp_dist = {s: 0.0 for s in CANON}, {s: 0.0 for s in CANON}
    for b in (dp.get("board") or []):
        s = tk_sector.get(b.get("ticker"))
        if s not in dp_raw:
            continue
        sc = b.get("score") or 0
        st = (b.get("state") or "").upper()
        if st == "ACCUMULATION":
            dp_acc[s] += sc
        elif st == "DISTRIBUTION":
            dp_dist[s] += sc
    for s in CANON:
        if dp_acc[s] or dp_dist[s]:
            dp_raw[s] = dp_acc[s] - dp_dist[s]

    # ---- z-score each family cross-sectionally ----
    z1, z2, z3, z4, z5 = zmap(f1_raw), zmap(f2_raw), zmap(f3_raw), zmap(f4_raw), zmap(dp_raw)

    # backdrop context (not a vote)
    liq_regime = liq.get("regime")
    rr_score = rr.get("risk_regime_score")
    rr_regime = rr.get("risk_regime")
    HIGH_BETA = {"Technology", "Consumer Discretionary", "Energy", "Financials",
                 "Materials", "Communication Services", "Industrials"}
    DEFENSIVE = {"Utilities", "Consumer Staples", "Healthcare", "Real Estate"}

    sectors_out = []
    for s in CANON:
        fam = {}
        present = []
        for tag, raw, z in (("rotation", f1_raw, z1), ("tape", f2_raw, z2), ("etf", f3_raw, z3),
                            ("institutional", f4_raw, z4), ("darkpool", dp_raw, z5)):
            zv = z.get(s)
            has = raw.get(s) is not None
            fam[tag] = {"z": round(zv, 2) if isinstance(zv, (int, float)) else None,
                        "raw": raw.get(s), "dir": sgn(zv) if has else 0, "present": has}
            if has:
                present.append(zv)
        net = round(statistics.fmean(present), 3) if present else 0.0
        net_dir = sgn(net, 0.2)
        confluence = sum(1 for tag in ("rotation", "tape", "etf", "institutional", "darkpool")
                         if fam[tag]["present"] and fam[tag]["dir"] == net_dir and net_dir != 0)

        price_dir = fam["rotation"]["dir"]
        smart_vals = [fam[t]["z"] for t in ("tape", "institutional", "darkpool")
                      if fam[t]["present"] and fam[t]["z"] is not None]
        smart_dir = sgn(statistics.fmean(smart_vals), 0.2) if smart_vals else 0
        divergence = (price_dir != 0 and smart_dir != 0 and price_dir != smart_dir)

        # conclusion
        if divergence and price_dir > 0 > smart_dir:
            conclusion = "DISTRIBUTION RISK — price leading but tape/institutions/dark-pool exiting"
            posture = "UNDERWEIGHT"
        elif divergence and price_dir < 0 < smart_dir:
            conclusion = "STEALTH ACCUMULATION — price lagging but money quietly entering"
            posture = "OVERWEIGHT"
        elif net_dir > 0 and confluence >= 4:
            conclusion = "CONFIRMED INFLOW — broad accumulation across lenses"
            posture = "OVERWEIGHT"
        elif net_dir > 0 and confluence >= 3:
            conclusion = "INFLOW — money rotating in"
            posture = "OVERWEIGHT"
        elif net_dir < 0 and confluence >= 4:
            conclusion = "CONFIRMED OUTFLOW — broad distribution across lenses"
            posture = "UNDERWEIGHT"
        elif net_dir < 0 and confluence >= 3:
            conclusion = "OUTFLOW — money rotating out"
            posture = "UNDERWEIGHT"
        else:
            conclusion = "MIXED — no flow consensus"
            posture = "NEUTRAL"

        # backdrop modifier note
        ctx = []
        if isinstance(rr_score, (int, float)):
            if rr_score <= -12 and s in HIGH_BETA:
                ctx.append("risk-off headwind (high-beta)")
            elif rr_score <= -12 and s in DEFENSIVE:
                ctx.append("risk-off tailwind (defensive)")
            elif rr_score >= 12 and s in HIGH_BETA:
                ctx.append("risk-on tailwind (high-beta)")
        if liq_regime == "draining" and s in HIGH_BETA:
            ctx.append("liquidity draining (headwind)")

        drivers = []
        for tag, lbl in (("rotation", "price-RS"), ("tape", "tape $"), ("etf", "ETF flow"),
                         ("institutional", "13F"), ("darkpool", "dark-pool")):
            f = fam[tag]
            if f["present"] and f["dir"] != 0:
                drivers.append(f"{lbl} {'+' if f['dir']>0 else '−'}")

        sectors_out.append({
            "sector": s,
            "net_score": net,
            "net_dir": net_dir,
            "confluence": confluence,
            "n_families": len(present),
            "conclusion": conclusion,
            "posture": posture,
            "divergence": divergence,
            "price_dir": price_dir,
            "smart_money_dir": smart_dir,
            "families": fam,
            "complex_pump_avg": round(cx_pump_avg[s], 1) if cx_pump_avg.get(s) is not None else None,
            "complex_divergence_flags": cx_div.get(s, 0),
            "context": ctx,
            "drivers": drivers,
            "rotation_meta": rot_meta.get(s, {}),
        })

    sectors_out.sort(key=lambda x: x["net_score"], reverse=True)
    divergences = [x for x in sectors_out if x["divergence"]]
    inflow = [x for x in sectors_out if x["net_dir"] > 0 and x["confluence"] >= 3]
    outflow = [x for x in sectors_out if x["net_dir"] < 0 and x["confluence"] >= 3]

    # overall read
    if divergences:
        head = (f"{len(divergences)} sector divergence(s): "
                + ", ".join(f"{d['sector']} ({'distribution' if d['price_dir']>0 else 'accumulation'})"
                            for d in divergences))
    elif inflow:
        head = "Money rotating into " + ", ".join(x["sector"] for x in inflow[:3])
    else:
        head = "No strong sector flow consensus"

    # -- ops 3396: TECHNICALS join (display layer; verdict lenses untouched) --
    ETF_OF = {"Technology": "XLK", "Financials": "XLF", "Healthcare": "XLV",
              "Consumer Discretionary": "XLY", "Consumer Staples": "XLP",
              "Energy": "XLE", "Industrials": "XLI", "Materials": "XLB",
              "Utilities": "XLU", "Real Estate": "XLRE",
              "Communication Services": "XLC"}
    try:
        cp = rj("data/chart-patterns.json")
        se = cp.get("sector_etfs") or {}
        ar = rj("data/accumulation-radar.json")
        ph_map = {str(k).upper(): v for k, v in (ar.get("etf_phases") or {}).items()}

        def _walk(o):
            if isinstance(o, dict):
                _tk = o.get("symbol") or o.get("ticker")
                if _tk and o.get("phase"):
                    ph_map.setdefault(str(_tk).upper(),
                                      {"phase": o.get("phase"),
                                       "flag": o.get("flag"),
                                       "top_score": o.get("top_score"),
                                       "bottom_score": o.get("bottom_score")})
                for v in o.values():
                    _walk(v)
            elif isinstance(o, list):
                for v in o:
                    _walk(v)
        if not ph_map:
            _walk(ar)
        of = rj("data/options-flow.json")
        of_map = {}

        def _walk2(o):
            if isinstance(o, dict):
                tk = o.get("ticker") or o.get("symbol")
                if tk and ("flow_5d" in o):
                    of_map.setdefault(str(tk).upper(), o.get("flow_5d"))
                for v in o.values():
                    _walk2(v)
            elif isinstance(o, list):
                for v in o:
                    _walk2(v)
        _walk2(of)
        for row in sectors_out:
            etf = ETF_OF.get(row.get("sector"))
            if not etf:
                continue
            t = {"etf": etf}
            if etf in se:
                r = se[etf]
                t["ma"] = r.get("ma")
                t["posture"] = r.get("posture")
                t["ladder_score"] = r.get("ladder_score")
                pat = r.get("double_top") or r.get("double_bottom")
                if pat:
                    t["pattern"] = dict(pat, kind=("double_top" if r.get("double_top") else "double_bottom"))
            if etf in ph_map:
                t["wyckoff"] = ph_map[etf]
            if etf in of_map:
                t["options_flow_5d"] = of_map[etf]
            row["technicals"] = t
    except Exception as _e:
        print(f"[technicals join] {str(_e)[:80]}")

    doc = {
        "engine": "justhodl-sector-capital-fusion", "version": "1.0.0",
        "generated_at": t0.isoformat(),
        "duration_s": round((datetime.now(timezone.utc) - t0).total_seconds(), 2),
        "headline": head,
        "backdrop": {"liquidity_regime": liq_regime, "risk_regime": rr_regime,
                     "risk_regime_score": rr_score},
        "sectors": sectors_out,
        "technicals_joined": True,
        "top_inflow": [x["sector"] for x in inflow[:5]],
        "top_outflow": [x["sector"] for x in outflow[:5]],
        "divergences": [{"sector": d["sector"], "type": ("distribution" if d["price_dir"] > 0 else "accumulation"),
                         "conclusion": d["conclusion"]} for d in divergences],
        "families": ["rotation (price-RS)", "tape ($ volume)", "etf (creation)",
                     "institutional (13F)", "darkpool (off-exchange)"],
        "methodology": ("Each lens z-scored across the 11 GICS sectors; net = mean of available z's; "
                        "confluence = lenses agreeing with net direction (|z|>=0.3); divergence = price-RS "
                        "vs smart-money (tape+13F+dark-pool) disagree. Sources: sector-rotation, money-flow-state, "
                        "capital-flow-radar, dark-pool, liquidity-flow, risk-regime."),
        "sources_ok": {"sector-rotation": bool(rot), "money-flow-state": bool(mfs),
                       "capital-flow-radar": bool(cfr), "dark-pool": bool(dp),
                       "liquidity-flow": bool(liq), "risk-regime": bool(rr)},
    }
    S3.put_object(Bucket=BUCKET, Key=OUT_KEY,
                  Body=json.dumps(doc, default=str).encode(),
                  ContentType="application/json", CacheControl="no-cache")
    print(f"[fusion] {len(sectors_out)} sectors, {len(divergences)} divergences -> {OUT_KEY}")
    return {"ok": True, "sectors": len(sectors_out), "divergences": len(divergences), "headline": head}
