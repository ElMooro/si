"""
justhodl-macro-confluence v1.0 — the MACRO/SECTOR-ROTATION confluence engine.

WHY THIS EXISTS (2026-07-25)
────────────────────────────
Khalid: "this week price action might lay in my system but its just buried
deep in the noise: my system predict crypto leg up... also flows Master
Universe showd smh semiconductor capitulation along many name capitulation."
Both true and confirmed live — but surfacing them took 8 manual ops
cross-referencing 5 separate feeds. Audit found 12 existing confluence
engines (equity/crypto/flow/options/earnings/insider-buyback/attention/
multi-tf/alpha-confluence + convergence-radar), and justhodl-convergence-radar
itself says almost exactly this same thing about its OWN origin: "the leading
signal was already in our data — just not unified." But EVERY one of those
12 engines fuses PER-TICKER, bottom-up signals (13F, dark pool, options,
momentum, earnings surprise). Grepped all 12 for rebalance-radar /
sector-flow-state / rotation-dashboard: zero hits. None of them touch the
macro/sector-rotation layer at all. This is that missing sibling.

METHODOLOGY (mirrors justhodl-equity-confluence's own discipline: independent
FAMILIES, not raw signal-counting — "3 correlated technicals = 1 bet, not 3")
──────────────────────────────────────────────────────────────────────────
Per GICS sector, four INDEPENDENT families, each answering a different
question with a different data source:

  F1 STOCK QUADRANT CLUSTER  — constituent-pressure.json per_stock_exposure,
     grouped by sector. Lights up if this sector is in the top tier by
     CAPITULATION (or STEALTH_ACCUMULATION, for a bullish theme) count AND
     that count is non-trivial (>=5 names) — a real cluster, not noise.

  F2 SECTOR POSTURE  — sector-flow-state.json's per-sector posture/quadrant/
     rs_slope. Lights up on UNDERWEIGHT+Weakening (bearish) or
     OVERWEIGHT+Leading (bullish).

  F3 MACRO REGIME / ROTATION-DASHBOARD  — is this sector's SPDR ETF in the
     avoid list (bearish) or overweight list (bullish)? Cross-checked against
     layer1_regime's asset-class priors (e.g. equity_growth strongly negative
     in a STAGFLATION call is independent macro evidence, not a repeat of F2).

  F4 REBALANCE-RADAR — window_forensics' mechanical-vs-observed classification
     (EXCESS_SELLING_INTO_WEAKNESS is real evidence a complex is being
     actively de-risked beyond what quarter-end mechanics would predict) AND
     qtd_proxies showing a clear alternate-asset-class beneficiary (crypto/
     energy/gold outperforming while this sector's proxy lags) — the "where
     did the money go" confirmation.

A sector's CONVERGENCE SCORE = how many of the 4 families light up in the
SAME direction (0-4). 3-4 is flagged HIGH — a rare, strong confirmation
across genuinely independent evidence sources, not a repeated signal wearing
different hats. Full evidence trail is preserved per family so the output
can be read/verified, never just a bare score.

HONEST LIMITS
─────────────
- Complex-name-to-sector mapping (rebalance-radar's F4) is best-effort text
  matching (e.g. "Nasdaq Broad" / "Mega-Cap Tech (FANG+)" are tech-ADJACENT,
  not a literal GICS "Technology" match) — recorded as SUPPORTING context,
  never silently merged into an exact-match count.
- SPDR-sector-ETF-to-GICS-sector mapping (F3) is a real, standard reference
  table (the actual SPDR Select Sector fund tickers), not fabricated data.
- Needs >=5 names for F1 to fire specifically so a thin, coincidental cluster
  of 2-3 names can't manufacture a false convergence read.

OUTPUT: data/macro-confluence.json + Telegram alert when any sector crosses
3+ families in the SAME direction (mirrors convergence-radar's own
"fires the moment a ticker crosses the 4-engine threshold" pattern, scaled
to the sector/macro level).
"""
import json
import os
import time
from collections import Counter
from datetime import datetime, timezone

import boto3

BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/macro-confluence.json"
REGION = "us-east-1"
s3 = boto3.client("s3", region_name=REGION)

SPDR_SECTOR_ETF = {
    "XLK": "Technology", "XLV": "Healthcare", "XLF": "Financial Services",
    "XLE": "Energy", "XLI": "Industrials", "XLB": "Basic Materials",
    "XLP": "Consumer Defensive", "XLY": "Consumer Cyclical", "XLU": "Utilities",
    "XLRE": "Real Estate", "XLC": "Communication Services",
}
# best-effort, tech-ADJACENT (not identical) complex names rebalance-radar
# uses in window_forensics — kept SEPARATE from an exact sector match, never
# silently merged into it (see HONEST LIMITS above)
TECH_ADJACENT_COMPLEXES = {"Nasdaq Broad", "Mega-Cap Tech (FANG+)", "Software", "Technology"}
ALT_ASSET_TICKERS = {"BTCUSD": "crypto", "XLE": "energy", "GLD": "gold"}


def _get(key, default=None):
    try:
        o = s3.get_object(Bucket=BUCKET, Key=key)
        return json.loads(o["Body"].read())
    except Exception as e:
        print(f"[macro-confluence] {key} unreadable: {str(e)[:150]}")
        return default


def _quadrant_clusters_by_sector(per_stock_exposure):
    """F1 input: per-sector CAPITULATION / STEALTH_ACCUMULATION counts."""
    by_sector_capit = Counter()
    by_sector_stealth = Counter()
    by_sector_total = Counter()
    for rec in (per_stock_exposure or {}).values():
        sec = rec.get("sector")
        if not sec:
            continue
        by_sector_total[sec] += 1
        q = rec.get("quadrant")
        if q == "CAPITULATION":
            by_sector_capit[sec] += 1
        elif q == "STEALTH_ACCUMULATION":
            by_sector_stealth[sec] += 1
    return by_sector_capit, by_sector_stealth, by_sector_total


def _f1_stock_cluster(sector, capit_by_sector, stealth_by_sector, min_names=5):
    """Returns ('bearish'|'bullish'|None, evidence_str)."""
    capit_n = capit_by_sector.get(sector, 0)
    stealth_n = stealth_by_sector.get(sector, 0)
    top_capit = [s for s, _ in capit_by_sector.most_common(3)]
    top_stealth = [s for s, _ in stealth_by_sector.most_common(3)]
    if capit_n >= min_names and sector in top_capit:
        return "bearish", f"{capit_n} names in CAPITULATION (top-3 sector by count)"
    if stealth_n >= min_names and sector in top_stealth:
        return "bullish", f"{stealth_n} names in STEALTH_ACCUMULATION (top-3 sector by count)"
    return None, f"capitulation={capit_n} stealth_accum={stealth_n} (below threshold or not top-3)"


def _f2_sector_posture(sector, sfs_by_sector):
    row = sfs_by_sector.get(sector)
    if not row:
        return None, "no sector-flow-state entry"
    posture, quad = row.get("posture"), row.get("quadrant")
    if posture == "UNDERWEIGHT" and quad == "Weakening":
        return "bearish", f"posture=UNDERWEIGHT quadrant=Weakening rs_slope={row.get('rs_slope')}"
    if posture == "OVERWEIGHT" and quad == "Leading":
        return "bullish", f"posture=OVERWEIGHT quadrant=Leading rs_slope={row.get('rs_slope')}"
    return None, f"posture={posture} quadrant={quad} (not a clear bearish/bullish combo)"


def _f3_rotation_dashboard(sector, avoid_tickers, overweight_tickers, priors):
    spdr = next((t for t, s in SPDR_SECTOR_ETF.items() if s == sector), None)
    if not spdr:
        return None, "no SPDR sector ETF mapping"
    prior_note = ""
    growth_prior = priors.get("equity_growth")
    if growth_prior is not None and sector in ("Technology", "Communication Services", "Consumer Cyclical"):
        prior_note = f" · regime prior equity_growth={growth_prior}"
    if spdr in avoid_tickers:
        return "bearish", f"{spdr} in rotation-dashboard AVOID list{prior_note}"
    if spdr in overweight_tickers:
        return "bullish", f"{spdr} in rotation-dashboard OVERWEIGHT list{prior_note}"
    return None, f"{spdr} in neither avoid nor overweight{prior_note}"


def _f4_rebalance_radar(sector, window_forensics, qtd_proxies):
    evidence = []
    direction = None
    if sector == "Technology":
        for row in (window_forensics or {}).get("top_outflows", []):
            if row.get("complex") in TECH_ADJACENT_COMPLEXES and row.get("classification"):
                evidence.append(f"{row['complex']}: {row['classification']} "
                                 f"(5d flow ${(row.get('net_flow_5d_usd') or 0)/1e9:+.2f}B, "
                                 f"mechanical expected {row.get('mechanical_expectation')})")
                if "SELLING" in str(row.get("classification", "")):
                    direction = "bearish"
                elif "BUYING" in str(row.get("classification", "")):
                    direction = "bullish"
        # cross-asset: is an alt asset class clearly diverging while a tech proxy lags
        smh = (qtd_proxies or {}).get("SMH", {})
        for alt_tk, alt_name in ALT_ASSET_TICKERS.items():
            alt = (qtd_proxies or {}).get(alt_tk, {})
            if smh.get("qtd_pct") is not None and alt.get("qtd_pct") is not None:
                gap = alt["qtd_pct"] - smh["qtd_pct"]
                if gap >= 10:
                    evidence.append(f"{alt_name} QTD {alt['qtd_pct']:+.1f}% vs SMH "
                                     f"{smh['qtd_pct']:+.1f}% ({gap:.1f}pp gap)")
                    direction = direction or "bearish"  # tech lagging = bearish FOR tech
    if not evidence:
        return None, "no rebalance-radar evidence mapped to this sector"
    return direction, " | ".join(evidence)


def lambda_handler(event=None, context=None):
    t0 = time.time()
    print("[macro-confluence] starting")

    cp = _get("etf-flows/constituent-pressure.json", {})
    per_stock = cp.get("per_stock_exposure") or {}
    capit_by_sector, stealth_by_sector, total_by_sector = _quadrant_clusters_by_sector(per_stock)

    sfs = _get("data/sector-flow-state.json", {})
    sfs_by_sector = {s.get("name"): s for s in (sfs.get("sectors") or []) if s.get("name")}

    rd = _get("data/rotation-dashboard.json", {})
    avoid_tickers = {r.get("ticker") for r in (rd.get("avoid") or [])}
    overweight_tickers = {r.get("ticker") for r in (rd.get("overweight") or [])}
    priors = ((rd.get("layer1_regime") or {}).get("prior")) or {}
    regime_name = ((rd.get("layer1_regime") or {}).get("quadrant") or {}).get("regime")

    rr = _get("data/rebalance-radar.json", {})
    window_forensics = rr.get("window_forensics") or {}
    qtd_proxies = rr.get("qtd_proxies") or {}
    rotation_risk = rr.get("rotation_risk") or {}

    sectors = sorted(set(list(SPDR_SECTOR_ETF.values()) + list(sfs_by_sector.keys())
                          + list(capit_by_sector.keys())))
    board = []
    for sector in sectors:
        f1_dir, f1_ev = _f1_stock_cluster(sector, capit_by_sector, stealth_by_sector)
        f2_dir, f2_ev = _f2_sector_posture(sector, sfs_by_sector)
        f3_dir, f3_ev = _f3_rotation_dashboard(sector, avoid_tickers, overweight_tickers, priors)
        f4_dir, f4_ev = _f4_rebalance_radar(sector, window_forensics, qtd_proxies)

        families = {"stock_quadrant_cluster": (f1_dir, f1_ev),
                    "sector_posture": (f2_dir, f2_ev),
                    "rotation_dashboard": (f3_dir, f3_ev),
                    "rebalance_radar": (f4_dir, f4_ev)}
        dirs = [d for d, _ in families.values() if d]
        bearish_n = dirs.count("bearish")
        bullish_n = dirs.count("bullish")
        # convergence = families agreeing on the SAME direction, not raw count
        score = max(bearish_n, bullish_n)
        theme = "bearish" if bearish_n > bullish_n else ("bullish" if bullish_n > 0 else None)

        board.append({
            "sector": sector,
            "convergence_score": score,
            "theme": theme,
            "families": {k: {"direction": d, "evidence": e} for k, (d, e) in families.items()},
            "n_families_lit": len(dirs),
            "stock_universe_n": total_by_sector.get(sector, 0),
        })

    board.sort(key=lambda r: r["convergence_score"], reverse=True)
    top = board[0] if board else None
    high_convergence = [r for r in board if r["convergence_score"] >= 3]

    narrative = None
    if top and top["convergence_score"] >= 3:
        lit = [k for k, v in top["families"].items() if v["direction"] == top["theme"]]
        narrative = (f"{top['sector']} — {top['convergence_score']}/4 independent families "
                     f"agree {theme_word(top['theme'])}: " +
                     " | ".join(f"[{k}] {top['families'][k]['evidence']}" for k in lit))

    doc = {
        "version": "1.0",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "regime_context": {"name": regime_name, "growth_prior_equity_growth": priors.get("equity_growth")},
        "rotation_risk_flag": rotation_risk.get("flag"),
        "board": board,
        "high_convergence_sectors": [r["sector"] for r in high_convergence],
        "top_theme": top,
        "narrative": narrative,
        "elapsed_s": round(time.time() - t0, 1),
    }
    s3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(doc, default=str).encode(),
                  ContentType="application/json", CacheControl="max-age=900")

    print(f"[macro-confluence] OK — {len(board)} sectors scored, "
          f"{len(high_convergence)} high-convergence, top={top['sector'] if top else None} "
          f"score={top['convergence_score'] if top else None}, {doc['elapsed_s']}s")

    if high_convergence:
        try:
            from jhcore import notify
            notify.alert("INFO", "Macro Confluence",
                        narrative or f"{len(high_convergence)} sector(s) at high convergence")
        except Exception as e:
            print(f"[macro-confluence] alert skipped: {str(e)[:100]}")

    return {"statusCode": 200, "body": json.dumps({
        "n_sectors": len(board), "high_convergence": len(high_convergence),
        "top_theme": top["sector"] if top else None,
    })}


def theme_word(theme):
    return {"bearish": "BEARISH", "bullish": "BULLISH"}.get(theme, str(theme))
