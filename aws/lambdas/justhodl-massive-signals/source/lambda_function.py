"""
justhodl-massive-signals — UNIFIED MASSIVE INTELLIGENCE LAYER
=============================================================
The platform pays for options, FX, futures, and ETF-flow data but most engines
never touch it. This consolidates every Massive-derived signal into ONE file so
any engine can tap it with a single read (see aws/shared/massive_signals.py):

  PER-TICKER (the pre-pump fuel):
    • gamma squeeze   — dealer-gex squeeze_candidates (negative GEX, call-heavy)
    • unusual flow    — polygon-options-flow (C/P ratio, OTM call sweeps, smart-money blocks)
    • prepump_score   — combined: a name that is a gamma-squeeze candidate AND has bullish
                        call flow is primed to move before the crowd

  MARKET CONTEXT:
    • gamma_regime    — dealer-gex market composite (pos/neg gamma)
    • smallcap_bid    — IWM ETF inflow (small-caps catching real $)
    • sector_flows    — real ETF Global $ flows per sector ETF (XLK/XLF/...)
    • fx_signals      — polygon-fx-regime (carry, USD)
    • futures_signals — polygon-futures-curves (curve/breakouts)

OUTPUT data/massive-signals.json   SCHEDULE daily 22:00 UTC (after producers, before harvester).
Real data, research only.
"""
import json
import time
from datetime import datetime, timezone

import boto3

VERSION = "1.0.0"
S3_BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/massive-signals.json"
SECTOR_ETFS = ["XLK", "XLF", "XLE", "XLV", "XLP", "XLY", "XLI", "XLB", "XLU", "XLRE", "XLC"]
s3 = boto3.client("s3", region_name="us-east-1")


def _read(key):
    try:
        return json.loads(s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read())
    except Exception:
        return None


def lambda_handler(event, context):
    t0 = time.time()
    tickers = {}

    # ── gamma squeeze candidates (dealer-gex) ──
    gex = _read("data/dealer-gex.json") or {}
    gamma_regime = (gex.get("market_composite") or {}).get("composite_regime")
    for c in gex.get("squeeze_candidates", []) or []:
        sym = c.get("symbol")
        if not sym:
            continue
        tickers.setdefault(sym, {})
        tickers[sym].update({
            "gamma_squeeze_score": c.get("score"),
            "gex_billions": c.get("gex_billions"),
            "pcr_oi": c.get("pcr_oi"),
        })

    # ── unusual options flow (polygon-options-flow: rich per-ticker) ──
    pof = _read("data/polygon-options-flow.json") or {}
    rows = pof.get("all_results") or (
        (pof.get("extreme_call_flow") or []) + (pof.get("bullish_call_flow") or [])
        + (pof.get("notable_flow") or []))
    for r in rows:
        sym = r.get("ticker") or r.get("symbol")
        if not sym:
            continue
        sigs = r.get("signals") or []
        bullish = any("BULLISH_CALL" in s or "EXTREME_CALL" in s for s in sigs)
        sweep = any("OTM_CALL_SWEEP" in s for s in sigs) or bool(r.get("otm_call_sweep"))
        smart = any("SMART_MONEY" in s for s in sigs)
        tickers.setdefault(sym, {})
        tickers[sym].update({
            "call_put_ratio": r.get("cv_pv_ratio") or r.get("pc_ratio"),
            "otm_call_sweep": sweep,
            "smart_money_blocks": smart,
            "bullish_flow": bullish,
            "options_alert": r.get("alert_level"),
            "options_signals": sigs[:4],
        })

    # ── simple-majors flow (options-flow, 8 names) for market read ──
    of = _read("data/options-flow.json") or {}
    majors = {f.get("ticker"): f.get("sentiment") for f in (of.get("all_qualifying") or [])
              if isinstance(f, dict) and f.get("ticker")}

    # ── ETF $ flows (sector + small-cap bid) ──
    etf = {}
    for m in (_read("etf-flows/daily.json") or {}).get("metrics", []) or []:
        if m.get("ticker") and not m.get("error"):
            etf[m["ticker"]] = m
    sector_flows = {e: (etf.get(e) or {}).get("flow_zscore_90d") for e in SECTOR_ETFS if e in etf}
    iwm_z = (etf.get("IWM") or {}).get("flow_zscore_90d")
    smallcap_bid = iwm_z is not None and iwm_z >= 1.0
    inflow = {k: v for k, v in sector_flows.items() if v is not None}
    strongest_in = max(inflow, key=inflow.get) if inflow else None
    strongest_out = min(inflow, key=inflow.get) if inflow else None

    # ── FX + futures market signals ──
    fxd = _read("data/polygon-fx-regime.json") or {}
    futd = _read("data/polygon-futures-curves.json") or {}
    fx_signals = fxd.get("regime_signals") or fxd.get("signals") or []
    futures_signals = futd.get("signals") or []

    # ── per-ticker prepump score (gamma + flow) ──
    for sym, d in tickers.items():
        score = 0.0
        why = []
        gs = d.get("gamma_squeeze_score") or 0
        if gs:
            score += gs * 0.5
            why.append(f"gamma squeeze {gs}")
        if d.get("otm_call_sweep"):
            score += 30
            why.append("OTM call sweep")
        if d.get("bullish_flow"):
            score += 20
            why.append("bullish call flow")
        if d.get("smart_money_blocks"):
            score += 18
            why.append("smart-money blocks")
        if d.get("options_alert") == 2:
            score += 10
        d["prepump_score"] = round(score, 1)
        d["massive_why"] = "; ".join(why)

    top = sorted(
        [{"symbol": s, **d} for s, d in tickers.items() if d.get("prepump_score", 0) > 0],
        key=lambda x: x["prepump_score"], reverse=True)

    out = {
        "engine": "massive-signals", "version": VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "thesis": "Unified Massive-data layer: gamma squeeze + unusual options flow per ticker, "
                  "plus market context (gamma regime, ETF sector $ flows, small-cap bid, FX, futures).",
        "market": {
            "gamma_regime": gamma_regime,
            "smallcap_bid": smallcap_bid,
            "iwm_flow_z": iwm_z,
            "sector_flows": sector_flows,
            "strongest_inflow_sector": strongest_in,
            "strongest_outflow_sector": strongest_out,
            "fx_signals": fx_signals,
            "futures_signals": futures_signals,
            "majors_flow": majors,
        },
        "n_tickers": len(tickers),
        "top_prepump": top[:30],
        "tickers": tickers,
        "sources": ["dealer-gex (Massive options/GEX)", "polygon-options-flow (Massive options)",
                    "etf-flows (ETF Global $)", "polygon-fx-regime (Massive FX)",
                    "polygon-futures-curves (Massive futures)"],
        "caveats": "Aggregates upstream engines; freshness depends on each producer's daily run. Gamma squeeze "
                   "and unusual flow are setup conditions, not triggers. Real data, research only — not advice.",
        "elapsed_s": round(time.time() - t0, 2),
    }
    s3.put_object(Bucket=S3_BUCKET, Key=OUT_KEY, Body=json.dumps(out).encode(), ContentType="application/json")
    print(f"[massive-signals] tickers={len(tickers)} top_prepump={len(top)} "
          f"gamma_regime={gamma_regime} smallcap_bid={smallcap_bid} "
          f"fx={len(fx_signals)} futures={len(futures_signals)} {out['elapsed_s']}s")
    return {"statusCode": 200, "body": json.dumps({"ok": True, "tickers": len(tickers), "top_prepump": len(top)})}
