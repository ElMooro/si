"""
justhodl-massive-signals — UNIFIED MASSIVE INTELLIGENCE LAYER
=============================================================
Repair (not replace) the shared synthesis layer. Honest provenance, honest
labels, per-source as-of. F14: no sweeps / smart money from aggregates.
F01: drop quarantined futures. F18: overwrite probe receipts that claimed LIVE.
"""
import json
import time
from datetime import datetime, timezone

import boto3

VERSION = "2.0.0"
S3_BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/massive-signals.json"
SECTOR_ETFS = ["XLK", "XLF", "XLE", "XLV", "XLP", "XLY", "XLI", "XLB", "XLU", "XLRE", "XLC"]
s3 = boto3.client("s3", region_name="us-east-1")


def _read(key):
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        body = json.loads(obj["Body"].read())
        return body, obj.get("LastModified")
    except Exception:
        return None, None


def _iso(dt):
    if dt is None:
        return None
    try:
        return dt.astimezone(timezone.utc).isoformat()
    except Exception:
        return str(dt)


def _put(key, obj, cache="public, max-age=300"):
    s3.put_object(
        Bucket=S3_BUCKET, Key=key,
        Body=json.dumps(obj, default=str).encode(),
        ContentType="application/json", CacheControl=cache,
    )


def lambda_handler(event, context):
    t0 = time.time()
    tickers = {}
    sources = {}

    gex, gex_lm = _read("data/dealer-gex.json")
    gex = __import__("option_population_context").context(gex)
    gamma_regime = (gex.get("market_composite") or {}).get("composite_regime")
    sources["dealer_gex"] = {
        "as_of": gex.get("source_capture_completed_at"),
        "provenance": "Retained option population research; source reference carries provider identity",
        "ok": False,  # source presence does not qualify a dealer signal
        "research_available": gex["native_reference_available"],
        "canonical": gex["canonical"],
        "reason": gex["evidence_note"],
    }
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

    pof, pof_lm = _read("data/polygon-options-flow.json")
    pof = pof or {}
    sources["options_flow"] = {
        "as_of": pof.get("generated_at") or _iso(pof_lm),
        "provenance": "Massive Options Starter — daily aggregates, no trades/quotes",
        "inference_type": pof.get("inference_type") or "aggregate_anomaly",
        "ok": bool(pof.get("all_results") or pof.get("extreme_call_flow")),
    }
    rows = pof.get("all_results") or (
        (pof.get("extreme_call_flow") or []) + (pof.get("bullish_call_flow") or [])
        + (pof.get("notable_flow") or []))
    for r in rows:
        sym = r.get("ticker") or r.get("symbol")
        if not sym:
            continue
        sigs = r.get("signals") or []
        bullish = any("BULLISH_CALL" in s or "EXTREME_CALL" in s for s in sigs)
        otm_vol = any("OTM_CALL_VOLUME" in s or "OTM_CALL_SWEEP" in s for s in sigs)
        hi_vol = any("HIGH_CONTRACT_VOLUME" in s or "SMART_MONEY" in s for s in sigs)
        tickers.setdefault(sym, {})
        tickers[sym].update({
            "call_put_ratio": r.get("cv_pv_ratio") or r.get("pc_ratio"),
            "otm_call_volume": otm_vol,
            "high_contract_volume": hi_vol,
            "bullish_flow": bullish,
            "options_alert": r.get("alert_level"),
            "options_signals": sigs[:4],
            "options_inference": "aggregate_anomaly",
        })

    of, _ = _read("flow-data.json")
    of = of or {}
    flow_rows = ((of.get("data") or {}).get("put_call") or {}).get("options_flow") or []
    majors = {f.get("ticker"): f.get("sentiment") for f in flow_rows
              if isinstance(f, dict) and f.get("ticker")}

    desk, desk_lm = _read("data/etf-desk.json")
    desk = desk or {}
    by_etf = desk.get("by_etf") or {}
    sources["etf_desk"] = {
        "as_of": desk.get("generated_at") or _iso(desk_lm),
        "provenance": "Massive ETF Global fund-flows (true $)",
        "ok": bool(by_etf),
        "n": len(by_etf),
    }
    sector_flows_usd = {}
    sector_flows_z = {}
    for e in SECTOR_ETFS:
        row = by_etf.get(e) or {}
        if row.get("flow_5d") is not None:
            sector_flows_usd[e] = row.get("flow_5d")
            sector_flows_z[e] = row.get("flow_z")
    iwm = by_etf.get("IWM") or {}
    iwm_flow = iwm.get("flow_5d")
    iwm_z = iwm.get("flow_z")
    smallcap_bid = iwm_z is not None and iwm_z >= 1.0
    strongest_in = max(sector_flows_usd, key=sector_flows_usd.get) if sector_flows_usd else None
    strongest_out = min(sector_flows_usd, key=sector_flows_usd.get) if sector_flows_usd else None

    fxd, fx_lm = _read("data/polygon-fx-regime.json")
    fxd = fxd or {}
    futd, fut_lm = _read("data/polygon-futures-curves.json")
    futd = futd or {}
    fx_signals = fxd.get("regime_signals") or fxd.get("signals") or []
    fut_ok = bool(futd.get("identity_ok"))
    futures_signals = (futd.get("signals") or []) if fut_ok else []
    sources["fx"] = {"as_of": fxd.get("generated_at") or _iso(fx_lm), "provenance": "Massive Currencies Starter", "ok": bool(fxd)}
    sources["futures"] = {
        "as_of": futd.get("generated_at") or _iso(fut_lm),
        "provenance": "Massive Futures Starter /futures/v1 dated contracts",
        "ok": fut_ok,
        "status": futd.get("status") or "UNKNOWN",
        "note": "F01: equity CL/ES/SI series are quarantined. Unused if identity_ok is false.",
    }

    for sym, d in tickers.items():
        score = 0.0
        why = []
        gs = d.get("gamma_squeeze_score") or 0
        if gs:
            score += gs * 0.5
            why.append("gamma squeeze %s (CBOE)" % gs)
        if d.get("bullish_flow"):
            score += 20
            why.append("call/put volume skew (aggregate)")
        if d.get("otm_call_volume"):
            score += 8
            why.append("far-OTM call volume (aggregate, not a sweep)")
        if d.get("high_contract_volume"):
            score += 6
            why.append("high contract volume (aggregate, not smart money)")
        if d.get("options_alert") == 2:
            score += 6
        d["prepump_score"] = round(score, 1)
        d["massive_why"] = "; ".join(why)
        d["inference_type"] = "mixed_aggregate"

    top = sorted(
        [{"symbol": s, **d} for s, d in tickers.items() if d.get("prepump_score", 0) > 0],
        key=lambda x: x["prepump_score"], reverse=True)

    now = datetime.now(timezone.utc).isoformat()
    out = {
        "engine": "massive-signals", "version": VERSION,
        "generated_at": now,
        "thesis": "Combined research inputs. Captured option populations have no qualified directional gamma signal; "
                  "other legacy model inputs retain their separate qualification limits.",
        "market": {
            "gamma_regime": gamma_regime,
            "gamma_provenance": "Unqualified dealer inference; see the canonical option population reference",
            "smallcap_bid": smallcap_bid,
            "iwm_flow_5d_usd": iwm_flow,
            "iwm_flow_z": iwm_z,
            "sector_flows_usd": sector_flows_usd,
            "sector_flows_z": sector_flows_z,
            "strongest_inflow_sector": strongest_in,
            "strongest_outflow_sector": strongest_out,
            "fx_signals": fx_signals,
            "futures_signals": futures_signals,
            "futures_identity_ok": fut_ok,
            "majors_flow": majors,
        },
        "n_tickers": len(tickers),
        "top_prepump": top[:30],
        "tickers": tickers,
        "sources": sources,
        "caveats": (
            "Dealer gamma inference is retired; retained option populations provide source evidence only. Other options features are daily-aggregate anomalies — "
            "Starter has no trades/quotes so there are no sweeps or smart-money blocks. "
            "Futures unused unless identity_ok. Benzinga Earnings is scheduled to cancel 2026-10-10."
        ),
        "elapsed_s": round(time.time() - t0, 2),
    }
    _put(OUT_KEY, out)

    capability = {
        "generated_at": now,
        "engine": "massive-signals",
        "version": VERSION,
        "products": {
            "stocks_starter": {"state": "active", "usd": 29},
            "options_starter": {"state": "active", "usd": 29, "excludes": ["trades", "quotes"]},
            "currencies_starter": {"state": "cancel_pending", "ends": "2026-10-10", "usd": 49},
            "futures_starter": {"state": "cancel_pending", "ends": "2026-10-10", "usd": 29,
                                "api": "/futures/v1", "equity_fallback": False},
            "indices_basic": {"state": "active", "usd": 0},
            "etf_global_flows": {"state": "active", "usd": 99},
            "etf_global_constituents": {"state": "active", "usd": 99},
            "etf_global_profiles": {"state": "active", "usd": 99},
            "benzinga_earnings": {"state": "cancel_pending", "ends": "2026-10-10", "usd": 99,
                                  "owner_decision": "cancel", "new_consumers": False},
            "financials_ratios": {"state": "not_entitled", "note": "Not on this plan. polygon-ratios.json is a probe."},
            "etf_taxonomies": {"state": "not_entitled"},
            "etf_analytics": {"state": "not_entitled"},
        },
        "sources": sources,
    }
    _put("data/massive-capability.json", capability)

    _put("data/polygon-ratios.json", {
        "generated_at": now,
        "schema_version": 2,
        "status": "NOT_ENTITLED",
        "entitled": False,
        "used": "https://financialmodelingprep.com/stable/ratios-ttm",
        "note": "F18: Massive Stocks Starter does not include Financials & Ratios. The live book is data/fmp-ratios.json (FMP Ultimate). Do not probe polygon financials.",
        "n_ok": 0,
        "n": 0,
        "tickers": {},
    })
    _put("data/polygon-options.json", {
        "generated_at": now,
        "schema_version": 2,
        "status": "PROBE_SAMPLE",
        "entitled": True,
        "endpoint": "/v3/reference/options/contracts",
        "underlying": "SPY",
        "n": 0,
        "note": "F18: 10-contract SPY sample is not a chain and is not LIVE coverage. Production chain is options-analytics / polygon-options-flow with completeness flags.",
        "contracts": [],
        "error": None,
    })

    print(f"[massive-signals] tickers={len(tickers)} top_prepump={len(top)} "
          f"gamma_regime={gamma_regime} fut_ok={fut_ok} {out['elapsed_s']}s")
    return {"statusCode": 200, "body": json.dumps({"ok": True, "tickers": len(tickers), "top_prepump": len(top), "fut_ok": fut_ok})}
