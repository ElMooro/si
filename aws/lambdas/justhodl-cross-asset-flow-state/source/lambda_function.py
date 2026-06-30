"""justhodl-cross-asset-flow-state — unified cross-asset flow synthesizer.

The full cross-asset picture (risk regime, asset-class rotation, dollar/FX/carry,
foreign bond flows, dark-pool accumulation/distribution) previously lived ONLY on
sector-flow.html, re-derived client-side from ~15 feeds. This emits it ONCE as
data/cross-asset-flow-state.json so downstream engines (master-allocator,
hedge-planner, morning-intelligence) consume one coherent signal instead of each
re-reading 15 files. No LLM — pure synthesis of existing feeds.
"""
import json
from datetime import datetime, timezone

import boto3

s3 = boto3.client("s3")
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/cross-asset-flow-state.json"


def rd(key):
    try:
        return json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception:
        return {}


def lambda_handler(event=None, context=None):
    rr = rd("data/risk-regime.json")
    fx = rd("data/polygon-fx-regime.json")
    ci = rd("data/capital-inflows.json")
    ger = rd("data/gold-equity-rotation.json")
    dr = rd("data/dollar-radar.json")
    etf = rd("data/etf-true-flows.json")
    dp = rd("data/dark-pool.json")

    # Asset-class rotation (ETF 5-day net creation flows by category)
    LAB = {"BROAD_EQUITY_US": "US Equities", "INTERNATIONAL": "Intl equity",
           "RATES_TREASURIES": "Treasuries", "CREDIT": "Credit (corp bonds)",
           "COMMODITIES": "Commodities", "CRYPTO": "Crypto", "GROWTH": "Growth equity",
           "DIVIDEND_VALUE": "Dividend / Value"}
    asset_rotation = []
    for c in (etf.get("category_rotation") or []):
        cat, v = c.get("category"), c.get("net_flow_5d_usd")
        if cat in LAB and v is not None:
            asset_rotation.append({"asset_class": LAB[cat], "net_flow_5d_usd": v})
    asset_rotation.sort(key=lambda x: -(x["net_flow_5d_usd"] or 0))

    gm = ger.get("current_metrics") or {}
    hard_assets = {
        "gold_20d_pct": gm.get("gld_20d_pct"), "silver_20d_pct": gm.get("slv_20d_pct"),
        "gold_miners_20d_pct": gm.get("gdx_20d_pct"), "long_bonds_20d_pct": gm.get("tlt_20d_pct"),
        "dollar_20d_pct": gm.get("uup_20d_pct"), "spy_20d_pct": gm.get("spy_20d_pct"),
        "metals_state": ger.get("state"),
    }

    roro = fx.get("fx_roro") or {}
    dollar_fx = {
        "dollar_regime": dr.get("regime"),
        "fx_roro_score": roro.get("fx_roro_score"), "fx_roro_regime": roro.get("fx_roro_regime"),
        "em_basket_5d_pct": roro.get("em_basket_5d_pct"),
        "gold_silver_ratio_chg_5d": roro.get("gold_silver_ratio_chg_5d"),
        "carry_drivers": [d.get("driver") for d in (roro.get("drivers") or [])[:4]],
    }

    foreign = {"regime": ci.get("regime"), "by_asset_class": ci.get("by_asset_class")}

    dist = dp.get("distribution") or {}
    dark = {
        "accumulation_n": dist.get("accumulation"), "distribution_n": dist.get("distribution"),
        "top_accumulation": [x.get("ticker") for x in (dp.get("top_accumulation") or [])[:8]],
        "top_distribution": [x.get("ticker") for x in (dp.get("top_distribution") or [])[:8]],
    }

    # One-line net read
    rs = rr.get("risk_regime_score")
    bits = []
    if rr.get("risk_regime"):
        bits.append(f"{rr.get('risk_regime')} ({rs})")
    if asset_rotation:
        top_in = asset_rotation[0]["asset_class"]
        top_out = asset_rotation[-1]["asset_class"]
        bits.append(f"into {top_in}, out of {top_out}")
    if dr.get("regime"):
        bits.append(f"dollar {dr.get('regime')}")
    if hard_assets.get("metals_state"):
        bits.append(f"metals {hard_assets['metals_state']}")

    out = {
        "engine": "cross-asset-flow-state", "version": "1.0",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "headline": " | ".join(bits),
        "risk_regime_score": rs, "risk_regime": rr.get("risk_regime"),
        "posture": rr.get("posture") or {},
        "asset_class_rotation": asset_rotation,
        "hard_assets_and_dollar": hard_assets,
        "dollar_fx_carry": dollar_fx,
        "foreign_flows": foreign,
        "dark_pool": dark,
        "sources": ["risk-regime", "polygon-fx-regime", "capital-inflows",
                    "gold-equity-rotation", "dollar-radar", "etf-true-flows", "dark-pool"],
    }
    s3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(out, default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=300")
    print(f"[cross-asset-flow-state] {out['headline']}")
    return {"statusCode": 200, "body": out["headline"]}
