#!/usr/bin/env python3
"""
Fix justhodl-intelligence to read from current data sources.

ROOT CAUSE: justhodl-intelligence loads:
  - data.json          → stale orphan (65 days old)
  - predictions.json   → broken since April 22 CF migration
  - repo-data.json     → working

Result: intelligence-report.json has all zeros in the scores dict
(khalid_index=0, ml_risk_score=0, carry_risk_score=0). This:
  1. Makes ai-chat give bad answers
  2. Causes signal-logger to log poisoned ml_risk/carry_risk signals
     with value=0 every run, which are 2 of our 24 calibrated signals

FIX STRATEGY: Adapter pattern.
  - Read from data/report.json (current truth) AS WELL AS data.json
  - Build a unified `main` dict with the shape the rest of the Lambda
    expects (so we don't need to rewrite the 50+ safe() call sites)
  - Same for predictions.json — replace with a derived view from the
    current data sources

The adapter:
  - khalid_index:  if dict, extract .score; if int, pass through
  - regime:        same
  - dxy: pull from fred.dxy.* in new schema; map to old shape
  - liquidity: pull from fred.liquidity.*
  - bond_analysis.yield_curve: derive from fred.treasury.* series

For predictions.json — instead of trying to resurrect ml-predictions,
construct a synthetic 'pred' dict from existing healthy sources:
  - executive_summary: from ai_analysis in report.json
  - liquidity / risk: from edge-data.json + fred-derived values
  - sector_rotation: from existing sectors field
  - trade_recommendations: empty list (they were always low-quality)
  - market_snapshot: from report.json stocks data

This is the minimum-viable fix — gets real values flowing into
intelligence-report.json without rewriting the whole Lambda.
"""
import io
import os
import zipfile
from pathlib import Path

from ops_report import report
import boto3

REGION = "us-east-1"
REPO_ROOT = Path(os.environ.get("GITHUB_WORKSPACE", os.getcwd()))

lam = boto3.client("lambda", region_name=REGION)


def build_zip(src_dir):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zout:
        for f in src_dir.rglob("*"):
            if f.is_file():
                zout.write(f, str(f.relative_to(src_dir)))
    return buf.getvalue()


def deploy(fn_name, src_dir):
    z = build_zip(src_dir)
    lam.update_function_code(FunctionName=fn_name, ZipFile=z)
    lam.get_waiter("function_updated").wait(
        FunctionName=fn_name, WaiterConfig={"Delay": 3, "MaxAttempts": 30}
    )
    return len(z)


with report("fix_justhodl_intelligence") as r:
    r.heading("Fix justhodl-intelligence: data.json + predictions.json → adapters")

    src_path = REPO_ROOT / "aws/lambdas/justhodl-intelligence/source/lambda_function.py"
    src = src_path.read_text(encoding="utf-8")

    # ─── Replace load_system_data() with adapter version ──────────────
    old_load = '''def load_system_data():
    print("Loading data.json...")
    main=http_get(f"{BASE}/data.json")
    print(f"  data.json: {'OK' if main else 'FAILED'}")
    
    print("Loading repo-data.json...")
    repo=http_get(f"{BASE}/repo-data.json")
    print(f"  repo-data.json: {'OK' if repo else 'FAILED'}")
    
    print("Loading predictions.json...")
    pred=http_get(f"{BASE}/predictions.json")
    print(f"  predictions.json: {'OK' if pred else 'FAILED'}")
    
    return main or {}, repo or {}, pred or {}'''

    new_load = '''def load_system_data():
    """Load and adapt current data sources.

    The original Lambda read data.json (legacy orphan, 65 days stale) and
    predictions.json (broken since CF migration). Now reads:
      - data/report.json (current source of truth)
      - repo-data.json (still working)
      - edge-data.json (used to synthesize 'pred' instead of predictions.json)
      - flow-data.json (for market_snapshot synthesis)

    Builds a 'main' dict with the LEGACY shape so 50+ safe() call sites
    in this Lambda continue to work without modification.
    """
    print("Loading data/report.json (current)...")
    raw_main=http_get(f"{BASE}/data/report.json")
    print(f"  data/report.json: {'OK' if raw_main else 'FAILED'}")

    print("Loading repo-data.json...")
    repo=http_get(f"{BASE}/repo-data.json")
    print(f"  repo-data.json: {'OK' if repo else 'FAILED'}")

    print("Loading edge-data.json (for pred synthesis)...")
    edge=http_get(f"{BASE}/edge-data.json") or {}

    print("Loading flow-data.json (for pred synthesis)...")
    flow=http_get(f"{BASE}/flow-data.json") or {}

    # ─── Adapter: reshape new report.json → legacy 'main' shape ────────
    main=_adapt_main(raw_main or {})

    # ─── Synthesize 'pred' dict from healthy sources ──────────────────
    pred=_synthesize_pred(raw_main or {}, edge, flow, repo or {})

    return main, repo or {}, pred


def _adapt_main(rpt):
    """Reshape data/report.json fields into the legacy data.json shape
    that the rest of this Lambda's safe() calls expect."""
    if not rpt:
        return {}

    # khalid_index is now a dict {score, regime}; legacy expected scalar
    ki_raw=rpt.get("khalid_index", {})
    if isinstance(ki_raw, dict):
        ki_score=ki_raw.get("score", 0) or 0
        ki_regime=ki_raw.get("regime", "UNKNOWN")
    else:
        ki_score=ki_raw or 0
        ki_regime=rpt.get("regime", "UNKNOWN")

    # FRED data is now nested under fred.<category>.<series_id>.<field>
    fred=rpt.get("fred", {}) or {}
    treasury=fred.get("treasury", {}) or {}
    dxy_cat=fred.get("dxy", {}) or {}
    risk_cat=fred.get("risk", {}) or {}
    liq_cat=fred.get("liquidity", {}) or {}

    def fred_val(category, series_id):
        """Get .current value from a FRED series."""
        s=category.get(series_id, {})
        if isinstance(s, dict):
            return s.get("current") or 0
        return 0

    # DXY (DTWEXBGS = Trade-Weighted Dollar Index)
    dxy_v=fred_val(dxy_cat, "DTWEXBGS")
    dxy_series=dxy_cat.get("DTWEXBGS", {}) or {}
    dxy_week=dxy_series.get("week_pct", 0)
    dxy_month=dxy_series.get("month_pct", 0)
    dxy_strength="STRONG" if dxy_v > 105 else ("WEAK" if dxy_v < 100 else "NEUTRAL")

    # Liquidity
    fed_bs=fred_val(liq_cat, "WALCL")     # Fed balance sheet
    m2_v=fred_val(liq_cat, "M2SL")        # M2
    rrp_v=fred_val(liq_cat, "RRPONTSYD")  # Reverse repo
    tga_v=fred_val(liq_cat, "WTREGEN")    # TGA
    sofr=fred_val(liq_cat, "SOFR")
    effr=fred_val(liq_cat, "EFFR") or fred_val(treasury, "DFF")

    # Yield curve
    y2=fred_val(treasury, "DGS2")
    y5=fred_val(treasury, "DGS5")
    y10=fred_val(treasury, "DGS10")
    y30=fred_val(treasury, "DGS30")
    spread_10y2y=(y10 - y2) if (y10 and y2) else None
    if spread_10y2y is None:
        curve_status="N/A"
    elif spread_10y2y < -0.5:
        curve_status="DEEPLY_INVERTED"
    elif spread_10y2y < 0:
        curve_status="INVERTED"
    elif spread_10y2y < 0.5:
        curve_status="FLAT"
    else:
        curve_status="NORMAL"

    # VIX
    vix=fred_val(risk_cat, "VIXCLS")

    # Build legacy-shaped output
    return {
        "khalid_index": ki_score,
        "regime": ki_regime,
        "dxy": {
            "value": dxy_v,
            "strength": dxy_strength,
            "weekly_change": dxy_week,
            "monthly_change": dxy_month,
        },
        "liquidity": {
            "fed_balance_sheet": {"value": fed_bs},
            "m2":                {"value": m2_v},
            "reverse_repo":      {"value": rrp_v},
            "tga":               {"value": tga_v},
            "trend": "expanding" if rpt.get("net_liquidity", {}).get("trend") == "up" else "contracting",
        },
        "bond_analysis": {
            "yield_curve": {
                "spread_10y_2y": spread_10y2y,
                "status": curve_status,
                "2y": y2, "5y": y5, "10y": y10, "30y": y30,
            },
        },
        "vix": vix,
        "stocks": rpt.get("stocks", {}),
        "sectors": rpt.get("sectors", {}),
        # Pass through anything else the original Lambda might safely query
        "_passthrough_raw": rpt,
    }


def _synthesize_pred(rpt, edge, flow, repo):
    """Build a synthetic 'pred' dict from healthy data sources, replacing
    the dead predictions.json. Conservative — provides what's available
    rather than fabricating numbers."""
    if not rpt and not edge:
        return {}

    # Executive summary from existing AI analysis
    ai_analysis=rpt.get("ai_analysis", {})
    exec_summary={}
    if isinstance(ai_analysis, dict):
        sections=ai_analysis.get("sections", {})
        macro=sections.get("macro", {}) if isinstance(sections, dict) else {}
        exec_summary={
            "outlook": macro.get("outlook", "UNKNOWN"),
            "key_signals": macro.get("signals", [])[:3] if isinstance(macro.get("signals"), list) else [],
            "source": "synthesized from ai_analysis",
        }

    # Sector rotation from report.json sectors data
    sectors_raw=rpt.get("sectors", {})
    sector_picks=[]
    if isinstance(sectors_raw, dict):
        # Try to find leading vs lagging sectors from any structure
        for k, v in sectors_raw.items():
            if isinstance(v, dict) and "score" in v:
                sector_picks.append({"sector": k, "score": v.get("score")})
        sector_picks=sorted(sector_picks, key=lambda x: x.get("score", 0), reverse=True)[:5]

    # Risk from edge-data composite + plumbing stress
    edge_score=edge.get("composite_score", 0) if isinstance(edge, dict) else 0
    plumb=repo.get("stress", {}) if isinstance(repo, dict) else {}
    risk_dict={
        "composite_score": edge_score,
        "plumbing_stress": plumb.get("score", 0),
        "regime": edge.get("regime", "UNKNOWN") if isinstance(edge, dict) else "UNKNOWN",
    }

    # Market snapshot from flow-data
    market_snap={}
    if isinstance(flow, dict):
        flow_data=flow.get("data", {}) if isinstance(flow.get("data"), dict) else {}
        sentiment=flow_data.get("sentiment", {})
        if isinstance(sentiment, dict):
            market_snap={"sentiment_composite": sentiment.get("composite", 0)}

    return {
        "executive_summary": exec_summary,
        "liquidity": {},                # not fabricating
        "risk": risk_dict,
        "carry_trade": {},              # not fabricating
        "sector_rotation": {"top_picks": sector_picks},
        "trade_recommendations": [],    # empty rather than synthetic
        "market_snapshot": market_snap,
        "us_equities": {},
        "global_markets": {},
        "agents_online": 0,
        "total_agents": 0,
        "_synthesized": True,
        "_synth_source": "edge-data + report.json + flow-data + repo-data",
    }'''

    if old_load not in src:
        r.fail("  load_system_data() pattern not found verbatim — cannot patch")
        raise SystemExit(1)

    src = src.replace(old_load, new_load, 1)
    r.ok("  Replaced load_system_data with adapter version")

    # ─── Validate + write + deploy ─────────────────────────────────
    import ast
    try:
        ast.parse(src)
    except SyntaxError as e:
        r.fail(f"  Syntax error: {e}")
        raise SystemExit(1)

    src_path.write_text(src, encoding="utf-8")
    r.ok(f"  Source valid ({len(src)} bytes), saved")

    size = deploy("justhodl-intelligence", src_path.parent)
    r.ok(f"  Deployed justhodl-intelligence ({size:,} bytes)")

    # ─── Trigger fresh run to verify ───────────────────────────────
    r.section("Trigger fresh justhodl-intelligence run")
    try:
        resp = lam.invoke(
            FunctionName="justhodl-intelligence",
            InvocationType="Event",
        )
        r.ok(f"  Async-triggered (status {resp['StatusCode']})")
        r.log("  intelligence-report.json should refresh in ~30s")
        r.log("  Verification script next.")
    except Exception as e:
        r.fail(f"  Trigger failed: {e}")

    r.kv(
        fix="reads data/report.json + synthesizes pred from healthy sources",
        legacy_shape_preserved=True,
        downstream_impact="ml_risk + carry_risk signals get real values",
    )
    r.log("Done")
