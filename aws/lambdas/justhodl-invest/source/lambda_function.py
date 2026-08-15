"""
aws/lambdas/justhodl-invest/source/lambda_function.py
════════════════════════════════════════════════════════════════════════════
justhodl-invest v0.1.0 — "why leave SPX?" made into a pipeline.

THE QUESTION THIS ANSWERS (Khalid, verbatim doctrine):
  A stock or ETF is only worth owning over SPX if there is a specific,
  evidenced reason to expect it to beat SPX on a RISK-ADJUSTED basis. The
  reason has to start upstream of price -- in what the world is actually
  building, evidenced by trade/commodity flows -- not in a chart that has
  already re-rated.

THREE TIERS (math lives in scoring.py, taxonomy lives in causal_graph.py):
  1. CONFIRM   -- does a leading trade/commodity indicator show a REAL,
                  multi-leg-corroborated demand signal, or is it one noisy
                  print? (scoring.confirm_indicator)
  2. GATE      -- for the end-use industry that signal points to, is its
                  risk-adjusted expected return over SPX big enough to
                  justify leaving the index? Two-part gate: information
                  ratio AND minimum excess-return magnitude both have to
                  clear. (scoring.spx_opportunity_cost_gate)
  3. RANK      -- only for industries that passed the gate: which specific
                  names should beat their OWN industry ETF (higher backlog,
                  cheaper vs. peers, real contract catalysts, not a
                  peak-margin cyclical trap), and which should not be
                  stock-picked at all -- just buy the ETF.
                  (scoring.stock_composite_score, vs_industry_etf_verdict)

WHAT THIS ENGINE DOES NOT DO (by design, to avoid rebuilding what already
ships):
  - It does not fetch trade/commodity data itself. canary-grid,
    divergence-engine-v2, boom-stage, portwatch, asia-leads, freight-pulse,
    grid-queue already do that; this engine only reads their S3 outputs
    (fleet_io.read_leg_value).
  - It does not recompute SPX/sector expected return from scratch for any
    industry that maps onto one of forward-returns' 11 SPDR sectors; it
    reads compass/forward-returns' own market-implied ER + sigma (one ER
    source of truth fleet-wide). It only computes its own ER for narrow
    thematic proxies (SMH/SOXX/LIT/ITB/IYT) that forward-returns doesn't
    carry -- see get_sector_er() and the recommendation in
    INVEST_DOCTRINE.md to extend forward-returns' ASSETS map instead.
  - It does not re-derive backlog, catalysts, or "the FIVE" -- it reads
    backlog-miner, justhodl-catalyst, and justhodl-stock-buying's existing
    outputs and only adds the industry-gate + composite-ranking layer on
    top.
  - It does not write to DynamoDB justhodl-signals directly. It emits a
    grading_candidates[] block in the exact shape signal-logger already
    consumes for other engines' OUTPERFORM/UNDERPERFORM-vs-SPY predictions
    (see SYSTEM_CATALOG.md "attention_stealth ... OUTPERFORM vs SPY on
    [10,20,30]d"); wiring signal-logger to read data/invest.json is a
    small, separate, explicitly-flagged follow-up ops task, not guessed
    here against an unconfirmed DynamoDB schema.

FIELD NAMES FLAGGED "CONFIRM ON FIRST RUN": every fleet_io.read_leg_value()
source string in causal_graph.py was written from the 2026-08-15 repo audit
but NOT from a live invocation of each upstream engine. Per the fleet's own
standing discipline ("every column bind preceded by a census probe dumping
real column names and sample values before any code was written"), run
`aws/ops/pending/ops_4716_invest_probe_fields.py` through the pipeline once
against live S3 BEFORE trusting Tier 1 output, and fix any renamed field in
causal_graph.py. This engine fails safe either way -- an unresolvable field
reads as an unavailable leg, never a fabricated zero.
"""
from __future__ import annotations
import json
import logging
import sys
import traceback
from datetime import datetime, timezone

import boto3

import causal_graph
import fleet_io
import scoring

# aws/shared/impact_mapper.py -- the one fleet-wide impact-map contract.
# Bundled into this Lambda's zip by _lambda_deploy_helpers.build_zip()
# (which copies aws/shared/*.py into every lambda package automatically).
import impact_mapper

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("justhodl-invest")

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/invest.json"

# Upstream S3 keys this engine reads whole (not single-leg Leg.source reads).
# CONFIRM ON FIRST RUN against live S3 -- see module docstring.
FORWARD_RETURNS_KEY = "data/forward-returns.json"     # compass.html engine output
INDUSTRY_BOOM_KEY = "data/industry-boom.json"
BACKLOG_MINER_KEY = "data/backlog-miner.json"
BACKLOG_XBRL_KEY = "data/backlog.json"                 # RPO/deferred-revenue engine
CATALYST_KEY = "data/catalyst.json"
STOCK_BUYING_KEY = "data/stock-buying.json"
OPPORTUNITY_ENGINE_KEY = "data/opportunity-engine.json"

SPY_LABEL = "SPY"


# ── Tier 1 ──────────────────────────────────────────────────────────────

def run_tier1():
    """Returns (list_of_indicator_results, updated_history_for_save)."""
    history = fleet_io.load_history()
    today_values = {}
    results = []

    for ind in causal_graph.LEADING_INDICATORS:
        leg_results = []
        for leg in ind.legs:
            val = fleet_io.read_leg_value(leg.source)
            today_values[leg.leg_id] = val
            hist_series = fleet_io.leg_history_series(history, leg.leg_id)
            z = scoring.zscore(val, hist_series)
            leg_results.append(scoring.LegResult(
                leg_id=leg.leg_id, z=z, direction=leg.direction,
                available=(val is not None), voting=leg.voting))

        verdict = scoring.confirm_indicator(leg_results)
        results.append({
            "indicator_id": ind.indicator_id,
            "label": ind.label,
            "candidate_industries": list(ind.candidate_industries),
            **verdict,
        })

    updated_history = fleet_io.append_today(history, today_values)
    return results, updated_history


# ── Tier 2 ──────────────────────────────────────────────────────────────

def _asset_row(ticker: str):
    """forward-returns' `assets` field is a dict KEYED BY TICKER (confirmed
    live 2026-08-15: assets["SPY"]["forward_er_10y_pct"], NOT a list of
    row dicts as first assumed -- that assumption crashed the first live
    invoke with 'str' object has no attribute 'get', since iterating a
    dict yields its keys). Ticker-keyed lookup also means the SAME helper
    works for both SPDR-sector-mapped and narrow thematic proxies -- no
    name-string matching needed at all."""
    doc = fleet_io.get_json(FORWARD_RETURNS_KEY)
    if not doc:
        return None
    row = (doc.get("assets") or {}).get(ticker)
    return row if isinstance(row, dict) else None


def get_spx_er(horizon: str = "10y") -> dict:
    row = _asset_row(SPY_LABEL)
    if not row:
        return {}
    return {"er_pp": row.get(f"forward_er_{horizon}_pct"),
            "sigma_pp": (row.get("risk") or {}).get("vol_pct_annualized")}


def get_sector_er(proxy, horizon: str = "10y") -> dict:
    """Reads forward-returns' assets[ticker] row directly -- one ER source
    of truth fleet-wide, for BOTH SPDR-sector-mapped and narrow thematic
    proxies alike, since `assets` is keyed by ticker regardless of asset
    kind. INSUFFICIENT_DATA only when the ticker genuinely isn't covered
    yet (true today for SMH/SOXX/LIT/ITB/IYT -- see INVEST_DOCTRINE.md
    open items recommending forward-returns' ASSETS map be extended)."""
    row = _asset_row(proxy.proxy_etf)
    if row:
        return {"status": "OK",
                "er_pp": row.get(f"forward_er_{horizon}_pct"),
                "sigma_pp": (row.get("risk") or {}).get("vol_pct_annualized"),
                "source": "forward-returns (single source of truth)"}
    return {"status": "INSUFFICIENT_DATA",
            "reason": f"{proxy.proxy_etf} not in forward-returns' assets map. "
                      f"Recommended fix: extend forward-returns' ASSETS to "
                      f"include {proxy.proxy_etf}, not fork the ER formula "
                      f"here — see INVEST_DOCTRINE.md open items."}


def run_tier2(tier1_results):
    """For every CONFIRMED or TURNING indicator's candidate industries,
    gate expected sector-ETF return vs SPX. Returns industry_key -> gate dict."""
    spx = get_spx_er()
    gates = {}
    for r in tier1_results:
        if r["status"] not in ("CONFIRMED", "TURNING"):
            continue
        for industry_key in r["candidate_industries"]:
            if industry_key in gates:
                continue  # already gated by an earlier/stronger indicator this run
            proxy = causal_graph.get_industry(industry_key)
            if proxy is None:
                continue
            sec = get_sector_er(proxy)
            if sec.get("status") != "OK" or not spx.get("er_pp"):
                gates[industry_key] = {"status": "INSUFFICIENT_DATA",
                                        "industry": proxy.industry,
                                        "proxy_etf": proxy.proxy_etf,
                                        "reason": sec.get("reason", "missing SPX ER")}
                continue

            sigma_sec = sec.get("sigma_pp")
            sigma_spx = spx.get("sigma_pp")
            tracking_error = None
            if sigma_sec is not None and sigma_spx is not None:
                # conservative proxy for tracking error absent a joint
                # covariance series: sqrt(sigma_sector^2 - sigma_spx^2) when
                # sector vol > SPX vol, else fall back to sigma_sector itself
                diff2 = sigma_sec ** 2 - sigma_spx ** 2
                tracking_error = (diff2 ** 0.5) if diff2 > 0 else sigma_sec

            gate = scoring.spx_opportunity_cost_gate(
                sec.get("er_pp"), spx.get("er_pp"), tracking_error,
                n_obs=r.get("available_legs"),
            )
            gate["industry"] = proxy.industry
            gate["proxy_etf"] = proxy.proxy_etf
            gate["industry_boom_label"] = proxy.industry_boom_label
            gate["triggered_by"] = r["indicator_id"]
            gates[industry_key] = gate
    return gates


# ── Tier 3 ──────────────────────────────────────────────────────────────

def get_stock_universe(proxy, limit: int = 25) -> list:
    """Seed universe from industry-boom league's top_names for the mapped
    industry label, falling back to an empty list (INSUFFICIENT_DATA at the
    stock level, never an invented ticker list)."""
    doc = fleet_io.get_json(INDUSTRY_BOOM_KEY)
    if not doc or not proxy.industry_boom_label:
        return []
    for row in doc.get("league", []):
        if row.get("industry") == proxy.industry_boom_label:
            return (row.get("top_names") or [])[:limit]
    return []


def _lookup(doc, ticker, *keys):
    if not doc:
        return None
    # Confirmed live 2026-08-15: data/backlog.json and data/catalyst.json
    # both key their per-ticker rows under "by_ticker" (a dict), not
    # "rows"/"results"/"data". Kept those three as a fallback chain for
    # docs this engine hasn't smoke-tested against yet (stock-buying.json's
    # per-ticker container name is still unconfirmed as of this probe
    # round -- see INVEST_DOCTRINE.md).
    rows = doc.get("by_ticker", doc.get("rows", doc.get("results", doc.get("data", []))))
    if isinstance(rows, dict):
        row = rows.get(ticker)
    else:
        row = next((r for r in rows if r.get("ticker") == ticker or r.get("symbol") == ticker), None)
    if not row:
        return None
    for k in keys:
        if k in row and row[k] is not None:
            return row[k]
    return None


def score_stock(ticker, industry_key, backlog_doc, backlog_xbrl_doc, catalyst_doc, stock_buying_doc):
    backlog_growth = _lookup(backlog_doc, ticker, "backlog_yoy_pct", "yoy_pct") \
        or _lookup(backlog_xbrl_doc, ticker, "rpo_yoy_pct", "deferred_revenue_yoy_pct")
    catalyst_strength = _lookup(catalyst_doc, ticker, "weight", "score")
    peg = _lookup(stock_buying_doc, ticker, "peg", "peg_ratio")
    net_retirement = _lookup(stock_buying_doc, ticker, "net_buyback_yield")
    qoq_accel = _lookup(stock_buying_doc, ticker, "qoq_acceleration_pct")
    pe_pctile = _lookup(stock_buying_doc, ticker, "pe_5y_percentile")
    margin_pctile = _lookup(stock_buying_doc, ticker, "margin_percentile")

    def to_0_100(x, lo, hi, invert=False):
        if x is None:
            return None
        v = max(0.0, min(1.0, (x - lo) / (hi - lo))) * 100
        return 100 - v if invert else v

    components = {
        "backlog_growth": to_0_100(backlog_growth, -20, 40),
        "valuation_discount": to_0_100(peg, 0.5, 2.5, invert=True),
        "catalyst_strength": to_0_100(catalyst_strength, 0, 1) if catalyst_strength is not None
        and catalyst_strength <= 1 else catalyst_strength,
        "net_share_retirement": to_0_100(net_retirement, -5, 10),
        "qoq_acceleration": to_0_100(qoq_accel, -10, 20),
    }
    result = scoring.stock_composite_score(components)
    result["ticker"] = ticker
    result["cycle_flag"] = scoring.cycle_awareness_flag(None, pe_pctile, margin_pctile)
    result["raw"] = {"backlog_yoy_pct": backlog_growth, "peg": peg,
                      "net_buyback_yield": net_retirement, "qoq_acceleration_pct": qoq_accel}
    return result


def run_tier3(tier2_gates):
    backlog_doc = fleet_io.get_json(BACKLOG_MINER_KEY)
    backlog_xbrl_doc = fleet_io.get_json(BACKLOG_XBRL_KEY)
    catalyst_doc = fleet_io.get_json(CATALYST_KEY)
    stock_buying_doc = fleet_io.get_json(STOCK_BUYING_KEY)

    picks = []
    for industry_key, gate in tier2_gates.items():
        if not gate.get("pass"):
            continue
        proxy = causal_graph.get_industry(industry_key)
        universe = get_stock_universe(proxy)
        if not universe:
            picks.append({"industry": proxy.industry, "status": "INSUFFICIENT_DATA",
                          "reason": "no stock universe resolved from industry-boom "
                                     "league for this industry label — verdict is "
                                     f"BUY_THE_ETF ({proxy.proxy_etf}) by default"})
            continue

        scored = [score_stock(t, industry_key, backlog_doc, backlog_xbrl_doc,
                               catalyst_doc, stock_buying_doc) for t in universe]
        ok_scores = [s["score"] for s in scored if s["status"] == "OK"]
        median = sorted(ok_scores)[len(ok_scores) // 2] if ok_scores else None

        for s in scored:
            verdict = scoring.vs_industry_etf_verdict(s.get("score"), median)
            picks.append({
                "industry": proxy.industry, "proxy_etf": proxy.proxy_etf,
                "ticker": s["ticker"], "status": s["status"],
                "composite_score": s.get("score"),
                "industry_median_score": median,
                "vs_industry_etf": verdict,
                "cycle_flag": s.get("cycle_flag"),
                "components_used": s.get("components_used"),
                "missing_components": s.get("missing_components"),
                "raw": s.get("raw"),
                "thesis": build_thesis(gate, proxy, s, verdict),
            })
    return picks


def build_thesis(gate, proxy, stock_result, verdict) -> str:
    parts = [
        f"{gate.get('triggered_by', 'leading indicator')} confirmed demand "
        f"pointing at {proxy.industry}.",
        f"{proxy.proxy_etf} priced excess return over SPX = "
        f"{gate.get('excess_return_pp')}pp (IR {gate.get('information_ratio')}, "
        f"{gate.get('pp_kind')}, n_obs={gate.get('n_obs')}).",
    ]
    if stock_result.get("status") == "OK":
        parts.append(
            f"{stock_result['ticker']} composite {stock_result.get('score')} vs "
            f"industry median — {verdict}.")
        if stock_result.get("cycle_flag") == "PEAK_MARGIN_TRAP":
            parts.append("CAUTION: cycle-awareness flags peak-margin trap — "
                          "cheap P/E may be a trailing-earnings illusion.")
    else:
        parts.append(f"{stock_result['ticker']}: insufficient data to rank — "
                      f"treat {proxy.proxy_etf} itself as the position.")
    return " ".join(parts)


# ── grading-candidate emission (for signal-logger, NOT written directly) ─

def build_grading_candidates(tier2_gates, tier3_picks) -> list:
    out = []
    for industry_key, gate in tier2_gates.items():
        if gate.get("status") != "OK":
            continue
        out.append({
            "kind": "industry", "name": gate["industry"], "symbol": gate["proxy_etf"],
            "signal_type": "invest_industry_outperform" if gate["pass"]
            else "invest_industry_neutral",
            "direction": "OUTPERFORM_SPY" if gate["pass"] else "NEUTRAL_SPY",
            "horizons_days": [90, 180, 365],
            "as_of": datetime.now(timezone.utc).isoformat(),
        })
    for p in tier3_picks:
        if p.get("vs_industry_etf") not in ("OUTPERFORM_EXPECTED", "UNDERPERFORM_EXPECTED"):
            continue
        out.append({
            "kind": "company", "name": p["ticker"], "symbol": p["ticker"],
            "signal_type": "invest_stock_outperform_etf" if p["vs_industry_etf"] == "OUTPERFORM_EXPECTED"
            else "invest_stock_underperform_etf",
            "direction": p["vs_industry_etf"], "benchmark": p.get("proxy_etf"),
            "horizons_days": [30, 60, 90],
            "as_of": datetime.now(timezone.utc).isoformat(),
        })
    return out


# ── handler ──────────────────────────────────────────────────────────────

def lambda_handler(event, context):
    t0 = datetime.now(timezone.utc)
    try:
        tier1_results, updated_history = run_tier1()
        fleet_io.save_history(updated_history)

        tier2_gates = run_tier2(tier1_results)
        tier3_picks = run_tier3(tier2_gates)
        grading_candidates = build_grading_candidates(tier2_gates, tier3_picks)

        out = {
            "schema": "invest/0.1",
            "engine": "justhodl-invest",
            "generated_at": t0.isoformat(),
            "gate_params": {"ir_min": scoring.IR_MIN_DEFAULT,
                             "min_excess_return_pp": scoring.MIN_EXCESS_RETURN_PP_DEFAULT},
            "leading_indicators": tier1_results,
            "industry_gates": tier2_gates,
            "stock_picks": tier3_picks,
            "grading_candidates": grading_candidates,
            "method_notes": (
                "Tier 1 requires >=2 independently-sourced legs with real S3 data "
                "and >=60% voting the same direction to CONFIRM; a single leg is "
                "TURNING at most. Tier 2 requires BOTH information_ratio >= "
                f"{scoring.IR_MIN_DEFAULT} AND excess_return_pp >= "
                f"{scoring.MIN_EXCESS_RETURN_PP_DEFAULT} to pass — SPX is the "
                "explicit default; an industry must earn its way out. Tier 3 only "
                "runs for industries that passed Tier 2, and a stock must beat its "
                "OWN industry median by >=8pp composite to be picked over just "
                "buying the proxy ETF."
            ),
            "elapsed_s": round((datetime.now(timezone.utc) - t0).total_seconds(), 2),
        }
        fleet_io.put_json(OUT_KEY, out)

        n_confirmed = sum(1 for r in tier1_results if r["status"] == "CONFIRMED")
        n_pass = sum(1 for g in tier2_gates.values() if g.get("pass"))
        log.info("[invest] indicators=%d confirmed=%d gates_pass=%d/%d picks=%d %.2fs",
                  len(tier1_results), n_confirmed, n_pass, len(tier2_gates),
                  len(tier3_picks), out["elapsed_s"])
        return {"statusCode": 200, "body": json.dumps({
            "ok": True, "confirmed": n_confirmed, "gates_pass": n_pass,
            "picks": len(tier3_picks),
        })}
    except Exception:
        log.error("justhodl-invest FAILED:\n%s", traceback.format_exc())
        return {"statusCode": 500, "body": json.dumps({"ok": False,
                "error": traceback.format_exc()[-2000:]})}


if __name__ == "__main__":
    print(json.dumps(lambda_handler({}, None), indent=2))
