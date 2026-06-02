"""justhodl-analytics-snapshot

Consolidates per-ticker JSON files (equity-research/*.json + edgar-insiders/*.json)
into two flat denormalized files optimized for DuckDB-WASM consumption:

  analytics/equity_research_flat.json  — {meta, rows: [{ticker, rating, pe_ttm, ...}, ...]}
  analytics/edgar_insiders_flat.json   — {meta, rows: [{ticker, signal_label, n_buys, ...}, ...]}

These are what the /analytics.html SQL workbench queries.

Why a separate snapshot Lambda (vs. having the prewarm do it):
  - Decoupled: analytics rebuild doesn't require a full prewarm run
  - Can be re-triggered on-demand if schema changes
  - Captures EVERYTHING in S3, including on-demand-queried tickers that
    weren't in the nightly prewarm universe
  - Cheap to run (no Claude calls, just S3 IO + flatten)

Schedule: cron(0 9 * * ? *) = 09:00 UTC = 04:00 ET — one hour after the
prewarm finishes at 03:00 ET so the snapshot picks up fresh data.
"""
import json
import os
import time
from datetime import datetime, timezone
import boto3

S3_BUCKET = "justhodl-dashboard-live"
RESEARCH_PREFIX = "equity-research/"
EDGAR_PREFIX = "edgar-insiders/"
CRITIQUE_PREFIX = "equity-critique/"
OUTPUT_PREFIX = "analytics/"

s3 = boto3.client("s3", region_name="us-east-1")


# ═══════════════════════════════════════════════════════════════════
# Flatteners — convert nested JSON to flat row dicts
# ═══════════════════════════════════════════════════════════════════
def _get(obj, *path, default=None):
    """Safe nested get."""
    cur = obj
    for p in path:
        if not isinstance(cur, dict):
            return default
        cur = cur.get(p)
        if cur is None:
            return default
    return cur


def flatten_research(doc: dict) -> dict:
    """Flatten equity-research JSON to a SQL-friendly row.

    Excludes long-text fields (executive_summary, thesis_paragraph, etc.) —
    those aren't useful for analytical filtering. We keep all the
    quantitative fields a PM would slice by.

    Field names match the actual research Lambda output (see compute_growth,
    compute_returns, build_valuation in the research Lambda source).
    """
    return {
        "ticker":              doc.get("ticker"),
        "company_name":        _get(doc, "company", "name"),
        "sector":              _get(doc, "company", "sector"),
        "industry":            _get(doc, "company", "industry"),
        "market_cap":          _get(doc, "company", "market_cap"),
        # Quote
        "price":               _get(doc, "quote", "price"),
        "change_pct":          _get(doc, "quote", "change_pct"),
        # Verdict
        "rating":              _get(doc, "verdict", "rating"),
        "conviction_grade":    _get(doc, "verdict", "conviction_grade"),
        "price_target_12m":    _get(doc, "verdict", "price_target_12m"),
        "upside_pct":          _get(doc, "verdict", "upside_pct"),
        "confidence_pct":      _get(doc, "verdict", "confidence_pct"),
        "position_size_pct":   _get(doc, "verdict", "position_size_pct"),
        "time_horizon_months": _get(doc, "verdict", "time_horizon_months"),
        # Scenarios
        "bull_target":         _get(doc, "scenarios", "bull_case", "price_target_12m"),
        "bull_prob":           _get(doc, "scenarios", "bull_case", "probability_pct"),
        "base_target":         _get(doc, "scenarios", "base_case", "price_target_12m"),
        "base_prob":           _get(doc, "scenarios", "base_case", "probability_pct"),
        "bear_target":         _get(doc, "scenarios", "bear_case", "price_target_12m"),
        "bear_prob":           _get(doc, "scenarios", "bear_case", "probability_pct"),
        "ev_upside_pct":       _get(doc, "scenarios", "expected_value_upside_pct"),
        "ev_12m":              _get(doc, "scenarios", "expected_value_12m"),
        "risk_reward_ratio":   _get(doc, "scenarios", "risk_reward_ratio"),
        # Valuation (correct keys per build_valuation)
        "pe_ttm":              _get(doc, "valuation", "pe_ttm"),
        "pe_5yr_avg":          _get(doc, "valuation", "pe_5yr_avg"),
        "pb_ttm":              _get(doc, "valuation", "pb_ttm"),
        "ps_ttm":              _get(doc, "valuation", "ps_ttm"),
        "pfcf_ttm":            _get(doc, "valuation", "pfcf_ttm"),
        "ev_ebitda":           _get(doc, "valuation", "ev_ebitda"),
        "peg_ratio":           _get(doc, "valuation", "peg_ratio"),
        "fcf_yield_pct":       _get(doc, "valuation", "fcf_yield_pct"),
        "div_yield_pct":       _get(doc, "valuation", "div_yield_pct"),
        "roe_ttm_pct":         _get(doc, "valuation", "roe_ttm_pct"),
        "roic_ttm_pct":        _get(doc, "valuation", "roic_ttm_pct"),
        "dcf_estimate":        _get(doc, "valuation", "dcf_estimate"),
        "dcf_upside_pct":      _get(doc, "valuation", "dcf_upside_pct"),
        "analyst_pt_median":   _get(doc, "valuation", "analyst_pt_median"),
        "analyst_pt_upside_pct": _get(doc, "valuation", "analyst_pt_upside_pct"),
        # Growth (correct keys — revenue_NYR_cagr, eps_NYR_cagr, fcf_NYR_cagr)
        "revenue_3yr_cagr":    _get(doc, "growth", "revenue_3yr_cagr"),
        "revenue_5yr_cagr":    _get(doc, "growth", "revenue_5yr_cagr"),
        "revenue_10yr_cagr":   _get(doc, "growth", "revenue_10yr_cagr"),
        "eps_3yr_cagr":        _get(doc, "growth", "eps_3yr_cagr"),
        "eps_5yr_cagr":        _get(doc, "growth", "eps_5yr_cagr"),
        "eps_10yr_cagr":       _get(doc, "growth", "eps_10yr_cagr"),
        "fcf_5yr_cagr":        _get(doc, "growth", "fcf_5yr_cagr"),
        "ni_5yr_cagr":         _get(doc, "growth", "ni_5yr_cagr"),
        # Health
        "health_score":        _get(doc, "financial_health", "overall_score"),
        # Returns (correct keys per compute_returns)
        "ytd_pct":             _get(doc, "returns", "ytd_pct"),
        "return_1yr_pct":      _get(doc, "returns", "1yr_pct"),
        "cagr_3yr_pct":        _get(doc, "returns", "3yr_cagr_pct"),
        "cagr_5yr_pct":        _get(doc, "returns", "5yr_cagr_pct"),
        "cagr_10yr_pct":       _get(doc, "returns", "10yr_cagr_pct"),
        "max_drawdown_pct":    _get(doc, "returns", "max_drawdown_pct"),
        # Earnings track record
        "eps_beat_rate_pct":   _get(doc, "earnings_track_record", "eps_beat_rate_pct"),
        "eps_current_streak":  _get(doc, "earnings_track_record", "eps_current_streak"),
        "eps_magnitude_trend": _get(doc, "earnings_track_record", "eps_magnitude_trend"),
        # Capital allocation
        "shareholder_yield_pct":  _get(doc, "capital_allocation", "shareholder_yield_pct"),
        "total_returned_10y":     _get(doc, "capital_allocation", "total_returned_10y"),
        "buyback_share_pct":      _get(doc, "capital_allocation", "buyback_share_of_return_pct"),
        # Earnings call sentiment
        "ec_tone":             _get(doc, "earnings_call_sentiment", "overall_tone"),
        "ec_guidance_change":  _get(doc, "earnings_call_sentiment", "guidance_change"),
        # Metadata
        "generated_at":        doc.get("generated_at"),
    }


def flatten_edgar(doc: dict) -> dict:
    """Flatten edgar-insiders JSON to a flat row."""
    return {
        "ticker":              doc.get("ticker"),
        "cik":                 doc.get("cik"),
        "n_filings_90d":       doc.get("n_filings_90d"),
        "n_buys":              doc.get("n_buys"),
        "n_sells":             doc.get("n_sells"),
        "total_dollars_buy":   doc.get("total_dollars_buy"),
        "total_dollars_sell":  doc.get("total_dollars_sell"),
        "net_dollars_90d":     doc.get("net_dollars_90d"),
        "net_shares_90d":      doc.get("net_shares_90d"),
        "prior_n_buys":        doc.get("prior_n_buys"),
        "prior_n_sells":       doc.get("prior_n_sells"),
        "prior_dollars_buy":   doc.get("prior_dollars_buy"),
        "prior_dollars_sell":  doc.get("prior_dollars_sell"),
        "sell_acceleration":   doc.get("sell_acceleration"),
        "buy_acceleration":    doc.get("buy_acceleration"),
        "n_csuite_sellers":    doc.get("n_csuite_sellers"),
        "signal_label":        doc.get("signal_label"),
        "signal_score":        doc.get("signal_score"),
        "signal_note":         doc.get("signal_note"),
        "cluster_detected":    doc.get("cluster_detected"),
        "lookback_days":       doc.get("lookback_days"),
        "generated_at":        doc.get("generated_at"),
    }


def flatten_critique(doc: dict) -> dict:
    """Flatten equity-critique JSON to a SQL-friendly row.

    The critique structure is {ticker, analyst_verdict, critique, critic, generated_at}.
    We extract the headline fields for analytical filtering — finding tickers where
    the critic disagrees most with the analyst.
    """
    c = doc.get("critique") or {}
    a = doc.get("analyst_verdict") or {}
    critic = doc.get("critic") or {}
    return {
        "ticker":                 doc.get("ticker"),
        "analyst_rating":         a.get("rating"),
        "analyst_pt":             a.get("price_target_12m"),
        "analyst_conviction":     a.get("conviction_grade"),
        "alternative_rating":     c.get("alternative_rating"),
        "alternative_pt":         c.get("alternative_pt"),
        "disagreement_score":     c.get("disagreement_score"),
        "rating_diverges":        bool(c.get("alternative_rating") and a.get("rating") and c.get("alternative_rating") != a.get("rating")),
        "pt_spread_pct":          (
            round(abs((c.get("alternative_pt") / a.get("price_target_12m")) - 1) * 100, 1)
            if (c.get("alternative_pt") and a.get("price_target_12m") and a.get("price_target_12m") > 0)
            else None
        ),
        "key_disagreement":       c.get("key_disagreement_1liner"),
        "anti_thesis_summary":    (c.get("anti_thesis") or "")[:300],
        "n_data_reinterpretations": len(c.get("data_reinterpretations") or []),
        "n_underweighted_risks":  len(c.get("underweighted_risks") or []),
        "n_bear_strengtheners":   len(c.get("bear_case_strengtheners") or []),
        "critic_model":           critic.get("model"),
        "critic_provider":        critic.get("provider"),
        "critic_cost_usd":        critic.get("cost_usd"),
        "generated_at":           doc.get("generated_at"),
    }


# ═══════════════════════════════════════════════════════════════════
# S3 readers
# ═══════════════════════════════════════════════════════════════════
def read_all_under_prefix(prefix: str, flattener) -> list:
    """List + read every JSON file under prefix, flatten each, return list of rows."""
    rows = []
    skipped = 0
    pag = s3.get_paginator("list_objects_v2")
    for page in pag.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        for obj in (page.get("Contents") or []):
            key = obj["Key"]
            # Skip non-JSON or manifests
            if not key.endswith(".json"):
                continue
            if key.endswith("manifest.json") or key.endswith("latest.json"):
                continue
            try:
                body = s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read()
                doc = json.loads(body)
                row = flattener(doc)
                if row.get("ticker"):
                    rows.append(row)
            except Exception as e:
                print(f"[skip] {key}: {type(e).__name__}: {str(e)[:120]}")
                skipped += 1
    return rows, skipped


# ═══════════════════════════════════════════════════════════════════
# Handler
# ═══════════════════════════════════════════════════════════════════
def lambda_handler(event, context):
    t0 = time.time()
    print(f"[snapshot] starting at {datetime.now(timezone.utc).isoformat()}")

    # Read + flatten
    research_rows, research_skipped = read_all_under_prefix(RESEARCH_PREFIX, flatten_research)
    edgar_rows,    edgar_skipped    = read_all_under_prefix(EDGAR_PREFIX,    flatten_edgar)
    critique_rows, critique_skipped = read_all_under_prefix(CRITIQUE_PREFIX, flatten_critique)
    print(f"[snapshot] research: {len(research_rows)} rows ({research_skipped} skipped)")
    print(f"[snapshot] edgar:    {len(edgar_rows)} rows ({edgar_skipped} skipped)")
    print(f"[snapshot] critique: {len(critique_rows)} rows ({critique_skipped} skipped)")

    # Sort by ticker for deterministic output
    research_rows.sort(key=lambda r: r.get("ticker") or "")
    edgar_rows.sort(key=lambda r: r.get("ticker") or "")
    critique_rows.sort(key=lambda r: r.get("ticker") or "")

    meta = {
        "generated_at":     datetime.now(timezone.utc).isoformat(),
        "schema_version":   "1.1",  # bumped to add critique table
        "n_rows":           None,
        "generation_elapsed_s": None,
    }

    # Write consolidated files
    written = []
    for name, rows, public_key in [
        ("equity_research", research_rows, "analytics/equity_research_flat.json"),
        ("edgar_insiders",  edgar_rows,    "analytics/edgar_insiders_flat.json"),
        ("research_critique", critique_rows, "analytics/research_critique_flat.json"),
    ]:
        out = {
            **meta,
            "table_name":           name,
            "n_rows":               len(rows),
            "generation_elapsed_s": round(time.time() - t0, 1),
            "rows":                 rows,
        }
        body = json.dumps(out, default=str).encode()
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=public_key,
            Body=body,
            ContentType="application/json",
            CacheControl="public, max-age=300",
        )
        size_kb = round(len(body) / 1024, 1)
        print(f"[snapshot] wrote {public_key} ({size_kb}KB · {len(rows)} rows)")
        written.append({"key": public_key, "n_rows": len(rows), "size_kb": size_kb})

    # Also write a manifest pointer
    manifest = {
        "snapshot_generated_at": meta["generated_at"],
        "tables": [
            {"name": "equity_research",   "key": "analytics/equity_research_flat.json"},
            {"name": "edgar_insiders",    "key": "analytics/edgar_insiders_flat.json"},
            {"name": "research_critique", "key": "analytics/research_critique_flat.json"},
        ],
        "schema_research_columns": list(flatten_research({}).keys()),
        "schema_edgar_columns":    list(flatten_edgar({}).keys()),
        "schema_critique_columns": list(flatten_critique({}).keys()),
    }
    s3.put_object(
        Bucket=S3_BUCKET,
        Key="analytics/manifest.json",
        Body=json.dumps(manifest, default=str, indent=2).encode(),
        ContentType="application/json",
        CacheControl="public, max-age=60",
    )

    elapsed = round(time.time() - t0, 1)
    print(f"[snapshot] DONE in {elapsed}s")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "n_research_rows": len(research_rows),
            "n_edgar_rows":    len(edgar_rows),
            "n_critique_rows": len(critique_rows),
            "research_skipped": research_skipped,
            "edgar_skipped":    edgar_skipped,
            "critique_skipped": critique_skipped,
            "written":         written,
            "elapsed_s":       elapsed,
        }),
    }
