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
        # Valuation
        "pe_ttm":              _get(doc, "valuation", "pe_ttm"),
        "pe_5yr_avg":          _get(doc, "valuation", "pe_5yr_avg"),
        "ev_ebitda":           _get(doc, "valuation", "ev_ebitda"),
        "pb_ttm":              _get(doc, "valuation", "pb_ttm"),
        "fcf_yield_pct":       _get(doc, "valuation", "fcf_yield_pct"),
        "div_yield_pct":       _get(doc, "valuation", "div_yield_pct"),
        "roe_ttm_pct":         _get(doc, "valuation", "roe_ttm_pct"),
        "roic_ttm_pct":        _get(doc, "valuation", "roic_ttm_pct"),
        "dcf_value":           _get(doc, "valuation", "dcf_value"),
        "dcf_upside_pct":      _get(doc, "valuation", "dcf_upside_pct"),
        # Growth
        "rev_5y_cagr_pct":     _get(doc, "growth", "rev_5y_cagr_pct"),
        "rev_3y_cagr_pct":     _get(doc, "growth", "rev_3y_cagr_pct"),
        "eps_5y_cagr_pct":     _get(doc, "growth", "eps_5y_cagr_pct"),
        "fcf_5y_cagr_pct":     _get(doc, "growth", "fcf_5y_cagr_pct"),
        # Health
        "health_score":        _get(doc, "financial_health", "overall_score"),
        # Returns
        "return_1y_pct":       _get(doc, "returns", "return_1y_pct"),
        "return_3y_pct":       _get(doc, "returns", "return_3y_pct"),
        "return_5y_pct":       _get(doc, "returns", "return_5y_pct"),
        "cagr_5y_pct":         _get(doc, "returns", "cagr_5y_pct"),
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
    print(f"[snapshot] research: {len(research_rows)} rows ({research_skipped} skipped)")
    print(f"[snapshot] edgar:    {len(edgar_rows)} rows ({edgar_skipped} skipped)")

    # Sort by ticker for deterministic output
    research_rows.sort(key=lambda r: r.get("ticker") or "")
    edgar_rows.sort(key=lambda r: r.get("ticker") or "")

    meta = {
        "generated_at":     datetime.now(timezone.utc).isoformat(),
        "schema_version":   "1.0",
        "n_rows":           None,  # filled per-file
        "generation_elapsed_s": None,
    }

    # Write consolidated files
    written = []
    for name, rows, public_key in [
        ("equity_research", research_rows, "analytics/equity_research_flat.json"),
        ("edgar_insiders",  edgar_rows,    "analytics/edgar_insiders_flat.json"),
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
            {"name": "equity_research", "key": "analytics/equity_research_flat.json"},
            {"name": "edgar_insiders",  "key": "analytics/edgar_insiders_flat.json"},
        ],
        "schema_research_columns": list(flatten_research({}).keys()),
        "schema_edgar_columns":    list(flatten_edgar({}).keys()),
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
            "research_skipped": research_skipped,
            "edgar_skipped":    edgar_skipped,
            "written":         written,
            "elapsed_s":       elapsed,
        }),
    }
