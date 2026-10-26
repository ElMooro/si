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

v2.0.0 (2026-10-26): tables are the top level of their prefix and reads are
incremental by ETag with a persisted state (see the S3 readers section).
"""
VERSION = "2.0.0"
import gzip
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from botocore.config import Config as BotoConfig
import os
import time
from datetime import datetime, timezone
import boto3

S3_BUCKET = "justhodl-dashboard-live"
RESEARCH_PREFIX = "equity-research/"
EDGAR_PREFIX = "edgar-insiders/"
CRITIQUE_PREFIX = "equity-critique/"
ETF_FLOWS_KEY = "etf-flows/daily.json"
OUTPUT_PREFIX = "analytics/"

s3 = boto3.client("s3", region_name="us-east-1", config=BotoConfig(max_pool_connections=32))


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
# S3 readers — incremental (v2.0.0, 2026-10-26)
# ═══════════════════════════════════════════════════════════════════
# ops 6674 sized the three prefixes: equity-research/ holds 35,572 objects / 11.9 GB, of which
# equity-research/industry2/ (the desk's 10-year peer-price cache, ~600 KB each, not research reports) is
# 15,566 objects / 9.4 GB.  Reading everything every night (16 workers, 600 s) timed out 3/3 on 10-26 at
# 282 MB.  Two changes:
#   1. A table is the TOP LEVEL of its prefix (Delimiter="/"): sub-folders are other document families.
#   2. Reads are incremental: analytics/_state/<table>.json.gz keeps {key: {etag, row}} from the previous
#      run; a listed object whose ETag is unchanged is not read again, a key that disappeared is dropped,
#      and only new / changed objects are fetched - under a deadline (the function's remaining time minus
#      a reserve for writing).  A changed object not reached before the deadline keeps its previous row
#      (real data, carrying its own generated_at) and is counted in `pending`; the flat files and the
#      manifest say `complete: false` with the count, and the next run continues.  Nothing stale is
#      presented as current: every row carries generated_at, and the meta carries the pending count.
STATE_PREFIX = "analytics/_state/"
WORKERS = 16
RESERVE_S = 45           # kept free at the end of the run for writing the tables + manifest
DEFAULT_BUDGET_S = 540   # when no context is given (local runs)
SCHEMA_VERSION = "1.2"   # 1.1 + meta.complete / meta.pending / meta.n_errors


def _read_one(key, flattener):
    try:
        body = s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read()
        row = flattener(json.loads(body))
        return (row if row.get("ticker") else None), None
    except Exception as e:
        return None, f"[skip] {key}: {type(e).__name__}: {str(e)[:120]}"


def list_prefix(prefix: str) -> dict:
    """{key: etag} for the engine-readable objects at the TOP LEVEL of prefix (sub-folders excluded)."""
    out = {}
    pag = s3.get_paginator("list_objects_v2")
    for page in pag.paginate(Bucket=S3_BUCKET, Prefix=prefix, Delimiter="/"):
        for obj in (page.get("Contents") or []):
            key = obj["Key"]
            if not key.endswith(".json") or key.endswith("manifest.json") or key.endswith("latest.json"):
                continue
            out[key] = (obj.get("ETag") or "").strip('"')
    return out


def _state_key(table: str) -> str:
    return f"{STATE_PREFIX}{table}.json.gz"


def load_state(table: str) -> dict:
    """Previous run's {key: {"etag", "row" | None, "error"?}}; {} when absent or unreadable."""
    try:
        raw = s3.get_object(Bucket=S3_BUCKET, Key=_state_key(table))["Body"].read()
        doc = json.loads(gzip.decompress(raw))
        rows = doc.get("rows") if isinstance(doc, dict) else None
        return rows if isinstance(rows, dict) else {}
    except Exception as e:
        print(f"[snapshot] state {table}: none ({type(e).__name__})")
        return {}


def save_state(table: str, rows: dict, generated_at: str):
    body = gzip.compress(json.dumps({"schema_version": SCHEMA_VERSION, "table": table, "updated_at": generated_at,
                                     "n_keys": len(rows), "rows": rows}, default=str).encode())
    s3.put_object(Bucket=S3_BUCKET, Key=_state_key(table), Body=body, ContentType="application/json",
                  ContentEncoding="gzip", CacheControl="no-cache")


def read_table(table: str, prefix: str, flattener, deadline: float) -> dict:
    """Incremental read of one table.  Returns {"rows": [...], "listed", "reused", "read", "errors", "pending",
    "dropped", "state": {key: {...}}}.  `pending` = changed/new objects not reached before the deadline."""
    listed = list_prefix(prefix)
    previous = load_state(table)
    state, todo, reused = {}, [], 0
    for key, etag in listed.items():
        old = previous.get(key)
        if old and old.get("etag") == etag:
            state[key] = old
            reused += 1
        else:
            todo.append(key)
    dropped = len(previous) - sum(1 for k in previous if k in listed)
    read = errors = pending = 0
    todo.sort()
    if todo:
        with ThreadPoolExecutor(max_workers=WORKERS) as ex:
            futures = {}
            idx = 0
            # submit in waves so a deadline stop does not leave thousands of queued GETs behind
            while idx < len(todo) or futures:
                while idx < len(todo) and len(futures) < WORKERS * 2 and time.time() < deadline:
                    k = todo[idx]
                    futures[ex.submit(_read_one, k, flattener)] = k
                    idx += 1
                if not futures:
                    break
                done = next(as_completed(list(futures)))
                k = futures.pop(done)
                row, err = done.result()
                read += 1
                if err:
                    print(err)
                    errors += 1
                    # keep the previous row of a changed object when the new body cannot be flattened
                    old = previous.get(k)
                    state[k] = {"etag": listed[k], "row": old.get("row") if old else None, "error": err[:160]}
                else:
                    state[k] = {"etag": listed[k], "row": row}
            left = todo[idx:]
            for k in left:  # not reached: previous row (if any) stays, the object stays pending
                old = previous.get(k)
                if old:
                    state[k] = old
                pending += 1
    rows = [v["row"] for v in state.values() if isinstance(v.get("row"), dict)]
    rows.sort(key=lambda r: r.get("ticker") or "")
    print(f"[snapshot] {prefix}: listed {len(listed)} top-level objects; reused {reused}, read {read}, errors {errors}, "
          f"pending {pending}, dropped {dropped}; rows {len(rows)}")
    return {"rows": rows, "listed": len(listed), "reused": reused, "read": read, "errors": errors,
            "pending": pending, "dropped": dropped, "state": state}


# ═══════════════════════════════════════════════════════════════════
# Handler
# ═══════════════════════════════════════════════════════════════════
# smallest first, so the small tables complete even on a run whose budget the big one exhausts
TABLES = (
    ("research_critique", CRITIQUE_PREFIX, "analytics/research_critique_flat.json"),
    ("edgar_insiders",    EDGAR_PREFIX,    "analytics/edgar_insiders_flat.json"),
    ("equity_research",   RESEARCH_PREFIX, "analytics/equity_research_flat.json"),
)


def _flattener(table):
    return {"equity_research": flatten_research, "edgar_insiders": flatten_edgar, "research_critique": flatten_critique}[table]


def lambda_handler(event, context):
    t0 = time.time()
    budget = DEFAULT_BUDGET_S
    try:
        budget = context.get_remaining_time_in_millis() / 1000.0
    except Exception:
        pass
    deadline = t0 + max(0.0, budget - RESERVE_S)   # no time left to read -> every changed object stays pending, tables still publish
    print(f"[snapshot] starting at {datetime.now(timezone.utc).isoformat()} (budget {budget:.0f}s, reserve {RESERVE_S}s)")

    results = {}
    for table, prefix, _ in TABLES:
        results[table] = read_table(table, prefix, _flattener(table), deadline)

    # ETF flows — read the single daily snapshot (already flattened per-ETF inside)
    etf_flow_rows = []
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=ETF_FLOWS_KEY)
        flow_doc = __import__("provider_flow_research").guard(ETF_FLOWS_KEY, json.loads(obj["Body"].read()))
        for m in (flow_doc.get("metrics") or []):
            if m.get("error"):
                continue
            etf_flow_rows.append(m)
        print(f"[snapshot] etf_flows: {len(etf_flow_rows)} rows")
    except Exception as e:
        print(f"[snapshot] etf_flows unavailable: {e}")
    etf_flow_rows.sort(key=lambda r: r.get("ticker") or "")

    generated_at = datetime.now(timezone.utc).isoformat()
    pending_total = sum(r["pending"] for r in results.values())
    meta = {
        "generated_at":     generated_at,
        "schema_version":   SCHEMA_VERSION,
        "n_rows":           None,
        "generation_elapsed_s": None,
        "complete":         pending_total == 0,
        "pending":          0,
    }

    # Write consolidated files (+ the per-table state for the next incremental run)
    written = []
    for table, prefix, public_key in TABLES:
        res = results[table]
        out = {
            **meta,
            "table_name":           table,
            "n_rows":               len(res["rows"]),
            "generation_elapsed_s": round(time.time() - t0, 1),
            "complete":             res["pending"] == 0,
            "pending":              res["pending"],
            "n_errors":             res["errors"],
            "source_prefix":        prefix,
            "rows":                 res["rows"],
        }
        body = json.dumps(out, default=str).encode()
        s3.put_object(Bucket=S3_BUCKET, Key=public_key, Body=body, ContentType="application/json",
                      CacheControl="public, max-age=300")
        size_kb = round(len(body) / 1024, 1)
        print(f"[snapshot] wrote {public_key} ({size_kb}KB · {len(res['rows'])} rows · pending {res['pending']})")
        written.append({"key": public_key, "n_rows": len(res["rows"]), "size_kb": size_kb, "pending": res["pending"]})
        try:
            save_state(table, res["state"], generated_at)
        except Exception as e:
            print(f"[snapshot] state {table} not saved: {type(e).__name__}: {str(e)[:120]}")
    out = {**meta, "table_name": "etf_flows", "n_rows": len(etf_flow_rows), "generation_elapsed_s": round(time.time() - t0, 1),
           "complete": True, "pending": 0, "rows": etf_flow_rows}
    body = json.dumps(out, default=str).encode()
    s3.put_object(Bucket=S3_BUCKET, Key="analytics/etf_flows_flat.json", Body=body, ContentType="application/json",
                  CacheControl="public, max-age=300")
    written.append({"key": "analytics/etf_flows_flat.json", "n_rows": len(etf_flow_rows), "size_kb": round(len(body) / 1024, 1), "pending": 0})

    # Also write a manifest pointer
    manifest = {
        "snapshot_generated_at": generated_at,
        "complete": pending_total == 0,
        "pending": {t: results[t]["pending"] for t, _, _ in TABLES},
        "read_stats": {t: {k: results[t][k] for k in ("listed", "reused", "read", "errors", "pending", "dropped")} for t, _, _ in TABLES},
        "tables": [
            {"name": "equity_research",   "key": "analytics/equity_research_flat.json"},
            {"name": "edgar_insiders",    "key": "analytics/edgar_insiders_flat.json"},
            {"name": "research_critique", "key": "analytics/research_critique_flat.json"},
            {"name": "etf_flows",         "key": "analytics/etf_flows_flat.json"},
        ],
        "schema_research_columns": list(flatten_research({}).keys()),
        "schema_edgar_columns":    list(flatten_edgar({}).keys()),
        "schema_critique_columns": list(flatten_critique({}).keys()),
        "schema_etf_flows_columns": (list(etf_flow_rows[0].keys()) if etf_flow_rows else []),
    }
    s3.put_object(
        Bucket=S3_BUCKET,
        Key="analytics/manifest.json",
        Body=json.dumps(manifest, default=str, indent=2).encode(),
        ContentType="application/json",
        CacheControl="public, max-age=60",
    )

    elapsed = round(time.time() - t0, 1)
    print(f"[snapshot] DONE in {elapsed}s · complete={pending_total == 0} pending={pending_total}")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "n_research_rows": len(results["equity_research"]["rows"]),
            "n_edgar_rows":    len(results["edgar_insiders"]["rows"]),
            "n_critique_rows": len(results["research_critique"]["rows"]),
            "n_etf_flow_rows": len(etf_flow_rows),
            "research_skipped": results["equity_research"]["errors"],
            "edgar_skipped":    results["edgar_insiders"]["errors"],
            "critique_skipped": results["research_critique"]["errors"],
            "complete":        pending_total == 0,
            "pending":         pending_total,
            "read_stats":      manifest["read_stats"],
            "written":         written,
            "elapsed_s":       elapsed,
        }),
    }
