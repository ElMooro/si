"""1171 — End-to-end: page accessible + flagship queries via simulated DuckDB SQL.

We can't actually run DuckDB-WASM here (no browser), but we can fetch the
consolidated files and run equivalent Python/SQL-via-sqlite to prove the
queries would work as intended in the browser.
"""
import json, time, urllib.request, ssl
import sqlite3
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/1171_analytics_e2e.json"
RAW_BASE = "https://raw.githubusercontent.com/ElMooro/si/main"
CDN_BASE = "https://justhodl-data-proxy.raafouis.workers.dev"
ctx = ssl.create_default_context()
s3 = boto3.client("s3", region_name="us-east-1")

out = {"started": datetime.now(timezone.utc).isoformat(), "checks": {}}

def http_get(url, t=20):
    t0 = time.time()
    try:
        with urllib.request.urlopen(
            urllib.request.Request(url, headers={"User-Agent": "JustHodl-Verify/1.0"}),
            timeout=t, context=ctx,
        ) as r:
            body = r.read()
            return {"http": r.status, "elapsed_s": round(time.time()-t0,2), "body": body}
    except Exception as e:
        return {"error": str(e)[:200]}

# 1. /analytics.html accessible via GitHub Pages (raw)
print("[1171] 1. /analytics.html accessible")
r = http_get(f"{RAW_BASE}/analytics.html")
if r.get("body"):
    txt = r["body"].decode("utf-8", errors="ignore")
    out["checks"]["page_accessible"] = {
        "size_kb": round(len(txt)/1024, 1),
        "has_duckdb_import": "@duckdb/duckdb-wasm" in txt,
        "has_sql_editor": 'id="sql-editor"' in txt,
        "has_run_btn":    'id="run-btn"' in txt,
        "has_results":    'id="results-body"' in txt,
        "has_examples":   "EXAMPLES" in txt,
        "n_example_queries": txt.count("data-example="),
        "has_research_url": "/analytics/equity_research_flat.json" in txt,
        "status": "✅",
    }
    print(f"   ✅ {out['checks']['page_accessible']['size_kb']}KB · {out['checks']['page_accessible']['n_example_queries']} example queries")
else:
    out["checks"]["page_accessible"] = {"error": r.get("error"), "status": "❌"}

# 2. CDN serves both flat files
print("\n[1171] 2. CDN serves consolidated files")
for key, name in [
    ("analytics/equity_research_flat.json", "research"),
    ("analytics/edgar_insiders_flat.json", "edgar"),
    ("analytics/manifest.json", "manifest"),
]:
    r = http_get(f"{CDN_BASE}/{key}?v={int(time.time())}")
    if r.get("body"):
        doc = json.loads(r["body"])
        out["checks"][f"cdn_{name}"] = {
            "http": r["http"], "elapsed_s": r["elapsed_s"],
            "size_kb": round(len(r["body"])/1024, 1),
            "n_rows": len(doc.get("rows", [])) if "rows" in doc else None,
            "status": "✅",
        }
        print(f"   ✅ {key}: {out['checks'][f'cdn_{name}']['size_kb']}KB · {out['checks'][f'cdn_{name}']['n_rows'] or '?'} rows · {r['elapsed_s']}s")
    else:
        out["checks"][f"cdn_{name}"] = {"error": r.get("error"), "status": "❌"}

# 3. Run the flagship queries against the data (using sqlite as DuckDB proxy)
print("\n[1171] 3. Simulating flagship queries")
research_obj = s3.get_object(Bucket="justhodl-dashboard-live",
                              Key="analytics/equity_research_flat.json")
edgar_obj = s3.get_object(Bucket="justhodl-dashboard-live",
                            Key="analytics/edgar_insiders_flat.json")
research_doc = json.loads(research_obj["Body"].read())
edgar_doc = json.loads(edgar_obj["Body"].read())

# Build SQLite DB
db = sqlite3.connect(":memory:")
db.row_factory = sqlite3.Row

def create_table(name, rows):
    if not rows:
        return
    cols = list(rows[0].keys())
    col_def = ", ".join(f'"{c}"' for c in cols)
    db.execute(f'CREATE TABLE {name} ({col_def})')
    placeholders = ", ".join("?" for _ in cols)
    for r in rows:
        vals = [r.get(c) for c in cols]
        # Coerce dicts/lists to str
        vals = [json.dumps(v) if isinstance(v, (dict, list)) else v for v in vals]
        db.execute(f'INSERT INTO {name} VALUES ({placeholders})', vals)
    db.commit()

create_table("equity_research", research_doc.get("rows", []))
create_table("edgar_insiders", edgar_doc.get("rows", []))

# Flagship queries to validate
queries = {
    "all_buys": """
        SELECT ticker, rating, conviction_grade, upside_pct
        FROM equity_research
        WHERE rating IN ('STRONG_BUY', 'BUY')
        ORDER BY upside_pct DESC
    """,
    "bullish_no_insider_sell": """
        SELECT r.ticker, r.rating, r.upside_pct, e.signal_label, e.sell_acceleration
        FROM equity_research r
        LEFT JOIN edgar_insiders e USING (ticker)
        WHERE r.rating IN ('STRONG_BUY', 'BUY')
          AND (e.sell_acceleration IS NULL OR e.sell_acceleration < 1.5)
    """,
    "value_growth": """
        SELECT ticker, sector, rating, pe_ttm, revenue_5yr_cagr, fcf_yield_pct, upside_pct
        FROM equity_research
        WHERE pe_ttm > 0 AND pe_ttm < 25
          AND revenue_5yr_cagr > 10
        ORDER BY revenue_5yr_cagr DESC
    """,
    "hidden_quality": """
        SELECT ticker, sector, roic_ttm_pct, pe_ttm, fcf_yield_pct
        FROM equity_research
        WHERE roic_ttm_pct > 15
          AND pe_ttm > 0 AND pe_ttm < 30
        ORDER BY roic_ttm_pct DESC
    """,
    "insider_red_flags": """
        SELECT e.ticker, e.signal_label, e.sell_acceleration, e.n_csuite_sellers,
               r.rating AS research_rating
        FROM edgar_insiders e
        LEFT JOIN equity_research r USING (ticker)
        WHERE e.signal_label IN ('ACCELERATING_SELL', 'LARGE_SELLING')
    """,
    "full_picture": """
        SELECT r.ticker, r.sector, r.rating, r.conviction_grade,
               r.upside_pct, r.pe_ttm, r.roic_ttm_pct, r.revenue_5yr_cagr,
               e.signal_label AS insider_signal, e.n_csuite_sellers
        FROM equity_research r
        LEFT JOIN edgar_insiders e USING (ticker)
        ORDER BY r.upside_pct DESC NULLS LAST
        LIMIT 5
    """,
    "sector_table": """
        SELECT sector,
               COUNT(*) AS n,
               ROUND(AVG(upside_pct), 1) AS avg_upside,
               ROUND(AVG(roic_ttm_pct), 1) AS avg_roic
        FROM equity_research
        WHERE sector IS NOT NULL
        GROUP BY sector
        ORDER BY avg_upside DESC
    """,
}

query_results = {}
for name, sql in queries.items():
    try:
        t0 = time.time()
        rows = [dict(r) for r in db.execute(sql).fetchall()]
        elapsed_ms = round((time.time() - t0) * 1000, 1)
        query_results[name] = {
            "ok": True,
            "n_rows": len(rows),
            "elapsed_ms": elapsed_ms,
            "sample": rows[:3] if rows else [],
        }
        print(f"   ✅ {name}: {len(rows)} rows ({elapsed_ms}ms)")
    except Exception as e:
        query_results[name] = {"ok": False, "error": str(e)[:300]}
        print(f"   ❌ {name}: {e}")

out["query_results"] = query_results

# Summary
all_ok = all(v.get("status") == "✅" or v.get("ok") for v in
              list(out["checks"].values()) + list(query_results.values()))
out["all_ok"] = all_ok

out["finished"] = datetime.now(timezone.utc).isoformat()
with open(REPORT, "w") as f:
    json.dump(out, f, indent=2, default=str)
print(f"\n[1171] DONE — all_ok={all_ok}")
