"""1165 — Re-audit using raw.githubusercontent for HTML pages (CDN proxies S3, not Pages)."""
import json, time, urllib.request, urllib.error, ssl
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/1165_final_audit.json"
RAW_BASE = "https://raw.githubusercontent.com/ElMooro/si/main"
CDN_BASE = "https://justhodl-data-proxy.raafouis.workers.dev"
EDGAR_LAMBDA_URL = "https://ru3djltl3oucvsocjrih37sowu0fxgkm.lambda-url.us-east-1.on.aws/"
S3_BUCKET = "justhodl-dashboard-live"

ctx = ssl.create_default_context()
lam = boto3.client("lambda", region_name="us-east-1")
s3  = boto3.client("s3", region_name="us-east-1")
events = boto3.client("events", region_name="us-east-1")

def http(url, t=20):
    t0 = time.time()
    try:
        with urllib.request.urlopen(urllib.request.Request(url, headers={"User-Agent":"JustHodl-Audit/1.0"}), timeout=t, context=ctx) as r:
            body = r.read()
            return {"http": r.status, "elapsed_s": round(time.time()-t0,2), "bytes": len(body), "body": body}
    except urllib.error.HTTPError as e:
        return {"http": e.code, "elapsed_s": round(time.time()-t0,2), "error": e.reason}
    except Exception as e:
        return {"error": str(e)[:200], "elapsed_s": round(time.time()-t0,2)}

out = {"started": datetime.now(timezone.utc).isoformat(), "audit": {}}

# ── Feature 1
try:
    fn = lam.get_function(FunctionName="justhodl-equity-prewarm")
    rule = events.describe_rule(Name="justhodl-equity-prewarm-nightly")
    out["audit"]["feature_1_prewarm"] = {
        "function_exists": True,
        "memory_mb": fn["Configuration"].get("MemorySize"),
        "timeout_s": fn["Configuration"].get("Timeout"),
        "schedule": rule.get("ScheduleExpression"),
        "schedule_state": rule.get("State"),
        "status": "✅",
    }
except Exception as e:
    out["audit"]["feature_1_prewarm"] = {"error": str(e)[:200], "status": "❌"}

# ── Feature 2: research search (raw GH)
r = http(f"{RAW_BASE}/index.html")
if r.get("body"):
    txt = r["body"].decode("utf-8", errors="ignore")
    out["audit"]["feature_2_search"] = {
        "size_kb": round(len(txt)/1024, 1),
        "has_research_search_input": 'id="research-search-input"' in txt,
        "has_goResearch_function": "function goResearch" in txt,
        "has_quick_picks": '/why.html?ticker=AAPL' in txt and '/why.html?ticker=NVDA' in txt,
        "has_compare_link": "/compare.html" in txt,
    }
    out["audit"]["feature_2_search"]["status"] = "✅" if (
        'id="research-search-input"' in txt and "function goResearch" in txt
    ) else "❌"

# ── Feature 3: compare.html (raw GH)
r = http(f"{RAW_BASE}/compare.html")
if r.get("body"):
    txt = r["body"].decode("utf-8", errors="ignore")
    out["audit"]["feature_3_compare"] = {
        "size_kb": round(len(txt)/1024, 1),
        "has_renderTickerCard": "function renderTickerCard" in txt,
        "has_renderWinnersTable": "function renderWinnersTable" in txt,
        "has_fetchOneTicker": "async function fetchOneTicker" in txt,
        "has_promise_allSettled": "Promise.allSettled" in txt,
    }
    out["audit"]["feature_3_compare"]["status"] = "✅" if all([
        "function renderTickerCard" in txt,
        "function renderWinnersTable" in txt,
        "async function fetchOneTicker" in txt,
    ]) else "❌"
else:
    out["audit"]["feature_3_compare"] = {"error": r.get("error") or f"HTTP {r.get('http')}", "status": "❌"}

# ── Feature 4: EDGAR (Lambda + CDN serve)
r_lam = http(f"{EDGAR_LAMBDA_URL}?ticker=NVDA", t=30)
r_cdn = http(f"{CDN_BASE}/edgar-insiders/NVDA.json?v={int(time.time())}", t=15)
edgar_check = {
    "lambda_url_works": r_lam.get("http") == 200,
    "lambda_elapsed_s": r_lam.get("elapsed_s"),
    "cdn_serves":       r_cdn.get("http") == 200,
    "cdn_elapsed_s":    r_cdn.get("elapsed_s"),
}
if r_cdn.get("body"):
    try:
        j = json.loads(r_cdn["body"])
        edgar_check["signal_v2_fields"] = all(k in j for k in ["sell_acceleration","signal_note","top_sellers","prior_dollars_sell"])
        edgar_check["signal_label"] = j.get("signal_label")
        edgar_check["signal_score"] = j.get("signal_score")
        edgar_check["n_filings_90d"] = j.get("n_filings_90d")
    except Exception as e:
        edgar_check["parse_err"] = str(e)
edgar_check["status"] = "✅" if (
    edgar_check.get("lambda_url_works") and edgar_check.get("cdn_serves")
    and edgar_check.get("signal_v2_fields")
) else "❌"
out["audit"]["feature_4_edgar"] = edgar_check

# ── Feature 5: watchlist (raw GH index.html + why.html star)
r_idx = http(f"{RAW_BASE}/index.html")
r_why = http(f"{RAW_BASE}/why.html")
wl_check = {}
if r_idx.get("body"):
    t = r_idx["body"].decode("utf-8", errors="ignore")
    wl_check["index_has_watchlist_card"] = 'id="watchlist-card"' in t
    wl_check["index_has_wlRender"] = "window.wlRender" in t
    wl_check["index_has_wlAdd"] = "window.wlAdd" in t
    wl_check["index_has_wlSeed"] = "window.wlSeed" in t
if r_why.get("body"):
    t = r_why["body"].decode("utf-8", errors="ignore")
    wl_check["why_has_wlToggleCurrent"] = "function wlToggleCurrent" in t
    wl_check["why_has_wlStarBtn"] = "wlStarBtn" in t
    wl_check["shared_localStorage_key"] = "justhodl_watchlist_v1" in t
wl_check["status"] = "✅" if all([
    wl_check.get("index_has_watchlist_card"),
    wl_check.get("index_has_wlRender"),
    wl_check.get("why_has_wlToggleCurrent"),
]) else "❌"
out["audit"]["feature_5_watchlist"] = wl_check

# ── why.html EDGAR v2 markers
if r_why.get("body"):
    t = r_why["body"].decode("utf-8", errors="ignore")
    out["audit"]["why_html_edgar_v2"] = {
        "size_kb": round(len(t)/1024, 1),
        "has_ACCELERATING_SELL": "ACCELERATING_SELL" in t,
        "has_LARGE_SELLING": "LARGE_SELLING" in t,
        "has_ROUTINE_SELLING": "ROUTINE_SELLING" in t,
        "has_INSIDER_BUYING": "INSIDER_BUYING" in t,
        "has_top_sellers_html": "top_sellers" in t,
        "has_top_buyers_html": "top_buyers" in t,
        "has_sell_acceleration": "sell_acceleration" in t,
        "has_renderInsiderActivity": "function renderInsiderActivity" in t,
        "has_compare_nav": "/compare.html\"" in t,
        "has_cache_headers": 'http-equiv="Cache-Control"' in t,
    }
    out["audit"]["why_html_edgar_v2"]["status"] = "✅" if all([
        "ACCELERATING_SELL" in t,
        "LARGE_SELLING" in t,
        "top_sellers" in t,
        "function renderInsiderActivity" in t,
    ]) else "❌"

# ── Cache stats
try:
    pag = s3.get_paginator("list_objects_v2")
    for prefix, key in [("equity-research/", "research_cache_count"),
                          ("edgar-insiders/", "edgar_cache_count"),
                          ("equity-prewarm/runs/", "prewarm_runs_count")]:
        n = 0
        for page in pag.paginate(Bucket=S3_BUCKET, Prefix=prefix):
            n += len(page.get("Contents") or [])
        out[key] = n
except Exception as e:
    out["cache_stats_err"] = str(e)

# Summary
n_pass = sum(1 for v in out["audit"].values() if v.get("status") == "✅")
n_total = len(out["audit"])
out["summary"] = f"{n_pass}/{n_total} checks passing"
out["finished"] = datetime.now(timezone.utc).isoformat()

with open(REPORT, "w") as f:
    json.dump(out, f, indent=2, default=str)
print(f"[1165] DONE — {n_pass}/{n_total} ✅")
