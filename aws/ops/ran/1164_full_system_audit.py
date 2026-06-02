"""1164 — Final audit: all 5 features + EDGAR v2 + CDN serves edgar/research files."""
import json, time, urllib.request, urllib.error, ssl
from datetime import datetime, timezone
import boto3
from botocore.exceptions import ClientError

REPORT = "aws/ops/reports/1164_full_audit.json"
CDN_BASE = "https://justhodl-data-proxy.raafouis.workers.dev"
S3_BUCKET = "justhodl-dashboard-live"
ctx = ssl.create_default_context()
lam = boto3.client("lambda", region_name="us-east-1")
s3  = boto3.client("s3", region_name="us-east-1")
events = boto3.client("events", region_name="us-east-1")

def http(url, t=15):
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

# Feature 1: prewarm Lambda + schedule
try:
    fn = lam.get_function(FunctionName="justhodl-equity-prewarm")
    rule = events.describe_rule(Name="justhodl-equity-prewarm-nightly")
    out["audit"]["feature_1_prewarm"] = {
        "function_exists": True,
        "last_modified": fn["Configuration"].get("LastModified"),
        "schedule": rule.get("ScheduleExpression"),
        "schedule_state": rule.get("State"),
        "status": "✅",
    }
except Exception as e:
    out["audit"]["feature_1_prewarm"] = {"error": str(e)[:200], "status": "❌"}

# Feature 2: search box on homepage (CDN-served index.html)
r = http(f"{CDN_BASE}/index.html?v={int(time.time())}", t=20)
if r.get("body"):
    txt = r["body"].decode("utf-8", errors="ignore")
    out["audit"]["feature_2_search"] = {
        "has_research_search": 'id="research-search-input"' in txt,
        "has_goResearch":      "function goResearch" in txt,
        "has_compare_link":    "/compare.html" in txt,
        "status": "✅" if all([
            'id="research-search-input"' in txt,
            "function goResearch" in txt,
        ]) else "❌",
    }

# Feature 3: compare.html
r = http(f"{CDN_BASE}/compare.html?v={int(time.time())}", t=20)
if r.get("body"):
    txt = r["body"].decode("utf-8", errors="ignore")
    out["audit"]["feature_3_compare"] = {
        "size_kb": round(len(txt)/1024, 1),
        "has_renderTickerCard": "function renderTickerCard" in txt,
        "has_renderWinnersTable": "function renderWinnersTable" in txt,
        "has_fetchOneTicker": "async function fetchOneTicker" in txt,
        "status": "✅" if all([
            "function renderTickerCard" in txt,
            "function renderWinnersTable" in txt,
            "async function fetchOneTicker" in txt,
        ]) else "❌",
    }
else:
    out["audit"]["feature_3_compare"] = {"http": r.get("http"), "error": r.get("error"), "status": "❌"}

# Feature 4: EDGAR Lambda + cached files in CDN
edgar_url_test = "https://ru3djltl3oucvsocjrih37sowu0fxgkm.lambda-url.us-east-1.on.aws/?ticker=NVDA"
r1 = http(edgar_url_test, t=30)
edgar_cdn_test = f"{CDN_BASE}/edgar-insiders/NVDA.json?v={int(time.time())}"
r2 = http(edgar_cdn_test, t=15)
edgar_signal = None
edgar_v2 = False
if r2.get("body"):
    try:
        j = json.loads(r2["body"])
        edgar_signal = j.get("signal_label")
        edgar_v2 = "sell_acceleration" in j and "signal_note" in j
    except: pass
out["audit"]["feature_4_edgar"] = {
    "lambda_url_works":  r1.get("http") == 200,
    "cdn_serves":        r2.get("http") == 200,
    "cdn_elapsed_s":     r2.get("elapsed_s"),
    "edgar_signal_label": edgar_signal,
    "signal_v2_fields_present": edgar_v2,
    "status": "✅" if (r1.get("http") == 200 and r2.get("http") == 200 and edgar_v2) else "❌",
}

# Feature 5: watchlist
r = http(f"{CDN_BASE}/index.html?v={int(time.time())}", t=20)
if r.get("body"):
    txt = r["body"].decode("utf-8", errors="ignore")
    out["audit"]["feature_5_watchlist"] = {
        "has_watchlist_card": 'id="watchlist-card"' in txt,
        "has_wlRender": "window.wlRender" in txt,
        "has_wlAdd": "window.wlAdd" in txt,
        "status": "✅" if all([
            'id="watchlist-card"' in txt,
            "window.wlRender" in txt,
            "window.wlAdd" in txt,
        ]) else "❌",
    }

# Bonus: why.html EDGAR v2 section
r = http(f"{CDN_BASE}/why.html?v={int(time.time())}", t=20)
if r.get("body"):
    txt = r["body"].decode("utf-8", errors="ignore")
    out["audit"]["why_html_edgar_v2"] = {
        "size_kb": round(len(txt)/1024, 1),
        "has_ACCELERATING_SELL": "ACCELERATING_SELL" in txt,
        "has_LARGE_SELLING": "LARGE_SELLING" in txt,
        "has_ROUTINE_SELLING": "ROUTINE_SELLING" in txt,
        "has_INSIDER_BUYING": "INSIDER_BUYING" in txt,
        "has_top_sellers": "top_sellers" in txt,
        "has_top_buyers": "top_buyers" in txt,
        "has_sell_acceleration": "sell_acceleration" in txt,
        "has_wlToggleCurrent": "function wlToggleCurrent" in txt,
        "has_renderInsiderActivity": "function renderInsiderActivity" in txt,
        "status": "✅" if all([
            "ACCELERATING_SELL" in txt, "LARGE_SELLING" in txt,
            "top_sellers" in txt, "function wlToggleCurrent" in txt,
        ]) else "❌",
    }

# Count edgar-insiders files
try:
    pag = s3.get_paginator("list_objects_v2")
    n_edgar = 0
    for page in pag.paginate(Bucket=S3_BUCKET, Prefix="edgar-insiders/"):
        n_edgar += len(page.get("Contents") or [])
    out["edgar_cache_count"] = n_edgar
except Exception as e:
    out["edgar_cache_count"] = f"err: {e}"

# Count equity-research files
try:
    pag = s3.get_paginator("list_objects_v2")
    n_research = 0
    for page in pag.paginate(Bucket=S3_BUCKET, Prefix="equity-research/"):
        n_research += len(page.get("Contents") or [])
    out["research_cache_count"] = n_research
except Exception as e:
    out["research_cache_count"] = f"err: {e}"

# Summary
n_pass = sum(1 for k, v in out["audit"].items() if isinstance(v, dict) and v.get("status") == "✅")
n_total = len(out["audit"])
out["summary"] = f"{n_pass}/{n_total} checks passing"
out["finished"] = datetime.now(timezone.utc).isoformat()

with open(REPORT, "w") as f:
    json.dump(out, f, indent=2, default=str)
print(f"[1164] DONE — {n_pass}/{n_total} ✅")
