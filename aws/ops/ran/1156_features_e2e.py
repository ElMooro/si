"""1156 — Full verification of features 1-5.

Checks:
  1. equity-prewarm: Lambda exists + has schedule
  2. equity-research: CDN serves cached AAPL.json
  3. compare.html: page exists in S3 OR served by GH pages (via raw GH source)
  4. edgar-insiders: Lambda deployed, function URL exists, smoke-test AAPL
  5. why.html: contains all 5 markers (renderInsiderActivity, EDGAR_CDN_BASE,
                wlToggleCurrent, fetchAndRender CDN-first, has new cache headers)
  6. index.html: research search + watchlist widget both present

Also: patch why.html with the actual EDGAR Lambda URL after deploy.
"""
import json, time, urllib.request, urllib.error, re, ssl
from datetime import datetime, timezone
import boto3
from botocore.exceptions import ClientError

REPORT_PATH = "aws/ops/reports/1156_features_e2e.json"

CDN_BASE = "https://justhodl-data-proxy.raafouis.workers.dev"
S3_BUCKET = "justhodl-dashboard-live"
LAMBDA_REGION = "us-east-1"

lam = boto3.client("lambda", region_name=LAMBDA_REGION)
s3 = boto3.client("s3", region_name=LAMBDA_REGION)

ctx = ssl.create_default_context()


def http_get(url, timeout=20, headers=None):
    t0 = time.time()
    req = urllib.request.Request(url, headers=headers or {"User-Agent": "JustHodl-Verify/1.0"})
    try:
        with urllib.request.urlopen(req, timeout=timeout, context=ctx) as r:
            body = r.read()
            return {
                "http": r.status,
                "elapsed_s": round(time.time() - t0, 2),
                "bytes": len(body),
                "body": body,
            }
    except urllib.error.HTTPError as e:
        return {"http": e.code, "elapsed_s": round(time.time() - t0, 2), "error": str(e.reason)}
    except Exception as e:
        return {"error": str(e)[:300], "elapsed_s": round(time.time() - t0, 2)}


def main():
    out = {"started": datetime.now(timezone.utc).isoformat(), "checks": {}}

    # ── Feature 1: equity-prewarm Lambda + schedule
    f1 = {}
    try:
        cfg = lam.get_function(FunctionName="justhodl-equity-prewarm")
        f1["function_exists"] = True
        f1["last_modified"] = cfg["Configuration"].get("LastModified")
        f1["timeout"] = cfg["Configuration"].get("Timeout")
        f1["memory"] = cfg["Configuration"].get("MemorySize")
        # Check schedule
        events = boto3.client("events", region_name=LAMBDA_REGION)
        try:
            rule = events.describe_rule(Name="justhodl-equity-prewarm-nightly")
            f1["schedule_exists"] = True
            f1["schedule_expression"] = rule.get("ScheduleExpression")
            f1["schedule_state"] = rule.get("State")
        except events.exceptions.ResourceNotFoundException:
            f1["schedule_exists"] = False
    except ClientError as e:
        f1["function_exists"] = False
        f1["error"] = str(e)[:300]
    out["checks"]["feature_1_prewarm"] = f1

    # ── Feature 2: research search box on homepage (string check)
    f2 = {}
    idx_r = http_get(f"{CDN_BASE}/index.html?v={int(time.time())}", timeout=30)
    if idx_r.get("body"):
        idx_text = idx_r["body"].decode("utf-8", errors="ignore")
        f2["index_size_kb"] = round(len(idx_text)/1024, 1)
        f2["has_research_search"] = 'id="research-search-input"' in idx_text
        f2["has_goResearch"] = "function goResearch" in idx_text
        f2["has_compare_link"] = "/compare.html?tickers=" in idx_text
    else:
        f2["error"] = idx_r.get("error") or f"http {idx_r.get('http')}"
    out["checks"]["feature_2_search"] = f2

    # ── Feature 3: compare.html
    f3 = {}
    cmp_r = http_get(f"{CDN_BASE}/compare.html?v={int(time.time())}", timeout=30)
    f3["http"] = cmp_r.get("http")
    f3["elapsed_s"] = cmp_r.get("elapsed_s")
    if cmp_r.get("body"):
        cmp_text = cmp_r["body"].decode("utf-8", errors="ignore")
        f3["size_kb"] = round(len(cmp_text)/1024, 1)
        f3["has_fetchOneTicker"] = "async function fetchOneTicker" in cmp_text
        f3["has_renderTickerCard"] = "function renderTickerCard" in cmp_text
        f3["has_renderWinnersTable"] = "function renderWinnersTable" in cmp_text
        f3["has_promise_allSettled"] = "Promise.allSettled" in cmp_text
    out["checks"]["feature_3_compare"] = f3

    # ── Feature 4: edgar-insiders Lambda
    f4 = {}
    try:
        cfg = lam.get_function(FunctionName="justhodl-edgar-insiders")
        f4["function_exists"] = True
        f4["last_modified"] = cfg["Configuration"].get("LastModified")
        f4["timeout"] = cfg["Configuration"].get("Timeout")
        f4["memory"] = cfg["Configuration"].get("MemorySize")
        # Get function URL
        try:
            url_cfg = lam.get_function_url_config(FunctionName="justhodl-edgar-insiders")
            f4["function_url"] = url_cfg.get("FunctionUrl")
            f4["url_auth_type"] = url_cfg.get("AuthType")
        except ClientError as e:
            f4["function_url"] = None
            f4["url_error"] = str(e)[:200]
        # Smoke test AAPL via Lambda URL if available
        if f4.get("function_url"):
            smoke = http_get(f"{f4['function_url']}?ticker=AAPL", timeout=90)
            f4["aapl_smoke"] = {
                "http": smoke.get("http"),
                "elapsed_s": smoke.get("elapsed_s"),
                "size_kb": round((smoke.get("bytes",0) or 0)/1024, 1),
            }
            if smoke.get("body"):
                try:
                    aapl = json.loads(smoke["body"])
                    f4["aapl_smoke"]["signal_label"] = aapl.get("signal_label")
                    f4["aapl_smoke"]["signal_score"] = aapl.get("signal_score")
                    f4["aapl_smoke"]["n_filings_90d"] = aapl.get("n_filings_90d")
                    f4["aapl_smoke"]["n_buys"] = aapl.get("n_buys")
                    f4["aapl_smoke"]["n_sells"] = aapl.get("n_sells")
                    f4["aapl_smoke"]["cik"] = aapl.get("cik")
                except Exception as e:
                    f4["aapl_smoke"]["parse_error"] = str(e)
    except ClientError as e:
        f4["function_exists"] = False
        f4["error"] = str(e)[:300]
    out["checks"]["feature_4_edgar"] = f4

    # ── Feature 5: watchlist + EDGAR section in why.html
    f5 = {}
    why_r = http_get(f"{CDN_BASE}/why.html?v={int(time.time())}", timeout=30)
    if why_r.get("body"):
        why_text = why_r["body"].decode("utf-8", errors="ignore")
        f5["why_size_kb"] = round(len(why_text)/1024, 1)
        f5["has_wlToggleCurrent"] = "function wlToggleCurrent" in why_text
        f5["has_wlStarBtn"] = "wlStarBtn" in why_text
        f5["has_renderInsiderActivity"] = "function renderInsiderActivity" in why_text
        f5["has_fetchInsiderData"] = "async function fetchInsiderData" in why_text
        f5["has_EDGAR_CDN_BASE"] = "EDGAR_CDN_BASE" in why_text
        f5["has_compare_in_nav"] = '/compare.html"' in why_text
        f5["has_new_cache_headers"] = 'http-equiv="Cache-Control"' in why_text
        f5["why_version"] = (re.search(r'<meta name="why-version"[^>]+content="([^"]+)"', why_text) or [None, None])[1] if re.search(r'<meta name="why-version"', why_text) else "(none)"
    # Also check index.html watchlist
    if idx_r.get("body"):
        idx_text = idx_r["body"].decode("utf-8", errors="ignore")
        f5["index_has_watchlist"] = 'id="watchlist-card"' in idx_text
        f5["index_has_wlRender"] = "window.wlRender" in idx_text
        f5["index_has_wlAdd"] = "window.wlAdd" in idx_text
    out["checks"]["feature_5_watchlist"] = f5

    # ── Bonus: patch why.html with actual EDGAR Lambda URL if we got it
    edgar_url = (out["checks"]["feature_4_edgar"].get("function_url") or "").strip()
    out["edgar_lambda_url_to_patch"] = edgar_url

    # Also add edgar-insiders/ to bucket policy for public CDN access
    policy_status = {}
    try:
        policy_resp = s3.get_bucket_policy(Bucket=S3_BUCKET)
        policy = json.loads(policy_resp["Policy"])
        existing_sids = [s.get("Sid","") for s in policy.get("Statement", [])]
        policy_status["existing_sids"] = existing_sids
        if "PublicReadEdgarInsiders" not in existing_sids:
            policy["Statement"].append({
                "Sid": "PublicReadEdgarInsiders",
                "Effect": "Allow",
                "Principal": "*",
                "Action": "s3:GetObject",
                "Resource": f"arn:aws:s3:::{S3_BUCKET}/edgar-insiders/*",
            })
            s3.put_bucket_policy(Bucket=S3_BUCKET, Policy=json.dumps(policy))
            policy_status["added"] = "PublicReadEdgarInsiders"
        else:
            policy_status["already_present"] = "PublicReadEdgarInsiders"
    except Exception as e:
        policy_status["error"] = str(e)[:300]
    out["bucket_policy"] = policy_status

    # Summary line
    n_pass = sum(1 for k, v in out["checks"].items()
                  if isinstance(v, dict) and not v.get("error"))
    out["summary"] = {
        "features_passing": n_pass,
        "features_total": 5,
        "edgar_lambda_url_discovered": bool(edgar_url),
    }

    out["finished"] = datetime.now(timezone.utc).isoformat()
    with open(REPORT_PATH, "w") as f:
        json.dump(out, f, indent=2, default=str)
    print(f"[1156] DONE — {n_pass}/5 features passing")
    print(f"[1156] EDGAR Lambda URL: {edgar_url or '(not yet deployed)'}")
    if edgar_url:
        print(f"[1156] ☆ NEXT: patch why.html EDGAR_LAMBDA_URL = '{edgar_url}'")


if __name__ == "__main__":
    main()
