"""1157 — Fix infra gaps from 1156:
  1. Create EventBridge schedule for justhodl-equity-prewarm (03:00 ET daily)
  2. Create function URL for justhodl-edgar-insiders (NONE auth + CORS *)
  3. Smoke-test EDGAR Lambda for AAPL + TSLA + NVDA via the new URL
  4. Pull GH Pages versions of index.html + compare.html + why.html
     (not the CDN-cached versions) and verify markers
  5. Print the EDGAR Lambda URL for hand-patching why.html
"""
import json, time, urllib.request, urllib.error, re, ssl
from datetime import datetime, timezone
import boto3
from botocore.exceptions import ClientError

REPORT = "aws/ops/reports/1157_fix_infra.json"

S3_BUCKET = "justhodl-dashboard-live"
LAMBDA_REGION = "us-east-1"

lam = boto3.client("lambda", region_name=LAMBDA_REGION)
events = boto3.client("events", region_name=LAMBDA_REGION)
s3 = boto3.client("s3", region_name=LAMBDA_REGION)
ctx = ssl.create_default_context()


def http_get(url, timeout=20, headers=None):
    t0 = time.time()
    req = urllib.request.Request(url, headers=headers or {"User-Agent": "JustHodl-Verify/1.0"})
    try:
        with urllib.request.urlopen(req, timeout=timeout, context=ctx) as r:
            body = r.read()
            return {"http": r.status, "elapsed_s": round(time.time()-t0,2),
                    "bytes": len(body), "body": body}
    except urllib.error.HTTPError as e:
        return {"http": e.code, "elapsed_s": round(time.time()-t0,2), "error": e.reason}
    except Exception as e:
        return {"error": str(e)[:300], "elapsed_s": round(time.time()-t0,2)}


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}

    # ─── 1. Create EventBridge schedule for prewarm
    rule_name = "justhodl-equity-prewarm-nightly"
    fn_name = "justhodl-equity-prewarm"
    sched = {}
    try:
        rule = events.put_rule(
            Name=rule_name,
            ScheduleExpression="cron(0 8 * * ? *)",  # 08:00 UTC = 03:00 ET
            State="ENABLED",
            Description="Nightly equity research pre-warm at 03:00 ET",
        )
        sched["rule_arn"] = rule.get("RuleArn")
        # Add Lambda target
        events.put_targets(
            Rule=rule_name,
            Targets=[{"Id": "1", "Arn": lam.get_function(FunctionName=fn_name)["Configuration"]["FunctionArn"]}],
        )
        sched["target_added"] = True
        # Grant EventBridge permission to invoke Lambda
        try:
            lam.add_permission(
                FunctionName=fn_name,
                StatementId="EventBridgeInvoke",
                Action="lambda:InvokeFunction",
                Principal="events.amazonaws.com",
                SourceArn=rule["RuleArn"],
            )
            sched["permission_added"] = True
        except lam.exceptions.ResourceConflictException:
            sched["permission_added"] = "already exists"
        sched["status"] = "ok"
    except Exception as e:
        sched["status"] = "error"
        sched["error"] = str(e)[:300]
    out["prewarm_schedule"] = sched

    # ─── 2. Create function URL for edgar-insiders
    edgar = {}
    edgar_fn = "justhodl-edgar-insiders"
    try:
        # Check if function URL already exists
        try:
            existing = lam.get_function_url_config(FunctionName=edgar_fn)
            edgar["function_url"] = existing.get("FunctionUrl")
            edgar["status"] = "already_existed"
        except lam.exceptions.ResourceNotFoundException:
            # Create it
            cfg = lam.create_function_url_config(
                FunctionName=edgar_fn,
                AuthType="NONE",
                Cors={
                    "AllowCredentials": False,
                    "AllowHeaders": ["*"],
                    "AllowMethods": ["*"],
                    "AllowOrigins": ["*"],
                    "ExposeHeaders": ["*"],
                    "MaxAge": 86400,
                },
            )
            edgar["function_url"] = cfg.get("FunctionUrl")
            edgar["status"] = "created"
            # Grant public invoke permission
            try:
                lam.add_permission(
                    FunctionName=edgar_fn,
                    StatementId="FunctionURLAllowPublicAccess",
                    Action="lambda:InvokeFunctionUrl",
                    Principal="*",
                    FunctionUrlAuthType="NONE",
                )
                edgar["public_permission"] = "added"
            except lam.exceptions.ResourceConflictException:
                edgar["public_permission"] = "already exists"
    except Exception as e:
        edgar["status"] = "error"
        edgar["error"] = str(e)[:400]
    out["edgar_function_url"] = edgar

    # ─── 3. Smoke-test EDGAR for AAPL (and pre-warm 2 others)
    edgar_url = edgar.get("function_url", "")
    smokes = {}
    if edgar_url:
        # AAPL — should be cached or compute fresh
        for t in ["AAPL", "TSLA", "NVDA"]:
            r = http_get(f"{edgar_url}?ticker={t}", timeout=120)
            if r.get("body"):
                try:
                    j = json.loads(r["body"])
                    smokes[t] = {
                        "http": r["http"],
                        "elapsed_s": r["elapsed_s"],
                        "size_kb": round(r["bytes"]/1024, 1),
                        "cik": j.get("cik"),
                        "n_filings_90d": j.get("n_filings_90d"),
                        "n_buys": j.get("n_buys"),
                        "n_sells": j.get("n_sells"),
                        "signal_label": j.get("signal_label"),
                        "signal_score": j.get("signal_score"),
                        "net_dollars_90d": j.get("net_dollars_90d"),
                        "cluster_detected": j.get("cluster_detected"),
                    }
                except Exception as e:
                    smokes[t] = {"http": r["http"], "parse_error": str(e),
                                   "body_snippet": r["body"][:300].decode("utf-8", errors="replace") if r.get("body") else ""}
            else:
                smokes[t] = {"http": r.get("http"), "error": r.get("error")}
    out["edgar_smokes"] = smokes

    # ─── 4. Pull GH Pages versions directly (bypass CDN)
    # GH Pages serves via DNS proxy, but the canonical URL is the username site
    # Since justhodl.ai is fronted by CF (and CF is what serves index.html etc),
    # use a cache-buster + check raw GitHub source as backup
    pages = {}
    for p in ["index.html", "compare.html", "why.html"]:
        # Try raw.githubusercontent (always fresh)
        r = http_get(f"https://raw.githubusercontent.com/ElMooro/si/main/{p}", timeout=20)
        if r.get("body"):
            t = r["body"].decode("utf-8", errors="ignore")
            markers = {}
            if p == "index.html":
                markers["has_research_search"] = 'id="research-search-input"' in t
                markers["has_watchlist_card"] = 'id="watchlist-card"' in t
                markers["has_wlAdd"] = "window.wlAdd" in t
                markers["has_compare_link"] = "/compare.html" in t
            elif p == "compare.html":
                markers["has_renderTickerCard"] = "function renderTickerCard" in t
                markers["has_renderWinnersTable"] = "function renderWinnersTable" in t
                markers["has_fetchOneTicker"] = "async function fetchOneTicker" in t
            elif p == "why.html":
                markers["has_wlToggleCurrent"] = "function wlToggleCurrent" in t
                markers["has_renderInsiderActivity"] = "function renderInsiderActivity" in t
                markers["has_EDGAR_CDN_BASE"] = "EDGAR_CDN_BASE" in t
                markers["has_compare_in_nav"] = '/compare.html"' in t
                markers["why_version"] = (re.search(r'<meta name="why-version"[^>]+content="([^"]+)"', t) or [None,None])[1]
                markers["has_new_cache_headers"] = 'http-equiv="Cache-Control"' in t
            pages[p] = {"size_kb": round(len(t)/1024, 1), "markers": markers}
        else:
            pages[p] = {"error": r.get("error") or f"HTTP {r.get('http')}"}
    out["gh_pages_source"] = pages

    # Patch why.html with EDGAR URL — write the patched file locally so a
    # subsequent commit ships it. Print instructions if no URL.
    if edgar_url:
        try:
            why = open("why.html").read()
            new_line = f'let EDGAR_LAMBDA_URL = "{edgar_url}"; // patched by ops 1157'
            why_patched = re.sub(
                r'let EDGAR_LAMBDA_URL\s*=\s*"[^"]*";\s*//[^\n]*',
                new_line, why)
            if why_patched != why:
                open("why.html","w").write(why_patched)
                out["why_html_patched"] = "ok"
            else:
                out["why_html_patched"] = "no change needed (already set or pattern didn't match)"
        except Exception as e:
            out["why_html_patched"] = f"error: {str(e)[:200]}"

    # ─── Summary
    n_smokes_ok = sum(1 for r in smokes.values() if isinstance(r,dict) and r.get("http") == 200 and "cik" in r and r["cik"])
    out["summary"] = {
        "prewarm_schedule_ok": sched.get("status") == "ok",
        "edgar_url_ok": bool(edgar_url),
        "edgar_smokes_ok": n_smokes_ok,
        "edgar_url": edgar_url,
    }
    out["finished"] = datetime.now(timezone.utc).isoformat()

    with open(REPORT, "w") as f:
        json.dump(out, f, indent=2, default=str)
    print(f"[1157] DONE — EDGAR URL: {edgar_url or '(none)'}, smokes ok: {n_smokes_ok}/3")


if __name__ == "__main__":
    main()
