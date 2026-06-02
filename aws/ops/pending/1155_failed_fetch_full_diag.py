"""
1155 — Full diagnostic for "Failed to fetch" bug.
- Probes Lambda URL CORS preflight
- Probes Lambda direct (cached ticker AAPL + fresh ticker SPOT)
- Probes CF proxy
- Probes S3 direct
- Probes live why.html for CDN_RESEARCH_BASE marker
"""
import json, time, urllib.request, urllib.error, ssl
from datetime import datetime, timezone

LAMBDA_URL = "https://6nkrwmk2ntjx54okqvtzokosb40whvfb.lambda-url.us-east-1.on.aws/"
CDN_BASE = "https://justhodl-data-proxy.raafouis.workers.dev/equity-research"
S3_BASE = "https://justhodl-dashboard-live.s3.us-east-1.amazonaws.com/equity-research"
WHY_URL = "https://justhodl.ai/why.html"

ctx = ssl.create_default_context()
out = {"started": datetime.now(timezone.utc).isoformat(), "tests": []}

def probe(label, url, method="GET", headers=None, timeout=200, want_text=False):
    headers = headers or {}
    headers.setdefault("User-Agent", "JustHodl-Diag/1.0")
    headers.setdefault("Origin", "https://justhodl.ai")
    t0 = time.time()
    rec = {"label": label, "url": url, "method": method}
    try:
        req = urllib.request.Request(url, method=method, headers=headers)
        with urllib.request.urlopen(req, timeout=timeout, context=ctx) as r:
            body = r.read()
            rec["http"] = r.status
            rec["elapsed_s"] = round(time.time()-t0, 2)
            rec["bytes"] = len(body)
            rec["cors_allow_origin"] = r.headers.get("access-control-allow-origin")
            rec["content_type"] = r.headers.get("content-type")
            rec["cache_control"] = r.headers.get("cache-control")
            if want_text:
                rec["body_snippet"] = body.decode("utf-8", errors="replace")[:500]
            else:
                # try to parse JSON for ticker probes
                try:
                    d = json.loads(body)
                    rec["json_ok"] = True
                    rec["ticker"] = d.get("ticker")
                    rec["verdict"] = d.get("verdict") or d.get("executive_summary",{}).get("verdict")
                    rec["generated_at"] = d.get("generated_at")
                    rec["has_scenarios"] = bool(d.get("scenarios"))
                except Exception:
                    rec["json_ok"] = False
                    rec["body_snippet"] = body.decode("utf-8", errors="replace")[:300]
    except urllib.error.HTTPError as e:
        rec["http"] = e.code
        rec["elapsed_s"] = round(time.time()-t0, 2)
        rec["error"] = f"HTTPError {e.code}: {e.reason}"
        try:
            rec["body_snippet"] = e.read().decode("utf-8", errors="replace")[:300]
        except: pass
    except Exception as e:
        rec["elapsed_s"] = round(time.time()-t0, 2)
        rec["error"] = f"{type(e).__name__}: {str(e)[:300]}"
    out["tests"].append(rec)
    return rec

# 1. CORS preflight to Lambda URL
probe("Lambda CORS preflight",
      f"{LAMBDA_URL}?ticker=AAPL",
      method="OPTIONS",
      headers={"Access-Control-Request-Method":"GET", "Access-Control-Request-Headers":"content-type"})

# 2. Lambda direct - cached ticker
probe("Lambda GET cached: AAPL", f"{LAMBDA_URL}?ticker=AAPL")

# 3. CDN proxy - cached ticker
probe("CDN cached: AAPL", f"{CDN_BASE}/AAPL.json?v={int(time.time())}")
probe("CDN cached: NVDA", f"{CDN_BASE}/NVDA.json?v={int(time.time())}")

# 4. S3 direct
probe("S3 direct: AAPL", f"{S3_BASE}/AAPL.json?v={int(time.time())}")

# 5. Lambda direct - fresh ticker (might 30-90s) - skip for now, would block
# probe("Lambda GET fresh: SPOT", f"{LAMBDA_URL}?ticker=SPOT", timeout=180)

# 6. live why.html — check version
r = probe("Live why.html source", WHY_URL, want_text=True)
if r.get("body_snippet"):
    text = r["body_snippet"]
    # try to fetch full file to look for markers
    try:
        req = urllib.request.Request(WHY_URL, headers={"User-Agent":"JustHodl-Diag/1.0"})
        with urllib.request.urlopen(req, timeout=30, context=ctx) as resp:
            full = resp.read().decode("utf-8", errors="replace")
            r["full_size"] = len(full)
            r["has_fetchAndRender"] = "fetchAndRender" in full
            r["has_CDN_RESEARCH_BASE"] = "CDN_RESEARCH_BASE" in full
            r["has_fetchWithTimeout"] = "fetchWithTimeout" in full
            r["has_phase1_cdn"] = "Phase 1: Try CDN" in full
            # find the old-style error if present
            r["has_old_error"] = "Check that the ticker is valid" in full
            r["has_new_error"] = "Popular pre-cached names" in full
            # find any version marker
            import re
            m = re.search(r'<meta name="why-version"[^>]+content="([^"]+)"', full)
            r["why_version"] = m.group(1) if m else "(none)"
    except Exception as e:
        r["full_fetch_error"] = str(e)
    r.pop("body_snippet", None)

# print summary
print(json.dumps(out, indent=2)[:6000])

with open("aws/ops/reports/1155_failed_fetch_full_diag.json","w") as f:
    json.dump(out, f, indent=2)
print(f"\n\nSaved report: aws/ops/reports/1155_failed_fetch_full_diag.json")
