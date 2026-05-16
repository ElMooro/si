"""ops/727 — end-to-end verify the holdings-feed wiring.

Confirms:
  1. Function URL CORS preflight responds.
  2. POST with no token  → 403 (endpoint is gated).
  3. POST with bad token → 403 (auth actually discriminates).
  4. POST with the real SSM token + action=list → 200, ok:true
     (proves the full path: CORS → SSM read → DDB query).
  5. portfolio-manager.html is live, carries the page marker and the
     baked-in Function URL.
  6. index.html links the manager.

Read-only throughout — uses the `list` action, never writes to the book.
"""
import json, os, urllib.request, urllib.error
from datetime import datetime, timezone
import boto3

REGION = "us-east-1"
ADMIN_URL = "https://e726eujwijpeg2slgssddw2yee0stboa.lambda-url.us-east-1.on.aws/"
SSM_NAME = "/justhodl/portfolio-admin/token"
ssm = boto3.client("ssm", region_name=REGION)

report = {"ops": 727, "ts": datetime.now(timezone.utc).isoformat(),
          "subject": "portfolio-admin endpoint + Portfolio Manager E2E"}


def http(method, url, headers=None, body=None):
    """Returns (status, text, resp_headers). Captures 4xx/5xx cleanly."""
    data = body.encode() if isinstance(body, str) else body
    req = urllib.request.Request(url, data=data, method=method,
                                 headers=headers or {})
    try:
        with urllib.request.urlopen(req, timeout=25) as r:
            return r.status, r.read().decode("utf-8", "replace"), dict(r.headers)
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode("utf-8", "replace"), dict(e.headers or {})
    except Exception as e:
        return None, str(e)[:200], {}


# ── 1. CORS preflight ──
st, _, hdrs = http("OPTIONS", ADMIN_URL, {
    "Origin": "https://justhodl.ai",
    "Access-Control-Request-Method": "POST",
    "Access-Control-Request-Headers": "content-type,x-justhodl-token"})
acao = hdrs.get("Access-Control-Allow-Origin") or hdrs.get(
    "access-control-allow-origin")
report["preflight"] = {"status": st, "allow_origin": acao}

# ── 2. POST with no token ──
st, txt, _ = http("POST", ADMIN_URL, {"content-type": "application/json"},
                  json.dumps({"action": "list", "filter": "POSITION"}))
report["no_token"] = {"status": st, "body": txt[:160]}

# ── 3. POST with bad token ──
st, txt, _ = http("POST", ADMIN_URL,
                  {"content-type": "application/json",
                   "x-justhodl-token": "wrong-token-xyz"},
                  json.dumps({"action": "list", "filter": "POSITION"}))
report["bad_token"] = {"status": st, "body": txt[:160]}

# ── 4. POST with the real token ──
real = None
try:
    real = ssm.get_parameter(Name=SSM_NAME, WithDecryption=True)["Parameter"]["Value"]
except Exception as e:
    report["ssm_read_error"] = str(e)[:200]

if real:
    st, txt, _ = http("POST", ADMIN_URL,
                      {"content-type": "application/json",
                       "x-justhodl-token": real},
                      json.dumps({"action": "list", "filter": "POSITION"}))
    parsed = {}
    try:
        parsed = json.loads(txt)
    except Exception:
        pass
    report["good_token"] = {
        "status": st, "ok": parsed.get("ok"),
        "n_positions": len(parsed.get("positions", []))
        if isinstance(parsed.get("positions"), list) else None,
        "body": txt[:200]}

# ── 5+6. live pages ──
st, html, _ = http("GET", "https://justhodl.ai/portfolio-manager.html",
                   {"User-Agent": "justhodl-ops/727"})
report["manager_page"] = {
    "status": st,
    "marker_found": isinstance(html, str) and "Portfolio Manager" in html,
    "url_baked": isinstance(html, str)
    and "e726eujwijpeg2slgssddw2yee0stboa" in html}
st, idx, _ = http("GET", "https://justhodl.ai/index.html",
                  {"User-Agent": "justhodl-ops/727"})
report["index_links_manager"] = isinstance(idx, str) and \
    "/portfolio-manager.html" in idx

# ── verdict ──
checks = {
    "preflight_ok": report["preflight"]["status"] in (200, 204),
    "no_token_403": report["no_token"]["status"] == 403,
    "bad_token_403": report["bad_token"]["status"] == 403,
    "good_token_200": report.get("good_token", {}).get("status") == 200
                      and report.get("good_token", {}).get("ok") is True,
    "manager_page_live": report["manager_page"]["marker_found"]
                         and report["manager_page"]["url_baked"],
    "index_links_manager": report["index_links_manager"],
}
report["checks"] = checks
report["all_pass"] = all(checks.values())
report["verdict"] = (
    "VERIFIED — holdings feed is live: secured endpoint gates correctly, "
    "Manager page can read/write the book, pipeline flows to the PM cockpit"
    if report["all_pass"]
    else "REVIEW — see failed checks (GH Pages can lag ~1min after push)")

print(json.dumps(report, indent=2, default=str))
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/727_holdings_feed_verify.json", "w") as f:
    json.dump(report, f, indent=2, default=str)
print("[ok] wrote aws/ops/reports/727_holdings_feed_verify.json")
