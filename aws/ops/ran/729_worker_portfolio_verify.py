"""ops/729 — verify the Worker-routed portfolio-admin path.

Confirms the full chain:
  browser → api.justhodl.ai/portfolio-admin (Worker, PIN-gated, injects
  the Lambda infra token) → justhodl-portfolio-admin → DynamoDB.

Checks:
  1. CORS preflight on the Worker route allows x-mgr-pass.
  2. POST with no PIN  → 403 (Worker gates).
  3. POST with bad PIN → 403.
  4. POST with the real PIN + action=list → 200, ok:true
     (Worker PIN check → token injection → Lambda → DDB all working).
  5. The Lambda Function URL still rejects un-tokened direct hits
     (defence in depth intact — the Worker is the only way in).
  6. portfolio-manager.html is live, points at the Worker, carries no
     raw Lambda URL.

Read-only — uses the `list` action, never writes to the book.
"""
import json, os, urllib.request, urllib.error
from datetime import datetime, timezone
import boto3

REGION = "us-east-1"
WORKER_URL = "https://api.justhodl.ai/portfolio-admin"
LAMBDA_URL = "https://e726eujwijpeg2slgssddw2yee0stboa.lambda-url.us-east-1.on.aws/"
PIN_SSM = "/justhodl/portfolio-admin/manager-pass"
ssm = boto3.client("ssm", region_name=REGION)

report = {"ops": 729, "ts": datetime.now(timezone.utc).isoformat(),
          "subject": "Worker-routed portfolio-admin E2E"}


def http(method, url, headers=None, body=None):
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


LIST = json.dumps({"action": "list", "filter": "POSITION"})

# ── 1. preflight ──
st, _, hdrs = http("OPTIONS", WORKER_URL, {
    "Origin": "https://justhodl.ai",
    "Access-Control-Request-Method": "POST",
    "Access-Control-Request-Headers": "content-type,x-mgr-pass"})
allow_h = (hdrs.get("Access-Control-Allow-Headers")
           or hdrs.get("access-control-allow-headers") or "")
report["preflight"] = {"status": st, "allow_headers": allow_h}

# ── 2. no PIN ──
st, txt, _ = http("POST", WORKER_URL,
                  {"content-type": "application/json",
                   "Origin": "https://justhodl.ai"}, LIST)
report["no_pin"] = {"status": st, "body": txt[:160]}

# ── 3. bad PIN ──
st, txt, _ = http("POST", WORKER_URL,
                  {"content-type": "application/json",
                   "Origin": "https://justhodl.ai",
                   "x-mgr-pass": "wrong-pin-xyz"}, LIST)
report["bad_pin"] = {"status": st, "body": txt[:160]}

# ── 4. real PIN ──
pin = None
try:
    pin = ssm.get_parameter(Name=PIN_SSM, WithDecryption=True)["Parameter"]["Value"]
except Exception as e:
    report["pin_ssm_error"] = str(e)[:200]
if pin:
    st, txt, _ = http("POST", WORKER_URL,
                      {"content-type": "application/json",
                       "Origin": "https://justhodl.ai",
                       "x-mgr-pass": pin}, LIST)
    parsed = {}
    try:
        parsed = json.loads(txt)
    except Exception:
        pass
    report["good_pin"] = {
        "status": st, "ok": parsed.get("ok"),
        "n_positions": len(parsed.get("positions", []))
        if isinstance(parsed.get("positions"), list) else None,
        "body": txt[:200]}

# ── 5. Lambda URL still gated directly ──
st, txt, _ = http("POST", LAMBDA_URL, {"content-type": "application/json"}, LIST)
report["lambda_direct_no_token"] = {"status": st, "body": txt[:120]}

# ── 6. live page ──
st, html, _ = http("GET", "https://justhodl.ai/portfolio-manager.html",
                   {"User-Agent": "justhodl-ops/729"})
report["manager_page"] = {
    "status": st,
    "routes_through_worker": isinstance(html, str)
    and "api.justhodl.ai/portfolio-admin" in html,
    "no_raw_lambda_url": isinstance(html, str)
    and "e726eujwijpeg2slgssddw2yee0stboa" not in html}

checks = {
    "preflight_allows_mgr_pass": "x-mgr-pass" in report["preflight"]["allow_headers"].lower(),
    "no_pin_403": report["no_pin"]["status"] == 403,
    "bad_pin_403": report["bad_pin"]["status"] == 403,
    "good_pin_200": report.get("good_pin", {}).get("status") == 200
                    and report.get("good_pin", {}).get("ok") is True,
    "lambda_still_gated": report["lambda_direct_no_token"]["status"] == 403,
    "page_worker_routed": report["manager_page"]["routes_through_worker"]
                          and report["manager_page"]["no_raw_lambda_url"],
}
report["checks"] = checks
report["all_pass"] = all(checks.values())
report["verdict"] = (
    "VERIFIED — portfolio-admin writes route through the Worker; the AWS "
    "infra token is vaulted, the Manager PIN gates access, the book is reachable"
    if report["all_pass"]
    else "REVIEW — see failed checks (Worker deploy / GH Pages can lag)")

print(json.dumps(report, indent=2, default=str))
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/729_worker_portfolio_verify.json", "w") as f:
    json.dump(report, f, indent=2, default=str)
print("[ok] wrote aws/ops/reports/729_worker_portfolio_verify.json")
