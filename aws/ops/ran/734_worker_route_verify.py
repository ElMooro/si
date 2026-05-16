"""ops/734 — confirm the Node-22 fix landed and the Worker route is live.

Two parts:
  1. GitHub Actions API — conclusion of the latest deploy-workers run
     (should now be `success` after the Node 20→22 fix).
  2. Probe justhodl-ai-proxy.raafouis.workers.dev/portfolio-admin (the
     workers.dev hostname bypasses the zone Bot Fight Mode that gave
     ops/729 a Cloudflare 1010): preflight, no-PIN, bad-PIN, real-PIN.
"""
import json, os, urllib.request, urllib.error
from datetime import datetime, timezone
import boto3

REPO = "ElMooro/si"
API = "https://api.github.com"
GH_TOKEN = os.environ.get("GH_API_TOKEN", "")
GH_HDRS = {"Authorization": f"Bearer {GH_TOKEN}",
           "Accept": "application/vnd.github+json",
           "User-Agent": "justhodl-ops/734"}

WORKER_URL = "https://justhodl-ai-proxy.raafouis.workers.dev/portfolio-admin"
PIN_SSM = "/justhodl/portfolio-admin/manager-pass"
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")
ssm = boto3.client("ssm", region_name="us-east-1")

report = {"ops": 734, "ts": datetime.now(timezone.utc).isoformat(),
          "subject": "deploy-workers Node fix + Worker route verify"}


def gh(path):
    req = urllib.request.Request(API + path, headers=GH_HDRS)
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return r.status, json.loads(r.read().decode("utf-8", "replace"))
    except urllib.error.HTTPError as e:
        return e.code, {"error": e.read().decode("utf-8", "replace")[:200]}
    except Exception as e:
        return None, {"error": str(e)[:200]}


def http(method, url, headers=None, body=None):
    data = body.encode() if isinstance(body, str) else body
    h = {"User-Agent": UA, "Origin": "https://justhodl.ai"}
    h.update(headers or {})
    req = urllib.request.Request(url, data=data, method=method, headers=h)
    try:
        with urllib.request.urlopen(req, timeout=25) as r:
            return r.status, r.read().decode("utf-8", "replace"), dict(r.headers)
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode("utf-8", "replace"), dict(e.headers or {})
    except Exception as e:
        return None, str(e)[:200], {}


# ── 1. deploy-workers run conclusion ──
if GH_TOKEN:
    st, runs = gh(f"/repos/{REPO}/actions/workflows/deploy-workers.yml/runs?per_page=3")
    rr = runs.get("workflow_runs", []) if isinstance(runs, dict) else []
    report["recent_deploy_runs"] = [
        {"created": r.get("created_at"), "conclusion": r.get("conclusion"),
         "head": (r.get("head_commit") or {}).get("message", "")[:60]}
        for r in rr]
    report["latest_deploy_conclusion"] = rr[0].get("conclusion") if rr else None
else:
    report["latest_deploy_conclusion"] = "GH_API_TOKEN missing"

# ── 2. Worker route probe ──
LIST = json.dumps({"action": "list", "filter": "POSITION"})

st, _, hdrs = http("OPTIONS", WORKER_URL, {
    "Access-Control-Request-Method": "POST",
    "Access-Control-Request-Headers": "content-type,x-mgr-pass"})
allow_h = (hdrs.get("Access-Control-Allow-Headers")
           or hdrs.get("access-control-allow-headers") or "")
report["preflight"] = {"status": st, "allow_headers": allow_h}

st, txt, _ = http("POST", WORKER_URL, {"content-type": "application/json"}, LIST)
report["no_pin"] = {"status": st, "body": txt[:200]}

st, txt, _ = http("POST", WORKER_URL,
                  {"content-type": "application/json",
                   "x-mgr-pass": "wrong-pin-xyz"}, LIST)
report["bad_pin"] = {"status": st, "body": txt[:200]}

pin = None
try:
    pin = ssm.get_parameter(Name=PIN_SSM, WithDecryption=True)["Parameter"]["Value"]
except Exception as e:
    report["pin_ssm_error"] = str(e)[:200]
if pin:
    st, txt, _ = http("POST", WORKER_URL,
                      {"content-type": "application/json",
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
        "body": txt[:240]}

checks = {
    "deploy_workers_success": report.get("latest_deploy_conclusion") == "success",
    "preflight_allows_mgr_pass":
        "x-mgr-pass" in report["preflight"]["allow_headers"].lower(),
    "no_pin_403": report["no_pin"]["status"] == 403,
    "bad_pin_403": report["bad_pin"]["status"] == 403,
    "good_pin_200": report.get("good_pin", {}).get("status") == 200
                    and report.get("good_pin", {}).get("ok") is True,
}
report["checks"] = checks
report["all_pass"] = all(checks.values())
report["verdict"] = (
    "VERIFIED — deploy-workers green, Worker /portfolio-admin route live: "
    "PIN gate enforces, infra token injected, book reachable through the Worker"
    if report["all_pass"]
    else "REVIEW — see checks")

print(json.dumps(report, indent=2, default=str))
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/734_worker_route_verify.json", "w") as f:
    json.dump(report, f, indent=2, default=str)
print("[ok] wrote aws/ops/reports/734_worker_route_verify.json")
