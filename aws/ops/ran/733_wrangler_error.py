"""ops/733 — pull the actual wrangler-deploy error from the CI job log.

ops/732 found the 'Deploy each changed Worker' step failing but couldn't
read the log (the /logs endpoint 302-redirects to an Azure blob URL and
the auth header must NOT be forwarded). This handles the redirect
correctly and extracts the wrangler error.
"""
import json, os, urllib.request, urllib.error
from datetime import datetime, timezone

TOKEN = os.environ.get("GH_API_TOKEN", "")
REPO = "ElMooro/si"
API = "https://api.github.com"
HDRS = {"Authorization": f"Bearer {TOKEN}",
        "Accept": "application/vnd.github+json",
        "User-Agent": "justhodl-ops/733"}

report = {"ops": 733, "ts": datetime.now(timezone.utc).isoformat(),
          "subject": "wrangler deploy error extraction"}


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None  # surface the 302 instead of following it


def gh_json(path):
    req = urllib.request.Request(API + path, headers=HDRS)
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return json.loads(r.read().decode("utf-8", "replace"))
    except Exception as e:
        return {"error": str(e)[:200]}


def job_log(job_id):
    """GET /jobs/{id}/logs -> 302 to a presigned blob URL -> fetch plainly."""
    opener = urllib.request.build_opener(_NoRedirect)
    req = urllib.request.Request(f"{API}/repos/{REPO}/actions/jobs/{job_id}/logs",
                                 headers=HDRS)
    loc = None
    try:
        opener.open(req, timeout=30)
    except urllib.error.HTTPError as e:
        if e.code in (301, 302, 303, 307):
            loc = e.headers.get("Location")
        else:
            return None, f"http {e.code}"
    except Exception as e:
        return None, str(e)[:160]
    if not loc:
        return None, "no redirect location"
    try:  # fetch the blob WITHOUT the GitHub auth header
        with urllib.request.urlopen(
                urllib.request.Request(loc, headers={"User-Agent": "ops/733"}),
                timeout=30) as r:
            return r.read().decode("utf-8", "replace"), None
    except Exception as e:
        return None, str(e)[:160]


if not TOKEN:
    report["fatal"] = "GH_API_TOKEN missing"
else:
    wfs = gh_json("/repos/%s/actions/workflows" % REPO)
    wid = next((w["id"] for w in wfs.get("workflows", [])
                if "deploy-workers" in (w.get("path") or "")), None)
    runs = gh_json(f"/repos/{REPO}/actions/workflows/{wid}/runs?per_page=1")
    run_id = runs.get("workflow_runs", [{}])[0].get("id")
    report["run_id"] = run_id
    jobs = gh_json(f"/repos/{REPO}/actions/runs/{run_id}/jobs")
    fjob = next((j for j in jobs.get("jobs", [])
                 if j.get("conclusion") == "failure"), None)
    if fjob:
        report["failing_job"] = {"id": fjob["id"], "name": fjob["name"]}
        log, err = job_log(fjob["id"])
        if log:
            lines = log.splitlines()
            # the deploy step — keep lines mentioning wrangler / errors
            kept = [ln for ln in lines if any(
                k in ln for k in ("wrangler", "Wrangler", "error", "Error",
                                   "ERROR", "✘", "✗", "[ERROR]", "Deploy",
                                   "Authentication", "auth", "permission",
                                   "Permission", "denied", "10000", "10001",
                                   "code:", "workers.dev", "custom_domain",
                                   "route", "Missing", "binding"))]
            report["deploy_log_tail"] = kept[-60:]
            report["raw_tail"] = lines[-25:]
        else:
            report["log_error"] = err

report["verdict"] = "inspect deploy_log_tail for the wrangler failure"
print(json.dumps(report, indent=2, default=str))
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/733_wrangler_error.json", "w") as f:
    json.dump(report, f, indent=2, default=str)
print("[ok] wrote aws/ops/reports/733_wrangler_error.json")
