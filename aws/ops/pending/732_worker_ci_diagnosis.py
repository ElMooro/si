"""ops/732 — read deploy-workers.yml CI logs to find the real failure.

Uses the GITHUB_TOKEN exposed by run-ops.yml (env GH_API_TOKEN, with the
actions:read permission) to query the Actions API: list recent
deploy-workers runs, get the latest run's per-step conclusions, and pull
the log text of the failing step so the error is visible.
"""
import json, os, io, zipfile, urllib.request, urllib.error
from datetime import datetime, timezone

TOKEN = os.environ.get("GH_API_TOKEN", "")
REPO = "ElMooro/si"
API = "https://api.github.com"
HDRS = {"Authorization": f"Bearer {TOKEN}",
        "Accept": "application/vnd.github+json",
        "User-Agent": "justhodl-ops/732"}

report = {"ops": 732, "ts": datetime.now(timezone.utc).isoformat(),
          "subject": "deploy-workers CI log diagnosis"}


def gh(path, raw=False):
    req = urllib.request.Request(API + path, headers=HDRS)
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            data = r.read()
            return r.status, (data if raw else
                              json.loads(data.decode("utf-8", "replace")))
    except urllib.error.HTTPError as e:
        return e.code, {"error": e.read().decode("utf-8", "replace")[:300]}
    except Exception as e:
        return None, {"error": str(e)[:240]}


if not TOKEN:
    report["fatal"] = "GH_API_TOKEN not present in env"
else:
    # find workflow
    st, wf = gh(f"/repos/{REPO}/actions/workflows")
    wid = None
    for w in (wf.get("workflows", []) if isinstance(wf, dict) else []):
        if "deploy-workers" in (w.get("path") or ""):
            wid = w.get("id")
            report["workflow"] = {"id": wid, "state": w.get("state"),
                                  "path": w.get("path")}
            break
    if not wid:
        report["fatal"] = "deploy-workers workflow not found"
        report["workflows"] = [w.get("path") for w in wf.get("workflows", [])] \
            if isinstance(wf, dict) else wf
    else:
        st, runs = gh(f"/repos/{REPO}/actions/workflows/{wid}/runs?per_page=6")
        runs_list = runs.get("workflow_runs", []) if isinstance(runs, dict) else []
        report["recent_runs"] = [{
            "id": r.get("id"), "created": r.get("created_at"),
            "event": r.get("event"), "status": r.get("status"),
            "conclusion": r.get("conclusion"),
            "head": (r.get("head_commit") or {}).get("message", "")[:60],
        } for r in runs_list]

        if runs_list:
            run_id = runs_list[0]["id"]
            st, jobs = gh(f"/repos/{REPO}/actions/runs/{run_id}/jobs")
            jlist = jobs.get("jobs", []) if isinstance(jobs, dict) else []
            failing_job_id = None
            jobs_summary = []
            for j in jlist:
                steps = [{"name": s.get("name"), "conclusion": s.get("conclusion")}
                         for s in j.get("steps", [])]
                jobs_summary.append({"job": j.get("name"),
                                     "conclusion": j.get("conclusion"),
                                     "steps": steps})
                if j.get("conclusion") not in ("success", "skipped", None):
                    failing_job_id = j.get("id")
            report["latest_run_jobs"] = jobs_summary

            # pull the failing job's log text (zip of step logs)
            if failing_job_id:
                st, raw = gh(f"/repos/{REPO}/actions/jobs/{failing_job_id}/logs",
                             raw=True)
                if isinstance(raw, (bytes, bytearray)):
                    try:
                        txt = raw.decode("utf-8", "replace")
                    except Exception:
                        txt = ""
                    # job logs come back as plain text (one stream)
                    lines = [ln for ln in txt.splitlines()
                             if any(k in ln for k in
                                    ("error", "Error", "ERROR", "✘", "fail",
                                     "Fail", "wrangler", "Wrangler",
                                     "Authentication", "denied", "Missing"))]
                    report["failing_log_excerpt"] = lines[-40:]
                else:
                    report["log_fetch_error"] = raw

report["verdict"] = "inspect latest_run_jobs + failing_log_excerpt"
print(json.dumps(report, indent=2, default=str))
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/732_worker_ci_diagnosis.json", "w") as f:
    json.dump(report, f, indent=2, default=str)
print("[ok] wrote aws/ops/reports/732_worker_ci_diagnosis.json")
