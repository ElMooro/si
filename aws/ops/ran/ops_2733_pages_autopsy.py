"""ops 2733 — PAGES DEPLOY ROOT CAUSE (two vectors, auth-redirect fixed).

Deploy step has failed 4x (incl API rerun). Vector A: GET /pages/builds/latest
— carries the build error message directly. Vector B: newest failed deploy-job
log via redirect handler that STRIPS Authorization before following GitHub's
302 to Azure SAS (prior 403 = replayed auth header). Diagnosis-only, fast.
Report: aws/ops/reports/2733_pages_autopsy.json.
"""
import os, json, urllib.request
from datetime import datetime, timezone

GH = "https://api.github.com/repos/ElMooro/si"
TOK = os.environ.get("GH_API_TOKEN") or os.environ.get("GITHUB_TOKEN") or ""
R = {"ops": 2733, "ts": datetime.now(timezone.utc).isoformat()}

class _StripAuthRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        new = super().redirect_request(req, fp, code, msg, headers, newurl)
        if new is not None and "api.github.com" not in newurl:
            new.headers.pop("Authorization", None)
        return new
OPENER = urllib.request.build_opener(_StripAuthRedirect)

def gh(path, raw=False):
    hdr = {"User-Agent": "jh-ops", "Accept": "application/vnd.github+json"}
    if TOK: hdr["Authorization"] = "token " + TOK
    with OPENER.open(urllib.request.Request(GH + path, headers=hdr), timeout=45) as r:
        b = r.read()
        return b if raw else json.loads(b)

print("== A) pages builds latest ==")
try:
    pb = gh("/pages/builds/latest")
    R["pages_build"] = {"status": pb.get("status"), "error": (pb.get("error") or {}).get("message"),
                        "commit": (pb.get("commit") or "")[:8], "created": pb.get("created_at")}
    print("  ", json.dumps(R["pages_build"]))
except Exception as e:
    R["pages_build"] = {"err": str(e)[:140]}; print("  ", R["pages_build"])

print("== B) newest failed deploy-job log ==")
try:
    runs = gh("/actions/runs?per_page=20")["workflow_runs"]
    fr = next(r for r in runs if "pages" in r["name"].lower() and r.get("conclusion") == "failure")
    R["run"] = {"id": fr["id"], "sha": fr["head_sha"][:8], "attempt": fr["run_attempt"]}
    jobs = gh("/actions/runs/%d/jobs" % fr["id"])["jobs"]
    jid = next(j["id"] for j in jobs if j["name"] == "deploy")
    log = gh("/actions/jobs/%d/logs" % jid, raw=True).decode("utf-8", "ignore")
    errs = [ln.strip() for ln in log.splitlines()
            if any(k in ln.lower() for k in ("error", "fail", "invalid", "exceed", "denied",
                                             "unable", "timeout", "artifact")) and "0 error" not in ln.lower()]
    tail = [ln.strip() for ln in log.splitlines() if ln.strip()][-14:]
    R["deploy_log"] = {"job": jid, "bytes": len(log), "errors": errs[:14], "tail": tail}
    print("  job %d log %dB" % (jid, len(log)))
    for ln in errs[:12]: print("   E:", ln[:160])
    if not errs:
        for ln in tail: print("   T:", ln[:160])
except Exception as e:
    R["deploy_log"] = {"err": str(e)[:160]}; print("  ", R["deploy_log"])

os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/2733_pages_autopsy.json", "w") as f:
    json.dump(R, f, indent=1, default=str)
assert R.get("pages_build", {}).get("error") is not None or (R.get("deploy_log", {}).get("errors") or R.get("deploy_log", {}).get("tail")), "no diagnostic surface reached"
print("OPS 2733 COMPLETE — root cause captured")
