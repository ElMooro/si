"""One bounded GitHub dispatch retries the complete failed core source range. No AWS API calls."""
from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
BASE_SHA = "c167a7541b9b02f849343bad1b4a231e758cadd8"
WORKFLOW = "deploy-lambdas.yml"
REPORT = ROOT / "aws/ops/reports/latest/ops_5233_core_recovery_dispatch.json"


def dispatch_payload(commit):
    if not re.fullmatch(r"[a-f0-9]{40}", commit):
        raise ValueError("Recovery requires an exact workflow commit")
    return {"ref": "main", "inputs": {"base_sha": BASE_SHA, "expected_sha": commit}}


def request(repository, token, path, payload=None):
    req = urllib.request.Request("https://api.github.com/repos/" + repository + path,
        data=None if payload is None else json.dumps(payload).encode(),
        headers={"Authorization": "Bearer " + token, "Accept": "application/vnd.github+json",
                 "Content-Type": "application/json", "X-GitHub-Api-Version": "2026-03-10"},
        method="GET" if payload is None else "POST")
    with urllib.request.urlopen(req, timeout=45) as response:
        body = response.read()
        return response.status, json.loads(body) if body else {}


def main():
    if REPORT.exists() and json.loads(REPORT.read_text()).get("request_sent"):
        print("Recovery dispatch was already attempted; no duplicate request sent")
        return
    repository = os.environ.get("GITHUB_REPOSITORY", "")
    token = os.environ.get("GH_API_TOKEN", "")
    if not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", repository) or not token:
        raise RuntimeError("GitHub repository and runner identity are required")
    commit = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
    payload = dispatch_payload(commit)
    subprocess.run(["git", "fetch", "-q", "--depth=250", "origin", BASE_SHA, commit], cwd=ROOT, check=True)
    subprocess.run(["git", "merge-base", "--is-ancestor", BASE_SHA, commit], cwd=ROOT, check=True)
    report = {"operation": "5233", "workflow": WORKFLOW, "workflow_sha": commit, "base_sha": BASE_SHA,
              "requested_at": datetime.now(timezone.utc).isoformat(), "request_sent": True,
              "status": "REQUESTING", "aws_calls": 0}
    REPORT.parent.mkdir(parents=True, exist_ok=True)
    REPORT.write_text(json.dumps(report, indent=2) + "\n")
    try:
        status, response = request(repository, token, "/actions/workflows/" + WORKFLOW + "/dispatches", payload)
        if status not in (200, 204):
            raise RuntimeError("GitHub did not accept the recovery dispatch")
        report.update(status="DISPATCHED", http_status=status)
        run_id = response.get("workflow_run_id")
        if isinstance(run_id, int) and not isinstance(run_id, bool):
            report["workflow_run_id"] = run_id
            report["run_url"] = "https://github.com/" + repository + "/actions/runs/" + str(run_id)
            _, run = request(repository, token, "/actions/runs/" + str(run_id))
            report["actual_workflow_sha"] = run.get("head_sha")
            report["workflow_sha_matches"] = run.get("head_sha") == commit
        # The workflow itself rejects an expected_sha mismatch before any AWS mutation.
    except Exception as exc:
        report.update(status="DISPATCH_STATUS_UNCERTAIN", error_type=type(exc).__name__)
        REPORT.write_text(json.dumps(report, indent=2) + "\n")
        raise RuntimeError("Recovery dispatch did not complete; inspect metadata report, no automatic duplicate") from None
    REPORT.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, sort_keys=True))


if __name__ == "__main__":
    sys.path.insert(0, str(ROOT / "aws/ops"))
    from ops_report import report as ops_report
    with ops_report("ops_5233_retry_audit_core_release") as rep:
        try:
            main()
            rep.ok("Exact-revision recovery dispatch recorded; deployment remains separately verified")
        except Exception as exc:
            rep.fail("Recovery dispatch failed or is uncertain: " + type(exc).__name__)
            sys.exit(1)
