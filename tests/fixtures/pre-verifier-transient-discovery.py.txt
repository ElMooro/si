#!/usr/bin/env python3
"""Verify a push on GitHub from any lane (2026-09-17): what did the runner do with commit <sha>?

Lists every workflow run GitHub started for that commit, each job's step conclusions, and, for
failures, the failing step -- the same data the Actions UI shows, without the UI (job LOGS are
served from an Azure blob host that most sandboxes cannot reach; the committed report in
aws/ops/reports/latest/ and the receipt are the readable evidence).

  python3 scripts/verify_push.py <sha>            # runs for that commit (prefix ok inside a checkout)
  python3 scripts/verify_push.py <sha> --wait 20  # poll up to 20 minutes until every run completes
  python3 scripts/verify_push.py --latest         # runs for the most recent commit on main

Then prove the AWS side:  python3 scripts/verify_release.py <function> --commit <sha> --data data/<engine>.json

Token: env GITHUB_TOKEN / GH_TOKEN / JH_PAT, or a file at ~/.jh_pat or /root/.jh_pat. Never printed.
Without one the public API still answers, at 60 requests/hour per egress IP.
Exit 0 = every run for the commit completed successfully (or the commit legitimately started none),
1 = a run failed / was cancelled, 2 = still in progress (no --wait, or --wait expired).
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

API = "https://api.github.com"
UA = "justhodl-verify-push/1.0"


def token() -> str | None:
    for var in ("GITHUB_TOKEN", "GH_TOKEN", "JH_PAT"):
        if os.environ.get(var):
            return os.environ[var].strip()
    for p in (Path.home() / ".jh_pat", Path("/root/.jh_pat")):
        if p.is_file():
            return p.read_text().strip()
    return None


def get(path: str):
    req = urllib.request.Request(API + path, headers={"User-Agent": UA, "Accept": "application/vnd.github+json"})
    t = token()
    if t:
        req.add_header("Authorization", "token " + t)
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read())


def full_sha(repo: str, ref: str) -> str:
    if len(ref) == 40:
        return ref
    try:
        out = subprocess.run(["git", "rev-parse", "--verify", ref + "^{commit}"], capture_output=True, text=True, timeout=10)
        if out.returncode == 0 and len(out.stdout.strip()) == 40:
            return out.stdout.strip()
    except Exception:  # noqa: BLE001
        pass
    return get(f"/repos/{repo}/commits/{ref}")["sha"]


def runs_for(repo: str, sha: str, everything: bool = False) -> list[dict]:
    """Runs the commit itself caused (push / workflow_dispatch). A commit that sits at HEAD for hours also
    collects every scheduled run and their workflow_run children -- those say nothing about the push,
    so they are hidden unless --all is given."""
    runs = []
    for page in (1, 2, 3):
        batch = get(f"/repos/{repo}/actions/runs?head_sha={sha}&per_page=100&page={page}").get("workflow_runs", [])
        runs.extend(batch)
        if len(batch) < 100:
            break
    if everything:
        return runs
    return [r for r in runs if r["event"] in ("push", "workflow_dispatch")]


def steps_for(repo: str, run_id: int) -> list[dict]:
    return get(f"/repos/{repo}/actions/runs/{run_id}/jobs?per_page=50").get("jobs", [])


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("sha", nargs="?", help="commit sha (prefix ok inside a checkout)")
    ap.add_argument("--latest", action="store_true", help="use the most recent commit on main")
    ap.add_argument("--repo", default="ElMooro/si")
    ap.add_argument("--wait", type=float, default=0, help="minutes to poll until every run completes")
    ap.add_argument("--all", action="store_true", help="also list scheduled / workflow_run runs that happened while this commit was HEAD")
    args = ap.parse_args(argv)
    if not args.sha and not args.latest:
        ap.error("give a sha or --latest")
    try:
        sha = get(f"/repos/{args.repo}/commits/main")["sha"] if args.latest else full_sha(args.repo, args.sha)
        commit = get(f"/repos/{args.repo}/commits/{sha}")
    except urllib.error.HTTPError as exc:
        print(f"GitHub API {exc.code}: {exc.reason} (token present: {bool(token())})")
        return 2
    msg = commit["commit"]["message"].splitlines()[0]
    author = commit["commit"]["author"]["name"]
    print(f"{sha[:10]} {author} | {msg[:110]}")
    if "[skip-deploy]" in commit["commit"]["message"]:
        print("   note: [skip-deploy] -- deploy-lambdas and pages exit early by design")

    deadline = time.time() + args.wait * 60
    while True:
        runs = runs_for(args.repo, sha, args.all)
        pending = [r for r in runs if r["status"] != "completed"]
        if not pending or time.time() >= deadline:
            break
        print(f"   waiting: {len(pending)} run(s) in progress ...")
        time.sleep(30)

    if not runs:
        print("   no push/dispatch run for this commit (no watched path changed, or GitHub has not registered it yet -- re-run in a minute; --all shows scheduled runs)")
        return 0
    worst = 0
    for r in sorted(runs, key=lambda x: x["created_at"]):
        wf = r["path"].rsplit("/", 1)[-1]
        concl = r["conclusion"] or r["status"]
        print(f"   {wf:<28} {r['event']:<17} {concl:<12} run {r['id']}  {r['html_url']}")
        if r["status"] != "completed":
            worst = max(worst, 2)
            continue
        if r["conclusion"] != "success":
            worst = 1
            for job in steps_for(args.repo, r["id"]):
                bad = [s for s in job.get("steps", []) if s.get("conclusion") not in (None, "success", "skipped")]
                for s in bad:
                    print(f"      job '{job['name']}' step {s['number']}: {s['conclusion']} -- {s['name']}")
    files = [f["filename"] for f in commit.get("files", [])]
    ops = [f for f in files if f.startswith("aws/ops/pending/")]
    if ops:
        print("   ops script(s) in this commit -> read aws/ops/reports/latest/<N>_<slug>.md after run-ops commits it (git pull)")
    lambdas = sorted({f.split("/")[2] for f in files if f.startswith("aws/lambdas/") and f.count("/") >= 3})
    for fn in lambdas:
        print(f"   engine touched: {fn} -> python3 scripts/verify_release.py {fn} --commit {sha[:10]}")
    print({0: "ALL RUNS GREEN", 1: "A RUN FAILED", 2: "STILL IN PROGRESS"}[worst])
    return worst


if __name__ == "__main__":
    sys.exit(main())
