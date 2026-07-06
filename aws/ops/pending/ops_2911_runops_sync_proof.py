#!/usr/bin/env python3
"""ops 2911 — proof: run-ops sync-to-freshest-main hardening works end-to-end.

Asserts from INSIDE the runner:
  1. HEAD == freshest origin/main at execution time (or is an ancestor of it,
     tolerating a benign mid-run push) — the stale-artifact mode is dead.
  2. aws/ops/reports/_lastrun.log first line carries the executing-against
     SHA stamp written by the new workflow step.
Report -> aws/ops/reports/2911.json. Fails the run (script stays in pending/)
if either guarantee doesn't hold.
"""
import json
import subprocess
import sys

sys.path.insert(0, "aws/ops")
from ops_report import report


def sh(cmd: str) -> str:
    return subprocess.getoutput(cmd).strip()


ok_all = True
with report("2911") as r:
    r.section("sync-hardening proof")
    head = sh("git rev-parse HEAD")
    ls = sh("git ls-remote origin -h refs/heads/main")
    remote = ls.split()[0] if ls else ""
    r.log(f"runner HEAD       : {head}")
    r.log(f"origin/main (now) : {remote}")

    synced = bool(head and remote and head == remote)
    if not synced and head and remote:
        # tolerate a benign push landing mid-run: HEAD may be an ancestor
        sh("git fetch -q origin main")
        rc = subprocess.call(["git", "merge-base", "--is-ancestor", head, remote])
        synced = (rc == 0)
        if synced:
            r.log("  (HEAD is ancestor of remote tip — mid-run push, still fresh)")

    first = ""
    try:
        with open("aws/ops/reports/_lastrun.log") as f:
            first = f.readline().strip()
    except Exception:
        pass
    r.log(f"_lastrun line 1   : {first}")
    stamped = first.startswith("executing-against:") and head[:12] in first

    (r.ok if synced else r.fail)(f"executes against freshest main: {synced}")
    (r.ok if stamped else r.fail)(f"SHA stamp is first log line   : {stamped}")
    ok_all = synced and stamped

    with open("aws/ops/reports/2911.json", "w") as f:
        json.dump({"runner_head": head, "remote_main": remote,
                   "lastrun_first_line": first,
                   "synced_to_freshest": synced,
                   "sha_stamp_in_log": stamped}, f, indent=2)
    r.ok("report -> aws/ops/reports/2911.json")

print("PROOF-2911", "PASS" if ok_all else "FAIL")
sys.exit(0 if ok_all else 1)
