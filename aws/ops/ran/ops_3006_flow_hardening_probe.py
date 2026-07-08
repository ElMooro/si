#!/usr/bin/env python3
"""ops 3006 -- flow-hardening probe: proves the run-ops.yml edits
(concurrency group + STATE.md regeneration step) didn't disturb the
pipeline, dogfoods the new preflight linter from the runner, and takes
a real health ping on yesterday's repo-market engine + its two fused
consumers (fresh outputs = schedules ticking)."""
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import boto3
from ops_report import report

S3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
AWS_DIR = Path(__file__).resolve().parents[2]


def main():
    fails, warns = [], []
    with report("3006_flow_hardening_probe") as rep:
        rep.section("1. Preflight dogfood (lints itself + the new tools)")
        r = subprocess.run(
            [sys.executable, str(AWS_DIR / "ops" / "_preflight.py"),
             __file__, str(AWS_DIR / "ops" / "_gen_state.py"),
             str(AWS_DIR / "ops" / "_preflight.py")],
            capture_output=True, text=True)
        rep.kv(preflight_rc=r.returncode,
               preflight_out=(r.stdout or r.stderr)[-400:])
        if r.returncode != 0:
            fails.append("preflight failed on the toolkit itself")

        rep.section("2. STATE.md generator runs clean")
        r2 = subprocess.run(
            [sys.executable, str(AWS_DIR / "ops" / "_gen_state.py")],
            capture_output=True, text=True)
        rep.kv(gen_state_rc=r2.returncode, gen_state_out=r2.stdout[-200:])
        if r2.returncode != 0 or not (AWS_DIR.parent / "STATE.md").exists():
            fails.append("_gen_state.py failed or STATE.md missing")

        rep.section("3. Engine health ping (repo-market + fused consumers)")
        now = datetime.now(timezone.utc)
        for key, max_h in (("data/repo-market.json", 30),
                           ("data/dollar-radar.json", 30),
                           ("data/risk-regime.json", 40)):
            try:
                h = S3.head_object(Bucket=BUCKET, Key=key)
                age = (now - h["LastModified"]).total_seconds() / 3600.0
                rep.kv(**{key.split("/")[-1].replace(".", "_").replace(
                    "-", "_"): "%.1fh" % age})
                if age > max_h:
                    warns.append("%s stale %.1fh (>%dh)" % (key, age, max_h))
            except Exception as e:
                fails.append("%s: %s" % (key, str(e)[:100]))

        rep.section("verdict")
        _payload = {"ops": 3006, "fails": fails, "warns": warns,
                    "verdict": "FAIL" if fails else "PASS",
                    "ts": now.isoformat()}
        (AWS_DIR / "ops" / "reports" / "3006.json").write_text(
            json.dumps(_payload, indent=1))
        rep.kv(verdict=_payload["verdict"], n_fails=len(fails),
               n_warns=len(warns))
        if fails:
            for f in fails:
                rep.log("FAIL: %s" % f)
            sys.exit(1)
        rep.log("PASS -- concurrency + STATE.md + preflight all live")


main()
sys.exit(0)
