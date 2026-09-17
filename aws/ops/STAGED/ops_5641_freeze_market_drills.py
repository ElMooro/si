"""ops 5641 -- freeze the market drills (Claude, 2026-09-17). Direct lane. Writes: drill objects create-if-absent under
factory/holdout/drills/{holdout,train}/ + factory/holdout/market-manifest.json (create-if-absent). Never touches the main
manifest. Then sits the holdout exam once (scripts/factory_market_exam.py) so the first market-skill number exists.
"""
from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve()
sys.path.insert(0, str(HERE.parents[1]))
from ops_report import report  # noqa: E402

REPO = HERE.parents[3]


def main():
    with report("5641_freeze_market_drills") as r:
        r.heading("ops 5641 -- freeze the anonymized market drills, then sit the holdout exam")
        r.section("1. Freeze")
        p = subprocess.run([sys.executable, "scripts/factory_holdout.py", "--market-only"], cwd=REPO, capture_output=True, text=True, timeout=1500)
        for line in (p.stdout or "").strip().splitlines()[-40:]:
            r.log(line[:300])
        if p.returncode != 0:
            r.fail("freeze exited %s: %s" % (p.returncode, (p.stderr or "")[-800:]))
            sys.exit(1)
        r.ok("market drills frozen (or already present)")
        for split, cap in (("holdout", "120"), ("train", "60")):
            r.section("2. %s exam" % split)
            p = subprocess.run([sys.executable, "scripts/factory_market_exam.py", "--split", split, "--max", cap, "--wait-min", "12"], cwd=REPO, capture_output=True, text=True, timeout=1500)
            for line in (p.stdout or "").strip().splitlines()[-12:]:
                r.log(line[:700])
            if p.returncode != 0:
                r.fail("%s exam exited %s: %s" % (split, p.returncode, (p.stderr or "")[-800:]))
                continue
            r.ok("%s exam written to factory/exams/market/" % split)
        r.section("3. Page projection")
        import boto3
        lam = boto3.client("lambda", region_name="us-east-1")
        lam.invoke(FunctionName="justhodl-ai", InvocationType="Event", Payload=json.dumps({"mode": "inventory"}).encode())
        r.ok("inventory tick queued: data/ai.json.market_exam refreshes within a minute")


if __name__ == "__main__":
    main()
