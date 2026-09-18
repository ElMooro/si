"""ops 5824 -- COVID-2020 drills (Claude, 2026-09-18). Direct lane.
1. scripts/backfill_etf_history.py --from 2020-01-02 --to 2020-05-29 (FMP daily bars for the five drill ETFs, create-if-absent,
   data/warm/etf-history/grouped/2020/ -- Polygon's entitlement window does not reach 2020)
2. scripts/factory_holdout.py --market-only (idempotent: adds the covid-2020 drills beside the existing ones)
3. scripts/factory_market_exam.py --split holdout (the owned model sits the enlarged held-out set)
4. inventory tick so data/ai.json.market_exam shows the new n
"""
import json, subprocess, sys
from pathlib import Path
import boto3
HERE = Path(__file__).resolve(); sys.path.insert(0, str(HERE.parents[1]))
from ops_report import report  # noqa: E402
REPO = HERE.parents[3]


def run(r, args, timeout):
    p = subprocess.run([sys.executable] + args, cwd=REPO, capture_output=True, text=True, timeout=timeout)
    for line in (p.stdout or "").strip().splitlines()[-14:]:
        r.log(line[:700])
    if p.returncode != 0:
        r.fail("%s exited %s: %s" % (args[0], p.returncode, (p.stderr or "")[-700:]))
    return p.returncode


def main():
    with report("5824_covid_backfill_and_exam") as r:
        r.heading("ops 5824 -- COVID-2020 drills: backfill, freeze, exam")
        r.section("1. Backfill (FMP -> etf-history prefix)")
        if run(r, ["scripts/backfill_etf_history.py", "--from", "2020-01-02", "--to", "2020-05-29"], 600):
            sys.exit(1)
        r.section("2. Freeze the new drills")
        if run(r, ["scripts/factory_holdout.py", "--market-only"], 1500):
            sys.exit(1)
        r.section("3. Holdout exam on the enlarged set")
        rc = run(r, ["scripts/factory_market_exam.py", "--split", "holdout", "--max", "120", "--wait-min", "14"], 1500)
        boto3.client("lambda", region_name="us-east-1").invoke(FunctionName="justhodl-ai", InvocationType="Event", Payload=json.dumps({"mode": "inventory"}).encode())
        (r.ok if rc == 0 else r.fail)("inventory tick queued; exam rc=%s" % rc)
        if rc:
            sys.exit(1)


if __name__ == "__main__":
    main()
