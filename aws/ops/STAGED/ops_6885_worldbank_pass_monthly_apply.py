"""ops 6885 - ACTING: the World Bank full pass, weekly -> monthly.  The plan is ops 6884's, unchanged (dry run first: ops 6884).

Khalid 2026-11-21: "World Bank pass monthly? Saves about $9 a month; World Bank series would be up to a month behind
instead of a week" - yes.  Changes one Scheduler expression (justhodl-worldbank-full-weekly: rate(7 days) ->
cron(0 1 1 * ? *)), backs the live schedule and the live manifest up to the private bucket first, reads both back.
"""
from pathlib import Path
import importlib.util
import sys

_spec = importlib.util.spec_from_file_location("ops6884", str(Path(__file__).resolve().parent / "ops_6884_worldbank_pass_monthly.py"))
plan = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(plan)

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--selftest":
        plan.selftest()
        print("selftest ok")
    else:
        plan.main(dry_run=False)
