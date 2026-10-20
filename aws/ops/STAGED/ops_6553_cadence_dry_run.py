"""ops 6553 - once-a-day cadence rollout, DRY RUN (reads only; computes every change, writes nothing).

Khalid, 2026-10-20: every engine runs at most once every 24 hours.  Plan: config/cadence-plan.json (commit 08a656cb).
See scripts/cadence_rollout.py for the order of operations and the safety properties.
"""
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
sys.path.insert(0, str(ROOT / "scripts"))
from ops_report import report  # noqa: E402
import cadence_rollout  # noqa: E402


def main():
    with report("ops_6553_cadence_dry_run") as r:
        cadence_rollout.run(r, dry_run=True)


if __name__ == "__main__":
    main()
