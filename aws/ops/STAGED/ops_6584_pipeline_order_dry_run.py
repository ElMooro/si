"""ops 6584 - pipeline order, DRY RUN (reads only; computes every change, writes nothing).

Khalid, 2026-10-20: "the pipeline-order retime (dry run first, this week)".  Readers fire after their writers:
plan config/pipeline-order-plan.json (scripts/pipeline_order.py), 82 retimes of about an hour at most inside each
trigger's own session window, three daily-morn members moved to a late tick a quarter hour later, 22 same-minute
races removed, 84 of 116 "reads yesterday's file" edges fixed.  See scripts/pipeline_rollout.py for the order of
operations; nothing is disabled, deleted or moved earlier.
"""
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
sys.path.insert(0, str(ROOT / "scripts"))
from ops_report import report  # noqa: E402
import pipeline_rollout  # noqa: E402


def main():
    with report("ops_6584_pipeline_order_dry_run") as r:
        pipeline_rollout.run(r, dry_run=True)


if __name__ == "__main__":
    main()
