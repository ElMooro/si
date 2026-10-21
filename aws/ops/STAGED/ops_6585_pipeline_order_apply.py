"""ops 6585 - pipeline order, APPLY (retimes the planned triggers, creates the late tick, updates the two S3 manifests;
backups first).  Runs only after Khalid's go on the ops 6584 dry run.

Khalid, 2026-10-20: readers fire after their writers.  Plan: config/pipeline-order-plan.json (scripts/pipeline_order.py).
See scripts/pipeline_rollout.py for the order of operations and the safety properties: precondition-checked
retimes, full backups to s3://justhodl-ops-private-.../backups/pipeline-order/<stamp>/, read-back after every write,
nothing disabled or deleted, cadence floor re-verified at the end.
"""
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
sys.path.insert(0, str(ROOT / "scripts"))
from ops_report import report  # noqa: E402
import pipeline_rollout  # noqa: E402


def main():
    with report("ops_6585_pipeline_order_apply") as r:
        pipeline_rollout.run(r, dry_run=False)


if __name__ == "__main__":
    main()
