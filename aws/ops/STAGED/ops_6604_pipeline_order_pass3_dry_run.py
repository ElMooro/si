"""ops 6604 - pipeline order pass 3, DRY RUN (reads only; computes every change, writes nothing).

Khalid, 2026-10-21: "treat each provider as a pinned writer at its publication time - dry run first, after a one-off
census confirms the arithmetic ... be careful not to pull all FRED and ECB data at once and other gov so that you
don't get blocked by API rate limit."  The census (ops 6587, re-run 2026-10-22 01:36 UTC after every daily engine
had run once since the thaw) confirmed the publication arithmetic file by file; one correction - Cboe's end-of-day
files were still on the previous day at 17:00 ET, so its print is now 17:30 ET (config/provider-publication.json).
Pass 3 (config/pipeline-order-plan-3.json, rebuilt timezone-aware after the first-light finding): 135 retimes - 37
pullers moved after their provider's print, 55 burst spreads (at most slot_max pullers of one provider per minute),
34 reader-after-writer orders, 7 tick members re-packed into daily-morn-late / -late2.  Order of operations and
safety properties: scripts/pipeline_rollout.py.
"""
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
sys.path.insert(0, str(ROOT / "scripts"))
from ops_report import report  # noqa: E402
import pipeline_rollout  # noqa: E402


def main():
    with report("ops_6604_pipeline_order_pass3_dry_run") as r:
        pipeline_rollout.run(r, dry_run=True, plan_path=ROOT / "config/pipeline-order-plan-3.json")


if __name__ == "__main__":
    main()
