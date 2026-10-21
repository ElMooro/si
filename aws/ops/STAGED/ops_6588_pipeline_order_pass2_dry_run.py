"""ops 6588 - pipeline order pass 2, DRY RUN (reads only; computes every change, writes nothing).

Khalid, 2026-10-21: (1) providers as pinned writers at their publication times - pullers that ran before the day's
print move after it inside their own window (config/provider-publication.json; FRED split into daily-series pullers,
freshest in the US morning, and macro-release pullers, freshest after 08:30 ET); (2) no provider burst: at most
slot_max pullers of one provider start on the same minute, fan-out ticks packed into late ticks the same way (FRED,
ECB, SEC, government portals, FMP); (3) the intraday lane: one Scheduler schedule in New York time fans the light
Polygon feeds out every five minutes 09:00-16:55 Mon-Fri, exempt from the once-a-day floor by name, the $50 cap and
the anomaly sentinel unchanged.  Plan: config/pipeline-order-plan-2.json; order of operations and safety properties:
scripts/pipeline_rollout.py.
"""
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
sys.path.insert(0, str(ROOT / "scripts"))
from ops_report import report  # noqa: E402
import pipeline_rollout  # noqa: E402


def main():
    with report("ops_6588_pipeline_order_pass2_dry_run") as r:
        pipeline_rollout.run(r, dry_run=True, plan_path=ROOT / "config/pipeline-order-plan-2.json")


if __name__ == "__main__":
    main()
