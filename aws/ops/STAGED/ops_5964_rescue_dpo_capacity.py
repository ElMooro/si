"""ops 5964 -- the first DPO generation has waited for g5.2xlarge SPOT capacity since 15:07 (Claude, 2026-09-23). Direct lane.
Runs the engine's own rescue (gear_b.rescue_capacity) now instead of at the next tick: stops the starved spot job only if it
is still 'Starting' on a capacity message, records the stop, so the 16:07 tick relaunches the pair dataset ON-DEMAND."""
import json, sys
from pathlib import Path
import boto3
HERE = Path(__file__).resolve(); sys.path.insert(0, str(HERE.parents[1]))
REPO = HERE.parents[3]
sys.path.insert(0, str(REPO / "aws/lambdas/justhodl-ai/source")); sys.path.insert(0, str(REPO / "aws/shared"))
from ops_report import report  # noqa: E402
PRI = "justhodl-ai-857687956942"


def main():
    import gear_b as gb
    sm = boto3.client("sagemaker", region_name="us-east-1"); s3 = boto3.client("s3", region_name="us-east-1")
    with report("5964_rescue_dpo_capacity") as r:
        r.heading("ops 5964 -- rescue the first DPO generation from the spot queue")
        stopped = gb.rescue_capacity(sm, s3, PRI, {"spot_patience_s": 900})
        (r.ok if stopped else r.log)("stopped for capacity: %s" % (stopped or "none (it has capacity or is not waiting)"))
        r.kv(next_launch_spot=gb.use_spot(s3, PRI, gb.load_control(s3, PRI)))


if __name__ == "__main__":
    main()
