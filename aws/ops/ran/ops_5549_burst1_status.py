"""ops 5549 -- burst 1 status (read-only): job state + output keys, so the verify dispatch can be timed. Nothing launched."""
from __future__ import annotations

import json
import sys
from pathlib import Path

import boto3

HERE = Path(__file__).resolve()
sys.path.insert(0, str(HERE.parents[1]))
from ops_report import report  # noqa: E402

REGION, PRI = "us-east-1", "justhodl-ai-857687956942"
BURST = "jh-burst-gen0b1-20260914-022049"


def main() -> int:
    s3 = boto3.client("s3", region_name=REGION)
    sm = boto3.client("sagemaker", region_name=REGION)
    with report("ops_5549_burst1_status") as R:
        R.heading("ops 5549 -- burst 1 job status + outputs (read-only)")
        d = sm.describe_training_job(TrainingJobName=BURST)
        R.ok("job %s status=%s secondary=%s training_s=%s billable_s=%s failure=%s" % (
            BURST, d.get("TrainingJobStatus"), d.get("SecondaryStatus"), d.get("TrainingTimeInSeconds"), d.get("BillableTimeInSeconds"), (d.get("FailureReason") or "")[:300]))
        for t in (d.get("SecondaryStatusTransitions") or [])[-6:]:
            R.log("  %s %s %s" % (t.get("Status"), t.get("StartTime"), (t.get("StatusMessage") or "")[:160]))
        keys = [(o["Key"], o["Size"]) for o in s3.list_objects_v2(Bucket=PRI, Prefix="factory/bursts/%s/" % BURST, MaxKeys=50).get("Contents", [])]
        R.ok("output keys: %s" % json.dumps(keys)[:800])
        try:
            logs = boto3.client("logs", region_name=REGION)
            streams = logs.describe_log_streams(logGroupName="/aws/sagemaker/TrainingJobs", logStreamNamePrefix=BURST, limit=5).get("logStreams", [])
            for st in streams[:1]:
                ev = logs.get_log_events(logGroupName="/aws/sagemaker/TrainingJobs", logStreamName=st["logStreamName"], limit=30, startFromHead=False).get("events", [])
                for e in ev[-20:]:
                    R.log("  " + e.get("message", "").rstrip()[:240])
        except Exception as e:  # noqa: BLE001
            R.warn("logs unavailable: %s" % str(e)[:160])
        R.ok("GREEN -- status recorded")
        return 0


if __name__ == "__main__":
    if main() != 0:
        sys.exit(1)
    sys.exit(0)
