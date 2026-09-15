"""ops 5573 -- why did jh-gearb-gen1-20260915-024201 fail (ExecuteUserScriptError after 305 s)? CloudWatch tail of the
training job (prefix-only listing), the dataset the tick built (first row + manifest), and the pinned bundle. Read-only."""
from __future__ import annotations

import json
import sys
from pathlib import Path

import boto3

HERE = Path(__file__).resolve()
sys.path.insert(0, str(HERE.parents[1]))
from ops_report import report  # noqa: E402

PRI = "justhodl-ai-857687956942"
JOB = "jh-gearb-gen1-20260915-024201"


def main() -> int:
    s3 = boto3.client("s3", region_name="us-east-1")
    logs = boto3.client("logs", region_name="us-east-1")
    with report("ops_5573_gen1_training_forensics") as R:
        R.heading("ops 5573 -- generation-1 training forensics (read-only)")
        try:
            streams = logs.describe_log_streams(logGroupName="/aws/sagemaker/TrainingJobs", logStreamNamePrefix=JOB, limit=5).get("logStreams", [])
            for st in streams[:1]:
                ev = logs.get_log_events(logGroupName="/aws/sagemaker/TrainingJobs", logStreamName=st["logStreamName"], limit=120, startFromHead=False).get("events", [])
                R.ok("log stream %s: %d events (tail)" % (st["logStreamName"], len(ev)))
                for e in ev[-60:]:
                    R.log("  " + e.get("message", "").rstrip()[:300])
        except Exception as e:  # noqa: BLE001
            R.warn("logs unavailable: %s" % str(e)[:200])
        keys = [o["Key"] for o in s3.list_objects_v2(Bucket=PRI, Prefix="factory/gearb/datasets/gen-1/", MaxKeys=50).get("Contents", [])]
        R.ok("dataset keys: %s" % json.dumps(keys)[:600])
        for k in keys:
            if k.endswith((".jsonl", ".json")):
                body = s3.get_object(Bucket=PRI, Key=k)["Body"].read(3000).decode("utf-8", "replace")
                R.log("  %s :: %s" % (k.split("/")[-1], body[:700].replace("\n", " | ")))
        pin = json.loads(s3.get_object(Bucket=PRI, Key="factory/training/current.json")["Body"].read())
        R.ok("pin: bundle=%s image=%s" % (pin.get("bundle_uri", "").split("/")[-1], pin.get("training_image", "")[-40:]))
        R.ok("GREEN -- forensics recorded")
        return 0


if __name__ == "__main__":
    if main() != 0:
        sys.exit(1)
    sys.exit(0)
