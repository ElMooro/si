"""ops 5539 -- forensics on the two failed weight-staging jobs (exit 2 within minutes). Reads the CloudWatch log
streams of /aws/sagemaker/ProcessingJobs for jh-stage-* (prefix only -- orderBy cannot be combined with a prefix),
lists whatever the continuous upload left under factory/models/base/qwen2-5-coder-7b-instruct/, and describes
the last job. Launches nothing.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import boto3

HERE = Path(__file__).resolve()
sys.path.insert(0, str(HERE.parents[1]))
from ops_report import report  # noqa: E402

REGION, PRI = "us-east-1", "justhodl-ai-857687956942"
BASE = "factory/models/base/qwen2-5-coder-7b-instruct/"


def main() -> int:
    s3 = boto3.client("s3", region_name=REGION)
    sm = boto3.client("sagemaker", region_name=REGION)
    logs = boto3.client("logs", region_name=REGION)
    with report("ops_5539_staging_job_forensics") as R:
        R.heading("ops 5539 -- why did jh-stage-* exit 2? log tails + bucket listing; nothing launched")
        jobs = sm.list_processing_jobs(NameContains="jh-stage-", SortBy="CreationTime", SortOrder="Descending", MaxResults=5).get("ProcessingJobSummaries", [])
        for j in jobs:
            d = sm.describe_processing_job(ProcessingJobName=j["ProcessingJobName"])
            R.ok("job %s status=%s exit=%s reason=%s start=%s end=%s" % (j["ProcessingJobName"], d.get("ProcessingJobStatus"), d.get("ExitMessage"),
                                                                       (d.get("FailureReason") or "")[:200], d.get("ProcessingStartTime"), d.get("ProcessingEndTime")))
        try:
            streams = logs.describe_log_streams(logGroupName="/aws/sagemaker/ProcessingJobs", logStreamNamePrefix="jh-stage-", limit=10).get("logStreams", [])
            streams = sorted(streams, key=lambda s: s.get("lastEventTimestamp") or 0, reverse=True)[:2]
            for st in streams:
                ev = logs.get_log_events(logGroupName="/aws/sagemaker/ProcessingJobs", logStreamName=st["logStreamName"], limit=60, startFromHead=False).get("events", [])
                R.ok("stream %s: %d events" % (st["logStreamName"], len(ev)))
                for e in ev[-40:]:
                    R.log("  " + e.get("message", "").rstrip()[:300])
        except Exception as e:  # noqa: BLE001
            R.warn("logs unavailable: %s" % str(e)[:200])
        resp = s3.list_objects_v2(Bucket=PRI, Prefix=BASE, MaxKeys=200)
        keys = [(o["Key"], o["Size"]) for o in resp.get("Contents", [])]
        R.ok("bucket objects under %s: %d -> %s" % (BASE, len(keys), json.dumps(keys[:30])[:1200]))
        R.ok("GREEN -- forensics recorded")
        return 0


if __name__ == "__main__":
    if main() != 0:
        sys.exit(1)
    sys.exit(0)
