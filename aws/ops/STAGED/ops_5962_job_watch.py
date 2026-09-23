"""ops 5962 -- watch a Gear B job (Claude, 2026-09-23). READ-ONLY: status, secondary status transitions, failure reason,
billable seconds, and the tail of its CloudWatch log (the first DPO generation's health)."""
import sys
from pathlib import Path
import boto3
HERE = Path(__file__).resolve(); sys.path.insert(0, str(HERE.parents[1]))
from ops_report import report  # noqa: E402
JOB_PREFIX = "jh-gearb-gen27-"


def main():
    sm = boto3.client("sagemaker", region_name="us-east-1"); logs = boto3.client("logs", region_name="us-east-1")
    with report("5962_job_watch") as r:
        r.heading("ops 5962 -- the first DPO generation")
        jobs = sm.list_training_jobs(NameContains=JOB_PREFIX, MaxResults=5, SortBy="CreationTime", SortOrder="Descending").get("TrainingJobSummaries", [])
        if not jobs:
            r.fail("no job"); sys.exit(1)
        name = jobs[0]["TrainingJobName"]
        d = sm.describe_training_job(TrainingJobName=name)
        r.kv(job=name, status=d["TrainingJobStatus"], secondary=d.get("SecondaryStatus"), failure=str(d.get("FailureReason"))[:300], billable_s=d.get("BillableTimeInSeconds"),
             mode=(d.get("HyperParameters") or {}).get("mode"))
        for t in (d.get("SecondaryStatusTransitions") or [])[-6:]:
            r.log("%s %s %s" % (t["StartTime"].strftime("%H:%M:%S"), t["Status"], str(t.get("StatusMessage"))[:140]))
        try:
            streams = logs.describe_log_streams(logGroupName="/aws/sagemaker/TrainingJobs", logStreamNamePrefix=name, limit=3).get("logStreams", [])
            for st in streams[:1]:
                ev = logs.get_log_events(logGroupName="/aws/sagemaker/TrainingJobs", logStreamName=st["logStreamName"], limit=40, startFromHead=False).get("events", [])
                for e in ev[-40:]:
                    r.log("  | " + e["message"].strip()[:220])
        except Exception as e:  # noqa: BLE001
            r.warn("logs: %s" % str(e)[:120])
        r.ok("done")


if __name__ == "__main__":
    main()
