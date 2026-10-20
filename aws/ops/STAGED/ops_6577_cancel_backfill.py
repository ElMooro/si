"""ops 6577 - cancel the archive backfill job for now (re-created after the live bucket's old copies are expired).

AWS: "with a generated manifest, Amazon S3 replicates all eligible versions of your objects" - the ops 6575 job would
carry the October loop's old copies (573,713 copies of one audit key, 1.74 TB, plus the storm copies under data/) into
Standard-IA, where each copy is billed for 30 days even after the archive's keep-3 rule expires it (~$30-60 for
nothing).  Ongoing replication (ops 6574) keeps covering every new write; the backfill of the 1.28 M current objects
is re-launched once the daily S3 Inventory shows the old copies down to 10 per file (the count rule, ops 6571).
"""
from pathlib import Path
import json
import sys
import time

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

REGION, ACCOUNT = "us-east-1", "857687956942"
DESC = "ops 6575 warehouse archive backfill (justhodl-dashboard-live -> justhodl-warehouse-archive-857687956942)"
CFG = Config(retries={"max_attempts": 10, "mode": "standard"}, connect_timeout=10, read_timeout=120)


def main():
    s3c = boto3.client("s3control", region_name=REGION, config=CFG)
    with report("ops_6577_cancel_backfill") as r:
        r.heading("ops 6577 - cancel the archive backfill job until the old copies are expired")
        found = None
        for status in (["Active", "Preparing", "Ready", "Suspended", "New", "Completing"], ["Complete", "Failed", "Cancelled", "Cancelling"]):
            for j in s3c.list_jobs(AccountId=ACCOUNT, JobStatuses=status).get("Jobs", []):
                if (j.get("Description") or "") == DESC:
                    found = j
                    break
            if found:
                break
        if not found:
            r.ok("no backfill job found")
            return
        d = s3c.describe_job(AccountId=ACCOUNT, JobId=found["JobId"])["Job"]
        p = d.get("ProgressSummary") or {}
        r.log("job %s: %s, tasks total %s succeeded %s failed %s" % (found["JobId"], d.get("Status"), p.get("TotalNumberOfTasks"), p.get("NumberOfTasksSucceeded"), p.get("NumberOfTasksFailed")))
        if d.get("Status") in ("Complete", "Failed", "Cancelled", "Cancelling"):
            r.ok("nothing to cancel (%s)" % d.get("Status"))
            return
        s3c.update_job_status(AccountId=ACCOUNT, JobId=found["JobId"], RequestedJobStatus="Cancelled",
                              StatusUpdateReason="ops 6577: re-launch after the live bucket's old copies are expired (count rule ops 6571)")
        time.sleep(15)
        d2 = s3c.describe_job(AccountId=ACCOUNT, JobId=found["JobId"])["Job"]
        p2 = d2.get("ProgressSummary") or {}
        r.kv(job_id=found["JobId"], status_after=d2.get("Status"), tasks_done=p2.get("NumberOfTasksSucceeded"))
        r.ok("backfill job %s -> %s" % (found["JobId"], d2.get("Status")))


if __name__ == "__main__":
    main()
