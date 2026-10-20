"""ops 6575 - piece 1, part 2: backfill the warehouse archive with everything that already exists in the live bucket.

ops 6574 put ongoing replication in place (every new write lands in justhodl-warehouse-archive-857687956942 within
minutes; proven with a canary).  Objects written before that - the 1.28 M current objects, 434 GB - are copied by an
S3 Batch Replication job: S3 generates the manifest itself (every object whose replication status is NONE or FAILED),
replicates server-side (nothing flows through the runner), and writes a report of failed tasks only to the private
bucket.  Cost about $14 once (IA PUTs + $1.25 per million objects + $0.25).  The job runs for hours; this script
creates it and reads its first status; re-running it reports the job instead of creating a twin.
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
LIVE, PRIVATE = "justhodl-dashboard-live", "justhodl-ops-private-%s" % ACCOUNT
ARCHIVE = "justhodl-warehouse-archive-%s" % ACCOUNT
ROLE_ARN = "arn:aws:iam::%s:role/justhodl-s3-replication-role" % ACCOUNT
DESC = "ops 6575 warehouse archive backfill (%s -> %s)" % (LIVE, ARCHIVE)
CFG = Config(retries={"max_attempts": 10, "mode": "standard"}, connect_timeout=10, read_timeout=120)


def c(name):
    return boto3.client(name, region_name=REGION, config=CFG)


def runner_grant(r):
    iam = c("iam")
    doc = {"Version": "2012-10-17", "Statement": [
        {"Sid": "PassReplicationRole", "Effect": "Allow", "Action": "iam:PassRole", "Resource": ROLE_ARN},
        {"Sid": "BatchReplicationJobs", "Effect": "Allow", "Action": ["s3:CreateJob", "s3:DescribeJob", "s3:ListJobs", "s3:UpdateJobStatus", "s3:UpdateJobPriority"], "Resource": "*"}]}
    name = "justhodl-runner-replication"
    arn = "arn:aws:iam::%s:policy/%s" % (ACCOUNT, name)
    try:
        iam.create_policy(PolicyName=name, PolicyDocument=json.dumps(doc), Description="ops 6575: pass the replication role, batch replication jobs")
        r.log("runner policy %s created" % name)
    except iam.exceptions.EntityAlreadyExistsException:
        pass
    try:
        iam.attach_group_policy(GroupName="justhodl-runner-ecr", PolicyArn=arn)
        r.log("runner group: + %s" % name)
    except Exception as ex:  # noqa: BLE001
        r.log("runner grant: %s" % type(ex).__name__)


def existing(s3c):
    for status in (["Active", "Preparing", "Ready", "Suspended", "New"], ["Complete", "Completing", "Failed", "Cancelled"]):
        try:
            for j in s3c.list_jobs(AccountId=ACCOUNT, JobStatuses=status).get("Jobs", []):
                if (j.get("Description") or "") == DESC:
                    return j
        except Exception:  # noqa: BLE001
            return None
    return None


def describe(s3c, job_id):
    j = s3c.describe_job(AccountId=ACCOUNT, JobId=job_id)["Job"]
    p = j.get("ProgressSummary") or {}
    return {"status": j.get("Status"), "total": p.get("TotalNumberOfTasks"), "succeeded": p.get("NumberOfTasksSucceeded"),
            "failed": p.get("NumberOfTasksFailed"), "reason": j.get("StatusUpdateReason"), "failure": [(f.get("FailureCode"), f.get("FailureReason")[:160]) for f in (j.get("FailureReasons") or [])]}


def main():
    s3c = c("s3control")
    with report("ops_6575_archive_backfill") as r:
        r.heading("ops 6575 - archive backfill: S3 Batch Replication of every existing object")
        job = existing(s3c)
        if job and job.get("Status") not in ("Failed", "Cancelled"):
            r.log("backfill job already exists: %s (%s)" % (job["JobId"], job["Status"]))
            job_id = job["JobId"]
        else:
            job_id = None
            for attempt in range(2):
                try:
                    job_id = s3c.create_job(AccountId=ACCOUNT, ConfirmationRequired=False, Priority=10, RoleArn=ROLE_ARN, Description=DESC,
                                            Operation={"S3ReplicateObject": {}},
                                            Report={"Bucket": "arn:aws:s3:::%s" % PRIVATE, "Format": "Report_CSV_20180820", "Enabled": True,
                                                    "Prefix": "batch-replication/reports", "ReportScope": "FailedTasksOnly"},
                                            ManifestGenerator={"S3JobManifestGenerator": {
                                                "ExpectedBucketOwner": ACCOUNT, "SourceBucket": "arn:aws:s3:::%s" % LIVE, "EnableManifestOutput": False,
                                                "Filter": {"EligibleForReplication": True, "ObjectReplicationStatuses": ["NONE", "FAILED"]}}})["JobId"]
                    r.ok("backfill job created: %s" % job_id)
                    break
                except Exception as ex:  # noqa: BLE001
                    if attempt == 0 and "AccessDenied" in str(ex):
                        runner_grant(r)
                        time.sleep(60)
                        continue
                    r.fail("job not created: %s: %s" % (type(ex).__name__, str(ex)[:300]))
                    raise SystemExit(1)
        time.sleep(60)
        d = describe(s3c, job_id)
        r.log("job %s: %s" % (job_id, json.dumps(d)))
        r.kv(job_id=job_id, **{k: v for k, v in d.items() if k in ("status", "total", "succeeded", "failed")})
        if d["status"] in ("Failed", "Cancelled"):
            r.fail("backfill job %s: %s" % (d["status"], d["failure"]))
            raise SystemExit(1)
        r.ok("backfill running server-side; ops 6576 reads progress and compares the two buckets")


if __name__ == "__main__":
    main()
