"""
ops/4722 — why does read_leg_value() work from an ops script (confirmed,
ops 4721: resolves to 47.96) but return nothing inside the deployed
Lambda's own execution (ops 4720: every leg available=0)? The two run
under different IAM identities -- the deploy/ops credentials vs.
lambda-execution-role. Check whether that role can actually read this
bucket, and pull CloudWatch logs from the real invoke for anything
_get_json's broad except Exception swallowed silently.
"""
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))

import boto3  # noqa: E402
from botocore.exceptions import ClientError  # noqa: E402
from ops_report import report  # noqa: E402

REGION = "us-east-1"
ACCOUNT = "857687956942"
ROLE_NAME = "lambda-execution-role"
BUCKET = "justhodl-dashboard-live"
FUNCTION_NAME = "justhodl-invest"
LOG_GROUP = f"/aws/lambda/{FUNCTION_NAME}"

iam = boto3.client("iam", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)
logs = boto3.client("logs", region_name=REGION)


def main():
    with report("4722_invest_iam_and_logs") as rep:
        rep.heading("ops 4722 — lambda-execution-role S3 access + CloudWatch logs")

        rep.section("1. lambda-execution-role policies")
        try:
            attached = iam.list_attached_role_policies(RoleName=ROLE_NAME)["AttachedPolicies"]
            rep.kv(attached_managed_policies=[p["PolicyName"] for p in attached])
            for p in attached:
                arn = p["PolicyArn"]
                ver = iam.get_policy(PolicyArn=arn)["Policy"]["DefaultVersionId"]
                doc = iam.get_policy_version(PolicyArn=arn, VersionId=ver)["PolicyVersion"]["Document"]
                rep.log(f"  {p['PolicyName']}: {doc}")
        except ClientError as e:
            rep.fail(f"  list_attached_role_policies: {e}")

        try:
            inline_names = iam.list_role_policies(RoleName=ROLE_NAME)["PolicyNames"]
            rep.kv(inline_policy_names=inline_names)
            for name in inline_names:
                doc = iam.get_role_policy(RoleName=ROLE_NAME, PolicyName=name)["PolicyDocument"]
                rep.log(f"  inline {name}: {doc}")
        except ClientError as e:
            rep.fail(f"  list_role_policies: {e}")

        rep.section("2. Does this bucket have a bucket policy that scopes access?")
        try:
            pol = s3.get_bucket_policy(Bucket=BUCKET)["Policy"]
            rep.log(f"  bucket policy: {pol}")
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchBucketPolicy":
                rep.ok("  no bucket policy (access governed by IAM only)")
            else:
                rep.fail(f"  get_bucket_policy: {e}")

        rep.section("3. Trigger a fresh invoke, then read CloudWatch logs for it")
        client = boto3.client("lambda", region_name=REGION)
        t0 = time.time()
        resp = client.invoke(FunctionName=FUNCTION_NAME, InvocationType="RequestResponse", Payload=b"{}")
        rep.kv(fresh_invoke_status=resp.get("StatusCode"),
               fresh_invoke_elapsed_s=round(time.time() - t0, 1))
        time.sleep(8)  # let CloudWatch ingest
        try:
            streams = logs.describe_log_streams(
                logGroupName=LOG_GROUP, orderBy="LastEventTime", descending=True, limit=1,
            )["logStreams"]
            if not streams:
                rep.fail("  no log streams found")
            else:
                stream_name = streams[0]["logStreamName"]
                events = logs.get_log_events(
                    logGroupName=LOG_GROUP, logStreamName=stream_name, limit=100, startFromHead=False,
                )["events"]
                rep.kv(log_stream=stream_name, n_events=len(events))
                for e in events:
                    msg = e["message"].rstrip()
                    rep.log(f"  {msg[:500]}")
        except ClientError as e:
            rep.fail(f"  CloudWatch logs read: {e}")

        rep.section("Verdict")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        import traceback
        print("IAM/LOGS DIAG ERROR:\n" + traceback.format_exc(), flush=True)
        sys.exit(1)
    sys.exit(0)
