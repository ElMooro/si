"""ops 6715 - let the shared EventBridge Scheduler role invoke treasury-api (one function, nothing else).

ops 6714 stopped before creating treasury-api-history-update: justhodl-scheduler-role (the role every Scheduler schedule
uses, e.g. treasury-rehypo-daily) answers implicitDeny for lambda:InvokeFunction on treasury-api - its grant is scoped to
the platform's function names and treasury-api is a 2025 name.  This adds ONE inline policy to that role:
  scheduler-invoke-treasury-api: Allow lambda:InvokeFunction on function:treasury-api and function:treasury-api:*
after printing the role's existing statements (resources only), then simulates until IAM agrees.  Reversible: delete the
inline policy.  ops 6714 is then re-run (idempotent) to create the schedule and finish.  No key involved.
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
ROLE = "justhodl-scheduler-role"
POLICY = "scheduler-invoke-treasury-api"
FN_ARN = "arn:aws:lambda:%s:%s:function:treasury-api" % (REGION, ACCOUNT)
DOC = {"Version": "2012-10-17", "Statement": [{"Sid": "TreasuryApiHistoryUpdate", "Effect": "Allow", "Action": "lambda:InvokeFunction",
                                               "Resource": [FN_ARN, FN_ARN + ":*"]}]}
CFG = Config(retries={"max_attempts": 8, "mode": "standard"})


def main():
    iam = boto3.client("iam", config=CFG)
    with report("ops_6715_scheduler_grant_treasury_api") as r:
        r.heading("ops 6715 - %s may invoke treasury-api (one inline grant)" % ROLE)
        role = iam.get_role(RoleName=ROLE)["Role"]
        r.log("role %s trust: %s" % (ROLE, [s.get("Principal") for s in role["AssumeRolePolicyDocument"]["Statement"]]))
        for p in iam.list_attached_role_policies(RoleName=ROLE)["AttachedPolicies"]:
            ver = iam.get_policy(PolicyArn=p["PolicyArn"])["Policy"]["DefaultVersionId"]
            doc = iam.get_policy_version(PolicyArn=p["PolicyArn"], VersionId=ver)["PolicyVersion"]["Document"]
            r.log("attached %s: %s" % (p["PolicyName"], json.dumps([(s.get("Effect"), s.get("Action"), s.get("Resource")) for s in doc["Statement"]])[:600]))
        for name in iam.list_role_policies(RoleName=ROLE)["PolicyNames"]:
            doc = iam.get_role_policy(RoleName=ROLE, PolicyName=name)["PolicyDocument"]
            r.log("inline %s: %s" % (name, json.dumps([(s.get("Effect"), s.get("Action"), s.get("Resource")) for s in doc["Statement"]])[:600]))
        iam.put_role_policy(RoleName=ROLE, PolicyName=POLICY, PolicyDocument=json.dumps(DOC))
        r.log("put inline %s: %s" % (POLICY, json.dumps(DOC)))
        dec = None
        for _ in range(12):
            sim = iam.simulate_principal_policy(PolicySourceArn=role["Arn"], ActionNames=["lambda:InvokeFunction"], ResourceArns=[FN_ARN])
            dec = sim["EvaluationResults"][0]["EvalDecision"]
            if dec == "allowed":
                break
            time.sleep(10)
        other = iam.simulate_principal_policy(PolicySourceArn=role["Arn"], ActionNames=["lambda:InvokeFunction"],
                                              ResourceArns=["arn:aws:lambda:%s:%s:function:ofrapi" % (REGION, ACCOUNT)])["EvaluationResults"][0]["EvalDecision"]
        (r.ok if dec == "allowed" else r.fail)("simulation: treasury-api %s | ofrapi (must stay denied) %s" % (dec, other))
        if dec != "allowed":
            raise SystemExit(1)
        r.ok("ops 6715 complete")


if __name__ == "__main__":
    main()
    sys.exit(0)
