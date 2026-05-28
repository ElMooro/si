"""ops 1117 — Create managed policy for Step Functions perms, attach to user, then create SM."""
import json, os, time, traceback
from datetime import datetime, timezone
import boto3
from botocore.exceptions import ClientError

REGION = "us-east-1"
ACCOUNT = "857687956942"
REPO_ROOT = os.environ.get("REPO_ROOT", os.getcwd())

USER = "github-actions-justhodl"
SM_NAME = "jhk-portfolio-orchestration"
SFN_ROLE = "jhk-step-fn-exec-role"
EB_ROLE = "jhk-eb-to-sfn-role"
POLICY_NAME = "JhkStepFunctionsManaged"

iam = boto3.client("iam", region_name=REGION)
sfn = boto3.client("stepfunctions", region_name=REGION)
events = boto3.client("events", region_name=REGION)


POLICY = {
    "Version": "2012-10-17",
    "Statement": [{
        "Effect": "Allow",
        "Action": "states:*",
        "Resource": [
            f"arn:aws:states:{REGION}:{ACCOUNT}:stateMachine:jhk-*",
            f"arn:aws:states:{REGION}:{ACCOUNT}:execution:jhk-*:*",
        ],
    }, {
        "Effect": "Allow",
        "Action": "iam:PassRole",
        "Resource": [
            f"arn:aws:iam::{ACCOUNT}:role/{SFN_ROLE}",
            f"arn:aws:iam::{ACCOUNT}:role/{EB_ROLE}",
        ],
    }]
}


def main():
    rpt = {"started": datetime.now(timezone.utc).isoformat()}
    try:
        # 1) Create-or-update managed policy
        policy_arn = f"arn:aws:iam::{ACCOUNT}:policy/{POLICY_NAME}"
        try:
            # If policy exists, create a new version + delete old non-default versions
            iam.get_policy(PolicyArn=policy_arn)
            # Delete old non-default versions to make room (5 max)
            versions = iam.list_policy_versions(PolicyArn=policy_arn).get("Versions", [])
            for v in versions:
                if not v["IsDefaultVersion"]:
                    iam.delete_policy_version(PolicyArn=policy_arn, VersionId=v["VersionId"])
            iam.create_policy_version(PolicyArn=policy_arn,
                                       PolicyDocument=json.dumps(POLICY),
                                       SetAsDefault=True)
            rpt["policy"] = "UPDATED"
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchEntity":
                iam.create_policy(PolicyName=POLICY_NAME,
                                   PolicyDocument=json.dumps(POLICY),
                                   Description="Step Functions perms for jhk-* SMs and execution roles")
                rpt["policy"] = "CREATED"
            else: raise

        # 2) Attach to user (idempotent)
        try:
            iam.attach_user_policy(UserName=USER, PolicyArn=policy_arn)
            rpt["attach"] = "OK"
        except ClientError as e:
            if e.response["Error"]["Code"] == "EntityAlreadyExists":
                rpt["attach"] = "ALREADY"
            else: raise

        time.sleep(12)  # IAM eventual consistency

        # 3) Create / update SM
        sfn_role_arn = f"arn:aws:iam::{ACCOUNT}:role/{SFN_ROLE}"
        eb_role_arn = f"arn:aws:iam::{ACCOUNT}:role/{EB_ROLE}"
        definition = json.load(open(os.path.join(REPO_ROOT,
                                                  "aws/step-functions/portfolio-orchestration.json")))
        sm_arn = f"arn:aws:states:{REGION}:{ACCOUNT}:stateMachine:{SM_NAME}"
        try:
            sfn.describe_state_machine(stateMachineArn=sm_arn)
            sfn.update_state_machine(stateMachineArn=sm_arn,
                                      definition=json.dumps(definition),
                                      roleArn=sfn_role_arn)
            rpt["sm"] = "UPDATED"
        except ClientError as e:
            if e.response["Error"]["Code"] in ("StateMachineDoesNotExist", "InvalidArn"):
                r = sfn.create_state_machine(name=SM_NAME, definition=json.dumps(definition),
                                              roleArn=sfn_role_arn, type="STANDARD")
                rpt["sm"] = "CREATED"; sm_arn = r["stateMachineArn"]
            else: raise
        rpt["sm_arn"] = sm_arn

        # 4) EB hourly trigger
        rule = "jhk-tick-hourly-orchestrate"
        events.put_rule(Name=rule, ScheduleExpression="cron(5 * * * ? *)", State="ENABLED",
                        Description="Triggers jhk-portfolio-orchestration hourly (arch #6).")
        events.put_targets(Rule=rule, Targets=[{
            "Id":"1","Arn":sm_arn,"RoleArn":eb_role_arn,"Input":json.dumps({})}])
        rpt["eb_rule"] = rule

        # 5) Smoke
        ex = sfn.start_execution(stateMachineArn=sm_arn,
                                  name=f"smoke-{int(time.time())}", input="{}")
        rpt["smoke_arn"] = ex["executionArn"]
        for _ in range(60):  # up to 5 min
            time.sleep(5)
            s = sfn.describe_execution(executionArn=ex["executionArn"])
            if s["status"] != "RUNNING":
                rpt["smoke_status"] = s["status"]
                if s.get("error"): rpt["smoke_err"] = s["error"]
                if s.get("cause"): rpt["smoke_cause"] = s["cause"][:600]
                if s.get("output"):
                    try: rpt["smoke_output"] = json.loads(s["output"])
                    except Exception: rpt["smoke_output"] = str(s["output"])[:300]
                break
        else:
            rpt["smoke_status"] = "POLL_TIMEOUT"
    except Exception as e:
        rpt["fatal_err"] = str(e)[:400]
        rpt["traceback"] = traceback.format_exc()[-1500:]

    rpt["finished"] = datetime.now(timezone.utc).isoformat()
    p = os.path.join(REPO_ROOT, "aws/ops/reports/1117.json")
    os.makedirs(os.path.dirname(p), exist_ok=True)
    json.dump(rpt, open(p,"w"), indent=2, default=str)
    print(json.dumps({k:v for k,v in rpt.items() if k!="traceback"}, indent=2, default=str)[:2400])


if __name__ == "__main__":
    main()
