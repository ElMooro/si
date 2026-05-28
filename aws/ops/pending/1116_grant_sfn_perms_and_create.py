"""ops 1116 — Grant github-actions-justhodl the Step Functions perms it needs, then create the SM."""
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

iam = boto3.client("iam", region_name=REGION)
sfn = boto3.client("stepfunctions", region_name=REGION)
events = boto3.client("events", region_name=REGION)


def main():
    rpt = {"started": datetime.now(timezone.utc).isoformat()}
    try:
        # 1) Grant the user Step Functions perms
        policy_doc = {
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Action": [
                    "states:CreateStateMachine", "states:UpdateStateMachine",
                    "states:DescribeStateMachine", "states:DeleteStateMachine",
                    "states:ListStateMachines", "states:StartExecution",
                    "states:DescribeExecution", "states:ListExecutions",
                    "states:StopExecution", "states:TagResource",
                ],
                "Resource": [
                    f"arn:aws:states:{REGION}:{ACCOUNT}:stateMachine:jhk-*",
                    f"arn:aws:states:{REGION}:{ACCOUNT}:execution:jhk-*:*",
                ],
            }, {
                # PassRole for the SFN execution role
                "Effect": "Allow",
                "Action": "iam:PassRole",
                "Resource": [
                    f"arn:aws:iam::{ACCOUNT}:role/{SFN_ROLE}",
                    f"arn:aws:iam::{ACCOUNT}:role/{EB_ROLE}",
                ],
            }]
        }
        iam.put_user_policy(UserName=USER, PolicyName="StepFunctionsForJhk",
                             PolicyDocument=json.dumps(policy_doc))
        rpt["user_policy"] = "ATTACHED"

        # IAM eventually consistent
        time.sleep(12)

        # 2) Create / update the state machine
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

        # 3) Hourly EB trigger
        rule = "jhk-tick-hourly-orchestrate"
        events.put_rule(Name=rule, ScheduleExpression="cron(5 * * * ? *)", State="ENABLED",
                        Description="Triggers jhk-portfolio-orchestration hourly (arch #6).")
        events.put_targets(Rule=rule, Targets=[{
            "Id":"1","Arn":sm_arn,"RoleArn":eb_role_arn,"Input":json.dumps({})}])
        rpt["eb_rule"] = rule

        # 4) Smoke
        ex = sfn.start_execution(stateMachineArn=sm_arn,
                                  name=f"smoke-{int(time.time())}", input="{}")
        rpt["smoke_arn"] = ex["executionArn"]
        for i in range(60):  # poll up to 5 min
            time.sleep(5)
            s = sfn.describe_execution(executionArn=ex["executionArn"])
            if s["status"] != "RUNNING":
                rpt["smoke_status"] = s["status"]
                if s.get("error"): rpt["smoke_err"] = s["error"]
                if s.get("cause"): rpt["smoke_cause"] = s["cause"][:600]
                if s.get("output"):
                    try: rpt["smoke_output"] = json.loads(s["output"])
                    except Exception: rpt["smoke_output"] = s["output"][:300]
                break
        else:
            rpt["smoke_status"] = "STILL_RUNNING_AT_POLL_TIMEOUT"
    except Exception as e:
        rpt["fatal_err"] = str(e)[:400]
        rpt["traceback"] = traceback.format_exc()[-1500:]

    rpt["finished"] = datetime.now(timezone.utc).isoformat()
    p = os.path.join(REPO_ROOT, "aws/ops/reports/1116.json")
    os.makedirs(os.path.dirname(p), exist_ok=True)
    json.dump(rpt, open(p,"w"), indent=2, default=str)
    print(json.dumps({k:v for k,v in rpt.items() if k!="traceback"}, indent=2, default=str)[:2200])


if __name__ == "__main__":
    main()
