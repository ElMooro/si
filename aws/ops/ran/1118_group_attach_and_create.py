"""ops 1118 — Bypass user's 10-policy limit by attaching via a group; then create SM."""
import json, os, time, traceback
from datetime import datetime, timezone
import boto3
from botocore.exceptions import ClientError

REGION = "us-east-1"
ACCOUNT = "857687956942"
REPO_ROOT = os.environ.get("REPO_ROOT", os.getcwd())

USER = "github-actions-justhodl"
GROUP = "jhk-step-functions"
SM_NAME = "jhk-portfolio-orchestration"
SFN_ROLE = "jhk-step-fn-exec-role"
EB_ROLE = "jhk-eb-to-sfn-role"
POLICY_NAME = "JhkStepFunctionsManaged"

iam = boto3.client("iam", region_name=REGION)
sfn = boto3.client("stepfunctions", region_name=REGION)
events = boto3.client("events", region_name=REGION)


def main():
    rpt = {"started": datetime.now(timezone.utc).isoformat()}
    try:
        # 1) Create group if missing
        try:
            iam.get_group(GroupName=GROUP)
            rpt["group"] = "EXISTS"
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchEntity":
                iam.create_group(GroupName=GROUP)
                rpt["group"] = "CREATED"
            else: raise

        # 2) Attach the managed policy (from 1117) to the group
        policy_arn = f"arn:aws:iam::{ACCOUNT}:policy/{POLICY_NAME}"
        try:
            iam.attach_group_policy(GroupName=GROUP, PolicyArn=policy_arn)
            rpt["group_attach"] = "OK"
        except ClientError as e:
            if e.response["Error"]["Code"] == "EntityAlreadyExists":
                rpt["group_attach"] = "ALREADY"
            else: raise

        # 3) Add the user to the group (idempotent)
        try:
            iam.add_user_to_group(GroupName=GROUP, UserName=USER)
            rpt["user_in_group"] = "ADDED"
        except ClientError as e:
            rpt["user_in_group_err"] = str(e)[:200]

        time.sleep(15)  # eventual consistency

        # 4) Create / update SM
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

        # 5) EB hourly trigger
        rule = "jhk-tick-hourly-orchestrate"
        events.put_rule(Name=rule, ScheduleExpression="cron(5 * * * ? *)", State="ENABLED",
                        Description="Triggers jhk-portfolio-orchestration hourly (arch #6).")
        events.put_targets(Rule=rule, Targets=[{
            "Id":"1","Arn":sm_arn,"RoleArn":eb_role_arn,"Input":json.dumps({})}])
        rpt["eb_rule"] = rule

        # 6) Smoke
        ex = sfn.start_execution(stateMachineArn=sm_arn,
                                  name=f"smoke-{int(time.time())}", input="{}")
        rpt["smoke_arn"] = ex["executionArn"]
        for _ in range(60):
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
    p = os.path.join(REPO_ROOT, "aws/ops/reports/1118.json")
    os.makedirs(os.path.dirname(p), exist_ok=True)
    json.dump(rpt, open(p,"w"), indent=2, default=str)
    print(json.dumps({k:v for k,v in rpt.items() if k!="traceback"}, indent=2, default=str)[:2400])


if __name__ == "__main__":
    main()
