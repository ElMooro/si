"""ops 1112 — Arch #6: Step Functions for portfolio chain orchestration.

Creates/updates:
  1. IAM role  jhk-step-fn-exec-role  (trust states.amazonaws.com, allow lambda:Invoke)
  2. State machine  jhk-portfolio-orchestration  from aws/step-functions/portfolio-orchestration.json
  3. EventBridge rule  jhk-tick-hourly-orchestrate  (hourly, +5min offset to land after data refresh)
     targeting the state machine (via a new role for EB->SFn)
  4. Optionally invokes one execution as smoke test
"""
import json, os, time, traceback
from datetime import datetime, timezone
import boto3
from botocore.exceptions import ClientError

REGION = "us-east-1"
ACCOUNT = "857687956942"
REPO_ROOT = os.environ.get("REPO_ROOT", os.getcwd())

SM_NAME = "jhk-portfolio-orchestration"
SM_DEF_PATH = "aws/step-functions/portfolio-orchestration.json"

SFN_ROLE = "jhk-step-fn-exec-role"      # for SFn → Lambda invokes
EB_ROLE  = "jhk-eb-to-sfn-role"          # for EventBridge → SFn start

iam = boto3.client("iam", region_name=REGION)
sfn = boto3.client("stepfunctions", region_name=REGION)
events = boto3.client("events", region_name=REGION)


SFN_TRUST = {
    "Version": "2012-10-17",
    "Statement": [{
        "Effect": "Allow",
        "Principal": {"Service": "states.amazonaws.com"},
        "Action": "sts:AssumeRole"
    }]
}
EB_TRUST = {
    "Version": "2012-10-17",
    "Statement": [{
        "Effect": "Allow",
        "Principal": {"Service": "events.amazonaws.com"},
        "Action": "sts:AssumeRole"
    }]
}


def ensure_role(name, trust, inline_policy_name, inline_policy):
    """Create the role if missing, attach the inline policy."""
    try:
        iam.get_role(RoleName=name)
        created = False
    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchEntity":
            iam.create_role(
                RoleName=name,
                AssumeRolePolicyDocument=json.dumps(trust),
                Description=f"Managed by ops 1112 — {name}",
            )
            created = True
        else:
            raise
    iam.put_role_policy(
        RoleName=name,
        PolicyName=inline_policy_name,
        PolicyDocument=json.dumps(inline_policy),
    )
    return name, created


def main():
    rpt = {"started": datetime.now(timezone.utc).isoformat()}
    try:
        # ─── Step 1: IAM roles ───
        sfn_role_arn = f"arn:aws:iam::{ACCOUNT}:role/{SFN_ROLE}"
        ensure_role(SFN_ROLE, SFN_TRUST, "InvokeLambdas", {
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Action": ["lambda:InvokeFunction"],
                "Resource": f"arn:aws:lambda:{REGION}:{ACCOUNT}:function:justhodl-*"
            }, {
                "Effect": "Allow",
                "Action": ["logs:CreateLogDelivery", "logs:GetLogDelivery", "logs:UpdateLogDelivery",
                          "logs:DeleteLogDelivery", "logs:ListLogDeliveries",
                          "logs:PutResourcePolicy", "logs:DescribeResourcePolicies",
                          "logs:DescribeLogGroups"],
                "Resource": "*"
            }]
        })
        rpt["sfn_role"] = sfn_role_arn

        eb_role_arn = f"arn:aws:iam::{ACCOUNT}:role/{EB_ROLE}"
        ensure_role(EB_ROLE, EB_TRUST, "StartSFn", {
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Action": ["states:StartExecution"],
                "Resource": f"arn:aws:states:{REGION}:{ACCOUNT}:stateMachine:jhk-*"
            }]
        })
        rpt["eb_role"] = eb_role_arn

        # IAM is eventually consistent — wait a moment before using the role
        time.sleep(8)

        # ─── Step 2: State Machine ───
        definition = json.load(open(os.path.join(REPO_ROOT, SM_DEF_PATH)))
        sm_arn = f"arn:aws:states:{REGION}:{ACCOUNT}:stateMachine:{SM_NAME}"

        try:
            existing = sfn.describe_state_machine(stateMachineArn=sm_arn)
            sfn.update_state_machine(
                stateMachineArn=sm_arn,
                definition=json.dumps(definition),
                roleArn=sfn_role_arn,
            )
            rpt["sm"] = "UPDATED"
        except ClientError as e:
            if e.response["Error"]["Code"] == "StateMachineDoesNotExist":
                r = sfn.create_state_machine(
                    name=SM_NAME,
                    definition=json.dumps(definition),
                    roleArn=sfn_role_arn,
                    type="STANDARD",
                )
                rpt["sm"] = "CREATED"
                sm_arn = r["stateMachineArn"]
            else:
                raise
        rpt["sm_arn"] = sm_arn

        # ─── Step 3: EventBridge hourly trigger ───
        rule_name = "jhk-tick-hourly-orchestrate"
        events.put_rule(
            Name=rule_name,
            ScheduleExpression="cron(5 * * * ? *)",  # hourly at :05
            State="ENABLED",
            Description="Triggers jhk-portfolio-orchestration Step Function hourly (arch #6).",
        )
        events.put_targets(
            Rule=rule_name,
            Targets=[{
                "Id": "1",
                "Arn": sm_arn,
                "RoleArn": eb_role_arn,
                "Input": json.dumps({}),
            }],
        )
        rpt["eb_rule"] = rule_name

        # ─── Step 4: Smoke test execution ───
        try:
            exec_resp = sfn.start_execution(
                stateMachineArn=sm_arn,
                name=f"smoke-{int(time.time())}",
                input="{}",
            )
            rpt["smoke_arn"] = exec_resp["executionArn"]
            # Poll for completion (≤ 4 min)
            for _ in range(48):
                time.sleep(5)
                ex = sfn.describe_execution(executionArn=exec_resp["executionArn"])
                if ex["status"] != "RUNNING":
                    rpt["smoke_status"] = ex["status"]
                    rpt["smoke_started"] = ex["startDate"].isoformat()
                    if ex.get("stopDate"):
                        rpt["smoke_stopped"] = ex["stopDate"].isoformat()
                    if ex.get("output"):
                        try:
                            rpt["smoke_output_sample"] = json.loads(ex["output"])
                        except Exception:
                            rpt["smoke_output_sample"] = ex["output"][:500]
                    if ex.get("error"):
                        rpt["smoke_err"] = ex["error"]
                    if ex.get("cause"):
                        rpt["smoke_cause"] = ex["cause"][:500]
                    break
            else:
                rpt["smoke_status"] = "TIMEOUT_AT_POLL"
        except Exception as e:
            rpt["smoke_err"] = str(e)[:400]

    except Exception as e:
        rpt["fatal_err"] = str(e)[:500]
        rpt["traceback"] = traceback.format_exc()[-1500:]

    rpt["finished"] = datetime.now(timezone.utc).isoformat()
    p = os.path.join(REPO_ROOT, "aws/ops/reports/1112.json")
    os.makedirs(os.path.dirname(p), exist_ok=True)
    json.dump(rpt, open(p, "w"), indent=2, default=str)
    print(json.dumps({k: v for k, v in rpt.items() if k != "traceback"}, indent=2, default=str)[:2200])


if __name__ == "__main__":
    main()
