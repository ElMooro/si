"""ops 1115 — retry arch #6 (IAM ASCII) + redeploy arch #7 (fix synthetic markers)."""
import io, json, os, time, traceback, zipfile, base64
from datetime import datetime, timezone
import boto3
from botocore.exceptions import ClientError
from botocore.config import Config

REGION = "us-east-1"
ACCOUNT = "857687956942"
REPO_ROOT = os.environ.get("REPO_ROOT", os.getcwd())

SM_NAME = "jhk-portfolio-orchestration"
SFN_ROLE = "jhk-step-fn-exec-role"
EB_ROLE  = "jhk-eb-to-sfn-role"

_cfg = Config(connect_timeout=10, read_timeout=240, retries={"max_attempts": 2})
iam = boto3.client("iam", region_name=REGION)
sfn = boto3.client("stepfunctions", region_name=REGION)
events = boto3.client("events", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION, config=_cfg)
s3 = boto3.client("s3", region_name=REGION)

SFN_TRUST = {"Version": "2012-10-17", "Statement": [
    {"Effect":"Allow","Principal":{"Service":"states.amazonaws.com"},"Action":"sts:AssumeRole"}]}
EB_TRUST = {"Version": "2012-10-17", "Statement": [
    {"Effect":"Allow","Principal":{"Service":"events.amazonaws.com"},"Action":"sts:AssumeRole"}]}


def ensure_role(name, trust, policy_name, policy):
    try:
        iam.get_role(RoleName=name)
    except ClientError as e:
        if e.response["Error"]["Code"] != "NoSuchEntity": raise
        iam.create_role(RoleName=name, AssumeRolePolicyDocument=json.dumps(trust),
                        Description="Managed by ops 1115")
    iam.put_role_policy(RoleName=name, PolicyName=policy_name, PolicyDocument=json.dumps(policy))


def zip_src(d):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        for root, _, files in os.walk(d):
            for f in files:
                if f.endswith(".pyc") or "__pycache__" in root: continue
                fp = os.path.join(root, f)
                z.write(fp, os.path.relpath(fp, d))
    return buf.getvalue()


def wait_active(fn, t=120):
    end = time.time() + t
    while time.time() < end:
        try:
            c = lam.get_function_configuration(FunctionName=fn)
            if c.get("State") == "Active" and c.get("LastUpdateStatus") in ("Successful", None):
                return True
            if c.get("LastUpdateStatus") == "Failed": return False
        except ClientError: pass
        time.sleep(2)
    return False


def deploy_step_functions(rpt):
    """Phase A: Step Functions for portfolio chain."""
    sfn_role_arn = f"arn:aws:iam::{ACCOUNT}:role/{SFN_ROLE}"
    ensure_role(SFN_ROLE, SFN_TRUST, "InvokeLambdas", {
        "Version":"2012-10-17","Statement":[
            {"Effect":"Allow","Action":["lambda:InvokeFunction"],
             "Resource":f"arn:aws:lambda:{REGION}:{ACCOUNT}:function:justhodl-*"},
            {"Effect":"Allow","Action":[
                "logs:CreateLogDelivery","logs:GetLogDelivery","logs:UpdateLogDelivery",
                "logs:DeleteLogDelivery","logs:ListLogDeliveries",
                "logs:PutResourcePolicy","logs:DescribeResourcePolicies","logs:DescribeLogGroups"],
             "Resource":"*"}]})
    rpt["sfn_role"] = sfn_role_arn

    eb_role_arn = f"arn:aws:iam::{ACCOUNT}:role/{EB_ROLE}"
    ensure_role(EB_ROLE, EB_TRUST, "StartSFn", {
        "Version":"2012-10-17","Statement":[
            {"Effect":"Allow","Action":["states:StartExecution"],
             "Resource":f"arn:aws:states:{REGION}:{ACCOUNT}:stateMachine:jhk-*"}]})
    rpt["eb_role"] = eb_role_arn

    time.sleep(8)  # IAM eventual consistency

    definition = json.load(open(os.path.join(REPO_ROOT, "aws/step-functions/portfolio-orchestration.json")))
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

    rule_name = "jhk-tick-hourly-orchestrate"
    events.put_rule(Name=rule_name, ScheduleExpression="cron(5 * * * ? *)", State="ENABLED",
                    Description="Triggers jhk-portfolio-orchestration hourly (arch #6).")
    events.put_targets(Rule=rule_name, Targets=[{
        "Id":"1","Arn":sm_arn,"RoleArn":eb_role_arn,"Input":json.dumps({})}])
    rpt["eb_rule"] = rule_name

    # Smoke
    try:
        ex = sfn.start_execution(stateMachineArn=sm_arn,
                                  name=f"smoke-{int(time.time())}", input="{}")
        rpt["smoke_arn"] = ex["executionArn"]
        for _ in range(48):
            time.sleep(5)
            s = sfn.describe_execution(executionArn=ex["executionArn"])
            if s["status"] != "RUNNING":
                rpt["smoke_status"] = s["status"]
                if s.get("error"): rpt["smoke_err"] = s["error"]
                if s.get("cause"): rpt["smoke_cause"] = s["cause"][:400]
                break
        else:
            rpt["smoke_status"] = "POLL_TIMEOUT"
    except Exception as e:
        rpt["smoke_err"] = str(e)[:300]


def redeploy_synthetic(rpt):
    """Phase B: redeploy synthetic monitor with corrected markers."""
    FN = "justhodl-synthetic-monitor"
    src_dir = os.path.join(REPO_ROOT, "aws/lambdas", FN, "source")
    wait_active(FN)
    lam.update_function_code(FunctionName=FN, ZipFile=zip_src(src_dir), Publish=False)
    wait_active(FN)
    rpt["synth_redeploy"] = "OK"

    inv = lam.invoke(FunctionName=FN, InvocationType="RequestResponse",
                      Payload=b"{}", LogType="Tail")
    body = json.loads(inv["Payload"].read() or b"{}")
    if isinstance(body, dict) and "body" in body:
        try: body = json.loads(body["body"])
        except Exception: pass
    rpt["synth_invoke_body"] = body
    rpt["synth_log_tail"] = base64.b64decode(inv.get("LogResult","")).decode("utf-8","replace")[-1200:]

    time.sleep(2)
    try:
        mr = json.loads(s3.get_object(Bucket="justhodl-dashboard-live",
                                       Key="data/synthetic-monitor.json")["Body"].read())
        rpt["synth_summary"] = {
            "all_ok": mr.get("all_ok"),
            "page_ok": mr.get("page_ok"), "pages_total": mr.get("pages_total"),
            "feed_ok": mr.get("feed_ok"), "feeds_total": mr.get("feeds_total"),
        }
        fails = [{"path": p.get("path"), "reason": p.get("reason")}
                  for p in mr.get("pages",[]) if not p.get("ok")]
        if fails: rpt["synth_page_failures"] = fails
    except Exception as e:
        rpt["synth_summary_err"] = str(e)[:200]


def main():
    rpt = {"started": datetime.now(timezone.utc).isoformat()}
    try:
        deploy_step_functions(rpt)
    except Exception as e:
        rpt["sfn_err"] = str(e)[:400]
        rpt["sfn_traceback"] = traceback.format_exc()[-1200:]
    try:
        redeploy_synthetic(rpt)
    except Exception as e:
        rpt["synth_err"] = str(e)[:400]
        rpt["synth_traceback"] = traceback.format_exc()[-1200:]
    rpt["finished"] = datetime.now(timezone.utc).isoformat()
    p = os.path.join(REPO_ROOT, "aws/ops/reports/1115.json")
    os.makedirs(os.path.dirname(p), exist_ok=True)
    json.dump(rpt, open(p,"w"), indent=2, default=str)
    print(json.dumps({k:v for k,v in rpt.items()
                      if k not in ("synth_log_tail","sfn_traceback","synth_traceback")},
                      indent=2, default=str)[:2400])


if __name__ == "__main__":
    main()
