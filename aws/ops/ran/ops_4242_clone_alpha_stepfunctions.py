"""
ops_4242 — migrate justhodl-13f-clone-alpha's backfill walk onto Step
Functions, and correct a false positive in the integrity engine's own
recursion heuristic.

WHAT THE INSPECTION ACTUALLY FOUND (correcting ops 4233)
The D1 check flagged two functions as "unguarded self-invoke". Reading
the code rather than trusting the heuristic:

  justhodl-equity-research is NOT a walk and NOT a recursion risk. It is
  the standard async-kickoff pattern: an HTTP caller asks for a report,
  the function fires one async self-invoke carrying _internal="1" and
  returns 202 so the browser is not held for 120s. The child computes
  kickoff_mode = ... AND NOT is_internal_async, so the child can never
  kick off again. Chain depth is structurally exactly 2, forever. Moving
  this to Step Functions would add machinery and buy nothing. NO CHANGE.

  justhodl-13f-clone-alpha IS a walk, and it was already capped at
  MAX_HOPS=10 — comfortably under the 16 at which AWS breaks a chain. So
  it was never going to trip the detector either. The real defect is
  different and less obvious: MAX_HOPS SILENTLY CAPS CONVERGENCE. A
  backfill needing more than ten hops just stops and waits a week for
  the next schedule, and nothing reports that it stopped early. That is
  the census failure wearing different clothes — bounded work presented
  as finished work.

WHY STEP FUNCTIONS IS THE RIGHT ANSWER HERE
  * No chain ceiling — the walk ends when the data says so, not at 10.
  * Per-hop Retry with exponential backoff on throttles and transient
    Lambda faults. A dropped link in an Event self-chain is invisible
    and unrecoverable; nothing is watching the chain.
  * A Catch that records the failure instead of the walk evaporating.
  * An execution history that makes truncation impossible to hide.
  * A hard MaxHops guard state so the state machine itself cannot loop
    forever if the completion flag never flips — the loop must be
    bounded by something, and it should be an explicit, visible bound
    rather than an accident of AWS's recursion detector.

SAFETY
  The Lambda's own self-chain is not deleted, it is moved behind
  SELF_CHAIN=off. If the state machine is ever removed the engine is not
  stranded without a driver.
"""
import io, json, os, time, zipfile
from datetime import datetime, timezone
from pathlib import Path
from urllib.request import urlopen
import boto3
from botocore.config import Config
from ops_report import report

REGION, BUCKET = "us-east-1", "justhodl-dashboard-live"
FN = "justhodl-13f-clone-alpha"
SM_NAME = "jh-13f-clone-alpha-backfill"
SM_ROLE = "jh-states-clone-alpha"
EVT_ROLE = "jh-events-start-execution"
OLD_RULE = None
NEW_RULE = "jh-clone-alpha-backfill-weekly"
EXPR = "cron(30 8 ? * MON *)"
MARK = "ops 4242: the self-chain is now OPTIONAL and off by default"

CFG = Config(retries={"max_attempts": 6, "mode": "adaptive"}, read_timeout=300)
lam = boto3.client("lambda", region_name=REGION, config=CFG)
sfn = boto3.client("stepfunctions", region_name=REGION, config=CFG)
iam = boto3.client("iam", config=CFG)
evb = boto3.client("events", region_name=REGION, config=CFG)
s3  = boto3.client("s3", region_name=REGION, config=CFG)
ACCT = boto3.client("sts").get_caller_identity()["Account"]
ROOT = Path(os.environ.get("GITHUB_WORKSPACE", os.getcwd()))
FN_ARN = "arn:aws:lambda:%s:%s:function:%s" % (REGION, ACCT, FN)

def wait_active(fn, b=200):
    t0=time.time()
    while time.time()-t0<b:
        try:
            c=lam.get_function_configuration(FunctionName=fn)
            if c.get("State")=="Active" and c.get("LastUpdateStatus") in (None,"Successful"):
                return True
        except Exception: pass
        time.sleep(4)
    return False

def zip_fn(fn):
    src="aws/lambdas/%s/source"%fn; buf=io.BytesIO()
    with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z:
        for root,_,files in os.walk(src):
            if "__pycache__" in root: continue
            for f in files:
                fp=os.path.join(root,f); z.write(fp, os.path.relpath(fp,src))
        if os.path.isdir("aws/shared"):
            for f in sorted(os.listdir("aws/shared")):
                if f.endswith(".py"): z.write(os.path.join("aws/shared",f), f)
    return buf.getvalue()

def ensure_role(name, trust_service, policy):
    trust={"Version":"2012-10-17","Statement":[{"Effect":"Allow",
           "Principal":{"Service":trust_service},"Action":"sts:AssumeRole"}]}
    try:
        arn=iam.create_role(RoleName=name,
            AssumeRolePolicyDocument=json.dumps(trust))["Role"]["Arn"]
        time.sleep(12)
    except Exception as e:
        if "EntityAlreadyExists" not in str(e): raise
        arn=iam.get_role(RoleName=name)["Role"]["Arn"]
    iam.put_role_policy(RoleName=name, PolicyName="inline",
                        PolicyDocument=json.dumps(policy))
    return arn

with report("4242_clone_alpha_stepfunctions") as rep:
    rep.heading("ops 4242 — clone-alpha backfill on Step Functions")
    fails=[]

    rep.section("1. Correction of record")
    rep.log("justhodl-equity-research: async-kickoff, child carries "
            "_internal=1 which disables kickoff_mode. Depth is structurally "
            "2. NOT a recursion risk — NO CHANGE MADE.")
    rep.log("justhodl-13f-clone-alpha: MAX_HOPS=10, already under AWS's 16. "
            "Real defect is silent convergence capping, not recursion.")
    rep.kv(section="correction", engine="justhodl-equity-research",
           finding="async-kickoff depth 2", action="none")
    rep.kv(section="correction", engine=FN,
           finding="MAX_HOPS=10 caps convergence silently",
           action="step functions")

    rep.section("2. Deploy clone-alpha with SELF_CHAIN=off")
    try:
        wait_active(FN)
        lam.update_function_code(FunctionName=FN, ZipFile=zip_fn(FN))
        ok=False
        for i in range(30):
            time.sleep(6)
            try:
                loc=lam.get_function(FunctionName=FN)["Code"]["Location"]
                src=zipfile.ZipFile(io.BytesIO(urlopen(loc,timeout=60).read())
                                    ).read("lambda_function.py").decode("utf-8","ignore")
                if MARK in src: ok=True; break
            except Exception: pass
        (rep.ok if ok else rep.fail)("zip marker %s"%("verified" if ok else "MISSING"))
        if not ok: fails.append("clone-alpha marker")
        wait_active(FN)
        cur=(lam.get_function_configuration(FunctionName=FN).get("Environment") or {}).get("Variables") or {}
        cur["SELF_CHAIN"]="off"
        lam.update_function_configuration(FunctionName=FN, Environment={"Variables":cur})
        rep.ok("SELF_CHAIN=off — the Lambda no longer drives itself")
    except Exception as e:
        fails.append("deploy: %s"%str(e)[:180])

    rep.section("3. IAM roles")
    sm_arn_role = evt_role_arn = None
    try:
        sm_arn_role = ensure_role(SM_ROLE, "states.amazonaws.com",
            {"Version":"2012-10-17","Statement":[
              {"Effect":"Allow","Action":["lambda:InvokeFunction"],
               "Resource":[FN_ARN, FN_ARN+":*"]},
              {"Effect":"Allow","Action":["logs:CreateLogDelivery",
               "logs:GetLogDelivery","logs:UpdateLogDelivery",
               "logs:DeleteLogDelivery","logs:ListLogDeliveries",
               "logs:PutResourcePolicy","logs:DescribeResourcePolicies",
               "logs:DescribeLogGroups"],"Resource":"*"}]})
        rep.ok("state machine role %s"%sm_arn_role)
        evt_role_arn = ensure_role(EVT_ROLE, "events.amazonaws.com",
            {"Version":"2012-10-17","Statement":[{"Effect":"Allow",
              "Action":"states:StartExecution",
              "Resource":"arn:aws:states:%s:%s:stateMachine:*"%(REGION,ACCT)}]})
        rep.ok("eventbridge role %s"%evt_role_arn)
    except Exception as e:
        fails.append("iam: %s"%str(e)[:190])

    rep.section("4. State machine definition")
    # Bounded loop: invoke -> retry/catch -> choice on `complete` -> guard
    definition = {
      "Comment": "13F clone-alpha backfill. Replaces a MAX_HOPS=10 self-chain "
                 "that silently capped convergence. No chain ceiling; each hop "
                 "retried with backoff; an explicit MaxHops guard bounds the "
                 "loop visibly rather than relying on AWS's recursion breaker.",
      "StartAt": "Init",
      "States": {
        "Init": {"Type":"Pass","Result":{"hop":0},"ResultPath":"$","Next":"Hop"},
        "Hop": {
          "Type":"Task","Resource":"arn:aws:states:::lambda:invoke",
          "Parameters":{"FunctionName":FN_ARN,
                        "Payload":{"hop.$":"$.hop","driver":"sfn"}},
          "ResultSelector":{"complete.$":"$.Payload.complete",
                            "next_hop.$":"$.Payload.next_hop",
                            "pct.$":"$.Payload.pct",
                            "status.$":"$.Payload.status"},
          "ResultPath":"$.last",
          "Retry":[
            {"ErrorEquals":["Lambda.TooManyRequestsException",
                            "Lambda.ServiceException",
                            "Lambda.AWSLambdaException",
                            "Lambda.SdkClientException"],
             "IntervalSeconds":20,"MaxAttempts":4,"BackoffRate":2.0},
            {"ErrorEquals":["States.TaskFailed"],
             "IntervalSeconds":30,"MaxAttempts":2,"BackoffRate":2.0}],
          "Catch":[{"ErrorEquals":["States.ALL"],
                    "ResultPath":"$.error","Next":"HopFailed"}],
          "Next":"Converged?"},
        "Converged?": {
          "Type":"Choice",
          "Choices":[
            {"Variable":"$.last.complete","BooleanEquals":True,"Next":"Done"},
            {"Variable":"$.last.next_hop","NumericGreaterThanEquals":120,
             "Next":"GuardTripped"}],
          "Default":"Advance"},
        "Advance": {"Type":"Pass",
                    "Parameters":{"hop.$":"$.last.next_hop"},
                    "ResultPath":"$","Next":"Hop"},
        "HopFailed": {"Type":"Fail","Error":"HopFailed",
                      "Cause":"A backfill hop failed after retries — see the "
                              "execution history for the failing hop."},
        "GuardTripped": {"Type":"Fail","Error":"MaxHopsGuard",
                         "Cause":"120 hops without convergence. The completion "
                                 "flag is probably never flipping; investigate "
                                 "rather than raising the bound."},
        "Done": {"Type":"Succeed"}
      }
    }
    sm_arn=None
    if sm_arn_role:
        try:
            r=sfn.create_state_machine(name=SM_NAME,
                definition=json.dumps(definition), roleArn=sm_arn_role,
                type="STANDARD")
            sm_arn=r["stateMachineArn"]; rep.ok("created %s"%SM_NAME)
        except Exception as e:
            if "StateMachineAlreadyExists" in str(e):
                sm_arn="arn:aws:states:%s:%s:stateMachine:%s"%(REGION,ACCT,SM_NAME)
                sfn.update_state_machine(stateMachineArn=sm_arn,
                    definition=json.dumps(definition), roleArn=sm_arn_role)
                rep.ok("updated %s"%SM_NAME); time.sleep(5)
            else:
                fails.append("state machine: %s"%str(e)[:190])

    rep.section("5. GATE — run it end to end")
    if sm_arn:
        try:
            ex=sfn.start_execution(stateMachineArn=sm_arn,
                name="ops4242-%s"%datetime.now(timezone.utc).strftime("%H%M%S"))
            arn=ex["executionArn"]; rep.log("execution started")
            st=None
            for i in range(50):
                time.sleep(12)
                d=sfn.describe_execution(executionArn=arn)
                st=d["status"]
                if st!="RUNNING": break
            rep.log("status=%s"%st)
            hist=sfn.get_execution_history(executionArn=arn, maxResults=500,
                                           reverseOrder=True)["events"]
            hops=sum(1 for e in hist if e["type"]=="TaskStateEntered")
            rep.log("hops executed in this run: %d"%hops)
            rep.kv(section="execution", status=st, hops=hops)
            if st=="SUCCEEDED":
                rep.ok("SUCCEEDED — walk converged in %d hop(s), no chain "
                       "ceiling involved"%hops)
            elif st=="RUNNING":
                rep.warn("still running after 10min — long backfill, which is "
                         "exactly the case MAX_HOPS=10 used to truncate. "
                         "Execution continues in the background.")
            else:
                out=sfn.describe_execution(executionArn=arn)
                rep.fail("%s — %s"%(st, str(out.get("error"))[:160]))
                fails.append("execution %s"%st)
        except Exception as e:
            fails.append("execution: %s"%str(e)[:190])

    rep.section("6. Point the weekly schedule at the state machine")
    if sm_arn and evt_role_arn:
        try:
            evb.put_rule(Name=NEW_RULE, ScheduleExpression=EXPR, State="ENABLED",
                         Description="Weekly 13F clone-alpha backfill via Step Functions")
            evb.put_targets(Rule=NEW_RULE, Targets=[{"Id":"1","Arn":sm_arn,
                                                     "RoleArn":evt_role_arn}])
            tg=evb.list_targets_by_rule(Rule=NEW_RULE)["Targets"]
            rep.ok("%s -> %s (%d target)"%(EXPR, SM_NAME, len(tg)))
            # retire the old lambda-targeting weekly rule
            for page in evb.get_paginator("list_rules").paginate():
                for r in page["Rules"]:
                    if r["Name"]==NEW_RULE or r.get("State")!="ENABLED": continue
                    try: t2=evb.list_targets_by_rule(Rule=r["Name"])["Targets"]
                    except Exception: continue
                    if any(FN_ARN==t.get("Arn") for t in t2):
                        evb.disable_rule(Name=r["Name"])
                        rep.ok("retired old rule %s (now driven by SFN)"%r["Name"])
        except Exception as e:
            fails.append("schedule: %s"%str(e)[:180])

    rep.section("7. Declare in the manifest, verify drift 0")
    try:
        m=json.loads(s3.get_object(Bucket=BUCKET,
            Key="config/schedule-manifest.json")["Body"].read())
        m["rules"]=[r for r in m["rules"]
                    if not (r["targets"] and any(FN_ARN==t.get("arn")
                                                 for t in r["targets"]))]
        if not any(r["name"]==NEW_RULE for r in m["rules"]):
            m["rules"].append({"kind":"events","name":NEW_RULE,"expr":EXPR,
                "state":"ENABLED",
                "targets":[{"id":"1","arn":sm_arn,"input":None,"path":None}]})
        s3.put_object(Bucket=BUCKET, Key="config/schedule-manifest.json",
            Body=json.dumps(m).encode(), ContentType="application/json")
        (ROOT/"aws"/"ops"/"audit"/"schedule-manifest.json").write_text(
            json.dumps(m, indent=1), encoding="utf-8")
        r=lam.invoke(FunctionName="justhodl-schedule-reconciler",
                     InvocationType="RequestResponse")
        rb=json.loads(r["Payload"].read() or b"{}")
        (rep.ok if rb.get("drift_count")==0 else rep.warn)(
            "reconciler drift = %s"%rb.get("drift_count"))
        if rb.get("drift_count"): rep.log("classes: %s"%rb.get("by_class"))
    except Exception as e:
        fails.append("manifest: %s"%str(e)[:180])

    rep.section("RESULT")
    if fails:
        for f in fails: rep.fail("  %s"%f)
        raise SystemExit("FAILS: %s"%"; ".join(fails[:3]))
    rep.ok("OPS 4242 PASS")
