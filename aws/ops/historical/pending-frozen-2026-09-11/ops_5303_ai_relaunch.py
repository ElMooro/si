"""ops_5303 -- RE-ARM of ops_5302 after its report: CreateEndpointConfig rejected a name that ended on a dash (63-char cut), and the market read came back empty (llm_router gates background calls; the read is now on_demand with a bounded direct Sonnet fallback). Also real keys for metals (screener/metals-miners.json), crypto (crypto-intel.json), btc cycle (data/crypto-cycle-risk.json).
refused ModelDataSource and the real-time container failed its ping (no SAGEMAKER_SUBMIT_DIRECTORY). v1.2.0 repacks the prefix
into a tarball for serverless, sets the script env on every prepacked path, and this op tails the endpoint's CloudWatch log on any
failure. Also: ledger + LLM permissions for the market read, the daily market-read schedule, the first read, and the verdict JSON the page shows. ops_5300 -- launch justhodl-ai (the SageMaker front window) + ai.html, and make it learn from the Brain.

Order of operations (every step idempotent; nothing here creates a persistent hourly-billing resource
without a TTL, and the only real-time endpoint this op may create is deleted again before it exits):

  1. IAM  -- justhodl-sagemaker-execution-role (trust sagemaker.amazonaws.com; SageMaker full access
             + the two buckets + the JumpStart cache) and an inline control policy on lambda-execution-role
             (sagemaker control plane, iam:PassRole on the execution role only, private bucket, pricing,
             Cost Explorer, CloudWatch metrics). AccessDenied here is reported with the exact remedy.
  2. S3   -- private ML bucket justhodl-ai-857687956942: public access blocked, AES256 default encryption,
             lifecycle expiring repacked model copies after 30 days. Note text / embeddings / datasets live
             here only; the public bucket gets counts.
  3. Lambda -- create/update justhodl-ai from source (env-first secrets: JH_SERVICE_TOKEN from SSM
             /justhodl/api-admin/token, SAGEMAKER_ROLE_ARN), Function URL (service-token gated inside),
             control pointer ai/control.json + data/ai/control.json (the worker reads the public one).
  4. Scheduler -- justhodl-ai-inventory cron(7 * * * ? *) UTC (EventBridge Scheduler, never a classic rule).
  5. Inventory -- async invoke + poll data/ai.json; audit every section (which SageMaker APIs the role can
             read), the hub catalog (which of the article's four RoBERTa-SEC cards exist today), policy.
  6. Learn from the Brain -- dataset build -> deploy the best available embedding card (RoBERTa-SEC-Base if
             present, else the first hub text-embedding card) serverless (real-time ml.m5.xlarge with a 2h
             TTL only as fallback) -> embed every row (resumable) -> XGBoost classifier on managed spot ->
             serverless classifier endpoint -> one inference + retrieval proof. Every wait is bounded.
  7. Page -- AI_DESK_V1 at the edge, Chrome 1440/390 render gate.
"""
import json
import re
import subprocess
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402
from _lambda_deploy_helpers import build_zip, create_or_update_lambda  # noqa: E402

FN = "justhodl-ai"
ACCT = "857687956942"
REGION = "us-east-1"
PUBLIC = "justhodl-dashboard-live"
PRIVATE = "justhodl-ai-%s" % ACCT
LAMBDA_ROLE = "lambda-execution-role"
SM_ROLE = "justhodl-sagemaker-execution-role"
SM_ROLE_ARN = "arn:aws:iam::%s:role/%s" % (ACCT, SM_ROLE)
SCHED_ROLE = "arn:aws:iam::%s:role/justhodl-scheduler-role" % ACCT
OUT = "data/ai.json"
CFG = Config(retries={"max_attempts": 4, "mode": "adaptive"}, read_timeout=120)
iam = boto3.client("iam", config=CFG)
s3 = boto3.client("s3", region_name=REGION, config=CFG)
lam = boto3.client("lambda", region_name=REGION, config=CFG)
# sync invokes (embed passes up to 10 min, the market read 1-3 min) must never be retried by botocore -- a retry re-runs the action
lam_sync = boto3.client("lambda", region_name=REGION, config=Config(read_timeout=910, connect_timeout=20, retries={"max_attempts": 0}))
sch = boto3.client("scheduler", region_name=REGION, config=CFG)
ssm = boto3.client("ssm", region_name=REGION, config=CFG)
SHOTS = ROOT / "aws" / "ops" / "reports" / "latest" / "shots"
UA = {"User-Agent": "justhodl-ops-5303", "Cache-Control": "no-cache", "Pragma": "no-cache"}
FAILS, WARNS, NEEDS_KHALID = [], [], []

LAMBDA_CONTROL_POLICY = {
    "Version": "2012-10-17",
    "Statement": [
        {"Sid": "SageMakerControlPlane", "Effect": "Allow", "Resource": "*", "Action": [
            "sagemaker:List*", "sagemaker:Describe*", "sagemaker:Search", "sagemaker:AddTags", "sagemaker:ListTags", "sagemaker:DeleteTags",
            "sagemaker:CreateModel", "sagemaker:DeleteModel", "sagemaker:CreateEndpointConfig", "sagemaker:DeleteEndpointConfig",
            "sagemaker:CreateEndpoint", "sagemaker:UpdateEndpoint", "sagemaker:DeleteEndpoint", "sagemaker:InvokeEndpoint",
            "sagemaker:CreateTrainingJob", "sagemaker:StopTrainingJob", "sagemaker:CreateAutoMLJobV2", "sagemaker:StopAutoMLJob",
            "sagemaker:CreateCluster", "sagemaker:DeleteCluster", "sagemaker:UpdateCluster", "sagemaker:DescribeHubContent", "sagemaker:ListHubContents"]},
        {"Sid": "PassSageMakerExecutionRole", "Effect": "Allow", "Action": "iam:PassRole", "Resource": SM_ROLE_ARN,
         "Condition": {"StringEquals": {"iam:PassedToService": "sagemaker.amazonaws.com"}}},
        {"Sid": "PrivateMlBucket", "Effect": "Allow", "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket", "s3:GetBucketLocation"],
         "Resource": ["arn:aws:s3:::%s" % PRIVATE, "arn:aws:s3:::%s/*" % PRIVATE]},
        {"Sid": "JumpStartCacheRead", "Effect": "Allow", "Action": ["s3:GetObject", "s3:ListBucket"],
         "Resource": ["arn:aws:s3:::jumpstart-cache-prod-us-east-1", "arn:aws:s3:::jumpstart-cache-prod-us-east-1/*"]},
        {"Sid": "PricingAndCost", "Effect": "Allow", "Resource": "*", "Action": ["pricing:GetProducts", "ce:GetCostAndUsage", "cloudwatch:GetMetricStatistics", "cloudwatch:GetMetricData"]},
        {"Sid": "SelfUrl", "Effect": "Allow", "Action": ["lambda:GetFunctionUrlConfig"], "Resource": "arn:aws:lambda:%s:%s:function:%s" % (REGION, ACCT, FN)},
        {"Sid": "ServiceToken", "Effect": "Allow", "Action": ["ssm:GetParameter"], "Resource": "arn:aws:ssm:%s:%s:parameter/justhodl/api-admin/token" % (REGION, ACCT)},
        {"Sid": "LlmKeys", "Effect": "Allow", "Action": ["ssm:GetParameter", "ssm:GetParameters"], "Resource": "arn:aws:ssm:%s:%s:parameter/justhodl/*" % (REGION, ACCT)},
        {"Sid": "SignalLedger", "Effect": "Allow", "Action": ["dynamodb:GetItem", "dynamodb:PutItem", "dynamodb:UpdateItem", "dynamodb:Query"],
         "Resource": ["arn:aws:dynamodb:%s:%s:table/justhodl-signals" % (REGION, ACCT), "arn:aws:dynamodb:%s:%s:table/justhodl-signals/index/*" % (REGION, ACCT)]},
        {"Sid": "EndpointLogs", "Effect": "Allow", "Action": ["logs:FilterLogEvents", "logs:DescribeLogStreams"], "Resource": "arn:aws:logs:%s:%s:log-group:/aws/sagemaker/Endpoints/*" % (REGION, ACCT)},
    ],
}
SM_ROLE_INLINE = {
    "Version": "2012-10-17",
    "Statement": [
        {"Effect": "Allow", "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket", "s3:GetBucketLocation"],
         "Resource": ["arn:aws:s3:::%s" % PRIVATE, "arn:aws:s3:::%s/*" % PRIVATE, "arn:aws:s3:::%s" % PUBLIC, "arn:aws:s3:::%s/*" % PUBLIC,
                      "arn:aws:s3:::jumpstart-cache-prod-us-east-1", "arn:aws:s3:::jumpstart-cache-prod-us-east-1/*",
                      "arn:aws:s3:::jumpstart-private-cache-prod-us-east-1", "arn:aws:s3:::jumpstart-private-cache-prod-us-east-1/*"]},
        {"Effect": "Allow", "Resource": "*", "Action": ["ecr:GetAuthorizationToken", "ecr:BatchCheckLayerAvailability", "ecr:GetDownloadUrlForLayer", "ecr:BatchGetImage",
                                                        "logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents", "logs:DescribeLogStreams",
                                                        "cloudwatch:PutMetricData", "sagemaker:*"]},
    ],
}


def get_json(bucket, key):
    return json.loads(s3.get_object(Bucket=bucket, Key=key)["Body"].read())


def invoke(mode, body=None, sync=True):
    payload = json.dumps({"mode": mode, "body": body or {}}).encode()
    if not sync:
        lam.invoke(FunctionName=FN, InvocationType="Event", Payload=payload)
        return None
    r = lam_sync.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=payload)
    out = json.loads(r["Payload"].read() or b"{}")
    if r.get("FunctionError"):
        raise RuntimeError("%s: %s" % (mode, json.dumps(out)[:400]))
    if out.get("ok") is False:
        raise RuntimeError("%s refused: %s" % (mode, out.get("error")))
    return out.get("result", out)


def denied(e):
    return "AccessDenied" in str(e) or "not authorized" in str(e)


def endpoint_log_tail(name, n=40):
    """Last error-ish lines of /aws/sagemaker/Endpoints/<name> so a failed ping names its cause in the report."""
    try:
        logs = boto3.client("logs", region_name=REGION, config=CFG)
        r = logs.filter_log_events(logGroupName="/aws/sagemaker/Endpoints/%s" % name, limit=400, interleaved=True)
        ev = [e.get("message", "").rstrip() for e in r.get("events") or []]
        keep = [m for m in ev if re.search(r"error|exception|traceback|fail|cannot|no module|not found|killed|worker", m, re.I)] or ev
        return keep[-n:]
    except Exception as e:
        return ["log tail unavailable: %s" % str(e)[:120]]


STEPS = []


def step(name, ok, detail=""):
    STEPS.append({"step": name, "status": "ok" if ok is True else ("warn" if ok is None else "fail"), "detail": str(detail)[:200]})


def write_verdict(status, title, t_start):
    doc = {"ops": 5303, "status": status, "title": title, "finished_at": datetime.now(timezone.utc).isoformat(), "duration_s": round(time.time() - t_start, 1),
           "steps": STEPS, "fails": [f[:300] for f in FAILS], "warns": [w[:300] for w in WARNS],
           "report": "https://github.com/ElMooro/si/blob/main/aws/ops/reports/latest/ops_5303_ai_relaunch.md"}
    try:
        s3.put_object(Bucket=PUBLIC, Key="data/ai/verdict.json", Body=json.dumps(doc).encode(), ContentType="application/json", CacheControl="max-age=60")
    except Exception as e:
        print("verdict write failed: %s" % e)


T_START = time.time()
with report("ops_5303_ai_relaunch") as R:
    R.heading("ops 5303 -- AI relaunch v1.2.1: SageMaker-safe names (5302 config name ended on a dash), real metals/crypto keys, on-demand LLM with a bounded direct fallback (5302 read came back empty)")

    # ───────────────────────────────────────────────────── 1. IAM
    R.section("1. IAM")
    try:
        iam.get_role(RoleName=SM_ROLE)
        R.log("   %s exists" % SM_ROLE)
    except ClientError as e:
        if "NoSuchEntity" in str(e):
            try:
                iam.create_role(RoleName=SM_ROLE, Description="justhodl-ai: SageMaker execution role (training jobs, endpoints, AutoML) -- ops 5300",
                                AssumeRolePolicyDocument=json.dumps({"Version": "2012-10-17", "Statement": [{"Effect": "Allow", "Principal": {"Service": "sagemaker.amazonaws.com"}, "Action": "sts:AssumeRole"}]}),
                                Tags=[{"Key": "justhodl", "Value": "ai"}])
                R.ok("   %s created" % SM_ROLE)
                time.sleep(12)
            except ClientError as e2:
                FAILS.append("create %s: %s" % (SM_ROLE, str(e2)[:160]))
        else:
            FAILS.append("get_role %s: %s" % (SM_ROLE, str(e)[:160]))
    try:
        iam.attach_role_policy(RoleName=SM_ROLE, PolicyArn="arn:aws:iam::aws:policy/AmazonSageMakerFullAccess")
        R.ok("   AmazonSageMakerFullAccess attached to %s" % SM_ROLE)
    except ClientError as e:
        WARNS.append("attach AmazonSageMakerFullAccess: %s (inline policy covers sagemaker:* anyway)" % str(e)[:120])
    try:
        iam.put_role_policy(RoleName=SM_ROLE, PolicyName="justhodl-ai-buckets", PolicyDocument=json.dumps(SM_ROLE_INLINE))
        R.ok("   inline bucket/ecr/logs policy on %s" % SM_ROLE)
    except ClientError as e:
        FAILS.append("put_role_policy %s: %s" % (SM_ROLE, str(e)[:160]))
    try:
        iam.put_role_policy(RoleName=LAMBDA_ROLE, PolicyName="justhodl-ai-sagemaker-control", PolicyDocument=json.dumps(LAMBDA_CONTROL_POLICY))
        R.ok("   control policy justhodl-ai-sagemaker-control on %s" % LAMBDA_ROLE)
    except ClientError as e:
        FAILS.append("put_role_policy %s: %s" % (LAMBDA_ROLE, str(e)[:160]))
        NEEDS_KHALID.append("aws iam put-role-policy --role-name %s --policy-name justhodl-ai-sagemaker-control --policy-document file://aws/lambdas/justhodl-ai/iam/lambda-control-policy.json" % LAMBDA_ROLE)

    step("IAM", not any("role" in f or "policy" in f for f in FAILS))
    # ───────────────────────────────────────────────────── 2. private bucket
    R.section("2. private ML bucket")
    try:
        s3.head_bucket(Bucket=PRIVATE)
        R.log("   %s exists" % PRIVATE)
    except ClientError:
        try:
            s3.create_bucket(Bucket=PRIVATE)
            R.ok("   %s created" % PRIVATE)
        except ClientError as e:
            FAILS.append("create bucket %s: %s" % (PRIVATE, str(e)[:160]))
    for name, fn in (("public access block", lambda: s3.put_public_access_block(Bucket=PRIVATE, PublicAccessBlockConfiguration={"BlockPublicAcls": True, "IgnorePublicAcls": True, "BlockPublicPolicy": True, "RestrictPublicBuckets": True})),
                     ("default encryption", lambda: s3.put_bucket_encryption(Bucket=PRIVATE, ServerSideEncryptionConfiguration={"Rules": [{"ApplyServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"}, "BucketKeyEnabled": True}]})),
                     ("lifecycle", lambda: s3.put_bucket_lifecycle_configuration(Bucket=PRIVATE, LifecycleConfiguration={"Rules": [{"ID": "expire-repacked-models", "Status": "Enabled", "Filter": {"Prefix": "ai/models/repacked/"}, "Expiration": {"Days": 30}}, {"ID": "abort-mpu", "Status": "Enabled", "Filter": {"Prefix": ""}, "AbortIncompleteMultipartUpload": {"DaysAfterInitiation": 2}}]})),
                     ("tags", lambda: s3.put_bucket_tagging(Bucket=PRIVATE, Tagging={"TagSet": [{"Key": "justhodl", "Value": "ai"}]}))):
        try:
            fn()
            R.ok("   %s ok" % name)
        except ClientError as e:
            (FAILS if name == "public access block" else WARNS).append("%s: %s" % (name, str(e)[:140]))

    step("private bucket", not any("bucket" in f or "public access" in f for f in FAILS))
    # ───────────────────────────────────────────────────── 3. Lambda
    R.section("3. Lambda %s" % FN)
    cfg_json = json.loads((ROOT / "aws" / "lambdas" / FN / "config.json").read_text())
    env = {}
    try:
        env = dict(lam.get_function_configuration(FunctionName=FN).get("Environment", {}).get("Variables", {}))
    except Exception:
        pass
    env.update(cfg_json.get("env") or {})
    env["SAGEMAKER_ROLE_ARN"] = SM_ROLE_ARN
    if not env.get("JH_SERVICE_TOKEN"):
        try:
            env["JH_SERVICE_TOKEN"] = ssm.get_parameter(Name="/justhodl/api-admin/token", WithDecryption=True)["Parameter"]["Value"]
            R.log("   JH_SERVICE_TOKEN taken from SSM (env-first doctrine, ops 5227)")
        except Exception as e:
            FAILS.append("service token unavailable: %s -- the Function URL would refuse every worker call" % str(e)[:100])
    create_or_update_lambda(report=R, function_name=FN, zip_bytes=build_zip(ROOT / "aws" / "lambdas" / FN / "source"), env_vars=env,
                            timeout=int(cfg_json.get("timeout") or 900), memory=int(cfg_json.get("memory") or 3008), description=cfg_json.get("description", "")[:250],
                            reserved_concurrency=None, create_function_url=True, ephemeral_storage=int(cfg_json.get("ephemeral_storage") or 2048))
    for _ in range(40):
        cfg = lam.get_function_configuration(FunctionName=FN)
        if cfg.get("LastUpdateStatus") in (None, "Successful") and cfg.get("State") == "Active":
            break
        time.sleep(5)
    fn_url = ""
    try:
        fn_url = lam.get_function_url_config(FunctionName=FN)["FunctionUrl"]
    except Exception as e:
        FAILS.append("function url: %s" % str(e)[:120])
    R.log("   state %s/%s %sMB/%ss url %s" % (cfg.get("State"), cfg.get("LastUpdateStatus"), cfg.get("MemorySize"), cfg.get("Timeout"), fn_url))
    ctl = {"function_url": fn_url, "sagemaker_role_arn": SM_ROLE_ARN, "private_bucket": PRIVATE, "updated_at": datetime.now(timezone.utc).isoformat(), "ops": 5303}
    try:
        s3.put_object(Bucket=PRIVATE, Key="ai/control.json", Body=json.dumps(ctl).encode(), ContentType="application/json", ServerSideEncryption="AES256")
        s3.put_object(Bucket=PUBLIC, Key="data/ai/control.json", Body=json.dumps({"function_url": fn_url, "updated_at": ctl["updated_at"], "version": "1.0.0"}).encode(),
                      ContentType="application/json", CacheControl="max-age=60")
        R.ok("   control pointers written (private ai/control.json, public data/ai/control.json for the worker bridge)")
    except ClientError as e:
        FAILS.append("control pointer: %s" % str(e)[:140])
    # Function-URL gate must hold: no token -> 401
    try:
        req = urllib.request.Request(fn_url + "inventory", data=b"{}", method="POST", headers={"Content-Type": "application/json", **UA})
        urllib.request.urlopen(req, timeout=30)
        FAILS.append("Function URL accepted an unauthenticated POST -- the service-token gate is not holding")
    except urllib.error.HTTPError as e:
        if e.code == 401:
            R.ok("   unauthenticated POST to the Function URL -> HTTP 401 (gate holds)")
        else:
            WARNS.append("unauthenticated POST -> HTTP %s (expected 401)" % e.code)
    except Exception as e:
        WARNS.append("Function URL probe: %s" % str(e)[:100])

    step("Lambda + Function URL gate", not any("function url" in f.lower() or "service token" in f.lower() or "gate" in f.lower() for f in FAILS), fn_url)
    # ───────────────────────────────────────────────────── 4. Scheduler
    R.section("4. schedule (EventBridge Scheduler)")
    sc = cfg_json["eventbridge_scheduler"]
    tgt = {"Arn": "arn:aws:lambda:%s:%s:function:%s" % (REGION, ACCT, FN), "RoleArn": SCHED_ROLE, "Input": json.dumps({"mode": "inventory"}),
           "RetryPolicy": {"MaximumRetryAttempts": 0, "MaximumEventAgeInSeconds": 900}}
    try:
        try:
            sch.get_schedule(Name=sc["schedule_name"], GroupName="default")
            sch.update_schedule(Name=sc["schedule_name"], GroupName="default", ScheduleExpression=sc["cron"], ScheduleExpressionTimezone="UTC", FlexibleTimeWindow={"Mode": "OFF"}, Target=tgt, State="ENABLED", Description=sc["description"])
            R.ok("   %s updated %s" % (sc["schedule_name"], sc["cron"]))
        except sch.exceptions.ResourceNotFoundException:
            sch.create_schedule(Name=sc["schedule_name"], GroupName="default", ScheduleExpression=sc["cron"], ScheduleExpressionTimezone="UTC", FlexibleTimeWindow={"Mode": "OFF"}, Target=tgt, State="ENABLED", Description=sc["description"])
            R.ok("   %s created %s" % (sc["schedule_name"], sc["cron"]))
    except Exception as e:
        FAILS.append("schedule: %s" % str(e)[:140])

    tgt2 = {"Arn": tgt["Arn"], "RoleArn": SCHED_ROLE, "Input": json.dumps({"mode": "market-read"}), "RetryPolicy": {"MaximumRetryAttempts": 0, "MaximumEventAgeInSeconds": 900}}
    try:
        try:
            sch.get_schedule(Name="justhodl-ai-market-read", GroupName="default")
            sch.update_schedule(Name="justhodl-ai-market-read", GroupName="default", ScheduleExpression="cron(45 5 * * ? *)", ScheduleExpressionTimezone="UTC", FlexibleTimeWindow={"Mode": "OFF"}, Target=tgt2, State="ENABLED", Description="AI market read daily 05:45 UTC after jh-fusion (05:20), katlin (04:10), bottom (03:45): board -> playbook -> LLM read -> dated calls into the signal ledger")
            R.ok("   justhodl-ai-market-read updated cron(45 5 * * ? *)")
        except sch.exceptions.ResourceNotFoundException:
            sch.create_schedule(Name="justhodl-ai-market-read", GroupName="default", ScheduleExpression="cron(45 5 * * ? *)", ScheduleExpressionTimezone="UTC", FlexibleTimeWindow={"Mode": "OFF"}, Target=tgt2, State="ENABLED", Description="AI market read daily 05:45 UTC after jh-fusion (05:20), katlin (04:10), bottom (03:45): board -> playbook -> LLM read -> dated calls into the signal ledger")
            R.ok("   justhodl-ai-market-read created cron(45 5 * * ? *)")
    except Exception as e:
        FAILS.append("market-read schedule: %s" % str(e)[:140])
    step("schedules", not any("schedule" in f for f in FAILS))
    # ───────────────────────────────────────────────────── 5. inventory
    R.section("5. inventory (async + poll data/ai.json)")
    before = None
    try:
        before = get_json(PUBLIC, OUT).get("generated_at")
    except Exception:
        pass
    invoke("inventory", {"refresh_catalog": True}, sync=False)
    D = None
    t0 = time.time()
    while time.time() - t0 < 600:
        time.sleep(15)
        try:
            d = get_json(PUBLIC, OUT)
        except Exception:
            continue
        if d.get("generated_at") and d.get("generated_at") != before:
            D = d
            break
    if not D:
        FAILS.append("no fresh %s after %.0fs (read /aws/lambda/%s)" % (OUT, time.time() - t0, FN))
    else:
        inv = D.get("inventory") or {}
        R.log("   v%s in %ss | domains %s apps %s endpoints %s models %s jobs %s notebooks %s feature_groups %s clusters %s" % (
            D.get("version"), D.get("elapsed_s"), len(inv.get("domains") or []), len(inv.get("apps") or []), len(inv.get("endpoints") or []), len(inv.get("models") or []),
            len(inv.get("jobs") or []), len(inv.get("notebooks") or []), len(inv.get("feature_groups") or []), len(inv.get("clusters") or [])))
        for d_ in inv.get("domains") or []:
            R.log("      domain %s %s status %s" % (d_.get("id"), d_.get("name"), d_.get("status")))
        for e_ in inv.get("endpoints") or []:
            R.log("      endpoint %s %s %s tags %s" % (e_.get("name"), e_.get("status"), (e_.get("variants") or [{}])[0], json.dumps(e_.get("tags"))[:120]))
        for j_ in (inv.get("jobs") or [])[:10]:
            R.log("      job %s %s %s" % (j_.get("kind"), j_.get("name"), j_.get("status")))
        errs = D.get("inventory_errors") or {}
        if errs:
            FAILS.append("inventory sections unreadable: %s -- the control policy on %s did not take (IAM eventual consistency? re-run) " % (json.dumps(errs)[:400], LAMBDA_ROLE))
        cat = D.get("catalog") or {}
        R.log("   catalog %s cards; article RoBERTa-SEC present %s missing %s; errors %s refresh_error %s" % (cat.get("n"), cat.get("article_models_present"), cat.get("article_models_missing"), cat.get("errors"), cat.get("refresh_error")))
        if not cat.get("n"):
            FAILS.append("hub catalog empty: %s %s" % (cat.get("errors"), cat.get("refresh_error")))
        elif not cat.get("article_models_present"):
            WARNS.append("none of the article's four RoBERTa-SEC cards is in the public hub today -- the pipeline uses the best available text-embedding card instead (named below)")
        cost = D.get("cost") or {}
        R.log("   run-rate %s | MTD %s | ttl ledger %s" % (json.dumps((cost.get("projected") or {}).get("usd_per_day")), json.dumps((cost.get("mtd") or {}).get("usd_mtd")), json.dumps(cost.get("ttl_ledger"))[:300]))
        if (cost.get("mtd") or {}).get("error"):
            WARNS.append("Cost Explorer read failed: %s" % cost["mtd"]["error"][:100])
        R.log("   policy %s" % json.dumps(D.get("policy"))[:300])

    step("inventory", bool(D) and not (D.get("inventory_errors") or {}), ("%s endpoints, catalog %s cards" % (len((D.get("inventory") or {}).get("endpoints") or []), (D.get("catalog") or {}).get("n"))) if D else "no read model")
    # ───────────────────────────────────────────────────── 6. learn from the Brain
    R.section("6. learn from the Brain (dataset -> embedding endpoint -> embed -> classifier -> serve -> infer)")
    realtime_ep_to_delete = None
    if D and not (D.get("inventory_errors") or {}) and (D.get("catalog") or {}).get("n"):
        try:
            man = invoke("dataset/build")
            R.log("   dataset %s: %s rows (%s train / %s validation) from %s notes; by_label %s; dropped %s" % (man.get("dataset_id"), man.get("n_rows"), man.get("n_train"), man.get("n_validation"), man.get("source_n_notes"), json.dumps(man.get("by_label")), json.dumps(man.get("dropped"))))
            if (man.get("n_rows") or 0) < 100 or (man.get("min_class_rows") or 0) < 5:
                WARNS.append("dataset thin (%s rows, smallest class %s) -- the classifier will be weak until the Brain has more labelled notes" % (man.get("n_rows"), man.get("min_class_rows")))
            cat = D["catalog"]
            cards = cat.get("cards") or []
            pick = None
            for m in cat.get("article_models_present") or []:
                if "base" in m:
                    pick = m
                    break
            pick = pick or (cat.get("article_models_present") or [None])[0]
            if not pick:
                for c in cards:
                    if c.get("embedding") and "textembedding" in c["model_id"]:
                        pick = c["model_id"]
                        break
            if not pick:
                raise RuntimeError("no embedding card in the catalog: %s" % [c["model_id"] for c in cards[:10]])
            R.log("   embedding card: %s" % pick)
            dep = None
            try:
                dep = invoke("deploy", {"model_id": pick, "serverless": True, "serverless_memory_mb": 6144, "serverless_max_conc": 6})
                R.log("   deployed serverless: %s" % json.dumps({k: dep.get(k) for k in ("endpoint", "artifact_how", "artifact_probes", "model_data")})[:500])
            except Exception as e:
                WARNS.append("serverless deploy of %s failed (%s) -- falling back to real-time ml.m5.xlarge with a 2h TTL" % (pick, str(e)[:160]))
                dep = invoke("deploy", {"model_id": pick, "serverless": False, "instance_type": "ml.m5.xlarge", "ttl_hours": 2})
                realtime_ep_to_delete = dep["endpoint"]
                R.log("   deployed real-time: %s" % json.dumps({k: dep.get(k) for k in ("endpoint", "artifact_how", "artifact_probes", "model_data", "instance_type")})[:500])
            ep = dep["endpoint"]
            sm = boto3.client("sagemaker", region_name=REGION, config=CFG)
            st = None
            t1 = time.time()
            while time.time() - t1 < 1800:
                st = sm.describe_endpoint(EndpointName=ep)
                if st.get("EndpointStatus") in ("InService", "Failed"):
                    break
                time.sleep(20)
            R.log("   endpoint %s -> %s after %.0fs %s" % (ep, st.get("EndpointStatus"), time.time() - t1, (st.get("FailureReason") or "")[:200]))
            if st.get("EndpointStatus") != "InService":
                for line in endpoint_log_tail(ep):
                    R.log("      log: " + line[:220])
                raise RuntimeError("embedding endpoint not InService: %s %s" % (st.get("EndpointStatus"), (st.get("FailureReason") or "")[:200]))
            est = None
            for i in range(14):
                est = invoke("embed", {"dataset_id": man["dataset_id"], "endpoint": ep, "auto_train": True, "max_seconds": 600})
                R.log("   embed pass %d: status %s cursor %s/%s dim %s errors %s elapsed %ss" % (i + 1, est.get("status"), est.get("cursor"), est.get("n_rows"), est.get("dim"), est.get("errors"), est.get("elapsed_s")))
                if est.get("status") == "complete":
                    break
            if est.get("status") != "complete":
                raise RuntimeError("embedding pass did not complete in 14 calls (cursor %s/%s)" % (est.get("cursor"), est.get("n_rows")))
            R.log("   embeddings: %s rows dim %s -> %s" % (est.get("n_embedded"), est.get("dim"), est.get("train_uri")))
            atr = est.get("auto_train_result") or {}
            job = atr.get("job_name")
            if not job:
                raise RuntimeError("classifier did not start: %s" % json.dumps(atr)[:300])
            R.log("   classifier job %s on %s spot=%s" % (job, atr.get("instance_type"), atr.get("spot")))
            jd = None
            t2 = time.time()
            while time.time() - t2 < 2700:
                jd = sm.describe_training_job(TrainingJobName=job)
                if jd.get("TrainingJobStatus") in ("Completed", "Failed", "Stopped"):
                    break
                time.sleep(30)
            metrics = {m["MetricName"]: m["Value"] for m in (jd.get("FinalMetricDataList") or [])}
            R.log("   job %s -> %s (%s) billable %ss metrics %s" % (job, jd.get("TrainingJobStatus"), jd.get("SecondaryStatus"), jd.get("BillableTimeInSeconds"), json.dumps(metrics)))
            if jd.get("TrainingJobStatus") != "Completed":
                raise RuntimeError("classifier job %s: %s %s" % (job, jd.get("TrainingJobStatus"), (jd.get("FailureReason") or "")[:300]))
            served = invoke("deploy-trained", {"job_name": job, "serverless": True})
            cep = served["endpoint"]
            t3 = time.time()
            while time.time() - t3 < 1500:
                cst = sm.describe_endpoint(EndpointName=cep)
                if cst.get("EndpointStatus") in ("InService", "Failed"):
                    break
                time.sleep(20)
            R.log("   classifier endpoint %s -> %s (%s)" % (cep, cst.get("EndpointStatus"), (cst.get("FailureReason") or "")[:160]))
            if cst.get("EndpointStatus") != "InService":
                for line in endpoint_log_tail(cep):
                    R.log("      log: " + line[:220])
                raise RuntimeError("classifier endpoint failed: %s" % (cst.get("FailureReason") or "")[:200])
            probe = invoke("infer", {"text": "Dollar funding is tightening as the Fed's balance sheet runoff drains reserves while Treasury bill issuance surges.",
                                     "embedding_endpoint": ep, "classifier_endpoint": cep, "dataset_id": man["dataset_id"], "k": 5})
            R.log("   inference proof: dim %s classification %s nearest %d notes (top similarity %s)" % (
                probe.get("dim"), json.dumps(probe.get("classification") or probe.get("classification_raw"))[:200], len(probe.get("nearest_notes") or []),
                (probe.get("nearest_notes") or [{}])[0].get("similarity")))
            if not probe.get("nearest_notes"):
                WARNS.append("retrieval returned no notes (index empty?)")
        except Exception as e:
            FAILS.append("brain pipeline: %s" % str(e)[:400])
        finally:
            if realtime_ep_to_delete:
                try:
                    invoke("endpoint/delete", {"endpoint": realtime_ep_to_delete})
                    R.ok("   real-time embedding endpoint %s deleted (no hourly bill left behind); the classifier endpoint is serverless" % realtime_ep_to_delete)
                except Exception as e:
                    FAILS.append("could not delete real-time endpoint %s: %s -- DELETE IT IN THE CONSOLE" % (realtime_ep_to_delete, str(e)[:120]))
        try:
            invoke("inventory", sync=False)
        except Exception:
            pass
    else:
        WARNS.append("brain pipeline skipped (inventory not clean or catalog empty)")

    step("Brain pipeline", not any("brain pipeline" in f for f in FAILS), next((f for f in FAILS if "brain pipeline" in f), ""))

    # ───────────────────────────────────────────────────── 6b. first market read (board -> playbook -> LLM -> ledger)
    R.section("6b. market read")
    try:
        rd = invoke("market-read", {"force": True})
        read = rd.get("read") or {}
        R.log("   read %s in %ss | playbook %s | sources %s" % (rd.get("read_id"), rd.get("elapsed_s"), rd.get("playbook_available"), json.dumps({k: v.get("status") for k, v in (rd.get("sources") or {}).items()})))
        R.log("   stances: %s" % json.dumps({k: (read.get(k) or {}).get("stance") for k in ("stocks", "bonds", "metals", "crypto")}))
        R.log("   overall: %s" % str(read.get("overall") or read.get("raw") or "")[:600])
        R.log("   opportunities %d: %s" % (len(read.get("best_opportunities") or []), ", ".join("%s %s" % (o.get("ticker"), o.get("side")) for o in (read.get("best_opportunities") or [])[:8])))
        R.log("   calls logged: %s" % json.dumps([(c.get("ticker"), c.get("direction"), c.get("logged"), c.get("why")) for c in (rd.get("calls_logged") or [])])[:400])
        if read.get("parse_error"):
            FAILS.append("market read: LLM answer did not parse as JSON")
        elif not read.get("overall"):
            FAILS.append("market read: empty read")
        stale = [k for k, v in (rd.get("sources") or {}).items() if v.get("status") != "FRESH"]
        if stale:
            WARNS.append("market read board: %s not FRESH (named in the read's data gaps)" % stale)
        step("market read", not any("market read" in f for f in FAILS), "stances %s" % json.dumps({k: (read.get(k) or {}).get("stance") for k in ("stocks", "bonds", "metals", "crypto")}))
    except Exception as e:
        FAILS.append("market read: %s" % str(e)[:300])
        step("market read", False, str(e)[:160])

    # ───────────────────────────────────────────────────── 7. page
    R.section("7. page")
    live = False
    for _ in range(40):
        try:
            with urllib.request.urlopen(urllib.request.Request("https://justhodl.ai/ai.html?v=%d" % int(time.time()), headers=UA), timeout=30) as r:
                live = b"AI_DESK_V1" in r.read()
        except Exception:
            live = False
        if live:
            break
        time.sleep(15)
    R.log("   ai.html carries marker AI_DESK_V1 at the edge: %s" % live)
    if live:
        try:
            import playwright  # noqa: F401
        except Exception:
            subprocess.run([sys.executable, "-m", "pip", "install", "-q", "playwright"], check=True)
        from playwright.sync_api import sync_playwright
        SHOTS.mkdir(parents=True, exist_ok=True)
        with sync_playwright() as p:
            try:
                browser = p.chromium.launch(channel="chrome", headless=True)
            except Exception:
                subprocess.run([sys.executable, "-m", "playwright", "install", "chromium", "--with-deps"], check=False)
                browser = p.chromium.launch(headless=True)
            for width, height in ((1440, 1100), (390, 844)):
                ctx = browser.new_context(viewport={"width": width, "height": height}, is_mobile=width < 700)
                pg = ctx.new_page()
                errors = []
                pg.on("pageerror", lambda e: errors.append(str(e)[:160]))
                pg.goto("https://justhodl.ai/ai.html?v=%d" % int(time.time()), wait_until="domcontentloaded", timeout=60000)
                pg.wait_for_timeout(9000)
                facts = pg.evaluate("""() => ({ headline: document.getElementById('wr-headline').textContent.slice(0, 90), legs: document.querySelectorAll('#wr-legs .leg').length,
                    steps: document.querySelectorAll('#pipe-steps .step').length, done: document.querySelectorAll('#pipe-steps .step.done').length, tabs: document.querySelectorAll('#inv-tabs button').length,
                    rows: document.querySelectorAll('#inv-body tbody tr').length, cards: document.querySelectorAll('#catalog .card').length, helps: document.querySelectorAll('.help').length,
                    tiers: document.querySelectorAll('#tiers .tier').length, defs: document.querySelectorAll('#defs dt').length, pol: document.querySelectorAll('#pol-body [data-pol]').length,
                    overflow: document.documentElement.scrollWidth - document.documentElement.clientWidth, err: document.getElementById('errbox').textContent })""")
                pg.screenshot(path=str(SHOTS / f"ops5300_ai_{width}.png"))
                R.log("   %4dpx: %s errors=%s" % (width, json.dumps(facts)[:420], errors[:2]))
                if facts["legs"] != 6 or facts["steps"] != 5 or facts["tabs"] != 7 or facts["helps"] < 8 or facts["tiers"] != 4 or facts["pol"] < 10 or errors or facts["err"]:
                    FAILS.append("%dpx render: %s errors=%s" % (width, json.dumps(facts)[:240], errors[:2]))
                if facts["cards"] == 0:
                    WARNS.append("%dpx: catalog rendered zero cards" % width)
                if width == 390 and facts["overflow"] > 0:
                    FAILS.append("390px overflow %dpx" % facts["overflow"])
                ctx.close()
            browser.close()
    else:
        FAILS.append("page deploy not observed at the edge")

    step("page gate", not any("render" in f or "overflow" in f or "edge" in f for f in FAILS))
    for w in WARNS:
        R.warn("   " + w)
    for f in FAILS:
        R.fail("   " + f)
    for n in NEEDS_KHALID:
        R.warn("   NEEDS KHALID (Git Bash): " + n)
    write_verdict("RED" if FAILS else "GREEN", "AI relaunch v1.2.1 -- safe names, real keys, on-demand read", T_START)
    if FAILS:
        sys.exit(1)
    R.ok("   GREEN: AI live -- IAM, private bucket, engine, Function URL gate, schedules, inventory, Brain pipeline, market read, page" + (" (with warnings)" if WARNS else ""))
    sys.exit(0)
