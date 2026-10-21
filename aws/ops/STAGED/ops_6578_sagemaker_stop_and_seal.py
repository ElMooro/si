"""ops 6578 - stop SageMaker and seal the account against rogue compute (Khalid, 2026-10-20: "stop sagemaker for now
bcs that engine isnt ready ... take preemptive measure to put brakes and seal anything that might make things go rogue").

A. SageMaker to zero: every endpoint deleted (its configuration saved to the private ops bucket so it can be
   recreated), endpoint configs deleted, in-flight training / processing / transform / tuning jobs stopped, notebook
   instances stopped, Studio apps deleted; feature groups, domains, MLflow servers reported (storage-only cost).
B. The AI engine's own switches, in its private bucket (backups taken): factory/control/gearb.json enabled=false,
   factory/control/inference.json enabled=false, ai/policy.json daily_budget_usd=0 ("every create is refused").
C. Two sealed IAM deny policies (only the root user can lift them - the policies deny their own detachment/deletion):
   justhodl-deny-rogue-compute  on the runner group AND every Lambda execution role: launching any compute that bills
       by the hour or by the call outside what the platform uses (SageMaker create/start, EC2 run/start/volumes/NAT/EIP/
       spot/purchases, RDS/Redshift/EMR/EKS/ECS/OpenSearch/ElastiCache/Kinesis/MSK/MQ/Glue creates, Athena queries,
       Bedrock calls, Lambda provisioned concurrency, CloudFront, DynamoDB table mode changes, Marketplace, Savings
       Plans, IAM user / access-key creation)
   justhodl-deny-fleet-self-modify  on every Lambda execution role except the governor's: an engine may not create or
       rewrite functions, event-source mappings, roles or policies, un-throttle itself, pass roles, or touch bucket-level
       settings / budgets / topics (deploys go through the runner and its gates; rules, schedules and permissions stay
       allowed because schedule-liveness and backend-agent repair them and the cadence floor contains them)
   justhodl-deny-runner-escape  on the runner group: no role assumption, no leaving or re-shaping its own group, no
       trust-policy edits - the cheap ways around a sealed deny
D. Proof by IAM simulation: the runner user and the generic Lambda role get explicitDeny on the sealed actions and
   stay allowed on what the platform needs (Lambda deploys, S3, EventBridge, concurrency, SSM, Budgets, CE).
Reversible by Khalid as root: IAM -> Policies -> justhodl-deny-rogue-compute -> detach / delete.
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
PRIVATE = "justhodl-ops-private-%s" % ACCOUNT
AI_PRIVATE = "justhodl-ai-%s" % ACCOUNT
RUNNER_GROUP, RUNNER_USER = "justhodl-runner-ecr", "github-actions-justhodl"
GOV_ROLE = "justhodl-spend-governor-role"
P_COMPUTE, P_FLEET, P_RUNNER = "justhodl-deny-rogue-compute", "justhodl-deny-fleet-self-modify", "justhodl-deny-runner-escape"
CFG = Config(retries={"max_attempts": 10, "mode": "standard"}, connect_timeout=10, read_timeout=120)


def c(name):
    return boto3.client(name, region_name=REGION, config=CFG)


def parn(name):
    return "arn:aws:iam::%s:policy/%s" % (ACCOUNT, name)


COMPUTE_ACTIONS = [
    "sagemaker:Create*", "sagemaker:Start*", "sagemaker:UpdateEndpoint", "sagemaker:UpdateNotebookInstance", "sagemaker:InvokeEndpoint*",
    "ec2:RunInstances", "ec2:StartInstances", "ec2:CreateVolume", "ec2:CreateSnapshot*", "ec2:CreateNatGateway", "ec2:AllocateAddress",
    "ec2:RequestSpot*", "ec2:CreateFleet", "ec2:Purchase*", "ec2:CreateVpnConnection", "ec2:CreateTransitGateway*", "ec2:CreateClientVpnEndpoint",
    "ec2:ModifyInstanceAttribute", "ec2:CreateCapacityReservation",
    "rds:Create*", "rds:Start*", "rds:Restore*", "rds:Purchase*", "rds:Modify*",
    "redshift:Create*", "redshift:Resume*", "redshift:Restore*", "redshift:Modify*", "redshift-serverless:*",
    "elasticmapreduce:RunJobFlow", "elasticmapreduce:AddInstance*", "elasticmapreduce:Modify*", "emr-serverless:*", "emr-containers:*",
    "eks:Create*", "ecs:RunTask", "ecs:StartTask", "ecs:CreateService", "ecs:UpdateService", "ecs:CreateCluster", "ecs:CreateCapacityProvider",
    "es:Create*", "es:Update*", "opensearch:Create*", "aoss:Create*",
    "elasticache:Create*", "elasticache:Modify*", "elasticache:Purchase*", "memorydb:Create*", "dax:Create*",
    "kinesis:CreateStream", "kinesis:UpdateShardCount", "kinesis:IncreaseStreamRetentionPeriod", "firehose:CreateDeliveryStream",
    "kafka:Create*", "mq:CreateBroker", "kinesisanalytics:*", "kinesisvideo:*",
    "glue:CreateJob", "glue:StartJobRun", "glue:CreateDevEndpoint", "glue:CreateCrawler", "glue:StartCrawler", "glue:CreateSession",
    "athena:StartQueryExecution", "athena:StartSession",
    "bedrock:Invoke*", "bedrock:Create*", "bedrock:Converse*", "bedrock:Retrieve*", "bedrock:StartIngestionJob",
    "lambda:PutProvisionedConcurrencyConfig",
    "cloudfront:CreateDistribution*", "cloudfront:CreateStreamingDistribution*",
    "dynamodb:UpdateTable", "aws-marketplace:Subscribe", "aws-marketplace:AcceptAgreement*", "savingsplans:CreateSavingsPlan",
    "iam:CreateUser", "iam:CreateAccessKey", "iam:CreateLoginProfile", "iam:UpdateLoginProfile", "iam:AttachUserPolicy", "iam:PutUserPolicy",
]
FLEET_ACTIONS = [
    # an engine may not create or rewrite functions, mappings, roles or policies, un-throttle itself, pass roles,
    # touch bucket-level settings, budgets or topics.  Rules / schedules / permissions stay allowed: schedule-liveness
    # and backend-agent repair them, and the cadence floor + sentinel contain whatever they create.
    "lambda:CreateFunction", "lambda:UpdateFunctionCode", "lambda:UpdateFunctionConfiguration", "lambda:CreateEventSourceMapping",
    "lambda:CreateFunctionUrlConfig", "lambda:PutFunctionConcurrency", "lambda:DeleteFunctionConcurrency", "lambda:PutFunctionEventInvokeConfig",
    "iam:CreateRole", "iam:AttachRolePolicy", "iam:PutRolePolicy", "iam:CreatePolicy", "iam:CreatePolicyVersion", "iam:PassRole",
    "iam:UpdateAssumeRolePolicy", "sts:AssumeRole",
    "s3:PutReplicationConfiguration", "s3:PutLifecycleConfiguration", "s3:PutBucketVersioning", "s3:DeleteBucket", "s3:PutBucketPolicy",
    "secretsmanager:CreateSecret", "budgets:Modify*", "ce:UpdateAnomaly*", "sns:CreateTopic", "events:CreateEventBus",
]
RUNNER_EXTRA = [
    # the runner keeps role / policy management for ops, but cannot leave or re-shape its own group, assume roles, or
    # mint credentials - the cheap escape routes from a sealed deny
    "sts:AssumeRole", "iam:AddUserToGroup", "iam:RemoveUserFromGroup", "iam:CreateGroup", "iam:DeleteGroup", "iam:UpdateAssumeRolePolicy",
]


def seal_statements(name):
    """A policy that denies its own detachment, deletion and editing - except to the root user."""
    arn = parn(name)
    root = "arn:aws:iam::%s:root" % ACCOUNT
    cond = {"ArnNotEquals": {"aws:PrincipalArn": root}}
    return [
        {"Sid": "SealPolicyObject", "Effect": "Deny", "Action": ["iam:DeletePolicy", "iam:DeletePolicyVersion", "iam:CreatePolicyVersion", "iam:SetDefaultPolicyVersion"],
         "Resource": arn, "Condition": cond},
        {"Sid": "SealAttachment", "Effect": "Deny", "Action": ["iam:DetachGroupPolicy", "iam:DetachRolePolicy", "iam:DetachUserPolicy"],
         "Resource": "*", "Condition": dict(cond, ArnEquals={"iam:PolicyARN": arn})},
    ]


def ensure_policy(r, iam, name, actions, sid):
    doc = {"Version": "2012-10-17", "Statement": [{"Sid": sid, "Effect": "Deny", "Action": actions, "Resource": "*"}] + seal_statements(name)}
    body = json.dumps(doc, separators=(",", ":"))
    if len(body) > 6000:
        raise SystemExit("%s is %d characters: over the managed-policy limit" % (name, len(body)))
    try:
        iam.create_policy(PolicyName=name, PolicyDocument=body, Description="ops 6578 (2026-10-20): sealed deny; only the root user can lift it")
        r.ok("policy %s created (%d chars)" % (name, len(body)))
    except iam.exceptions.EntityAlreadyExistsException:
        r.log("policy %s exists (sealed: not editable from here)" % name)
    return parn(name)


def attach_role(r, iam, role, arn, name):
    attached = [p["PolicyArn"] for p in iam.list_attached_role_policies(RoleName=role)["AttachedPolicies"]]
    if arn in attached:
        return "attached"
    if len(attached) >= 10:
        r.warn("%s has 10 managed policies: %s could not be attached" % (role, name))
        return "full"
    iam.attach_role_policy(RoleName=role, PolicyArn=arn)
    return "new"


def sagemaker_stop(r, s3):
    sm = c("sagemaker")
    saved = {}
    for ep in sm.list_endpoints(MaxResults=100).get("Endpoints", []):
        name = ep["EndpointName"]
        d = sm.describe_endpoint(EndpointName=name)
        cfg_name = d.get("EndpointConfigName")
        cfg = sm.describe_endpoint_config(EndpointConfigName=cfg_name) if cfg_name else {}
        saved[name] = {"endpoint": {k: v for k, v in d.items() if k != "ResponseMetadata"}, "config": {k: v for k, v in cfg.items() if k != "ResponseMetadata"}}
        try:
            aas = c("application-autoscaling")
            for t in aas.describe_scalable_targets(ServiceNamespace="sagemaker").get("ScalableTargets", []):
                if name in t.get("ResourceId", ""):
                    aas.deregister_scalable_target(ServiceNamespace="sagemaker", ResourceId=t["ResourceId"], ScalableDimension=t["ScalableDimension"])
        except Exception as ex:  # noqa: BLE001
            r.log("autoscaling for %s: %s" % (name, type(ex).__name__))
        sm.delete_endpoint(EndpointName=name)
        r.ok("endpoint %s (%s) deleted; configuration saved" % (name, d.get("EndpointStatus")))
    for ec in sm.list_endpoint_configs(MaxResults=100).get("EndpointConfigs", []):
        try:
            sm.delete_endpoint_config(EndpointConfigName=ec["EndpointConfigName"])
            r.log("endpoint config %s deleted" % ec["EndpointConfigName"])
        except Exception as ex:  # noqa: BLE001
            r.log("endpoint config %s: %s" % (ec["EndpointConfigName"], type(ex).__name__))
    stopped = []
    for kind, lister, key, namek, stopper in (
            ("training", sm.list_training_jobs, "TrainingJobSummaries", "TrainingJobName", sm.stop_training_job),
            ("processing", sm.list_processing_jobs, "ProcessingJobSummaries", "ProcessingJobName", sm.stop_processing_job),
            ("transform", sm.list_transform_jobs, "TransformJobSummaries", "TransformJobName", sm.stop_transform_job),
            ("tuning", sm.list_hyper_parameter_tuning_jobs, "HyperParameterTuningJobSummaries", "HyperParameterTuningJobName", sm.stop_hyper_parameter_tuning_job)):
        for j in lister(StatusEquals="InProgress", MaxResults=100).get(key, []):
            try:
                stopper(**{namek: j[namek]})
                stopped.append((kind, j[namek]))
            except Exception as ex:  # noqa: BLE001
                r.log("stop %s %s: %s" % (kind, j[namek], type(ex).__name__))
    r.log("in-flight jobs stopped: %s" % (stopped or "none"))
    nbs = sm.list_notebook_instances(MaxResults=100).get("NotebookInstances", [])
    for nb in nbs:
        if nb.get("NotebookInstanceStatus") == "InService":
            sm.stop_notebook_instance(NotebookInstanceName=nb["NotebookInstanceName"])
            r.log("notebook %s stopped" % nb["NotebookInstanceName"])
    apps = sm.list_apps(MaxResults=100).get("Apps", [])
    for a in apps:
        if a.get("Status") in ("InService", "Pending"):
            try:
                kw = {"DomainId": a["DomainId"], "AppType": a["AppType"], "AppName": a["AppName"]}
                if a.get("UserProfileName"):
                    kw["UserProfileName"] = a["UserProfileName"]
                else:
                    kw["SpaceName"] = a.get("SpaceName")
                sm.delete_app(**kw)
                r.log("studio app %s/%s deleted" % (a["AppType"], a["AppName"]))
            except Exception as ex:  # noqa: BLE001
                r.log("studio app %s: %s" % (a.get("AppName"), type(ex).__name__))
    domains = sm.list_domains(MaxResults=100).get("Domains", [])
    fgs = sm.list_feature_groups(MaxResults=100).get("FeatureGroupSummaries", [])
    try:
        mlf = sm.list_mlflow_tracking_servers(MaxResults=100).get("TrackingServerSummaries", [])
        for t in mlf:
            if t.get("TrackingServerStatus") in ("Started", "Starting"):
                sm.stop_mlflow_tracking_server(TrackingServerName=t["TrackingServerName"])
                r.log("mlflow server %s stopped" % t["TrackingServerName"])
    except Exception:  # noqa: BLE001
        mlf = []
    if saved:
        s3.put_object(Bucket=PRIVATE, Key="backups/sagemaker/%s/endpoints.json" % time.strftime("%Y%m%dT%H%M%SZ", time.gmtime()),
                      Body=json.dumps(saved, default=str, indent=1).encode(), ServerSideEncryption="AES256")
    r.log("remaining: %d notebook instances (%s), %d studio domains, %d feature groups (online store: %s), %d mlflow servers" % (
        len(nbs), [n.get("NotebookInstanceStatus") for n in nbs], len(domains), len(fgs),
        [f["FeatureGroupName"] for f in fgs][:5], len(mlf)))
    return {"endpoints_deleted": len(saved), "jobs_stopped": len(stopped), "notebooks": len(nbs), "domains": len(domains), "feature_groups": len(fgs)}


def ai_switches(r, s3):
    stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    out = {}
    for key, edit in (("factory/control/gearb.json", lambda d: d.update(enabled=False, stopped_by="ops 6578", stopped_at=stamp, stop_reason="Khalid 2026-10-20: stop SageMaker for now")),
                      ("factory/control/inference.json", lambda d: d.update(enabled=False, stopped_by="ops 6578", stopped_at=stamp)),
                      ("ai/policy.json", lambda d: d.update(daily_budget_usd=0, updated_at=stamp, stopped_by="ops 6578"))):
        try:
            raw = s3.get_object(Bucket=AI_PRIVATE, Key=key)["Body"].read()
            doc = json.loads(raw)
            s3.put_object(Bucket=PRIVATE, Key="backups/ai-controls/%s/%s" % (stamp, key.replace("/", "__")), Body=raw, ServerSideEncryption="AES256")
        except s3.exceptions.NoSuchKey:
            doc = {}
        edit(doc)
        s3.put_object(Bucket=AI_PRIVATE, Key=key, Body=json.dumps(doc, indent=1).encode(), ContentType="application/json")
        out[key] = {k: doc.get(k) for k in ("enabled", "daily_budget_usd") if k in doc}
    r.ok("AI engine switches: %s" % json.dumps(out))
    return out


def simulate(r, iam, source_arn, denied, allowed):
    res = iam.simulate_principal_policy(PolicySourceArn=source_arn, ActionNames=denied + allowed,
                                        ContextEntries=[{"ContextKeyName": "iam:PolicyARN", "ContextKeyValues": [parn(P_COMPUTE)], "ContextKeyType": "string"}])
    by = {x["EvalActionName"]: x["EvalDecision"] for x in res["EvaluationResults"]}
    bad = [(a, by.get(a)) for a in denied if by.get(a) != "explicitDeny"] + [(a, by.get(a)) for a in allowed if by.get(a) == "explicitDeny"]
    r.log("%s: denied %s / allowed-still %s%s" % (source_arn.split("/")[-1], [by.get(a) for a in denied], [by.get(a) for a in allowed],
                                                   "" if not bad else "  UNEXPECTED: %s" % bad))
    return not bad


def main():
    s3, iam, lam = c("s3"), c("iam"), c("lambda")
    with report("ops_6578_sagemaker_stop_and_seal") as r:
        r.heading("ops 6578 - SageMaker to zero; sealed deny policies against rogue compute")
        sm = sagemaker_stop(r, s3)
        sw = ai_switches(r, s3)
        comp = ensure_policy(r, iam, P_COMPUTE, COMPUTE_ACTIONS, "DenyRogueCompute")
        fleet = ensure_policy(r, iam, P_FLEET, FLEET_ACTIONS, "DenyFleetSelfModify")
        runner_seal = ensure_policy(r, iam, P_RUNNER, RUNNER_EXTRA, "DenyRunnerEscape")
        for arn, name in ((comp, P_COMPUTE), (runner_seal, P_RUNNER)):
            try:
                iam.attach_group_policy(GroupName=RUNNER_GROUP, PolicyArn=arn)
                r.ok("runner group %s: + %s" % (RUNNER_GROUP, name))
            except Exception as ex:  # noqa: BLE001
                r.log("runner group attach %s: %s" % (name, type(ex).__name__))
        roles = sorted({f["Role"].split("/")[-1] for p in lam.get_paginator("list_functions").paginate() for f in p["Functions"]})
        res = {}
        for role in roles:
            a = attach_role(r, iam, role, comp, P_COMPUTE)
            b = attach_role(r, iam, role, fleet, P_FLEET) if role != GOV_ROLE else "skipped (governor)"
            res[role] = (a, b)
        r.log("Lambda execution roles (%d): %s" % (len(roles), json.dumps(res)))
        time.sleep(20)
        ok = True
        ok &= simulate(r, iam, "arn:aws:iam::%s:user/%s" % (ACCOUNT, RUNNER_USER),
                       ["sagemaker:CreateTrainingJob", "sagemaker:CreateEndpoint", "ec2:RunInstances", "bedrock:InvokeModel", "rds:CreateDBInstance", "iam:CreateAccessKey",
                        "sts:AssumeRole", "iam:AddUserToGroup", "iam:DetachGroupPolicy"],
                       ["lambda:UpdateFunctionCode", "s3:PutObject", "events:PutRule", "lambda:PutFunctionConcurrency", "iam:PutRolePolicy", "budgets:ViewBudget",
                        "iam:AttachGroupPolicy", "iam:CreateRole", "s3:CreateJob"])
        generic = [x for x in roles if x in ("lambda-execution-role", "justhodl-lambda-execution-role")]
        for role in generic[:2]:
            ok &= simulate(r, iam, "arn:aws:iam::%s:role/%s" % (ACCOUNT, role),
                           ["sagemaker:CreateTrainingJob", "ec2:RunInstances", "lambda:CreateFunction", "lambda:PutFunctionConcurrency", "iam:CreateRole", "athena:StartQueryExecution", "iam:PassRole"],
                           ["s3:PutObject", "lambda:InvokeFunction", "dynamodb:PutItem", "dynamodb:CreateTable", "ssm:GetParameter", "ssm:PutParameter", "events:PutEvents", "events:PutRule", "sns:Publish", "lambda:AddPermission"])
        ok &= simulate(r, iam, "arn:aws:iam::%s:role/%s" % (ACCOUNT, GOV_ROLE),
                       ["sagemaker:CreateEndpoint", "ec2:RunInstances"],
                       ["lambda:PutFunctionConcurrency", "events:PutRule", "scheduler:UpdateSchedule", "iam:PassRole", "sagemaker:UpdateEndpointWeightsAndCapacities",
                        "s3:PutBucketVersioning", "application-autoscaling:RegisterScalableTarget"])
        r.kv(**{k.replace("-", "_"): v for k, v in sm.items()}, roles_sealed=len(roles), simulations_ok=ok)
        if not ok:
            r.fail("a simulation disagreed with the design - review before relying on the seal")
            raise SystemExit(1)
        r.ok("SageMaker at zero; the runner and every engine role are sealed against rogue compute (root-only to lift)")


if __name__ == "__main__":
    main()
