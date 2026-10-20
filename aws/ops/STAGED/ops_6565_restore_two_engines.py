"""ops 6565 - revert two of the 24 deletions of ops 6563 and tidy the one dangling trigger.

Audit after the fact (pages deploy gate, 2026-10-20): "no trigger and no invocation in 27 days" was the wrong test for
  * justhodl-insider-cluster-scanner - data/insider-clusters.json is declared by insider-desk.html and read by 22 engines
    (199 Lambda fetches a day); the engine had simply lost its trigger (last output 2026-08-04).  Restored from the backup
    taken before deletion and given a daily EventBridge Scheduler schedule at 13:50 UTC, ahead of insider-industry-cluster
    (14:20) which consumes it.
  * justhodl-warroom-weights - a MONTHLY engine (cron 1st 07:10 UTC, last output 2026-10-01) consumed by canary-warroom and
    canaries.html.  Restored from the same backup; its schedule justhodl-warroom-weights-monthly was never deleted.
  * multi-agent-orchestrator-schedule (rule, default bus, 14:29 UTC) still starts the Step Functions state machine
    multi-agent-orchestrator, whose states invoke the deleted Lambda: backed up and deleted (the state machine itself is
    left in place; it costs nothing idle).  The live S3 copy of config/schedule-manifest.json gets the same two edits
    as the committed one (rule out, new schedule in) so the reconciler sees no drift.

Restore = create_function from the saved package + configuration (runtime, role, handler, env, memory, timeout, layers,
architecture, ephemeral storage, DLQ, tracing, VPC, tags).  No reserved concurrency is set: the fleet is FROZEN, the
governor's hourly tick throttles any function it finds unthrottled (and re-holds ledger entries), and the Nov 1 thaw
restores them with the rest of the fleet.  Idempotent: an existing function or schedule is left alone.
"""
from pathlib import Path
import hashlib
import io
import json
import sys
import time
import zipfile

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

REGION, ACCOUNT = "us-east-1", "857687956942"
PRIVATE = "justhodl-ops-private-%s" % ACCOUNT
BACKUP = "backups/deleted-functions/20261020T205550Z"
RESTORE = ["justhodl-insider-cluster-scanner", "justhodl-warroom-weights"]
NEW_SCHEDULE = {"name": "justhodl-insider-cluster-scanner-daily", "function": "justhodl-insider-cluster-scanner",
                "expr": "cron(50 13 * * ? *)",
                "description": "Insider cluster detection (SEC Form 4) - daily 13:50 UTC, before insider-industry-cluster (14:20). Restored 2026-10-20 (ops 6565)."}
DANGLING_RULE = ("multi-agent-orchestrator-schedule", "default", "multi-agent-orchestrator")
CFG = Config(retries={"max_attempts": 10, "mode": "standard"}, connect_timeout=10, read_timeout=120)


def c(name):
    return boto3.client(name, region_name=REGION, config=CFG)


def restore_function(r, lam, s3, fn):
    try:
        cur = lam.get_function_configuration(FunctionName=fn)
        r.log("%s: already exists (%s, %s) - left alone" % (fn, cur.get("State"), cur.get("LastModified")))
        return "exists"
    except lam.exceptions.ResourceNotFoundException:
        pass
    meta = json.loads(s3.get_object(Bucket=PRIVATE, Key="%s/%s.json" % (BACKUP, fn))["Body"].read())
    cfg = meta["configuration"]
    blob = s3.get_object(Bucket=PRIVATE, Key="%s/%s.zip" % (BACKUP, fn))["Body"].read()
    # is the saved package the repo's source?  (information for the report, not a gate)
    with zipfile.ZipFile(io.BytesIO(blob)) as z:
        names = z.namelist()
        deployed = z.read("lambda_function.py") if "lambda_function.py" in names else b""
    repo = (ROOT / "aws/lambdas" / fn / "source/lambda_function.py")
    same = repo.is_file() and hashlib.sha256(repo.read_bytes()).hexdigest() == hashlib.sha256(deployed).hexdigest()
    args = {"FunctionName": fn, "Runtime": cfg["Runtime"], "Role": cfg["Role"], "Handler": cfg["Handler"],
            "Code": {"S3Bucket": PRIVATE, "S3Key": "%s/%s.zip" % (BACKUP, fn)}, "Description": cfg.get("Description") or "",
            "Timeout": cfg.get("Timeout", 300),
            "MemorySize": cfg.get("MemorySize", 512), "Publish": False, "PackageType": "Zip",
            "Architectures": cfg.get("Architectures") or ["x86_64"]}
    if (cfg.get("Environment") or {}).get("Variables"):
        args["Environment"] = {"Variables": cfg["Environment"]["Variables"]}
    if cfg.get("Layers"):
        args["Layers"] = [l["Arn"] for l in cfg["Layers"]]
    if (cfg.get("EphemeralStorage") or {}).get("Size"):
        args["EphemeralStorage"] = {"Size": cfg["EphemeralStorage"]["Size"]}
    if (cfg.get("DeadLetterConfig") or {}).get("TargetArn"):
        args["DeadLetterConfig"] = {"TargetArn": cfg["DeadLetterConfig"]["TargetArn"]}
    if (cfg.get("TracingConfig") or {}).get("Mode"):
        args["TracingConfig"] = {"Mode": cfg["TracingConfig"]["Mode"]}
    if (cfg.get("VpcConfig") or {}).get("SubnetIds"):
        args["VpcConfig"] = {"SubnetIds": cfg["VpcConfig"]["SubnetIds"], "SecurityGroupIds": cfg["VpcConfig"].get("SecurityGroupIds", [])}
    if cfg.get("LoggingConfig"):
        args["LoggingConfig"] = {k: v for k, v in cfg["LoggingConfig"].items() if k in ("LogFormat", "ApplicationLogLevel", "SystemLogLevel", "LogGroup")}
    if meta.get("tags"):
        args["Tags"] = meta["tags"]
    for attempt in range(6):
        try:
            lam.create_function(**args)
            break
        except lam.exceptions.InvalidParameterValueException as ex:   # role not yet assumable etc.
            if attempt == 5:
                raise
            r.log("%s: create retry (%s)" % (fn, str(ex)[:120]))
            time.sleep(10)
    for _ in range(30):
        st = lam.get_function_configuration(FunctionName=fn)
        if st.get("State") == "Active":
            break
        time.sleep(2)
    st = lam.get_function_configuration(FunctionName=fn)
    r.log("%s: created from %s/%s.zip (%d bytes, %d files), state %s, %s MB / %ss, runtime %s, env vars %d, layers %d, %s" % (
        fn, BACKUP, fn, len(blob), len(names), st.get("State"), st.get("MemorySize"), st.get("Timeout"), st.get("Runtime"),
        len((cfg.get("Environment") or {}).get("Variables") or {}), len(cfg.get("Layers") or []),
        "package == repo source" if same else "package differs from repo source (deploy-time bundling)"))
    url = meta.get("url")
    if url and url.get("FunctionUrl"):
        kw = {"FunctionName": fn, "AuthType": url.get("AuthType", "AWS_IAM")}
        if url.get("Cors"):
            kw["Cors"] = url["Cors"]
        if url.get("InvokeMode"):
            kw["InvokeMode"] = url["InvokeMode"]
        lam.create_function_url_config(**kw)
        if kw["AuthType"] == "NONE":
            lam.add_permission(FunctionName=fn, StatementId="FunctionURLAllowPublicAccess", Action="lambda:InvokeFunctionUrl",
                               Principal="*", FunctionUrlAuthType="NONE")
        r.log("%s: function URL recreated (auth %s)" % (fn, kw["AuthType"]))
    return "created"


def ensure_schedule(r, scheduler, iam, lam):
    name = NEW_SCHEDULE["name"]
    arn = lam.get_function_configuration(FunctionName=NEW_SCHEDULE["function"])["FunctionArn"]
    try:
        old = scheduler.get_schedule(Name=name, GroupName="default")
        assert old["Target"]["Arn"] == arn, "schedule name belongs to another target"
        r.log("schedule %s already exists: %s %s" % (name, old["ScheduleExpression"], old["State"]))
        return "exists"
    except scheduler.exceptions.ResourceNotFoundException:
        pass
    role = iam.get_role(RoleName="justhodl-scheduler-role")["Role"]["Arn"]
    scheduler.create_schedule(Name=name, GroupName="default", ScheduleExpression=NEW_SCHEDULE["expr"], ScheduleExpressionTimezone="UTC",
                              State="ENABLED", Description=NEW_SCHEDULE["description"], FlexibleTimeWindow={"Mode": "OFF"},
                              Target={"Arn": arn, "RoleArn": role, "RetryPolicy": {"MaximumEventAgeInSeconds": 3600, "MaximumRetryAttempts": 0}})
    chk = scheduler.get_schedule(Name=name, GroupName="default")
    assert chk["State"] == "ENABLED" and chk["ScheduleExpression"] == NEW_SCHEDULE["expr"] and chk["Target"]["Arn"] == arn
    r.ok("schedule %s created: %s UTC, one attempt, no retries" % (name, NEW_SCHEDULE["expr"]))
    return "created"


def drop_dangling_rule(r, events, s3, stamp):
    name, bus, fn = DANGLING_RULE
    try:
        rule = events.describe_rule(Name=name, EventBusName=bus)
    except events.exceptions.ResourceNotFoundException:
        r.log("rule %s already gone" % name)
        return "gone"
    targets = events.list_targets_by_rule(Rule=name, EventBusName=bus).get("Targets", [])
    # the rule drives the Step Functions state machine of the same name, whose states invoke the deleted Lambda
    others = [t for t in targets if not t.get("Arn", "").endswith((":function:" + fn, ":stateMachine:" + fn))]
    if others:
        r.warn("rule %s has other targets (%d) - left alone" % (name, len(others)))
        return "kept"
    s3.put_object(Bucket=PRIVATE, Key="backups/retire-2026-10-20/%s/rule/%s/%s.json" % (stamp, bus, name), ServerSideEncryption="AES256",
                  Body=json.dumps({"rule": {k: v for k, v in rule.items() if k != "ResponseMetadata"}, "targets": targets}, default=str, indent=1).encode())
    if targets:
        events.remove_targets(Rule=name, EventBusName=bus, Ids=[t["Id"] for t in targets])
    events.delete_rule(Name=name, EventBusName=bus)
    r.ok("rule %s (%s, target: deleted %s) backed up and deleted" % (name, rule.get("ScheduleExpression"), fn))
    return "deleted"


def sync_manifest(r, s3, stamp):
    """The reconciler reads s3://justhodl-dashboard-live/config/schedule-manifest.json: apply the same two edits the repo
    copy carries (rule out, schedule in) to the live copy, keeping its formatting; report whether both copies now agree."""
    key = "config/schedule-manifest.json"
    old = s3.get_object(Bucket="justhodl-dashboard-live", Key=key)["Body"].read().decode("utf-8")
    doc = json.loads(old)
    style = next(((ind, ea, nl) for ind in (1, 2, None) for ea in (True, False) for nl in ("\n", "")
                  if json.dumps(doc, indent=ind, ensure_ascii=ea) + nl == old), (1, True, "\n"))
    doc["rules"] = [x for x in doc["rules"] if x["name"] != DANGLING_RULE[0]]
    if not any(x["name"] == NEW_SCHEDULE["name"] for x in doc["schedules"]):
        doc["schedules"].append({"expr": NEW_SCHEDULE["expr"], "group": "default", "kind": "scheduler", "name": NEW_SCHEDULE["name"],
                                 "state": "ENABLED", "timezone": "UTC",
                                 "targets": [{"arn": "arn:aws:lambda:%s:%s:function:%s" % (REGION, ACCOUNT, NEW_SCHEDULE["function"]),
                                              "input": None, "path": None}]})
        doc["schedules"].sort(key=lambda x: x["name"])
    new = json.dumps(doc, indent=style[0], ensure_ascii=style[1]) + style[2]
    repo = json.loads((ROOT / key).read_text(encoding="utf-8"))
    same = {k: v for k, v in doc.items() if k != "generated_at"} == {k: v for k, v in repo.items() if k != "generated_at"}
    if new != old:
        s3.put_object(Bucket=PRIVATE, Key="backups/retire-2026-10-20/%s/manifest/schedule-manifest.json" % stamp, Body=old.encode("utf-8"),
                      ServerSideEncryption="AES256")
        s3.put_object(Bucket="justhodl-dashboard-live", Key=key, Body=new.encode("utf-8"), ContentType="application/json", CacheControl="no-cache")
    r.log("S3 schedule manifest: %s; %s the committed repo file" % ("updated" if new != old else "already current",
                                                                   "equals" if same else "DIFFERS from (apart from generated_at)"))
    return "updated" if new != old else "current"


def main():
    with report("ops_6565_restore_two_engines") as r:
        r.heading("ops 6565 - restore insider-cluster-scanner + warroom-weights from backup; schedule the scanner daily; drop one dangling rule")
        lam, s3, scheduler, events, iam = c("lambda"), c("s3"), c("scheduler"), c("events"), c("iam")
        stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
        results = {fn: restore_function(r, lam, s3, fn) for fn in RESTORE}
        results[NEW_SCHEDULE["name"]] = ensure_schedule(r, scheduler, iam, lam)
        results[DANGLING_RULE[0]] = drop_dangling_rule(r, events, s3, stamp)
        results["s3_manifest"] = sync_manifest(r, s3, stamp)
        # warroom-weights' monthly schedule must still exist and point at the restored function
        ws = scheduler.get_schedule(Name="justhodl-warroom-weights-monthly", GroupName="default")
        r.log("justhodl-warroom-weights-monthly: %s %s -> %s" % (ws["ScheduleExpression"], ws["State"], ws["Target"]["Arn"].split(":")[-1]))
        n = sum(len(p["Functions"]) for p in lam.get_paginator("list_functions").paginate())
        r.kv(functions_now=n, **{k.replace("-", "_"): v for k, v in results.items()})
        r.ok("restored; fleet is %d functions" % n)


if __name__ == "__main__":
    main()
