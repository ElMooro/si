"""ops 4361 — event-driven fan-in. Fresh upstream feed writes now trigger the
join engine in seconds instead of waiting for the 15-min cron (which stays as
floor). Safety: bucket notification config is fetched and MERGED, never
replaced blind — existing Lambda/Queue/Topic configs preserved. Canary proves
the path live: put data/fanin-canary/ping.json -> engine START observed."""
import json, os, time
from datetime import datetime, timezone
import boto3

REGION="us-east-1"; BUCKET="justhodl-dashboard-live"; FN="justhodl-crypto-intel"
RULE="justhodl-crypto-fanin"
s3=boto3.client("s3",region_name=REGION); ev=boto3.client("events",region_name=REGION)
lam=boto3.client("lambda",region_name=REGION); logs=boto3.client("logs",region_name=REGION)
t0=datetime.now(timezone.utc)
R={"ops":4361,"started":t0.isoformat()}

# 1. enable EventBridge notifications on bucket (merge-preserve)
try:
    cfg=s3.get_bucket_notification_configuration(Bucket=BUCKET)
    keep={k:v for k,v in cfg.items() if k in
          ("TopicConfigurations","QueueConfigurations","LambdaFunctionConfigurations",
           "EventBridgeConfiguration")}
    R["existing_notification_keys"]=sorted(keep.keys())
    if "EventBridgeConfiguration" not in keep:
        keep["EventBridgeConfiguration"]={}
        s3.put_bucket_notification_configuration(Bucket=BUCKET,
                                                 NotificationConfiguration=keep)
        R["eventbridge_enabled"]="now"
    else:
        R["eventbridge_enabled"]="already"
except Exception as e:
    R["notif_err"]=f"{type(e).__name__}: {e}"

# 2. rule: upstream feed prefixes -> crypto-intel
WATCH=["data/cq-feed.json","data/cryptoquant-onchain.json","data/altseason.json",
       "data/crypto-dvol.json","data/crypto-funding.json","data/dealer-gex.json",
       "data/fanin-canary"]
try:
    pat={"source":["aws.s3"],"detail-type":["Object Created"],
         "detail":{"bucket":{"name":[BUCKET]},
                   "object":{"key":[{"prefix":p} for p in WATCH]}}}
    arn=ev.put_rule(Name=RULE,EventPattern=json.dumps(pat),State="ENABLED",
                    Description="ops4361 fan-in: fresh upstream feeds trigger crypto-intel join")["RuleArn"]
    fn_arn=lam.get_function_configuration(FunctionName=FN)["FunctionArn"]
    ev.put_targets(Rule=RULE,Targets=[{"Id":"crypto-intel","Arn":fn_arn}])
    try:
        lam.add_permission(FunctionName=FN,StatementId="ops4361-fanin",
                           Action="lambda:InvokeFunction",
                           Principal="events.amazonaws.com",SourceArn=arn)
    except lam.exceptions.ResourceConflictException:
        pass
    R["rule"]={"arn":arn[-70:],"watch":WATCH}
except Exception as e:
    R["rule_err"]=f"{type(e).__name__}: {e}"

# 3. canary: prove event -> invoke live
try:
    s3.put_object(Bucket=BUCKET,Key="data/fanin-canary/ping.json",
                  Body=json.dumps({"t":t0.isoformat()}).encode())
    time.sleep(35)
    resp=logs.filter_log_events(logGroupName=f"/aws/lambda/{FN}",
                                startTime=int(t0.timestamp()*1000),
                                filterPattern="START",limit=20)
    starts=len(resp.get("events",[]))
    R["canary"]={"event_driven_starts_observed":starts,"window_s":35}
    d=json.loads(s3.get_object(Bucket=BUCKET,Key="crypto-intel.json")["Body"].read())
    R["canary"]["doc_generated_at"]=d.get("generated_at")
except Exception as e:
    R["canary_err"]=f"{type(e).__name__}: {e}"

R["verdict"]=("PASS — fan-in live" if (R.get("canary",{}).get("event_driven_starts_observed",0)>=1
              and "rule" in R) else "PARTIAL — see fields")
R["finished"]=datetime.now(timezone.utc).isoformat()
os.makedirs("aws/ops/reports",exist_ok=True)
json.dump(R,open("aws/ops/reports/4361_fanin.json","w"),indent=1,default=str)
open("aws/ops/reports/4361_fanin.md","w").write(
    f"# ops 4361 — event-driven fan-in — {R['verdict']}\n"
    f"- eventbridge on bucket: {R.get('eventbridge_enabled')} (existing keys preserved: {R.get('existing_notification_keys')})\n"
    f"- rule watches: {len(WATCH)} prefixes -> {FN}\n"
    f"- canary: {json.dumps(R.get('canary') or R.get('canary_err'))}\n")
print(json.dumps(R,indent=1,default=str))
