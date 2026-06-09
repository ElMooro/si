"""Re-enable the cascade-recalibrator (turns backtest outcomes → signal weights),
run it now to refresh the 6-day-stale weights, then re-run best-setups so
conviction reflects the updated/validated weights. From AWS."""
import json, boto3, time
from botocore.config import Config
cfg=Config(read_timeout=180,retries={"max_attempts":1})
events=boto3.client("events",region_name="us-east-1",config=cfg)
lam=boto3.client("lambda",region_name="us-east-1",config=cfg)
s3=boto3.client("s3",region_name="us-east-1",config=cfg)
out={}
# 1) ensure a daily-weekday rule exists + enabled for the recalibrator
FN="justhodl-cascade-recalibrator"; SCHED="cron(5 13 * * MON-FRI *)"
nt=None; rules=[]; found=None
while True:
    resp=events.list_rules(**({"NextToken":nt} if nt else {})); rules+=resp.get("Rules",[]); nt=resp.get("NextToken")
    if not nt: break
for r in rules:
    for t in events.list_targets_by_rule(Rule=r["Name"]).get("Targets",[]):
        if FN in t.get("Arn",""): found=r["Name"]
try:
    if found:
        events.put_rule(Name=found,ScheduleExpression=SCHED,State="ENABLED"); out["rule"]={"name":found,"ENABLED":True}
    else:
        rn=FN+"-weekday"; events.put_rule(Name=rn,ScheduleExpression=SCHED,State="ENABLED")
        arn=lam.get_function(FunctionName=FN)["Configuration"]["FunctionArn"]
        events.put_targets(Rule=rn,Targets=[{"Id":"1","Arn":arn}])
        try: lam.add_permission(FunctionName=FN,StatementId="EB-"+rn,Action="lambda:InvokeFunction",Principal="events.amazonaws.com",SourceArn="arn:aws:events:us-east-1:857687956942:rule/"+rn)
        except Exception: pass
        out["rule"]={"created":rn}
except Exception as e: out["rule_err"]=str(e)[:80]
# 2) run recalibrator now
try:
    r=lam.invoke(FunctionName=FN,InvocationType="RequestResponse",Payload=b"{}"); out["recalib_run"]=r["Payload"].read().decode()[:120]
except Exception as e: out["recalib_err"]=str(e)[:90]
time.sleep(4)
# check calibration freshness now
try:
    from datetime import datetime, timezone
    o=s3.get_object(Bucket="justhodl-dashboard-live",Key="data/cascade-calibration.json")
    out["calib_age_h_now"]=round((datetime.now(timezone.utc)-o["LastModified"]).total_seconds()/3600,2)
except Exception as e: out["calib_err"]=str(e)[:60]
# 3) re-run best-setups so it picks up refreshed weights
try:
    r=lam.invoke(FunctionName="justhodl-best-setups",InvocationType="RequestResponse",Payload=b"{}"); out["best_setups_run"]=r["Payload"].read().decode()[:120]
except Exception as e: out["bs_err"]=str(e)[:90]
time.sleep(5)
# read new weight_source + top setups
try:
    b=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/best-setups.json")["Body"].read())
    out["weight_source"]=b.get("weight_source")
    out["top3"]=[{"t":s.get("ticker"),"v":s.get("verdict"),"c":s.get("conviction")} for s in (b.get("top_setups") or [])[:3]]
except Exception as e: out["read_err"]=str(e)[:60]
open("aws/ops/reports/1460_rc.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
