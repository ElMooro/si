"""1295 — ensure vintage-fred runs daily (point-in-time data must accrue)."""
import boto3, json
from botocore.config import Config
cfg=Config(read_timeout=60,retries={"max_attempts":1})
lam=boto3.client("lambda",region_name="us-east-1",config=cfg); events=boto3.client("events",region_name="us-east-1",config=cfg)
out={}
RULE="justhodl-vintage-fred-daily"; SCHED="cron(0 13 * * ? *)"; ACC="857687956942"
try:
    events.put_rule(Name=RULE,ScheduleExpression=SCHED,State="ENABLED",Description="Daily point-in-time FRED capture")
    fn=lam.get_function(FunctionName="justhodl-vintage-fred")
    events.put_targets(Rule=RULE,Targets=[{"Id":"1","Arn":fn["Configuration"]["FunctionArn"]}])
    try: lam.add_permission(FunctionName="justhodl-vintage-fred",StatementId=f"EB-{RULE}",Action="lambda:InvokeFunction",Principal="events.amazonaws.com",SourceArn=f"arn:aws:events:us-east-1:{ACC}:rule/{RULE}")
    except lam.exceptions.ResourceConflictException: pass
    out["scheduled"]=SCHED
except Exception as e: out["err"]=str(e)[:200]
open("aws/ops/reports/1295_sched.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
