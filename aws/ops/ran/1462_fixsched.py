import json, boto3
from botocore.config import Config
cfg=Config(read_timeout=90,retries={"max_attempts":1})
events=boto3.client("events",region_name="us-east-1",config=cfg)
lam=boto3.client("lambda",region_name="us-east-1",config=cfg)
out={}
FN="justhodl-cascade-recalibrator"
# the prior failure was likely a bad existing rule name; use a clean daily cron (every day, simpler than MON-FRI)
rn="justhodl-cascade-recalibrator-daily"
try:
    events.put_rule(Name=rn,ScheduleExpression="cron(0 23 * * ? *)",State="ENABLED")  # daily 23:00, after outcomes graded
    arn=lam.get_function(FunctionName=FN)["Configuration"]["FunctionArn"]
    events.put_targets(Rule=rn,Targets=[{"Id":"1","Arn":arn}])
    try: lam.add_permission(FunctionName=FN,StatementId="EB-"+rn,Action="lambda:InvokeFunction",Principal="events.amazonaws.com",SourceArn="arn:aws:events:us-east-1:857687956942:rule/"+rn)
    except Exception: pass
    out["recalibrator"]={"rule":rn,"sched":"cron(0 23 * * ? *)","ENABLED":True}
except Exception as e: out["err"]=str(e)[:100]
# final audit: confirm the WHOLE validation pipeline is enabled
nt=None; rules=[]
while True:
    resp=events.list_rules(**({"NextToken":nt} if nt else {})); rules+=resp.get("Rules",[]); nt=resp.get("NextToken")
    if not nt: break
pipeline={}
for r in rules:
    if r.get("State")!="ENABLED": continue
    for t in events.list_targets_by_rule(Rule=r["Name"]).get("Targets",[]):
        fn=t.get("Arn","").split(":function:")[-1].split(":")[0]
        if fn in ("justhodl-signal-logger","justhodl-backtest-engine","justhodl-backtest-harness","justhodl-outcome-checker","justhodl-cascade-recalibrator","justhodl-best-setups"):
            pipeline[fn]=r.get("ScheduleExpression")
out["enabled_pipeline"]=pipeline
open("aws/ops/reports/1462_fs.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
