"""(b) Re-enable the validation pipeline (DAILY, cost-conscious) so forward
returns mature and signals get proven hit-rates. From AWS."""
import json, boto3
from botocore.config import Config
cfg=Config(read_timeout=120,retries={"max_attempts":1})
events=boto3.client("events",region_name="us-east-1",config=cfg)
lam=boto3.client("lambda",region_name="us-east-1",config=cfg)
out={}
# Re-enable + set to DAILY (staggered). These are the validation engine.
# signal-logger: record signals daily. backtest-engine: compute forward returns.
# outcome-checker + calibrator: grade + update weights.
PLAN={
  "justhodl-signal-logger-6h": "cron(0 21 * * ? *)",       # log signals daily 21:00 UTC (after close)
  "justhodl-backtest-engine-6h": "cron(30 22 * * ? *)",    # compute forward returns daily 22:30
}
for rule,sched in PLAN.items():
    try:
        r=events.describe_rule(Name=rule)
        events.put_rule(Name=rule,ScheduleExpression=sched,State="ENABLED")
        out[rule]={"old":r.get("ScheduleExpression"),"new":sched,"now":"ENABLED"}
    except Exception as e: out[rule]={"err":str(e)[:80]}
# Also ensure backtest-harness (the summary writer) + outcome-checker run daily.
# Find their rules; if none, create daily ones.
for fn,sched in [("justhodl-backtest-harness","cron(0 22 * * ? *)"),
                 ("justhodl-outcome-checker","cron(0 23 * * ? *)")]:
    try:
        # does a rule target it?
        nt=None; found=None; rules=[]
        while True:
            resp=events.list_rules(**({"NextToken":nt} if nt else {})); rules+=resp.get("Rules",[]); nt=resp.get("NextToken")
            if not nt: break
        for r in rules:
            for t in events.list_targets_by_rule(Rule=r["Name"]).get("Targets",[]):
                if fn in t.get("Arn",""): found=r["Name"]
        if found:
            events.put_rule(Name=found,ScheduleExpression=sched,State="ENABLED"); out[fn]={"rule":found,"set":sched,"ENABLED":True}
        else:
            rn=fn+"-daily"; events.put_rule(Name=rn,ScheduleExpression=sched,State="ENABLED")
            arn=lam.get_function(FunctionName=fn)["Configuration"]["FunctionArn"]
            events.put_targets(Rule=rn,Targets=[{"Id":"1","Arn":arn}])
            try: lam.add_permission(FunctionName=fn,StatementId="EB-"+rn,Action="lambda:InvokeFunction",Principal="events.amazonaws.com",SourceArn="arn:aws:events:us-east-1:857687956942:rule/"+rn)
            except Exception: pass
            out[fn]={"created_rule":rn,"set":sched}
    except Exception as e: out[fn]={"err":str(e)[:80]}
open("aws/ops/reports/1455_ev.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
