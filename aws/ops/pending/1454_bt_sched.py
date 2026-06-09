import json, boto3
from botocore.config import Config
cfg=Config(read_timeout=90,retries={"max_attempts":1})
events=boto3.client("events",region_name="us-east-1",config=cfg)
lam=boto3.client("lambda",region_name="us-east-1",config=cfg)
out={}
# find rules targeting backtest-harness
nt=None; rules=[]
while True:
    resp=events.list_rules(**({"NextToken":nt} if nt else {}))
    rules+=resp.get("Rules",[]); nt=resp.get("NextToken")
    if not nt: break
for r in rules:
    try:
        for t in events.list_targets_by_rule(Rule=r["Name"]).get("Targets",[]):
            fn=t.get("Arn","").split(":function:")[-1].split(":")[0]
            if fn in ("justhodl-backtest-harness","justhodl-backtest-engine","justhodl-signal-logger","justhodl-outcome-checker","justhodl-calibrator"):
                out.setdefault(fn,[]).append({"rule":r["Name"],"sched":r.get("ScheduleExpression"),"state":r.get("State")})
    except Exception: pass
# also list any signal-logging/outcome lambdas that exist
out["_existing_validation_lambdas"]=[]
import subprocess
open("aws/ops/reports/1454_bt.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
