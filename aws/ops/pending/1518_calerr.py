"""Get the actual error from justhodl-calibrator (100% error rate) + run it to capture the traceback."""
import json, boto3, time
from datetime import datetime, timezone, timedelta
from botocore.config import Config
cfg=Config(read_timeout=120,retries={"max_attempts":1})
logs=boto3.client("logs",region_name="us-east-1",config=cfg)
lam=boto3.client("lambda",region_name="us-east-1",config=cfg)
out={}
# 1) run it now, capture the error response
try:
    r=lam.invoke(FunctionName="justhodl-calibrator",InvocationType="RequestResponse",Payload=b"{}")
    out["live_invoke"]=r["Payload"].read().decode()[:500]
    out["function_error"]=r.get("FunctionError")
except Exception as e: out["invoke_err"]=str(e)[:100]
# 2) recent log errors
try:
    lg="/aws/lambda/justhodl-calibrator"
    streams=logs.describe_log_streams(logGroupName=lg,orderBy="LastEventTime",descending=True,limit=2)
    for st in streams.get("logStreams",[])[:2]:
        ev=logs.get_log_events(logGroupName=lg,logStreamName=st["logStreamName"],limit=30,startFromHead=False)
        errs=[e["message"][:200] for e in ev.get("events",[]) if any(x in e["message"] for x in ["Error","error","Traceback","Exception","Task timed"])]
        if errs: out.setdefault("log_errors",[]).extend(errs[:8]); break
except Exception as e: out["log_err"]=str(e)[:80]
open("aws/ops/reports/1518_ce.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
