"""Audit the backbone: daily-report-v3 + calibrator invocation counts, error rates,
schedules, and recent CloudWatch errors. From AWS."""
import json, boto3
from datetime import datetime, timezone, timedelta
from botocore.config import Config
cfg=Config(read_timeout=90,retries={"max_attempts":1})
cw=boto3.client("cloudwatch",region_name="us-east-1",config=cfg)
logs=boto3.client("logs",region_name="us-east-1",config=cfg)
events=boto3.client("events",region_name="us-east-1",config=cfg)
lam=boto3.client("lambda",region_name="us-east-1",config=cfg)
s3=boto3.client("s3",region_name="us-east-1",config=cfg)
out={}
now=datetime.now(timezone.utc); start=now-timedelta(hours=48)
def metrics(fn):
    r={}
    for m in ["Invocations","Errors"]:
        d=cw.get_metric_statistics(Namespace="AWS/Lambda",MetricName=m,
            Dimensions=[{"Name":"FunctionName","Value":fn}],
            StartTime=start,EndTime=now,Period=172800,Statistics=["Sum"])
        r[m]=sum(p["Sum"] for p in d.get("Datapoints",[]))
    return r
for fn in ["justhodl-daily-report-v3","justhodl-calibrator","justhodl-signal-logger","justhodl-best-setups"]:
    try:
        m=metrics(fn); inv=m["Invocations"]; err=m["Errors"]
        out[fn]={"invocations_48h":int(inv),"errors_48h":int(err),"error_rate":round(100*err/inv,1) if inv else None}
        # schedule
        try:
            cfg2=lam.get_function_configuration(FunctionName=fn)
            out[fn]["timeout"]=cfg2.get("Timeout"); out[fn]["last_modified"]=cfg2.get("LastModified","")[:19]
        except Exception: pass
    except Exception as e: out[fn]={"err":str(e)[:60]}
# is daily-report-v3 scheduled + enabled?
nt=None; rules=[]
while True:
    resp=events.list_rules(**({"NextToken":nt} if nt else {})); rules+=resp.get("Rules",[]); nt=resp.get("NextToken")
    if not nt: break
for r in rules:
    try:
        for t in events.list_targets_by_rule(Rule=r["Name"]).get("Targets",[]):
            fn=t.get("Arn","").split(":function:")[-1].split(":")[0]
            if fn in ("justhodl-daily-report-v3","justhodl-calibrator"):
                out.setdefault("schedules",[]).append({"fn":fn,"rule":r["Name"],"sched":r.get("ScheduleExpression"),"state":r.get("State")})
    except Exception: pass
# report.json freshness (the audit said 1 day stale)
try:
    o=s3.get_object(Bucket="justhodl-dashboard-live",Key="data/report.json")
    out["report_json_age_h"]=round((now-o["LastModified"]).total_seconds()/3600,1)
except Exception as e: out["report_json"]=str(e)[:40]
open("aws/ops/reports/1517_bb.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
