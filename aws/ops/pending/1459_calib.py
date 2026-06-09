import json, boto3
from datetime import datetime, timezone
from botocore.config import Config
cfg=Config(read_timeout=90,retries={"max_attempts":1})
s3=boto3.client("s3",region_name="us-east-1",config=cfg)
events=boto3.client("events",region_name="us-east-1",config=cfg)
out={}
# calibration file freshness + content
try:
    o=s3.get_object(Bucket="justhodl-dashboard-live",Key="data/cascade-calibration.json")
    d=json.loads(o["Body"].read())
    out["calibration"]={"age_h":round((datetime.now(timezone.utc)-o["LastModified"]).total_seconds()/3600,1),
                        "keys":list(d.keys())[:10],
                        "generated_at":str(d.get("generated_at",""))[:19],
                        "has_attribution":bool(d.get("feature_attribution_by_tier"))}
except Exception as e: out["calibration"]="MISSING/"+str(e)[:50]
# is the cascade-recalibrator (or calibrator) scheduled/enabled?
nt=None; rules=[]
while True:
    resp=events.list_rules(**({"NextToken":nt} if nt else {})); rules+=resp.get("Rules",[]); nt=resp.get("NextToken")
    if not nt: break
for r in rules:
    try:
        for t in events.list_targets_by_rule(Rule=r["Name"]).get("Targets",[]):
            fn=t.get("Arn","").split(":function:")[-1].split(":")[0]
            if "calibrat" in fn or "cascade" in fn:
                out.setdefault("calib_rules",[]).append({"fn":fn,"rule":r["Name"],"sched":r.get("ScheduleExpression"),"state":r.get("State")})
    except Exception: pass
open("aws/ops/reports/1459_cal.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
