"""1353 — full health check of everything built this session."""
import json, time, urllib.request
import boto3
from botocore.config import Config
cfg=Config(read_timeout=60,retries={"max_attempts":1})
lam=boto3.client("lambda",region_name="us-east-1",config=cfg); s3=boto3.client("s3",region_name="us-east-1",config=cfg)
events=boto3.client("events",region_name="us-east-1",config=cfg)
out={"lambdas":{},"data_files":{},"pages":{},"schedules":{}}

# 1) New Lambdas — exist + scheduled?
NEW_LAMBDAS=["justhodl-crypto-cycle-risk","justhodl-funding-plumbing","justhodl-brain-sync",
  "justhodl-journal-grader","justhodl-devils-advocate","justhodl-my-brief","justhodl-regime-playbook",
  "justhodl-narrative-vs-tape","justhodl-position-sizer","justhodl-engine-conflicts"]
for fn in NEW_LAMBDAS:
    try:
        c=lam.get_function_configuration(FunctionName=fn)
        out["lambdas"][fn]={"state":c.get("State"),"lastmod":c.get("LastModified","")[:10]}
    except Exception as e: out["lambdas"][fn]="MISSING:"+str(e)[:40]

# 2) Data files freshness (hours since generated_at)
DATA=["data/crypto-cycle-risk.json","data/funding-plumbing.json","data/brain.json","data/journal-graded.json",
  "data/devils-advocate.json","data/my-brief.json","data/regime-playbook.json","data/narrative-vs-tape.json",
  "data/position-sizing.json","data/engine-conflicts.json","data/best-setups.json"]
now=time.time()
for k in DATA:
    try:
        d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key=k)["Body"].read())
        ga=d.get("generated_at") or d.get("source_updated_at")
        age="?"
        if ga:
            try:
                from datetime import datetime
                t=datetime.fromisoformat(ga.replace("Z","+00:00")).timestamp()
                age=round((now-t)/3600,1)
            except: pass
        out["data_files"][k]={"exists":True,"age_hrs":age}
    except Exception as e: out["data_files"][k]="MISSING"

open("aws/ops/reports/1353_hc.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
