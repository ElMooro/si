import json, time
from datetime import datetime, timezone
import boto3
from botocore.config import Config
REPORT="aws/ops/reports/1262_fwd_full.json"; BUCKET="justhodl-dashboard-live"; REGION="us-east-1"
cfg=Config(read_timeout=650,retries={"max_attempts":1})
lam=boto3.client("lambda",region_name=REGION,config=cfg); s3=boto3.client("s3",region_name=REGION,config=cfg)
out={"started":datetime.now(timezone.utc).isoformat()}
try:
    t0=time.time(); r=lam.invoke(FunctionName="justhodl-opportunity-engine",InvocationType="RequestResponse",Payload=b"{}")
    out["invoke"]={"status":r.get("StatusCode"),"elapsed_s":round(time.time()-t0,1),"body":r.get("Payload").read().decode()[:150]}
except Exception as e: out["invoke"]={"error":str(e)[:200]}
time.sleep(3)
try:
    doc=json.loads(s3.get_object(Bucket=BUCKET,Key="data/opportunities.json")["Body"].read())
    allr=doc.get("all",[])
    fwd=[r for r in allr if (r.get("growth_intel") or {}).get("expected_company_growth_pct") is not None]
    out["result"]={"covered":doc.get("n_covered"),"with_fwd_growth":len(fwd),"pct":round(len(fwd)/max(1,len(allr))*100)}
except Exception as e: out["result"]={"error":str(e)[:200]}
out["finished"]=datetime.now(timezone.utc).isoformat()
open(REPORT,"w").write(json.dumps(out,indent=2,default=str)); print(json.dumps(out["result"],default=str))
