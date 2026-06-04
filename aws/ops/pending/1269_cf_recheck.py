"""1269 — recheck capital-flow with ETF fix."""
import json, time, boto3
from datetime import datetime, timezone
from botocore.config import Config
cfg=Config(read_timeout=300,retries={"max_attempts":1})
lam=boto3.client("lambda",region_name="us-east-1",config=cfg); s3=boto3.client("s3",region_name="us-east-1",config=cfg)
out={"started":datetime.now(timezone.utc).isoformat()}
try:
    r=lam.invoke(FunctionName="justhodl-capital-flow",InvocationType="RequestResponse",Payload=b"{}")
    out["invoke"]=r.get("Payload").read().decode()[:200]
except Exception as e: out["invoke"]=str(e)[:200]
time.sleep(2)
try:
    cf=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/capital-flow.json")["Body"].read())
    out["sources"]=cf.get("sources")
    out["etf_in"]=[{"t":e["ticker"],"cat":e.get("category"),"z":e.get("dvol_z"),"r5":e.get("return_5d_pct"),"sig":e.get("flow_signal")} for e in cf.get("etf_flows_in",[])[:8]]
    out["cat_rotation"]=[{"c":c["category"],"sig":c.get("signal"),"z":c.get("avg_dvol_z")} for c in cf.get("category_rotation",[])[:8]]
    out["top_accum"]=[{"t":x["ticker"],"score":x["flow_score"]} for x in cf.get("accumulating",[])[:6]]
except Exception as e: out["err"]=str(e)[:200]
open("aws/ops/reports/1269_cf.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
