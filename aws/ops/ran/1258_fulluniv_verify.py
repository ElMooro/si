"""1258 — verify opportunity-engine full-universe (all caps) + creative metrics."""
import json, time
from datetime import datetime, timezone
import boto3
from botocore.config import Config
REPORT="aws/ops/reports/1258_fulluniv.json"; BUCKET="justhodl-dashboard-live"; REGION="us-east-1"
cfg=Config(read_timeout=650,retries={"max_attempts":1})
lam=boto3.client("lambda",region_name=REGION,config=cfg); s3=boto3.client("s3",region_name=REGION,config=cfg)
out={"started":datetime.now(timezone.utc).isoformat()}
time.sleep(30)
try:
    t0=time.time()
    r=lam.invoke(FunctionName="justhodl-opportunity-engine",InvocationType="RequestResponse",Payload=b"{}")
    out["invoke"]={"status":r.get("StatusCode"),"elapsed_s":round(time.time()-t0,1),"fe":r.get("FunctionError"),"body":r.get("Payload").read().decode()[:250]}
    print("invoke",out["invoke"]["status"],out["invoke"]["elapsed_s"],"s")
except Exception as e: out["invoke"]={"error":str(e)[:300]}
time.sleep(3)
try:
    doc=json.loads(s3.get_object(Bucket=BUCKET,Key="data/opportunities.json")["Body"].read())
    allr=doc.get("all",[])
    caps={}; 
    for r in allr: caps[r.get("growth_intel",{}).get("range_position_52w") is not None and "x" or "y"]=0
    capdist={}
    for r in allr:
        cb=r.get("cap_bucket") or (r.get("growth_intel") or {}).get("cap_bucket") or "?"
        capdist[cb]=capdist.get(cb,0)+1
    comp=[r for r in allr if r.get("compounder_score") is not None]
    comp.sort(key=lambda r:-(r.get("compounder_score") or 0))
    out["result"]={"n_covered":doc.get("n_covered"),"cap_distribution":capdist,
        "with_compounder":len(comp),
        "top_compounders":[{"t":r["ticker"],"comp":r.get("compounder_score"),"go":r.get("growth_opportunity_score"),"lynch":r.get("lynch_ratio"),"verdict":r.get("verdict"),"cap":r.get("cap_bucket")} for r in comp[:10]]}
    print("covered:",doc.get("n_covered"),"caps:",capdist)
    for r in comp[:10]: print(f"  {r['ticker']:<6s} comp={r.get('compounder_score')} GO={r.get('growth_opportunity_score')} lynch={r.get('lynch_ratio')} [{r.get('verdict')}] {r.get('cap_bucket')}")
except Exception as e: out["result"]={"error":str(e)[:300]}
out["finished"]=datetime.now(timezone.utc).isoformat()
open(REPORT,"w").write(json.dumps(out,indent=2,default=str)); print("DONE")
