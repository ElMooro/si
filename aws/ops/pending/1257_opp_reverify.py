"""1257 — re-invoke opportunity-engine (deploy landed) + verify growth_intel."""
import json, time
from datetime import datetime, timezone
import boto3
from botocore.config import Config
REPORT="aws/ops/reports/1257_opp_reverify.json"
BUCKET="justhodl-dashboard-live"; LAMBDA="justhodl-opportunity-engine"; REGION="us-east-1"
cfg=Config(read_timeout=600,retries={"max_attempts":1})
lam=boto3.client("lambda",region_name=REGION,config=cfg); s3=boto3.client("s3",region_name=REGION,config=cfg)
out={"started":datetime.now(timezone.utc).isoformat()}
# confirm code has new func
try:
    c=lam.get_function(FunctionName=LAMBDA); out["last_modified"]=c["Configuration"].get("LastModified")
except Exception as e: out["cfg_err"]=str(e)[:100]
try:
    t0=time.time()
    r=lam.invoke(FunctionName=LAMBDA,InvocationType="RequestResponse",Payload=b"{}")
    out["invoke"]={"status":r.get("StatusCode"),"elapsed_s":round(time.time()-t0,1),"fe":r.get("FunctionError"),"body":r.get("Payload").read().decode()[:200]}
    print("invoke",out["invoke"]["status"],out["invoke"]["elapsed_s"],"s")
except Exception as e: out["invoke"]={"error":str(e)[:300]}
time.sleep(3)
try:
    doc=json.loads(s3.get_object(Bucket=BUCKET,Key="data/opportunities.json")["Body"].read())
    allrows=doc.get("all",[])
    gi=[x for x in allrows if x.get("growth_intel")]
    fwd=[x for x in gi if x["growth_intel"].get("expected_company_growth_pct") is not None]
    bl=[x for x in allrows if x.get("backlog")]
    out["has_growth_intel"]=len(gi); out["has_fwd_growth"]=len(fwd); out["has_backlog"]=len(bl)
    out["n_industries"]=len(doc.get("industry_benchmarks",{}))
    fwd.sort(key=lambda x:-(x.get("growth_opportunity_score") or 0))
    out["samples"]=[]
    for x in fwd[:10]:
        g=x["growth_intel"]
        out["samples"].append(f"{x.get('ticker')} GO={x.get('growth_opportunity_score')} [{x.get('verdict')}] co {g.get('company_rev_growth_pct')}%->{g.get('expected_company_growth_pct')}% vs ind {g.get('industry_growth_pct')}%->{g.get('expected_industry_growth_pct')}% PEvsInd {g.get('pe_vs_industry_pct')}% PEG {g.get('peg_forward')}"+(" 📋" if x.get('backlog') else ""))
    print("growth_intel:",len(gi),"fwd:",len(fwd),"backlog:",len(bl),"industries:",out["n_industries"])
    for s in out["samples"]: print("  ",s)
except Exception as e: out["verify_err"]=str(e)[:200]
out["finished"]=datetime.now(timezone.utc).isoformat()
open(REPORT,"w").write(json.dumps(out,indent=2,default=str))
print("DONE")
