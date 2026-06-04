"""1261 — verify (a) full-universe forward growth (b) compounder/revision in board (c) page data."""
import json, time
from datetime import datetime, timezone
import boto3
from botocore.config import Config
REPORT="aws/ops/reports/1261_abc.json"; BUCKET="justhodl-dashboard-live"; REGION="us-east-1"
cfg=Config(read_timeout=650,retries={"max_attempts":1})
lam=boto3.client("lambda",region_name=REGION,config=cfg); s3=boto3.client("s3",region_name=REGION,config=cfg)
out={"started":datetime.now(timezone.utc).isoformat()}
time.sleep(75)
# (a) re-run opportunity-engine (now full-universe forward growth)
try:
    t0=time.time(); r=lam.invoke(FunctionName="justhodl-opportunity-engine",InvocationType="RequestResponse",Payload=b"{}")
    out["opp_invoke"]={"status":r.get("StatusCode"),"elapsed_s":round(time.time()-t0,1)}
except Exception as e: out["opp_invoke"]={"error":str(e)[:200]}
time.sleep(3)
try:
    doc=json.loads(s3.get_object(Bucket=BUCKET,Key="data/opportunities.json")["Body"].read())
    allr=doc.get("all",[])
    fwd=[r for r in allr if (r.get("growth_intel") or {}).get("expected_company_growth_pct") is not None]
    comp=[r for r in allr if (r.get("compounder_score") or 0)>=70]
    out["a"]={"covered":doc.get("n_covered"),"with_fwd_growth":len(fwd),"compounders70plus":len(comp)}
except Exception as e: out["a"]={"error":str(e)[:200]}
# (b) re-run best-setups, check COMPOUNDER/REVISION_UP present
try:
    r=lam.invoke(FunctionName="justhodl-best-setups",InvocationType="RequestResponse",Payload=b"{}")
    time.sleep(2)
    bs=json.loads(s3.get_object(Bucket=BUCKET,Key="data/best-setups.json")["Body"].read())
    setups=bs.get("top_setups",[])
    cmp_n=sum(1 for s in setups if "COMPOUNDER" in (s.get("signal_keys") or []))
    rev_n=sum(1 for s in setups if "REVISION_UP" in (s.get("signal_keys") or []))
    out["b"]={"setups":len(setups),"with_compounder":cmp_n,"with_revision":rev_n,
        "sample":[{"t":s["ticker"],"conv":s["conviction"],"sigs":s["signal_keys"]} for s in setups if "COMPOUNDER" in (s.get("signal_keys") or [])][:4]}
except Exception as e: out["b"]={"error":str(e)[:200]}
out["finished"]=datetime.now(timezone.utc).isoformat()
open(REPORT,"w").write(json.dumps(out,indent=2,default=str)); print(json.dumps(out,indent=2,default=str))
