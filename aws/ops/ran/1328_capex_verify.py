import json, os, time, zipfile, io
import boto3
from botocore.config import Config
cfg=Config(read_timeout=500,retries={"max_attempts":1})
lam=boto3.client("lambda",region_name="us-east-1",config=cfg); s3=boto3.client("s3",region_name="us-east-1",config=cfg)
out={}
def zd(src):
    buf=io.BytesIO()
    with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as zf:
        for r,_,fs in os.walk(src):
            for f in fs:
                if "__pycache__" in r or f.endswith(".pyc"): continue
                fp=os.path.join(r,f); zf.write(fp,arcname=os.path.relpath(fp,src))
    return buf.getvalue()
# redeploy both
for n,src in [("justhodl-crypto-cycle-risk","aws/lambdas/justhodl-crypto-cycle-risk/source"),("justhodl-opportunity-engine","aws/lambdas/justhodl-opportunity-engine/source")]:
    lam.update_function_code(FunctionName=n,ZipFile=zd(src))
    for _ in range(30):
        time.sleep(2); c=lam.get_function_configuration(FunctionName=n)
        if c.get("LastUpdateStatus") in ("Successful",None): break
# crypto ETF factor check
lam.invoke(FunctionName="justhodl-crypto-cycle-risk",InvocationType="RequestResponse",Payload=b"{}"); time.sleep(2)
cd=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/crypto-cycle-risk.json")["Body"].read())
out["etf_flows_note"]=(cd.get("factors",{}).get("etf_flows",{}).get("note") or "")[:110]
out["ai_rotation_note"]=(cd.get("factors",{}).get("ai_rotation",{}).get("note") or "")[:110]
# opportunity engine — long run
try:
    t0=time.time(); lam.invoke(FunctionName="justhodl-opportunity-engine",InvocationType="RequestResponse",Payload=b"{}")
    out["opp_elapsed"]=round(time.time()-t0,1); time.sleep(3)
    op=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/opportunities.json")["Body"].read())
    # find names with buyback/capex signals
    bb=[r for r in (op.get("all") or []) if (r.get("growth_intel") or {}).get("buyback_yield_pct")]
    cx=[r for r in (op.get("all") or []) if (r.get("growth_intel") or {}).get("capex_signal","").startswith(("surging","rising"))]
    out["n_with_buyback"]=len(bb); out["n_with_capex_signal"]=len(cx)
    out["buyback_sample"]=[{"t":r["ticker"],"bby":(r.get("growth_intel") or {}).get("buyback_yield_pct"),"sig":(r.get("growth_intel") or {}).get("buyback_signal")} for r in sorted(bb,key=lambda r:-((r.get("growth_intel") or {}).get("buyback_yield_pct") or 0))[:5]]
    out["capex_sample"]=[{"t":r["ticker"],"cgr":(r.get("growth_intel") or {}).get("capex_growth_pct"),"sig":(r.get("growth_intel") or {}).get("capex_signal")} for r in cx[:5]]
except Exception as e: out["opp_err"]=str(e)[:150]
open("aws/ops/reports/1328_cx.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
