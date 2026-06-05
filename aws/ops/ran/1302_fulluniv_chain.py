"""1302 — deploy full-universe backlog + opp(reverse-DCF) + overlap + best-setups; verify chain."""
import json, os, time, zipfile, io
import boto3
from botocore.config import Config
cfg=Config(read_timeout=650,retries={"max_attempts":1})
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
def rd(n,src,mem=None,to=None):
    lam.update_function_code(FunctionName=n,ZipFile=zd(src))
    for _ in range(30):
        time.sleep(2); c=lam.get_function_configuration(FunctionName=n)
        if c.get("LastUpdateStatus") in ("Successful",None): break
    if mem or to:
        try: lam.update_function_configuration(FunctionName=n,**({"MemorySize":mem} if mem else {}),**({"Timeout":to} if to else {})); time.sleep(4)
        except Exception as e: out[n+"_cfg"]=str(e)[:80]
rd("justhodl-backlog","aws/lambdas/justhodl-backlog/source",1024,600)
rd("justhodl-opportunity-engine","aws/lambdas/justhodl-opportunity-engine/source")
rd("justhodl-deep-value-overlap","aws/lambdas/justhodl-deep-value-overlap/source")
rd("justhodl-best-setups","aws/lambdas/justhodl-best-setups/source")
# 1) backlog full universe (long run)
try:
    t0=time.time(); r=lam.invoke(FunctionName="justhodl-backlog",InvocationType="RequestResponse",Payload=b"{}")
    out["backlog_invoke"]={"elapsed":round(time.time()-t0,1),"body":r.get("Payload").read().decode()[:120]}
    time.sleep(2); bl=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/backlog.json")["Body"].read())
    out["backlog"]={"covered":bl.get("n_covered"),"caps":bl.get("cap_distribution"),"accel":len(bl.get("accelerating",[]))}
except Exception as e: out["backlog"]=str(e)[:150]
# 2) opp (reverse-DCF) — quick check a name has implied_growth
try:
    lam.invoke(FunctionName="justhodl-opportunity-engine",InvocationType="RequestResponse",Payload=b"{}"); time.sleep(3)
    op=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/opportunities.json")["Body"].read())
    rd_names=[r for r in (op.get("all") or []) if (r.get("growth_intel") or {}).get("reverse_dcf_mispriced")]
    out["reverse_dcf"]={"n_mispriced":len(rd_names),"sample":[{"t":r["ticker"],"impl":(r.get("growth_intel") or {}).get("implied_growth_pct"),"exp":(r.get("growth_intel") or {}).get("expected_company_growth_pct"),"gap":(r.get("growth_intel") or {}).get("growth_gap_pct")} for r in rd_names[:5]]}
except Exception as e: out["reverse_dcf"]=str(e)[:150]
# 3) overlap
try:
    lam.invoke(FunctionName="justhodl-deep-value-overlap",InvocationType="RequestResponse",Payload=b"{}"); time.sleep(2)
    ov=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/deep-value-overlap.json")["Body"].read())
    out["overlap"]={"scored":ov.get("n_scored"),"prime":len(ov.get("prime_setups",[])),"elite":len(ov.get("elite_setups",[])),"top":[r["ticker"] for r in ov.get("prime_setups",[])[:6]]}
except Exception as e: out["overlap"]=str(e)[:150]
# 4) best-setups picks up overlap
try:
    lam.invoke(FunctionName="justhodl-best-setups",InvocationType="RequestResponse",Payload=b"{}"); time.sleep(2)
    bs=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/best-setups.json")["Body"].read())
    keys=set()
    for s in bs.get("top_setups",[]):
        for k in (s.get("signal_keys") or []): keys.add(k)
    out["board_has_overlap"]="DEEP_VALUE_OVERLAP" in keys
    out["board_signals"]=sorted(keys)
except Exception as e: out["board"]=str(e)[:150]
open("aws/ops/reports/1302_chain.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
