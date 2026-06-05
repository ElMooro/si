import json, os, time, zipfile, io
import boto3
from botocore.config import Config
cfg=Config(read_timeout=320,retries={"max_attempts":1})
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
def rd(n,src):
    lam.update_function_code(FunctionName=n,ZipFile=zd(src))
    for _ in range(25):
        time.sleep(2); c=lam.get_function_configuration(FunctionName=n)
        if c.get("LastUpdateStatus") in ("Successful",None): break
rd("justhodl-backlog","aws/lambdas/justhodl-backlog/source")
rd("justhodl-deep-value-overlap","aws/lambdas/justhodl-deep-value-overlap/source")
try:
    lam.invoke(FunctionName="justhodl-backlog",InvocationType="RequestResponse",Payload=b"{}"); time.sleep(2)
    bl=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/backlog.json")["Body"].read())
    out["backlog"]=[{"t":r["ticker"],"rpo_yoy":r.get("rpo_yoy"),"rev_yoy":r.get("rev_yoy"),"div":r.get("rpo_minus_rev_growth"),"ev_rpo":r.get("ev_to_rpo")} for r in bl.get("accelerating",[])[:6]]
except Exception as e: out["backlog"]=str(e)[:150]
try:
    lam.invoke(FunctionName="justhodl-deep-value-overlap",InvocationType="RequestResponse",Payload=b"{}"); time.sleep(2)
    ov=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/deep-value-overlap.json")["Body"].read())
    out["overlap"]={"scored":ov.get("n_scored"),"prime":len(ov.get("prime_setups",[])),"elite":len(ov.get("elite_setups",[])),
        "top":[{"t":r["ticker"],"score":r["overlap_score"],"L":r["n_value_lenses"],"C":r["n_catalysts"],"I":r["n_inflection"],"lenses":r.get("value_lenses"),"cats":r.get("catalysts")} for r in ov.get("prime_setups",[])[:6]]}
except Exception as e: out["overlap"]=str(e)[:150]
open("aws/ops/reports/1301_fixes.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
