import json, os, time, zipfile, io
import boto3
from botocore.config import Config
cfg=Config(read_timeout=300,retries={"max_attempts":1})
lam=boto3.client("lambda",region_name="us-east-1",config=cfg)
s3=boto3.client("s3",region_name="us-east-1",config=cfg)
out={}
buf=io.BytesIO(); src="aws/lambdas/justhodl-short-interest/source"
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as zf:
    for r,_,fs in os.walk(src):
        for f in fs:
            if "__pycache__" in r or f.endswith(".pyc"): continue
            zf.write(os.path.join(r,f),arcname=os.path.relpath(os.path.join(r,f),src))
lam.update_function_code(FunctionName="justhodl-short-interest",ZipFile=buf.getvalue())
for _ in range(25):
    time.sleep(2); c=lam.get_function_configuration(FunctionName="justhodl-short-interest")
    if c.get("LastUpdateStatus") in ("Successful",None): break
try:
    r=lam.invoke(FunctionName="justhodl-short-interest",InvocationType="RequestResponse",Payload=b"{}"); out["run"]=r["Payload"].read().decode()[:80]
except Exception as e: out["run_err"]=str(e)[:80]
time.sleep(4)
# recheck stale rows
si=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/short-interest.json")["Body"].read())
sit=si.get("by_ticker") or {}
items=sit.items() if isinstance(sit,dict) else [(x.get("ticker"),x) for x in (sit if isinstance(sit,list) else [])]
old=[t for t,v in items if str((v or {}).get("settlement_date") or "")<"2025-01-01" and (v or {}).get("settlement_date")]
out["pre2025_rows_now"]=len(old); out["sample"]=old[:4]; out["total_tickers"]=len(list(items))
open("aws/ops/reports/1473_si.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
