"""PROOF: list deployed ECB lambdas, their S3 output, and live page status."""
import json, os, time, zipfile, io, boto3
from botocore.config import Config
from datetime import datetime, timezone
cfg=Config(read_timeout=90,retries={"max_attempts":1})
lam=boto3.client("lambda",region_name="us-east-1",config=cfg); s3=boto3.client("s3",region_name="us-east-1",config=cfg)
ROLE="arn:aws:iam::857687956942:role/lambda-execution-role"; FN="tmp-proof"
out={}
# 1) deployed ECB lambdas
fns=[]
paginator=lam.get_paginator("list_functions")
for pg in paginator.paginate():
    for f in pg["Functions"]:
        if "ecb" in f["FunctionName"].lower() or "eurodollar" in f["FunctionName"].lower():
            fns.append({"name":f["FunctionName"],"modified":f["LastModified"][:19],"runtime":f["Runtime"]})
out["ecb_lambdas_deployed"]=fns
# 2) S3 output files (the audit said data/ecb/* is missing — check what DOES exist)
for k in ["data/ecb-derived.json","data/ecb-detail.json","data/ecb-hist/_manifest.json","data/ecb-hist/ciss_ea.json","data/move-index.json","data/basket-var.json"]:
    try:
        o=s3.get_object(Bucket="justhodl-dashboard-live",Key=k)
        age=round((datetime.now(timezone.utc)-o["LastModified"]).total_seconds()/3600,1)
        out.setdefault("s3_files",{})[k]={"exists":True,"bytes":o["ContentLength"],"age_h":age}
    except Exception as e: out.setdefault("s3_files",{})[k]={"exists":False}
# 3) live page HTTP status + content proof
code=r'''
import json,urllib.request
def lambda_handler(e,c):
    out={}
    for p in ["/eu-dump-radar.html","/ecb-history.html","/ecb-detail.html"]:
        try:
            h=urllib.request.urlopen(urllib.request.Request("https://justhodl.ai"+p+"?t=9",headers={"User-Agent":"Mozilla/5.0"}),timeout=15).read().decode()
            out[p]={"status":200,"bytes":len(h)}
            if "dump-radar" in p: out[p]["renders"]=("CISS Acceleration" in h and "Bank Pass-Through" in h)
        except Exception as ex: out[p]=str(ex)[:50]
    # the live ecb-derived data
    try:
        d=json.loads(urllib.request.urlopen(urllib.request.Request("https://justhodl-data-proxy.raafouis.workers.dev/data/ecb-derived.json?t=9",headers={"User-Agent":"jh"}),timeout=12).read().decode())
        out["ecb_derived_live"]={"headline":d.get("headline"),"signals":{k:v.get("signal") for k,v in d.get("indicators",{}).items()}}
    except Exception as ex: out["data_err"]=str(ex)[:50]
    return out
'''
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as zf: zf.writestr("lambda_function.py",code)
try:
    try: lam.get_function_configuration(FunctionName=FN); lam.update_function_code(FunctionName=FN,ZipFile=buf.getvalue())
    except lam.exceptions.ResourceNotFoundException:
        lam.create_function(FunctionName=FN,Runtime="python3.12",Role=ROLE,Handler="lambda_function.lambda_handler",Code={"ZipFile":buf.getvalue()},Timeout=40,MemorySize=128)
    for _ in range(20):
        time.sleep(2); c=lam.get_function_configuration(FunctionName=FN)
        if c.get("State")=="Active" and c.get("LastUpdateStatus") in ("Successful",None): break
    out["live"]=json.loads(lam.invoke(FunctionName=FN,InvocationType="RequestResponse",Payload=b"{}")["Payload"].read())
    lam.delete_function(FunctionName=FN)
except Exception as e: out["live_err"]=str(e)[:120]
open("aws/ops/reports/1504_proof.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
