import json, os, time, zipfile, io, boto3
from datetime import datetime, timezone
from botocore.config import Config
cfg=Config(read_timeout=90,retries={"max_attempts":1})
lam=boto3.client("lambda",region_name="us-east-1",config=cfg)
s3=boto3.client("s3",region_name="us-east-1",config=cfg)
ROLE="arn:aws:iam::857687956942:role/lambda-execution-role"; FN="tmp-proof"
out={}
# PROOF 1: Lambdas exist? (the names I ACTUALLY built, not the audit's expected names)
for name in ["justhodl-ecb-derived","justhodl-ecb-history","justhodl-move-index","justhodl-basket-var"]:
    try:
        c=lam.get_function_configuration(FunctionName=name)
        out["lambda_"+name]={"EXISTS":True,"arn":c["FunctionArn"],"last_modified":c["LastModified"],"runtime":c["Runtime"]}
    except Exception as e: out["lambda_"+name]={"EXISTS":False,"err":str(e)[:40]}
# PROOF 2: S3 data files exist? (with size + age)
for k in ["data/ecb-derived.json","data/move-index.json","data/basket-var.json","data/ecb-hist/_manifest.json","data/ecb-hist/ciss_ea.json"]:
    try:
        o=s3.get_object(Bucket="justhodl-dashboard-live",Key=k)
        out["s3_"+k]={"EXISTS":True,"bytes":o["ContentLength"],"age_h":round((datetime.now(timezone.utc)-o["LastModified"]).total_seconds()/3600,1)}
    except Exception as e: out["s3_"+k]={"EXISTS":False,"err":str(e)[:40]}
# PROOF 3: pages serve HTTP 200 with the real content? (via probe lambda from AWS)
code=r'''
import json,urllib.request
def lambda_handler(e,c):
    res={}
    for p,marker in [("/eu-dump-radar.html","EU Dump"),("/ecb-history.html","ECB"),("/engine.html?e=ecb-derived","Engine")]:
        u="https://justhodl.ai"+p+("&t=9" if "?" in p else "?t=9")
        try:
            h=urllib.request.urlopen(urllib.request.Request(u,headers={"User-Agent":"Mozilla/5.0"}),timeout=15).read().decode()
            res[p]={"status":200,"has_marker":marker in h,"bytes":len(h)}
        except urllib.error.HTTPError as ex: res[p]={"status":ex.code}
        except Exception as ex: res[p]=str(ex)[:40]
    return res
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
    out["PAGES_LIVE"]=json.loads(lam.invoke(FunctionName=FN,InvocationType="RequestResponse",Payload=b"{}")["Payload"].read())
    lam.delete_function(FunctionName=FN)
except Exception as e: out["page_err"]=str(e)[:80]
open("aws/ops/reports/1504_proof.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
