import json, os, time, zipfile, io
from datetime import datetime, timezone
import boto3
from botocore.config import Config
REPORT="aws/ops/reports/1291_nextlevel.json"; BUCKET="justhodl-dashboard-live"; REGION="us-east-1"
ROLE="arn:aws:iam::857687956942:role/lambda-execution-role"
cfg=Config(read_timeout=120,retries={"max_attempts":1})
lam=boto3.client("lambda",region_name=REGION,config=cfg); s3=boto3.client("s3",region_name=REGION,config=cfg)
out={}
def zipdir(src):
    buf=io.BytesIO()
    with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as zf:
        for r,_,fs in os.walk(src):
            for f in fs:
                if f.endswith(".pyc") or "__pycache__" in r: continue
                fp=os.path.join(r,f); zf.write(fp,arcname=os.path.relpath(fp,src))
    return buf.getvalue()
try:
    zb=zipdir("aws/lambdas/justhodl-ask/source")
    try: lam.get_function_configuration(FunctionName="justhodl-ask"); lam.update_function_code(FunctionName="justhodl-ask",ZipFile=zb); out["ask_deploy"]="updated"
    except lam.exceptions.ResourceNotFoundException:
        lam.create_function(FunctionName="justhodl-ask",Runtime="python3.12",Role=ROLE,Handler="lambda_function.lambda_handler",Code={"ZipFile":zb},Description="NLQ",Timeout=60,MemorySize=512,Architectures=["x86_64"]); out["ask_deploy"]="created"
    for _ in range(30):
        time.sleep(2); c=lam.get_function_configuration(FunctionName="justhodl-ask")
        if c.get("State")=="Active" and c.get("LastUpdateStatus") in ("Successful",None): break
    try:
        u=lam.create_function_url_config(FunctionName="justhodl-ask",AuthType="NONE",Cors={"AllowOrigins":["*"],"AllowMethods":["*"],"AllowHeaders":["*"]}); out["ask_url"]=u["FunctionUrl"]
    except lam.exceptions.ResourceConflictException:
        out["ask_url"]=lam.get_function_url_config(FunctionName="justhodl-ask")["FunctionUrl"]
    try: lam.add_permission(FunctionName="justhodl-ask",StatementId="fnurl",Action="lambda:InvokeFunctionUrl",Principal="*",FunctionUrlAuthType="NONE")
    except lam.exceptions.ResourceConflictException: pass
except Exception as e: out["ask_err"]=str(e)[:300]
try:
    zb=zipdir("aws/lambdas/justhodl-best-setups/source"); lam.update_function_code(FunctionName="justhodl-best-setups",ZipFile=zb)
    for _ in range(20):
        time.sleep(2); c=lam.get_function_configuration(FunctionName="justhodl-best-setups")
        if c.get("LastUpdateStatus") in ("Successful",None): break
    lam.invoke(FunctionName="justhodl-best-setups",InvocationType="RequestResponse",Payload=b"{}"); time.sleep(2)
    bs=json.loads(s3.get_object(Bucket=BUCKET,Key="data/best-setups.json")["Body"].read())
    out["best_setups_why"]=(bs.get("top_setups") or [{}])[0].get("why")
except Exception as e: out["bs_err"]=str(e)[:200]
try:
    r=lam.invoke(FunctionName="justhodl-ask",InvocationType="RequestResponse",Payload=json.dumps({"q":"what are today's highest conviction setups?"}).encode())
    body=json.loads(r.get("Payload").read().decode())
    inner=json.loads(body.get("body","{}")) if isinstance(body,dict) and body.get("body") else body
    out["ask_test"]={"answer":(inner.get("answer") or "")[:220],"n_results":len(inner.get("results",[]))}
except Exception as e: out["ask_test"]=str(e)[:200]
try:
    idx=json.loads(s3.get_object(Bucket=BUCKET,Key="data/vintage/_index.json")["Body"].read())
    out["vintage_n_series"]=idx.get("n_series")
    if idx.get("series"):
        sid=idx["series"][0]; v=json.loads(s3.get_object(Bucket=BUCKET,Key=f"data/vintage/{sid}.json")["Body"].read())
        out["vintage_sample"]={"series":sid,"n_vintages":len(v.get("vintages",[]))}
except Exception as e: out["vintage_err"]=str(e)[:150]
open(REPORT,"w").write(json.dumps(out,indent=2,default=str)); print("done")
