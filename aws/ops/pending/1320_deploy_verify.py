import json, os, time, zipfile, io
import boto3
from botocore.config import Config
cfg=Config(read_timeout=300,retries={"max_attempts":1})
lam=boto3.client("lambda",region_name="us-east-1",config=cfg); s3=boto3.client("s3",region_name="us-east-1",config=cfg)
events=boto3.client("events",region_name="us-east-1",config=cfg)
ROLE="arn:aws:iam::857687956942:role/lambda-execution-role"; ACC="857687956942"
out={}
def zd(src):
    buf=io.BytesIO()
    with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as zf:
        for r,_,fs in os.walk(src):
            for f in fs:
                if "__pycache__" in r or f.endswith(".pyc"): continue
                fp=os.path.join(r,f); zf.write(fp,arcname=os.path.relpath(fp,src))
    return buf.getvalue()
# deploy crypto-cycle-risk (new)
try:
    zb=zd("aws/lambdas/justhodl-crypto-cycle-risk/source")
    try: lam.get_function_configuration(FunctionName="justhodl-crypto-cycle-risk"); lam.update_function_code(FunctionName="justhodl-crypto-cycle-risk",ZipFile=zb); out["crypto_deploy"]="updated"
    except lam.exceptions.ResourceNotFoundException:
        lam.create_function(FunctionName="justhodl-crypto-cycle-risk",Runtime="python3.12",Role=ROLE,Handler="lambda_function.lambda_handler",Code={"ZipFile":zb},Timeout=60,MemorySize=256,Architectures=["x86_64"]); out["crypto_deploy"]="created"
    for _ in range(25):
        time.sleep(2); c=lam.get_function_configuration(FunctionName="justhodl-crypto-cycle-risk")
        if c.get("State")=="Active" and c.get("LastUpdateStatus") in ("Successful",None): break
    rule="justhodl-crypto-cycle-risk-6h"; events.put_rule(Name=rule,ScheduleExpression="rate(6 hours)",State="ENABLED")
    fn=lam.get_function(FunctionName="justhodl-crypto-cycle-risk"); events.put_targets(Rule=rule,Targets=[{"Id":"1","Arn":fn["Configuration"]["FunctionArn"]}])
    try: lam.add_permission(FunctionName="justhodl-crypto-cycle-risk",StatementId=f"EB-{rule}",Action="lambda:InvokeFunction",Principal="events.amazonaws.com",SourceArn=f"arn:aws:events:us-east-1:{ACC}:rule/{rule}")
    except lam.exceptions.ResourceConflictException: pass
except Exception as e: out["crypto_deploy"]="ERR:"+str(e)[:150]
# redeploy best-setups (extended calibration)
try:
    lam.update_function_code(FunctionName="justhodl-best-setups",ZipFile=zd("aws/lambdas/justhodl-best-setups/source"))
    for _ in range(20):
        time.sleep(2); c=lam.get_function_configuration(FunctionName="justhodl-best-setups")
        if c.get("LastUpdateStatus") in ("Successful",None): break
    out["bestsetups_deploy"]="updated"
except Exception as e: out["bestsetups_deploy"]="ERR:"+str(e)[:100]
# invoke crypto-cycle-risk + read
try:
    lam.invoke(FunctionName="justhodl-crypto-cycle-risk",InvocationType="RequestResponse",Payload=b"{}"); time.sleep(2)
    d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/crypto-cycle-risk.json")["Body"].read())
    out["crypto"]={"score":d.get("dump_risk_score"),"level":d.get("risk_level"),
        "drivers":[{"f":x["factor"],"risk":x["risk"]} for x in d.get("top_drivers",[])],
        "halving_mo":d.get("factors",{}).get("halving_cycle",{}).get("months_since_halving")}
except Exception as e: out["crypto"]=str(e)[:150]
open("aws/ops/reports/1320_verify.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
