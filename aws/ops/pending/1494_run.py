import json, os, time, zipfile, io, boto3
from botocore.config import Config
cfg=Config(read_timeout=180,retries={"max_attempts":1})
lam=boto3.client("lambda",region_name="us-east-1",config=cfg); s3=boto3.client("s3",region_name="us-east-1",config=cfg)
ev=boto3.client("events",region_name="us-east-1",config=cfg); ROLE="arn:aws:iam::857687956942:role/lambda-execution-role"; ACC="857687956942"
out={}
buf=io.BytesIO(); src="aws/lambdas/justhodl-ecb-derived/source"
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as zf:
    for r,_,fs in os.walk(src):
        for f in fs:
            if "__pycache__" in r or f.endswith(".pyc"): continue
            zf.write(os.path.join(r,f),arcname=os.path.relpath(os.path.join(r,f),src))
zb=buf.getvalue()
try:
    lam.get_function_configuration(FunctionName="justhodl-ecb-derived"); lam.update_function_code(FunctionName="justhodl-ecb-derived",ZipFile=zb)
except lam.exceptions.ResourceNotFoundException:
    lam.create_function(FunctionName="justhodl-ecb-derived",Runtime="python3.12",Role=ROLE,Handler="lambda_function.lambda_handler",Code={"ZipFile":zb},Timeout=120,MemorySize=256,Architectures=["x86_64"],Environment={"Variables":{"FRED_API_KEY":"2f057499936072679d8843d7fce99989"}})
for _ in range(25):
    time.sleep(2); c=lam.get_function_configuration(FunctionName="justhodl-ecb-derived")
    if c.get("State")=="Active" and c.get("LastUpdateStatus") in ("Successful",None): break
ev.put_rule(Name="justhodl-ecb-derived-daily",ScheduleExpression="cron(40 14 * * ? *)",State="ENABLED")
fn=lam.get_function(FunctionName="justhodl-ecb-derived"); ev.put_targets(Rule="justhodl-ecb-derived-daily",Targets=[{"Id":"1","Arn":fn["Configuration"]["FunctionArn"]}])
try: lam.add_permission(FunctionName="justhodl-ecb-derived",StatementId="EB-ecbderiv",Action="lambda:InvokeFunction",Principal="events.amazonaws.com",SourceArn=f"arn:aws:events:us-east-1:{ACC}:rule/justhodl-ecb-derived-daily")
except Exception: pass
r=lam.invoke(FunctionName="justhodl-ecb-derived",InvocationType="RequestResponse",Payload=b"{}")
out["run"]=r["Payload"].read().decode()[:80]
time.sleep(3)
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/ecb-derived.json")["Body"].read())
out["headline"]=d.get("headline"); out["n_flashing"]=d.get("n_flashing")
out["indicators"]={k:{kk:vv for kk,vv in v.items() if kk in ('signal','ciss_level','delta_30d','ltro_share_pct','mlf_eur_mn','divergence_bn','ecb_balance_sheet_6m_chg_eur_bn','fed_net_liquidity_6m_chg_usd_bn','net_pct_tightening','err')} for k,v in d.get("indicators",{}).items()}
open("aws/ops/reports/1494_r.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
