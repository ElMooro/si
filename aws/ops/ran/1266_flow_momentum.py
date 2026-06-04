"""1266 — deploy capital-flow, re-run dislocation (momentum overlay), verify both."""
import json, os, time, zipfile, io
from datetime import datetime, timezone
import boto3
from botocore.config import Config
REPORT="aws/ops/reports/1266_flow_momentum.json"; BUCKET="justhodl-dashboard-live"
LAMBDA="justhodl-capital-flow"; SRC="aws/lambdas/justhodl-capital-flow/source"
ROLE="arn:aws:iam::857687956942:role/lambda-execution-role"; ACC="857687956942"; REGION="us-east-1"
RULE="justhodl-capital-flow-daily"; SCHED="cron(30 16 * * ? *)"
cfg=Config(read_timeout=420,retries={"max_attempts":1})
lam=boto3.client("lambda",region_name=REGION,config=cfg); s3=boto3.client("s3",region_name=REGION,config=cfg)
events=boto3.client("events",region_name=REGION,config=cfg)
out={"started":datetime.now(timezone.utc).isoformat()}

def zipit():
    buf=io.BytesIO()
    with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as zf:
        for r,_,fs in os.walk(SRC):
            for f in fs:
                if f.endswith(".pyc") or "__pycache__" in r: continue
                fp=os.path.join(r,f); zf.write(fp,arcname=os.path.relpath(fp,SRC))
    return buf.getvalue()

# deploy capital-flow
try:
    zb=zipit()
    try: lam.get_function_configuration(FunctionName=LAMBDA); lam.update_function_code(FunctionName=LAMBDA,ZipFile=zb); act="updated"
    except lam.exceptions.ResourceNotFoundException:
        lam.create_function(FunctionName=LAMBDA,Runtime="python3.12",Role=ROLE,Handler="lambda_function.lambda_handler",
            Code={"ZipFile":zb},Description="Capital flow tracker",Timeout=180,MemorySize=512,Architectures=["x86_64"],Publish=False); act="created"
    for _ in range(30):
        time.sleep(2); c=lam.get_function_configuration(FunctionName=LAMBDA)
        if c.get("State")=="Active" and c.get("LastUpdateStatus") in ("Successful",None): break
    out["cf_deploy"]=act
except Exception as e: out["cf_deploy_err"]=str(e)[:300]
try:
    events.put_rule(Name=RULE,ScheduleExpression=SCHED,State="ENABLED",Description="Daily capital flow")
    fn=lam.get_function(FunctionName=LAMBDA); events.put_targets(Rule=RULE,Targets=[{"Id":"1","Arn":fn["Configuration"]["FunctionArn"]}])
    try: lam.add_permission(FunctionName=LAMBDA,StatementId=f"EB-{RULE}",Action="lambda:InvokeFunction",Principal="events.amazonaws.com",SourceArn=f"arn:aws:events:{REGION}:{ACC}:rule/{RULE}")
    except lam.exceptions.ResourceConflictException: pass
except Exception as e: out["cf_sched_err"]=str(e)[:200]
# invoke capital-flow
try:
    r=lam.invoke(FunctionName=LAMBDA,InvocationType="RequestResponse",Payload=b"{}")
    out["cf_invoke"]=r.get("Payload").read().decode()[:250]
except Exception as e: out["cf_invoke"]=str(e)[:200]
time.sleep(2)
try:
    cf=json.loads(s3.get_object(Bucket=BUCKET,Key="data/capital-flow.json")["Body"].read())
    out["capital_flow"]={"sources":cf.get("sources"),"n_accumulating":len(cf.get("accumulating",[])),
        "n_distributing":len(cf.get("distributing",[])),"n_etf_in":len(cf.get("etf_flows_in",[])),
        "top_accum":[{"t":x["ticker"],"score":x["flow_score"],"lenses":x["lenses"]} for x in cf.get("accumulating",[])[:8]],
        "etf_in":[{"t":e["ticker"],"flow":e.get("net_flow")} for e in cf.get("etf_flows_in",[])[:6]]}
except Exception as e: out["capital_flow"]={"error":str(e)[:200]}

# re-run dislocation detector (momentum overlay) — wait for its deploy first
time.sleep(60)
try:
    r=lam.invoke(FunctionName="justhodl-dislocation-detector",InvocationType="RequestResponse",Payload=b"{}")
    out["disl_invoke"]=r.get("Payload").read().decode()[:200]
except Exception as e: out["disl_invoke"]=str(e)[:200]
time.sleep(3)
try:
    d=json.loads(s3.get_object(Bucket=BUCKET,Key="data/dislocations.json")["Body"].read())
    ci=d.get("cheap_and_inflecting",[])
    out["dislocation"]={"n_cheap_inflecting":len(ci),
        "sample":[{"t":x["ticker"],"score":x["dislocation_score"],"mom":(x.get("momentum") or {}).get("ret_20d"),
                    "inflecting":x.get("cheap_and_inflecting")} for x in ci[:8]]}
except Exception as e: out["dislocation"]={"error":str(e)[:200]}
out["finished"]=datetime.now(timezone.utc).isoformat()
open(REPORT,"w").write(json.dumps(out,indent=2,default=str)); print(json.dumps(out,indent=2,default=str)[:1400])
