"""1270 — deploy etf-true-flows; run chain; verify real $ flows + quad threats."""
import json, os, time, zipfile, io
from datetime import datetime, timezone
import boto3
from botocore.config import Config
REPORT="aws/ops/reports/1270_trueflows.json"; BUCKET="justhodl-dashboard-live"
LAMBDA="justhodl-etf-true-flows"; SRC="aws/lambdas/justhodl-etf-true-flows/source"
ROLE="arn:aws:iam::857687956942:role/lambda-execution-role"; ACC="857687956942"; REGION="us-east-1"
RULE="justhodl-etf-true-flows-daily"; SCHED="cron(45 15 * * ? *)"
cfg=Config(read_timeout=400,retries={"max_attempts":1})
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
try:
    zb=zipit()
    try: lam.get_function_configuration(FunctionName=LAMBDA); lam.update_function_code(FunctionName=LAMBDA,ZipFile=zb); act="updated"
    except lam.exceptions.ResourceNotFoundException:
        lam.create_function(FunctionName=LAMBDA,Runtime="python3.12",Role=ROLE,Handler="lambda_function.lambda_handler",
            Code={"ZipFile":zb},Description="True ETF flows",Timeout=240,MemorySize=512,Architectures=["x86_64"],
            Environment={"Variables":{"FMP_KEY":"wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"}},Publish=False); act="created"
    for _ in range(30):
        time.sleep(2); c=lam.get_function_configuration(FunctionName=LAMBDA)
        if c.get("State")=="Active" and c.get("LastUpdateStatus") in ("Successful",None): break
    out["deploy"]=act
except Exception as e: out["deploy_err"]=str(e)[:300]
try:
    events.put_rule(Name=RULE,ScheduleExpression=SCHED,State="ENABLED",Description="Daily true ETF flows")
    fn=lam.get_function(FunctionName=LAMBDA); events.put_targets(Rule=RULE,Targets=[{"Id":"1","Arn":fn["Configuration"]["FunctionArn"]}])
    try: lam.add_permission(FunctionName=LAMBDA,StatementId=f"EB-{RULE}",Action="lambda:InvokeFunction",Principal="events.amazonaws.com",SourceArn=f"arn:aws:events:{REGION}:{ACC}:rule/{RULE}")
    except lam.exceptions.ResourceConflictException: pass
except Exception as e: out["sched_err"]=str(e)[:200]
# run chain
try:
    r=lam.invoke(FunctionName=LAMBDA,InvocationType="RequestResponse",Payload=b"{}")
    out["true_flows_invoke"]=r.get("Payload").read().decode()[:250]
except Exception as e: out["true_flows_invoke"]=str(e)[:200]
time.sleep(2)
try:
    tf=json.loads(s3.get_object(Bucket=BUCKET,Key="data/etf-true-flows.json")["Body"].read())
    out["true_flows"]={"n_etfs":tf.get("n_etfs"),
        "top_inflows":[{"t":e["ticker"],"flow5d":e.get("net_flow_5d_usd"),"aum":e.get("aum_est_b")} for e in tf.get("inflows",[])[:6]],
        "cat":[{"c":c["category"],"flow":c.get("net_flow_5d_usd")} for c in tf.get("category_rotation",[])[:6]]}
except Exception as e: out["true_flows"]={"error":str(e)[:200]}
# capital-flow then best-setups
for fn_name in ["justhodl-capital-flow","justhodl-best-setups"]:
    try:
        r=lam.invoke(FunctionName=fn_name,InvocationType="RequestResponse",Payload=b"{}"); time.sleep(2)
        out[fn_name]=r.get("Payload").read().decode()[:150]
    except Exception as e: out[fn_name]=str(e)[:150]
try:
    bs=json.loads(s3.get_object(Bucket=BUCKET,Key="data/best-setups.json")["Body"].read())
    setups=bs.get("top_setups",[])
    cf_n=sum(1 for s in setups if "CAPITAL_FLOW" in (s.get("signal_keys") or []))
    out["board"]={"quad_threats":len(bs.get("quad_threats",[])),"triple_threats":len(bs.get("triple_threats",[])),
        "capital_flow_signals":cf_n,
        "quad_sample":[{"t":s["ticker"],"conv":s["conviction"],"sigs":s["signal_keys"]} for s in bs.get("quad_threats",[])[:3]]}
except Exception as e: out["board"]={"error":str(e)[:150]}
out["finished"]=datetime.now(timezone.utc).isoformat()
open(REPORT,"w").write(json.dumps(out,indent=2,default=str)); print(json.dumps(out,indent=2,default=str)[:1400])
