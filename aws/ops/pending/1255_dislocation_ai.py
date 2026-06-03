"""1255 — deploy dislocation-ai + invoke + verify AI output."""
import json, os, time, zipfile, io
from datetime import datetime, timezone
import boto3
from botocore.config import Config
REPORT="aws/ops/reports/1255_dislocation_ai.json"
BUCKET="justhodl-dashboard-live"; LAMBDA="justhodl-dislocation-ai"
SRC="aws/lambdas/justhodl-dislocation-ai/source"; ROLE="arn:aws:iam::857687956942:role/lambda-execution-role"
ACC="857687956942"; REGION="us-east-1"; RULE="justhodl-dislocation-ai-daily"; SCHED="cron(15 15 * * ? *)"
cfg=Config(read_timeout=620,retries={"max_attempts":1})
lam=boto3.client("lambda",region_name=REGION,config=cfg); s3=boto3.client("s3",region_name=REGION,config=cfg)
events=boto3.client("events",region_name=REGION,config=cfg)
out={"started":datetime.now(timezone.utc).isoformat()}
def zipit():
    buf=io.BytesIO()
    with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as zf:
        for root,_,files in os.walk(SRC):
            for f in files:
                if f.endswith(".pyc") or "__pycache__" in root: continue
                fp=os.path.join(root,f); zf.write(fp,arcname=os.path.relpath(fp,SRC))
    return buf.getvalue()
print(f"[1255] deploy {LAMBDA}")
try:
    zb=zipit()
    try:
        lam.get_function_configuration(FunctionName=LAMBDA); lam.update_function_code(FunctionName=LAMBDA,ZipFile=zb); action="updated"
    except lam.exceptions.ResourceNotFoundException:
        lam.create_function(FunctionName=LAMBDA,Runtime="python3.12",Role=ROLE,Handler="lambda_function.lambda_handler",
            Code={"ZipFile":zb},Description="Dislocation AI analyst",Timeout=600,MemorySize=512,Architectures=["x86_64"],
            Environment={"Variables":{"FMP_KEY":"wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"}},Publish=False); action="created"
    for _ in range(30):
        time.sleep(2); c=lam.get_function_configuration(FunctionName=LAMBDA)
        if c.get("State")=="Active" and c.get("LastUpdateStatus") in ("Successful",None): break
    out["deploy"]={"action":action}; print(f"  ✓ {action}")
except Exception as e: out["deploy_err"]=str(e)[:300]
try:
    events.put_rule(Name=RULE,ScheduleExpression=SCHED,State="ENABLED",Description="Daily dislocation AI")
    fn=lam.get_function(FunctionName=LAMBDA); events.put_targets(Rule=RULE,Targets=[{"Id":"1","Arn":fn["Configuration"]["FunctionArn"]}])
    try: lam.add_permission(FunctionName=LAMBDA,StatementId=f"EB-{RULE}",Action="lambda:InvokeFunction",Principal="events.amazonaws.com",SourceArn=f"arn:aws:events:{REGION}:{ACC}:rule/{RULE}")
    except lam.exceptions.ResourceConflictException: pass
    out["schedule"]=SCHED
except Exception as e: out["schedule_err"]=str(e)[:200]
print("[1255] invoke (Claude on top dislocations — may take minutes)")
try:
    t0=time.time()
    r=lam.invoke(FunctionName=LAMBDA,InvocationType="RequestResponse",Payload=b"{}")
    out["invoke"]={"status":r.get("StatusCode"),"elapsed_s":round(time.time()-t0,1),"fe":r.get("FunctionError"),"body":r.get("Payload").read().decode()[:300]}
    print(f"  {out['invoke']['status']} {out['invoke']['elapsed_s']}s {out['invoke']['body'][:150]}")
except Exception as e: out["invoke"]={"error":str(e)[:300]}
print("[1255] verify dislocation-ai.json")
try:
    doc=json.loads(s3.get_object(Bucket=BUCKET,Key="data/dislocation-ai.json")["Body"].read())
    bt=doc.get("by_ticker",{})
    out["result"]={"analyzed":doc.get("n_analyzed"),"themes":doc.get("themes_ranked"),
        "by_theme_counts":{k:len(v) for k,v in (doc.get("by_theme") or {}).items()},
        "samples":[{"t":tk,"theme":a.get("theme"),"verdict":a.get("cheap_verdict"),
                     "pt":a.get("price_target_12m"),"upside":a.get("pt_upside_pct"),
                     "summary":(a.get("summary") or "")[:160],"backlog":(a.get("backlog_note") or "")[:80]}
                    for tk,a in list(bt.items())[:6]]}
    print(f"  analyzed={doc.get('n_analyzed')} themes={doc.get('themes_ranked')}")
    for tk,a in list(bt.items())[:6]:
        print(f"\n  {tk} [{a.get('theme')}] {a.get('cheap_verdict')} PT=${a.get('price_target_12m')} ({a.get('pt_upside_pct')}%)")
        print(f"    {(a.get('summary') or '')[:180]}")
except Exception as e: out["result"]={"error":str(e)[:300]}
out["finished"]=datetime.now(timezone.utc).isoformat()
open(REPORT,"w").write(json.dumps(out,indent=2,default=str))
print("[1255] DONE")
