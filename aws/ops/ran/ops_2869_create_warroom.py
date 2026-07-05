import os, io, json, time, zipfile, urllib.request, boto3
from datetime import datetime, timezone
from botocore.config import Config
REGION="us-east-1"; ACCT="857687956942"; FN="justhodl-canary-warroom"
ROLE="arn:aws:iam::%s:role/lambda-execution-role"%ACCT
lam=boto3.client("lambda",region_name=REGION,config=Config(read_timeout=120,retries={"max_attempts":0}))
events=boto3.client("events",region_name=REGION); s3=boto3.client("s3",region_name=REGION)
R={"ops":2869,"ts":datetime.now(timezone.utc).isoformat()}
src=open("aws/lambdas/justhodl-canary-warroom/source/lambda_function.py",encoding="utf-8").read()
buf=io.BytesIO(); z=zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED); z.writestr("lambda_function.py",src); z.close(); code=buf.getvalue()
def ready():
    for _ in range(40):
        try:
            c=lam.get_function_configuration(FunctionName=FN)
            if c.get("State")=="Active" and c.get("LastUpdateStatus")=="Successful": return
        except Exception: pass
        time.sleep(3)
try: lam.get_function(FunctionName=FN); ex=True
except Exception: ex=False
if ex: lam.update_function_code(FunctionName=FN,ZipFile=code); ready(); R["action"]="updated"
else:
    lam.create_function(FunctionName=FN,Runtime="python3.12",Role=ROLE,Handler="lambda_function.lambda_handler",
        Code={"ZipFile":code},Timeout=60,MemorySize=256,Description="Canary War Room aggregator"); ready(); R["action"]="created"
rule="justhodl-canary-warroom-hourly"
events.put_rule(Name=rule,ScheduleExpression="cron(50 * * * ? *)",State="ENABLED",Description="Hourly canary war-room")
try: lam.add_permission(FunctionName=FN,StatementId="warroom-sched",Action="lambda:InvokeFunction",Principal="events.amazonaws.com",SourceArn="arn:aws:events:%s:%s:rule/%s"%(REGION,ACCT,rule))
except Exception: pass
events.put_targets(Rule=rule,Targets=[{"Id":"1","Arn":"arn:aws:lambda:%s:%s:function:%s"%(REGION,ACCT,FN)}])
try: R["invoke"]=json.loads(lam.invoke(FunctionName=FN,InvocationType="RequestResponse")["Payload"].read().decode())
except Exception as e: R["invoke_err"]=str(e)[:150]
time.sleep(2)
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/canary-warroom.json")["Body"].read())
R["master"]=d.get("master")
R["mechanisms"]=[{"key":m.get("key"),"label":m.get("label"),"score":m.get("score"),"band":m.get("band"),"n_firing":m.get("n_firing"),"n_total":m.get("n_total")} for m in d.get("mechanisms",[])]
R["n_firing"]=len(d.get("firing") or []); R["top_firing"]=[{"mech":c.get("mechanism"),"name":c.get("name"),"stress":c.get("stress"),"band":c.get("band")} for c in (d.get("firing") or [])[:8]]
R["divergences"]=[x.get("title") for x in (d.get("divergences") or [])]
R["brain_playbook_n"]=len(d.get("brain_playbook") or []); R["playbook_sample"]=[p.get("text","")[:90] for p in (d.get("brain_playbook") or [])[:3]]
# verify page live (pages deploy may lag)
try:
    req=urllib.request.Request("https://justhodl.ai/canaries.html",headers={"User-Agent":"Mozilla/5.0"})
    h=urllib.request.urlopen(req,timeout=20).read().decode("utf-8","ignore")
    R["page_live"]=("Early-Warning War Room" in h)
except Exception as e: R["page_live"]="pending: "+str(e)[:50]
R["status"]="LIVE" if (d.get("master",{}).get("early_warning_0_100") is not None and len(d.get("mechanisms",[]))==6) else "CHECK"
print(json.dumps(R,ensure_ascii=False,indent=1,default=str)[:2800])
os.makedirs("aws/ops/reports",exist_ok=True); json.dump(R,open("aws/ops/reports/2869_create_warroom.json","w"),ensure_ascii=False,indent=1,default=str)
print("OPS 2869 COMPLETE")
