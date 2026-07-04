"""ops 2821 — provision BLS/BEA/Census keys (from injected secrets) onto their
agents + SSM, ensure daily schedules, seed, verify S3 output. Never prints keys."""
import os, json, time
from datetime import datetime, timezone
import boto3
from botocore.config import Config
REGION="us-east-1"; ACCT="857687956942"
lam=boto3.client("lambda",region_name=REGION,config=Config(read_timeout=300,retries={"max_attempts":0}))
events=boto3.client("events",region_name=REGION); s3=boto3.client("s3",region_name=REGION); ssm=boto3.client("ssm",region_name=REGION)
R={"ops":2821,"ts":datetime.now(timezone.utc).isoformat(),"agents":{}}
AGENTS={
 "bls-labor-agent":     ("BLS_API_KEY","/justhodl/bls-api-key","data/bls-labor.json","bls-labor-agent-daily","cron(0 14 * * ? *)"),
 "bea-economic-agent":  ("BEA_API_KEY","/justhodl/bea-api-key","data/bea-economic.json","bea-economic-agent-daily","cron(15 14 * * ? *)"),
 "census-economic-agent":("CENSUS_API_KEY","/justhodl/census-api-key","data/census-economic.json","census-economic-agent-daily","cron(30 14 * * ? *)"),
}
def wait_ready(fn,t=40):
    for _ in range(t):
        try:
            c=lam.get_function_configuration(FunctionName=fn)
            if c.get("State")=="Active" and c.get("LastUpdateStatus")=="Successful": return True
        except Exception: pass
        time.sleep(3)
    return False
for fn,(kenv,ssm_p,out_key,rule,cron) in AGENTS.items():
    a={}
    key=os.environ.get(kenv,"").strip()
    a["key_present"]=bool(key)
    try:
        if not key: a["status"]="NO KEY IN ENV"; R["agents"][fn]=a; continue
        # 1) key -> Lambda env (merge) + SSM
        cur=lam.get_function_configuration(FunctionName=fn).get("Environment",{}).get("Variables",{})
        cur[kenv]=key
        wait_ready(fn); lam.update_function_configuration(FunctionName=fn,Environment={"Variables":cur}); wait_ready(fn)
        ssm.put_parameter(Name=ssm_p,Value=key,Type="SecureString",Overwrite=True)
        a["key_wired"]=True
        # 2) ensure schedule
        events.put_rule(Name=rule,ScheduleExpression=cron,State="ENABLED",Description=fn+" daily")
        arn=lam.get_function(FunctionName=fn)["Configuration"]["FunctionArn"]
        try: lam.add_permission(FunctionName=fn,StatementId=rule+"-inv",Action="lambda:InvokeFunction",Principal="events.amazonaws.com",SourceArn="arn:aws:events:%s:%s:rule/%s"%(REGION,ACCT,rule))
        except lam.exceptions.ResourceConflictException: pass
        events.put_targets(Rule=rule,Targets=[{"Id":"1","Arn":arn}])
        a["scheduled"]=True
        # 3) seed + verify
        lam.invoke(FunctionName=fn,InvocationType="RequestResponse")["Payload"].read(); time.sleep(2)
        d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key=out_key)["Body"].read())
        a["out_generated_at"]=d.get("generated_at")
        a["series_live"]=d.get("_series_live") or d.get("_blocks_live")
        a["summary"]=d.get("summary") or {k:v for k,v in d.items() if k in ("gdp","pce_inflation","income")}
        a["errors"]=d.get("_error")
        a["status"]="LIVE" if (a["series_live"] or 0)>=1 else "CHECK (0 series)"
    except Exception as e:
        a["status"]="ERR"; a["error"]=repr(e)[:160]
    R["agents"][fn]=a
ok=sum(1 for a in R["agents"].values() if a.get("status")=="LIVE")
R["status"]="%d/3 gov-data agents LIVE"%ok
print(json.dumps(R,indent=1,default=str))
os.makedirs("aws/ops/reports",exist_ok=True)
json.dump(R,open("aws/ops/reports/2821_gov_data.json","w"),indent=1,default=str)
print("OPS 2821 COMPLETE")
