import io,json,zipfile,os,time,boto3
from botocore.config import Config
REGION="us-east-1"; FN="justhodl-hkma-monitor"; ROLE="arn:aws:iam::857687956942:role/lambda-execution-role"
lam=boto3.client("lambda",region_name=REGION,config=Config(read_timeout=300,retries={"max_attempts":0}))
ev=boto3.client("events",region_name=REGION); s3=boto3.client("s3",region_name=REGION); B="justhodl-dashboard-live"
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z:
    added=set(); src="aws/lambdas/%s/source"%FN
    for root,_,files in os.walk(src):
        for f in files:
            if f.endswith(".pyc"):continue
            p=os.path.join(root,f);arc=os.path.relpath(p,src);z.write(p,arc);added.add(arc)
    for f in os.listdir("aws/shared"):
        if f.endswith(".py") and f not in added: z.write(os.path.join("aws/shared",f),f)
zb=buf.getvalue(); print("zip",len(zb))
try:
    lam.get_function(FunctionName=FN); ex=True
except lam.exceptions.ResourceNotFoundException: ex=False
if ex:
    lam.update_function_code(FunctionName=FN,ZipFile=zb); lam.get_waiter("function_updated").wait(FunctionName=FN); print("UPDATED")
else:
    lam.create_function(FunctionName=FN,Runtime="python3.12",Role=ROLE,Handler="lambda_function.lambda_handler",
        Code={"ZipFile":zb},Timeout=300,MemorySize=512,Architectures=["x86_64"],
        Environment={"Variables":{"S3_BUCKET":"justhodl-dashboard-live"}},
        Description="HKMA funding monitor → data/hkma.json"); print("CREATED")
lam.get_waiter("function_active_v2").wait(FunctionName=FN)
try:
    rule=ev.put_rule(Name="hkma-monitor-daily",ScheduleExpression="cron(0 9 * * ? *)",State="ENABLED",Description="HKMA daily 09:00 UTC")["RuleArn"]
    arn=lam.get_function(FunctionName=FN)["Configuration"]["FunctionArn"]
    try: lam.add_permission(FunctionName=FN,StatementId="hkma-daily-evt",Action="lambda:InvokeFunction",Principal="events.amazonaws.com",SourceArn=rule)
    except lam.exceptions.ResourceConflictException: pass
    ev.put_targets(Rule="hkma-monitor-daily",Targets=[{"Id":"1","Arn":arn}]); print("scheduled")
except Exception as e: print("sched err",e)
r=lam.invoke(FunctionName=FN,InvocationType="RequestResponse"); print("invoke:",r["Payload"].read().decode()[:220])
d=json.loads(s3.get_object(Bucket=B,Key="data/hkma.json")["Body"].read())
print("\nhk_funding:",d["hk_funding"],"| reds:",d["red_flags"],"| yellows:",d["yellow_flags"])
ab=d["aggregate_balance"]; print("Aggregate Balance: %s HK$bn (pctile %s, 30d %s, n=%s from %s)"%(ab["latest_bn"],ab["pctile"],ab["trend_30d_bn"],ab["n_history"],ab["window_from"]))
print("HIBOR:",d["hibor"]["overnight"],"O/N |",d["hibor"]["1m"],"1M |",d["hibor"]["3m"],"3M | curve keys:",list(d["hibor"]["curve"].keys()))
print("USD/HKD:",d["usd_hkd"]["spot"],"dist_to_weak%",d["usd_hkd"]["distance_to_weak_pct"],"| HIBOR-SOFR:",d["hibor_sofr_bp"],"bp | base_rate:",d["base_rate"])
for m in d["metrics"]: print("  ",m["status"],m["label"],"=",m["value"],m["unit"])
