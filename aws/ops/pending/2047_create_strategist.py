"""ops 2047: create justhodl-strategist via boto3 (bundling aws/shared/llm_router.py), schedule, invoke, verify."""
import boto3, json, time, io, os, zipfile
REGION="us-east-1"; FN="justhodl-strategist"; B="justhodl-dashboard-live"
ROLE="arn:aws:iam::857687956942:role/lambda-execution-role"
lam=boto3.client("lambda",REGION); events=boto3.client("events",REGION); s3=boto3.client("s3",REGION)
ENV={"Variables":{"S3_BUCKET":B}}
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z:
    src=f"aws/lambdas/{FN}/source"
    for r,_,fs in os.walk(src):
        for f in fs:
            if f.endswith(".py"): p=os.path.join(r,f); z.write(p,os.path.relpath(p,src))
    # bundle shared modules (llm_router + deps) at top level
    for f in os.listdir("aws/shared"):
        if f.endswith(".py"): z.write(os.path.join("aws/shared",f),f)
zb=buf.getvalue()
try: lam.get_function(FunctionName=FN); ex=True
except lam.exceptions.ResourceNotFoundException: ex=False
if ex:
    print("update"); lam.update_function_code(FunctionName=FN,ZipFile=zb)
    for _ in range(30):
        if lam.get_function(FunctionName=FN)["Configuration"].get("LastUpdateStatus")!="InProgress":break
        time.sleep(4)
    lam.update_function_configuration(FunctionName=FN,Environment=ENV,Timeout=600,MemorySize=1024,Runtime="python3.12",Handler="lambda_function.lambda_handler")
else:
    print("create"); lam.create_function(FunctionName=FN,Runtime="python3.12",Role=ROLE,Handler="lambda_function.lambda_handler",
        Code={"ZipFile":zb},Timeout=600,MemorySize=1024,Environment=ENV,Architectures=["x86_64"],Description="The Strategist — whole-fleet interpretation")
for _ in range(40):
    c=lam.get_function(FunctionName=FN)["Configuration"]
    if c.get("State")=="Active" and c.get("LastUpdateStatus")=="Successful":break
    time.sleep(4)
arn=c["FunctionArn"];print("active")
rule="justhodl-strategist-daily"
rarn=events.put_rule(Name=rule,ScheduleExpression="cron(0 14 ? * MON-FRI *)",State="ENABLED",Description="weekday strategist")["RuleArn"]
try: lam.add_permission(FunctionName=FN,StatementId="evt-strat",Action="lambda:InvokeFunction",Principal="events.amazonaws.com",SourceArn=rarn)
except lam.exceptions.ResourceConflictException: pass
events.put_targets(Rule=rule,Targets=[{"Id":"1","Arn":arn}]);print("scheduled")
r=lam.invoke(FunctionName=FN,InvocationType="RequestResponse")
print("invoke:",r["StatusCode"],"|",r["Payload"].read().decode()[:400])
time.sleep(2)
d=json.loads(s3.get_object(Bucket=B,Key="data/strategist.json")["Body"].read())
fl=d["fleet"]
print("\nFLEET: feeds_read",fl["n_feeds_read"],"fresh",fl["n_fresh"],"| consensus",fl["consensus"],
      "| risk-on wt",fl["risk_on_weight"],"vs risk-off",fl["risk_off_weight"],"| +/-/neu",fl["n_positive"],fl["n_negative"],fl["n_neutral"])
print("model:",d.get("model"))
print("\nLOUDEST (top 12):")
for i in d["loudest_engines"][:12]: print(f"  {i['engine']:<26} {str(i['verdict'])[:26]:<26} dir {i['direction']} t{i['trust']} ext{i['extremity']}")
print("\nCONTRADICTIONS:",d["contradictions"][:5])
print("MOST-BACKED:",[x['ticker'] for x in d["most_backed_names"][:10]])
interp=d.get("interpretation") or {}
if interp.get("error"): print("\nINTERP ERROR:",interp.get("error"))
else:
    print("\n=== STRATEGIST READ ===")
    print("DOMINANT DRIVER:",interp.get("dominant_driver"))
    print("MECHANISM:",str(interp.get("mechanism"))[:240])
    print("CONFIRMING:",interp.get("confirming"))
    print("CONTRADICTING:",json.dumps(interp.get("contradicting"))[:300])
    print("SECOND-ORDER:",interp.get("second_order"))
    print("DECISIVE CALL:",interp.get("decisive_call"))
    print("CONVICTION:",interp.get("conviction"))
    print("FALSIFIERS:",interp.get("falsifiers"))
    print("KEY_CLAIMS:",json.dumps(interp.get("key_claims"))[:300])
print("DONE 2047")
