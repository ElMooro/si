import boto3, json, io, zipfile, time, os
lam=boto3.client("lambda","us-east-1"); events=boto3.client("events","us-east-1")
cfg=json.load(open("aws/lambdas/justhodl-crypto-options/config.json"))
src="aws/lambdas/justhodl-crypto-options/source/lambda_function.py"
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z:
    z.write(src,"lambda_function.py")
    if os.path.isdir("aws/shared"):
        for f in os.listdir("aws/shared"):
            if f.endswith(".py"): z.write(os.path.join("aws/shared",f),f)
zb=buf.getvalue(); fn=cfg["function_name"]
try:
    lam.get_function(FunctionName=fn); ex=True
except Exception: ex=False
if ex:
    lam.update_function_code(FunctionName=fn,ZipFile=zb); print("updated existing code")
else:
    r=lam.create_function(FunctionName=fn,Runtime=cfg["runtime"],Role=cfg["role"],
        Handler=cfg["handler"],Code={"ZipFile":zb},Timeout=cfg["timeout"],MemorySize=cfg["memory"],
        Architectures=cfg.get("architectures",["x86_64"]),
        Environment={"Variables":cfg.get("environment",{})},Description=cfg["description"][:255])
    print("CREATED:",r["FunctionArn"])
time.sleep(10)
sch=cfg.get("schedule") or {}
if sch:
    rule=sch["rule_name"]
    events.put_rule(Name=rule,ScheduleExpression=sch["cron"],State="ENABLED",Description=sch.get("description","")[:255])
    arn=lam.get_function(FunctionName=fn)["Configuration"]["FunctionArn"]
    try:
        lam.add_permission(FunctionName=fn,StatementId=rule+"-perm",Action="lambda:InvokeFunction",
            Principal="events.amazonaws.com",SourceArn="arn:aws:events:us-east-1:857687956942:rule/"+rule)
    except Exception as e: print("perm note:",str(e)[:50])
    events.put_targets(Rule=rule,Targets=[{"Id":"1","Arn":arn}])
    print("scheduled:",rule,"|",sch["cron"])
time.sleep(3)
inv=lam.invoke(FunctionName=fn,InvocationType="RequestResponse",Payload=b"{}")
print("invoke err:",inv.get("FunctionError"),"| resp:",inv["Payload"].read().decode()[:170])
print("DONE 2382")
