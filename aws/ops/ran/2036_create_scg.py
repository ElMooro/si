"""ops 2036: create justhodl-supply-chain-graph via boto3 (deploy-lambdas didn't), schedule, invoke, verify."""
import boto3, json, time, io, os, zipfile
REGION="us-east-1"; FN="justhodl-supply-chain-graph"; B="justhodl-dashboard-live"
ROLE="arn:aws:iam::857687956942:role/lambda-execution-role"
lam=boto3.client("lambda",REGION); events=boto3.client("events",REGION); s3=boto3.client("s3",REGION); ddb=boto3.client("dynamodb",REGION)
ENV={"Variables":{"POLYGON_KEY":"zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d","S3_BUCKET":B}}
SRC=f"aws/lambdas/{FN}/source"; buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z:
    for r,_,fs in os.walk(SRC):
        for f in fs:
            if f.endswith(".py"): p=os.path.join(r,f); z.write(p,os.path.relpath(p,SRC))
zb=buf.getvalue()
try: lam.get_function(FunctionName=FN); ex_=True
except lam.exceptions.ResourceNotFoundException: ex_=False
if ex_:
    print("update"); lam.update_function_code(FunctionName=FN,ZipFile=zb)
    for _ in range(24):
        if lam.get_function(FunctionName=FN)["Configuration"].get("LastUpdateStatus")!="InProgress":break
        time.sleep(4)
    lam.update_function_configuration(FunctionName=FN,Environment=ENV,Timeout=180,MemorySize=512,Runtime="python3.12",Handler="lambda_function.lambda_handler")
else:
    print("create"); lam.create_function(FunctionName=FN,Runtime="python3.12",Role=ROLE,Handler="lambda_function.lambda_handler",
        Code={"ZipFile":zb},Timeout=180,MemorySize=512,Environment=ENV,Architectures=["x86_64"],Description="Supply-chain graph + supplier-laggard alpha")
for _ in range(40):
    c=lam.get_function(FunctionName=FN)["Configuration"]
    if c.get("State")=="Active" and c.get("LastUpdateStatus")=="Successful":break
    time.sleep(4)
arn=c["FunctionArn"];print("active")
rule="justhodl-supply-chain-graph-daily"
rarn=events.put_rule(Name=rule,ScheduleExpression="cron(45 13 ? * TUE-SAT *)",State="ENABLED",Description="daily scg")["RuleArn"]
try: lam.add_permission(FunctionName=FN,StatementId="evt-scg",Action="lambda:InvokeFunction",Principal="events.amazonaws.com",SourceArn=rarn)
except lam.exceptions.ResourceConflictException: pass
events.put_targets(Rule=rule,Targets=[{"Id":"1","Arn":arn}]);print("scheduled")
r=lam.invoke(FunctionName=FN,InvocationType="RequestResponse")
print("invoke:",r["StatusCode"],"|",r["Payload"].read().decode()[:500])
time.sleep(2)
d=json.loads(s3.get_object(Bucket=B,Key="data/supply-chain-graph.json")["Body"].read())
print("\nnodes:",d.get("n_nodes"),"edges:",d.get("n_edges"),"booming_hubs:",d.get("booming_hubs"))
print("LAGGARDS (suppliers of booming hubs, not yet moved):")
for l in (d.get("supply_chain_laggards") or [])[:12]:
    print(f"  {l['ticker']:<6} ({l['theme']}) {l['relationship']}→{l['supplies_to']} | own {l['own_perf_30d']}% vs cust {l['customer_perf_30d']}% gap {l['lag_gap_pct']}")
rr=lam.invoke(FunctionName="justhodl-signal-harvester",InvocationType="RequestResponse"); print("harvester:",rr["Payload"].read().decode()[:120])
time.sleep(3)
n=0
for pg in ddb.get_paginator("scan").paginate(TableName="justhodl-signals",FilterExpression="signal_type=:t",
        ExpressionAttributeValues={":t":{"S":"eng:supply-chain-graph"}},ProjectionExpression="signal_id",Limit=100):
    n+=len(pg.get("Items",[]))
    if n>=100:break
print("eng:supply-chain-graph rows:",n)
print("DONE 2036")
