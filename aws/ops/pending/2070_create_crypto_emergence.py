import boto3, json, time, io, os, zipfile
REGION="us-east-1"; FN="justhodl-crypto-emergence"; B="justhodl-dashboard-live"
ROLE="arn:aws:iam::857687956942:role/lambda-execution-role"
lam=boto3.client("lambda",REGION); events=boto3.client("events",REGION); s3=boto3.client("s3",REGION)
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z:
    for r,_,fs in os.walk(f"aws/lambdas/{FN}/source"):
        for f in fs:
            if f.endswith(".py"): p=os.path.join(r,f); z.write(p,os.path.relpath(p,f"aws/lambdas/{FN}/source"))
zb=buf.getvalue()
try: lam.get_function(FunctionName=FN); ex=True
except lam.exceptions.ResourceNotFoundException: ex=False
ENV={"Variables":{"S3_BUCKET":B}}
if ex:
    lam.update_function_code(FunctionName=FN,ZipFile=zb)
    for _ in range(30):
        if lam.get_function(FunctionName=FN)["Configuration"].get("LastUpdateStatus")!="InProgress":break
        time.sleep(3)
    lam.update_function_configuration(FunctionName=FN,Environment=ENV,Timeout=180,MemorySize=512,Runtime="python3.12",Handler="lambda_function.lambda_handler"); print("updated")
else:
    lam.create_function(FunctionName=FN,Runtime="python3.12",Role=ROLE,Handler="lambda_function.lambda_handler",Code={"ZipFile":zb},Timeout=180,MemorySize=512,Environment=ENV,Architectures=["x86_64"],Description="Crypto emergence"); print("created")
for _ in range(40):
    c=lam.get_function(FunctionName=FN)["Configuration"]
    if c.get("State")=="Active" and c.get("LastUpdateStatus") in ("Successful",None):break
    time.sleep(3)
arn=c["FunctionArn"]; rule="justhodl-crypto-emergence-daily"
rarn=events.put_rule(Name=rule,ScheduleExpression="cron(0 13,23 * * ? *)",State="ENABLED")["RuleArn"]
try: lam.add_permission(FunctionName=FN,StatementId="evt-ce",Action="lambda:InvokeFunction",Principal="events.amazonaws.com",SourceArn=rarn)
except lam.exceptions.ResourceConflictException: pass
events.put_targets(Rule=rule,Targets=[{"Id":"1","Arn":arn}]); print("scheduled")
print("invoke:",lam.invoke(FunctionName=FN,InvocationType="RequestResponse")["Payload"].read().decode()[:220])
time.sleep(2)
d=json.loads(s3.get_object(Bucket=B,Key="data/crypto-emergence.json")["Body"].read())
print("\nCRYPTO COMPLEX:",d["complex_stage"])
print(" ",d["complex_read"])
print(f"  breadth >50d {d['breadth_pct_above_50d']}% | >200d {d['breadth_pct_above_200d']}% | BTC>200d {d['btc_above_200d']} (200d rising {d['btc_200d_rising']}) | ETH/BTC 2m {d['ethbtc_2m_trend_pct']}% | cycle_risk {d['cycle_risk']}")
tr=d["triggers"]
print("  TRIGGER:",tr["early_bull_confirms_when"])
print(f"  BTC {tr['btc_price']:,} vs 200d {tr['btc_200d_level']:,} ({tr['btc_pct_to_200d']:+}%) | alt-season: {tr['alt_season']}")
print(f"\n{'coin':<14}{'sector':<12}{'stage':<11}{'score':>6}  rs/trend  signals")
for o in d["coins"]:
    print(f"  {o['name']:<12}{o['sector']:<12}{o['stage']:<11}{o['emergence_score']:>6}  {o['rs_score']}/{o['trend_score']:<3} {', '.join(o['signals'][:3])}")
print("DONE 2070")
