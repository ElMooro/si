import boto3, json, zipfile, io, glob, time
from botocore.exceptions import ClientError
lam=boto3.client("lambda","us-east-1"); s3=boto3.client("s3","us-east-1"); events=boto3.client("events","us-east-1")
B="justhodl-dashboard-live"; REGION="us-east-1"; ACCT="857687956942"
ROLE="arn:aws:iam::857687956942:role/lambda-execution-role"
SPECS=[("justhodl-finnhub-signals","finnhub-signals-daily","cron(0 12 * * ? *)",300,256,"data/finnhub-signals.json"),
       ("justhodl-gdelt-buzz","gdelt-buzz-daily","cron(45 12 * * ? *)",180,256,"data/gdelt-buzz.json"),
       ("justhodl-stocktwits","stocktwits-daily","cron(15 12 * * ? *)",240,256,"data/stocktwits.json")]
def deploy(FN,RULE,CRON,TO,MEM):
    src=open(glob.glob("**/%s/source/lambda_function.py"%FN,recursive=True)[0]).read()
    buf=io.BytesIO()
    with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z: z.writestr("lambda_function.py",src)
    code=buf.getvalue()
    try:
        lam.create_function(FunctionName=FN,Runtime="python3.12",Role=ROLE,Handler="lambda_function.lambda_handler",
            Code={"ZipFile":code},Timeout=TO,MemorySize=MEM,Architectures=["x86_64"],Description=FN)
        print("CREATED",FN)
    except ClientError as e:
        if "already exist" in str(e).lower() or "ResourceConflict" in str(e):
            for _ in range(24):
                try: lam.update_function_code(FunctionName=FN,ZipFile=code); print("UPDATED",FN); break
                except ClientError as e2:
                    if "ResourceConflict" in str(e2): time.sleep(5); continue
                    raise
        else: raise
    for _ in range(50):
        st=lam.get_function_configuration(FunctionName=FN)
        if st.get("State")=="Active" and st.get("LastUpdateStatus")!="InProgress": break
        time.sleep(3)
    events.put_rule(Name=RULE,ScheduleExpression=CRON,State="ENABLED",Description=RULE)
    arn=lam.get_function(FunctionName=FN)["Configuration"]["FunctionArn"]
    try:
        lam.add_permission(FunctionName=FN,StatementId=RULE+"-evt",Action="lambda:InvokeFunction",
            Principal="events.amazonaws.com",SourceArn="arn:aws:events:%s:%s:rule/%s"%(REGION,ACCT,RULE))
    except ClientError as e:
        if "ResourceConflict" not in str(e): print("perm:",str(e)[:40])
    events.put_targets(Rule=RULE,Targets=[{"Id":"1","Arn":arn}])
for FN,RULE,CRON,TO,MEM,OUT in SPECS: deploy(FN,RULE,CRON,TO,MEM)
print("all created + scheduled\n")
for FN,RULE,CRON,TO,MEM,OUT in SPECS:
    r=lam.invoke(FunctionName=FN,InvocationType="RequestResponse"); print(FN,"->",r["Payload"].read().decode()[:150])
time.sleep(2)
fh=json.loads(s3.get_object(Bucket=B,Key="data/finnhub-signals.json")["Body"].read())
print("\nFINNHUB top accumulation:")
for r in (fh.get("summary",{}) or {}).get("top_accumulation",[])[:8]:
    print("   %-6s acc=%-6s mspr=%-6s rec_mom=%-6s surp=%-5s | %s"%(r["symbol"],r["accumulation_score"],r["mspr"],r["rec_momentum"],r["last_surprise_pct"],(r["why"] or "")[:40]))
gd=json.loads(s3.get_object(Bucket=B,Key="data/gdelt-buzz.json")["Body"].read())
print("\nGDELT themes (accel%):", [(t["theme"][:22],t.get("accel_pct"),t.get("status")) for t in gd.get("themes",[])][:10])
st=json.loads(s3.get_object(Bucket=B,Key="data/stocktwits.json")["Body"].read())
print("\nSTOCKTWITS trending:",st.get("trending_equities",[])[:12])
print("top bullish buzz:",[(b["symbol"],b["bull_pct"],b["n_msgs"]) for b in st.get("top_bullish_buzz",[])][:8])
