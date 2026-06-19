import boto3, json, zipfile, io, glob, time
from botocore.exceptions import ClientError
lam=boto3.client("lambda","us-east-1"); s3=boto3.client("s3","us-east-1"); events=boto3.client("events","us-east-1")
B="justhodl-dashboard-live"; FN="justhodl-attention-signals"; REGION="us-east-1"; ACCT="857687956942"
ROLE="arn:aws:iam::857687956942:role/lambda-execution-role"; RULE="attention-signals-daily"
src=open(glob.glob("**/justhodl-attention-signals/source/lambda_function.py",recursive=True)[0]).read()
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z: z.writestr("lambda_function.py",src)
code=buf.getvalue()
try:
    lam.create_function(FunctionName=FN,Runtime="python3.12",Role=ROLE,Handler="lambda_function.lambda_handler",
        Code={"ZipFile":code},Timeout=300,MemorySize=512,Architectures=["x86_64"],
        Description="Pre-pump attention layer (Finnhub+Stocktwits+GDELT)")
    print("CREATED")
except ClientError as e:
    if "already exist" in str(e).lower() or "ResourceConflict" in str(e):
        for _ in range(24):
            try: lam.update_function_code(FunctionName=FN,ZipFile=code); print("UPDATED"); break
            except ClientError as e2:
                if "ResourceConflict" in str(e2): time.sleep(5); continue
                raise
    else: raise
for _ in range(60):
    st=lam.get_function_configuration(FunctionName=FN)
    if st.get("State")=="Active" and st.get("LastUpdateStatus")!="InProgress": break
    time.sleep(3)
events.put_rule(Name=RULE,ScheduleExpression="cron(45 14 * * ? *)",State="ENABLED",Description="Daily attention 14:45 UTC")
arn=lam.get_function(FunctionName=FN)["Configuration"]["FunctionArn"]
try:
    lam.add_permission(FunctionName=FN,StatementId="attention-signals-daily-evt",Action="lambda:InvokeFunction",
        Principal="events.amazonaws.com",SourceArn="arn:aws:events:%s:%s:rule/%s"%(REGION,ACCT,RULE))
except ClientError as e:
    if "ResourceConflict" not in str(e): print("perm:",str(e)[:50])
events.put_targets(Rule=RULE,Targets=[{"Id":"1","Arn":arn}])
print("SCHEDULED 14:45 UTC")
r=lam.invoke(FunctionName=FN,InvocationType="RequestResponse"); print("INVOKE:",r["Payload"].read().decode()[:220])
time.sleep(2)
d=json.loads(s3.get_object(Bucket=B,Key="data/attention-signals.json")["Body"].read())
print("\ntickers=%s with_insider=%s trending=%s themes=%s elapsed=%ss"%(d.get("n_tickers"),d.get("n_with_insider"),len(d.get("stocktwits_trending",[])),len(d.get("theme_pulse",[])),d.get("elapsed_s")))
print("\nTOP ATTENTION:")
for r in (d.get("top_attention") or [])[:10]:
    print("  %-6s score=%-6s mspr=%-6s buy%%=%-5s upg=%-6s retail=%-5s trend=%-5s | %s"%(
        r["symbol"],r["attention_score"],r.get("insider_mspr"),r.get("analyst_buy_pct"),r.get("analyst_upgrade_mom"),r.get("retail_bull_pct"),r.get("trending"),(r.get("why") or "")[:42]))
print("\nINSIDER BUYING:", [r["symbol"] for r in (d.get("insider_buying") or [])][:10] or "none")
print("ANALYST UPGRADING:", [r["symbol"] for r in (d.get("analyst_upgrading") or [])][:10] or "none")
print("\nTHEME PULSE (narrative tone trend):")
for t in (d.get("theme_pulse") or [])[:6]:
    print("  %-32s tone_recent=%-6s trend=%s"%(t["theme"],t["tone_recent"],t["tone_trend"]))
print("\nSTOCKTWITS TRENDING:", (d.get("stocktwits_trending") or [])[:12])
