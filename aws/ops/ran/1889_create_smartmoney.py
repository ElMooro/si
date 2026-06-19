import boto3, json, zipfile, io, glob, time
from botocore.exceptions import ClientError
lam=boto3.client("lambda","us-east-1"); s3=boto3.client("s3","us-east-1"); events=boto3.client("events","us-east-1")
B="justhodl-dashboard-live"; FN="justhodl-smart-money-13f"; REGION="us-east-1"; ACCT="857687956942"
ROLE="arn:aws:iam::857687956942:role/lambda-execution-role"; RULE="smart-money-13f-weekly"
src=open(glob.glob("**/justhodl-smart-money-13f/source/lambda_function.py",recursive=True)[0]).read()
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z: z.writestr("lambda_function.py",src)
code=buf.getvalue()
try:
    lam.create_function(FunctionName=FN,Runtime="python3.12",Role=ROLE,Handler="lambda_function.lambda_handler",
        Code={"ZipFile":code},Timeout=180,MemorySize=256,Architectures=["x86_64"],
        Description="Smart-money thematic 13F tracker (SEC EDGAR free)")
    print("CREATED")
except ClientError as e:
    if "already exist" in str(e).lower() or "ResourceConflict" in str(e):
        for _ in range(24):
            try: lam.update_function_code(FunctionName=FN,ZipFile=code); print("UPDATED"); break
            except ClientError as e2:
                if "ResourceConflict" in str(e2): time.sleep(5); continue
                raise
    else: raise
for _ in range(50):
    st=lam.get_function_configuration(FunctionName=FN)
    if st.get("State")=="Active" and st.get("LastUpdateStatus")!="InProgress": break
    time.sleep(3)
events.put_rule(Name=RULE,ScheduleExpression="cron(0 11 ? * MON *)",State="ENABLED",Description="Weekly 13F Mon 11:00 UTC")
arn=lam.get_function(FunctionName=FN)["Configuration"]["FunctionArn"]
try:
    lam.add_permission(FunctionName=FN,StatementId="smart-money-13f-weekly-evt",Action="lambda:InvokeFunction",
        Principal="events.amazonaws.com",SourceArn="arn:aws:events:%s:%s:rule/%s"%(REGION,ACCT,RULE))
except ClientError as e:
    if "ResourceConflict" not in str(e): print("perm:",str(e)[:50])
events.put_targets(Rule=RULE,Targets=[{"Id":"1","Arn":arn}])
print("SCHEDULED weekly Mon 11:00 UTC")
r=lam.invoke(FunctionName=FN,InvocationType="RequestResponse"); print("INVOKE:",r["Payload"].read().decode()[:260])
time.sleep(2)
d=json.loads(s3.get_object(Bucket=B,Key="data/smart-money-13f.json")["Body"].read())
for f in d.get("funds",[]):
    print("\n%s (%s) — 13F report %s, filed %s — longs=%s puts=%s"%(f["fund"],f["manager"],f["report_date"],f["filing_date"],f["n_longs"],f["n_puts"]))
    print("  TOP LONGS:")
    for h in f["top_longs"][:10]:
        print("    %-7s %-26s layer=%-13s val=%s"%(h.get("ticker") or "?",h["issuer"][:26],h.get("layer") or "-",h.get("value")))
    print("  PUTS (shorting):", [h.get("ticker") or h["issuer"][:10] for h in f["puts"][:12]])
print("\nLONG BY LAYER:", {k:[x["ticker"] for x in v] for k,v in (d.get("smart_money_long_by_layer") or {}).items()})
print("\nCONFLUENCE (smart money long + your radar calls cheap):")
for c in (d.get("confluence_cheap_and_backed") or [])[:8]:
    print("  %-7s layer=%-12s your_discount=%s%% growth=%s%%"%(c["ticker"],c.get("layer"),c.get("your_discount_pct"),c.get("your_growth_pct")))
if not d.get("confluence_cheap_and_backed"): print("  (no overlap this run — their longs aren't currently in your radar's cheap top_setups)")
