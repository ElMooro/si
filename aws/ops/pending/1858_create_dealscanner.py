import boto3, json, zipfile, io, glob, time
lam=boto3.client("lambda","us-east-1"); ev=boto3.client("events","us-east-1"); s3=boto3.client("s3","us-east-1")
FN="justhodl-deal-scanner"; ROLE="arn:aws:iam::857687956942:role/lambda-execution-role"; RULE="deal-scanner-daily"
src=open(glob.glob("**/justhodl-deal-scanner/source/lambda_function.py",recursive=True)[0]).read()
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z: z.writestr("lambda_function.py",src)
code=buf.getvalue()
try: lam.get_function(FunctionName=FN); ex=True
except Exception: ex=False
if ex: lam.update_function_code(FunctionName=FN,ZipFile=code); print("UPDATED")
else:
    r=lam.create_function(FunctionName=FN,Runtime="python3.12",Role=ROLE,Handler="lambda_function.lambda_handler",
        Code={"ZipFile":code},Timeout=300,MemorySize=512,Architectures=["x86_64"],Description="deal scanner")
    print("CREATED",r["FunctionArn"])
for _ in range(40):
    st=lam.get_function_configuration(FunctionName=FN)
    if st.get("State")=="Active" and st.get("LastUpdateStatus")!="InProgress": break
    time.sleep(2)
arn=lam.get_function_configuration(FunctionName=FN)["FunctionArn"]
ev.put_rule(Name=RULE,ScheduleExpression="cron(0 22 * * ? *)",State="ENABLED",Description="daily 22:00 UTC deal scan")
try: lam.add_permission(FunctionName=FN,StatementId="evb-"+RULE,Action="lambda:InvokeFunction",Principal="events.amazonaws.com",SourceArn=ev.describe_rule(Name=RULE)["Arn"])
except Exception as e: print("perm:",str(e)[:60])
ev.put_targets(Rule=RULE,Targets=[{"Id":"1","Arn":arn}]); print("RULE wired (daily 22:00 UTC)")
r=lam.invoke(FunctionName=FN,InvocationType="RequestResponse"); print("INVOKE:",r["Payload"].read().decode()[:260])
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/deal-scanner.json")["Body"].read())
sm=d["summary"]
print("SUMMARY: prs=%s deals=%s sized=%s small=%s highmat=%s"%(sm["n_prs_scanned"],sm["n_deals"],sm["n_with_size"],sm["n_small_cap"],sm["n_high_materiality"]))
print("\nTOP DEALS:")
for x in sm["top_deals"][:8]:
    print("  %-6s %-7s mat=%s%% val=%s age=%sh score=%s"%(x["symbol"],x["cap_bucket"] or "?",x["materiality_pct"],x["deal_value_str"],x["age_h"],x["score"]))
    print("        \"%s\""%(x["title"][:95]))
print("\nTOP SMALL-CAP DEALS:")
for x in sm["top_smallcap_deals"][:8]:
    print("  %-6s %-7s mat=%s%% val=%s rev=%s"%(x["symbol"],x["cap_bucket"],x["materiality_pct"],x["deal_value_str"],("$%.0fM"%(x["revenue_fy"]/1e6)) if x.get("revenue_fy") else "n/a"))
    print("        \"%s\""%(x["title"][:95]))
