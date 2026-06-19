import boto3, json, zipfile, io, glob, time
lam=boto3.client("lambda","us-east-1"); ev=boto3.client("events","us-east-1"); s3=boto3.client("s3","us-east-1")
FN="justhodl-ai-rerating-radar"; ROLE="arn:aws:iam::857687956942:role/lambda-execution-role"; RULE="ai-rerating-radar-daily"
src=open(glob.glob("**/justhodl-ai-rerating-radar/source/lambda_function.py",recursive=True)[0]).read()
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z: z.writestr("lambda_function.py",src)
code=buf.getvalue()
try: lam.get_function(FunctionName=FN); ex=True
except Exception: ex=False
if ex: lam.update_function_code(FunctionName=FN,ZipFile=code); print("UPDATED")
else:
    r=lam.create_function(FunctionName=FN,Runtime="python3.12",Role=ROLE,Handler="lambda_function.lambda_handler",
        Code={"ZipFile":code},Timeout=300,MemorySize=512,Architectures=["x86_64"],Description="AI re-rating radar")
    print("CREATED",r["FunctionArn"])
for _ in range(40):
    st=lam.get_function_configuration(FunctionName=FN)
    if st.get("State")=="Active" and st.get("LastUpdateStatus")!="InProgress": break
    time.sleep(2)
arn=lam.get_function_configuration(FunctionName=FN)["FunctionArn"]
ev.put_rule(Name=RULE,ScheduleExpression="cron(15 14 * * ? *)",State="ENABLED",Description="daily 14:15 UTC AI re-rating")
try: lam.add_permission(FunctionName=FN,StatementId="evb-"+RULE,Action="lambda:InvokeFunction",Principal="events.amazonaws.com",SourceArn=ev.describe_rule(Name=RULE)["Arn"])
except Exception as e: print("perm:",str(e)[:50])
ev.put_targets(Rule=RULE,Targets=[{"Id":"1","Arn":arn}]); print("RULE wired (daily 14:15 UTC)")
r=lam.invoke(FunctionName=FN,InvocationType="RequestResponse"); print("INVOKE:",r["Payload"].read().decode()[:200])
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/ai-rerating-radar.json")["Body"].read())
sm=d["summary"]; reg=d["regression"]
print("REGRESSION: EV/Sales = %.2f + %.2f*growth  (n=%s)"%(reg["intercept"] or 0,reg["slope_evsales_per_growth"] or 0,reg["n_points"]))
print("SUMMARY: priced=%s candidates=%s small_mid=%s elapsed=%s"%(sm["n_priced"],sm["n_candidates"],sm["n_small_mid_candidates"],d.get("elapsed_s")))
print("\n=== TOP RE-RATING SETUPS (high fwd growth, cheap for it, lagged) ===")
for x in sm["top_setups"][:12]:
    print("  %-6s %-6s %-9s fwdG=%-5s%% EV/S=%-5s impl=%-5s disc=%-6s lag=%-6s %s%s"%(
        x["symbol"],x["cap_bucket"],x["layer"][:9],x["fwd_growth_pct"],x["ev_sales"],x["ev_sales_implied"],
        ("%s%%"%x["discount_to_implied_pct"]) if x["discount_to_implied_pct"] is not None else "-",
        ("%spp"%x["laggard_gap_pp"]) if x["laggard_gap_pp"] is not None else "-",
        "ACC " if x["accelerating"] else "","BN" if x["bottleneck"] else ""))
print("\n=== DEEPEST DISCOUNTS TO GROWTH-IMPLIED MULTIPLE ===")
for x in sm["deepest_discounts"][:8]:
    print("  %-6s %-6s fwdG=%-5s%% EV/S=%-5s vs impl=%-5s  %s%% below | %s"%(x["symbol"],x["cap_bucket"],x["fwd_growth_pct"],x["ev_sales"],x["ev_sales_implied"],x["discount_to_implied_pct"],x["why"][:60]))
