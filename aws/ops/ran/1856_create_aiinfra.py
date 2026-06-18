import boto3, json, zipfile, io, glob, time
lam=boto3.client("lambda","us-east-1"); ev=boto3.client("events","us-east-1"); s3=boto3.client("s3","us-east-1")
FN="justhodl-ai-infra-stack"; ROLE="arn:aws:iam::857687956942:role/lambda-execution-role"; RULE="ai-infra-stack-daily"
src=open(glob.glob("**/justhodl-ai-infra-stack/source/lambda_function.py",recursive=True)[0]).read()
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z: z.writestr("lambda_function.py",src)
code=buf.getvalue()
try: lam.get_function(FunctionName=FN); ex=True
except Exception: ex=False
if ex:
    lam.update_function_code(FunctionName=FN,ZipFile=code); print("UPDATED")
else:
    r=lam.create_function(FunctionName=FN,Runtime="python3.12",Role=ROLE,Handler="lambda_function.lambda_handler",
        Code={"ZipFile":code},Timeout=300,MemorySize=512,Architectures=["x86_64"],Description="AI infra stack")
    print("CREATED",r["FunctionArn"])
for _ in range(40):
    st=lam.get_function_configuration(FunctionName=FN)
    if st.get("State")=="Active" and st.get("LastUpdateStatus")!="InProgress": break
    time.sleep(2)
arn=lam.get_function_configuration(FunctionName=FN)["FunctionArn"]
ev.put_rule(Name=RULE,ScheduleExpression="cron(45 13 * * ? *)",State="ENABLED",Description="daily 13:45 UTC AI-infra stack")
try: lam.add_permission(FunctionName=FN,StatementId="evb-"+RULE,Action="lambda:InvokeFunction",Principal="events.amazonaws.com",SourceArn=ev.describe_rule(Name=RULE)["Arn"])
except Exception as e: print("perm:",str(e)[:60])
ev.put_targets(Rule=RULE,Targets=[{"Id":"1","Arn":arn}]); print("RULE wired (daily 13:45 UTC)")
r=lam.invoke(FunctionName=FN,InvocationType="RequestResponse"); print("INVOKE:",r["Payload"].read().decode()[:260])
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/ai-infra-stack.json")["Body"].read())
sm=d["summary"]
print("SUMMARY: layers=%s names=%s small=%s"%(sm["n_layers"],sm["n_names"],sm["n_small_cap"]))
print("HOTTEST LAYERS:",[(h["layer"],h["heat_1m_pct"]) for h in sm["hottest_layers"]])
print("\nSTACK (per layer: n / small / heat):")
for s in d["stack"]:
    print("  %-22s n=%-3d small=%-3d heat=%s%%"%(s["label"],s["n_names"],s["n_small_cap"],s["layer_heat_1m_pct"]))
    for x in s["names"][:3]:
        print("       %-6s %-7s 1m=%s%% sigs=%s%s%s"%(x["symbol"],x["cap_bucket"],x["ret_1m_pct"],x["flow_signals"][:2],
              " BN" if x["bottleneck"] else ""," RA" if x["rev_accel"] else ""))
print("\nTOP SMALL-CAP PICKS across stack:")
for x in sm["top_small_cap_picks"][:10]:
    print("  %-6s %-7s %-20s 1m=%s%% comp=%s sigs=%s"%(x["symbol"],x["cap_bucket"],x["layer"],x["ret_1m_pct"],x["composite"],x["flow_signals"][:2]))
