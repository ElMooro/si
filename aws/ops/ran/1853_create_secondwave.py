import boto3, json, zipfile, io, glob, time
lam=boto3.client("lambda","us-east-1"); ev=boto3.client("events","us-east-1"); s3=boto3.client("s3","us-east-1")
FN="justhodl-theme-second-wave"; ROLE="arn:aws:iam::857687956942:role/lambda-execution-role"; RULE="theme-second-wave-daily"
p=glob.glob("**/justhodl-theme-second-wave/source/lambda_function.py", recursive=True)
print("source:",p[0]); src=open(p[0]).read()
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z: z.writestr("lambda_function.py",src)
code=buf.getvalue()
try: lam.get_function(FunctionName=FN); exists=True
except Exception: exists=False
if exists:
    lam.update_function_code(FunctionName=FN, ZipFile=code); print("UPDATED code")
else:
    r=lam.create_function(FunctionName=FN,Runtime="python3.12",Role=ROLE,Handler="lambda_function.lambda_handler",
        Code={"ZipFile":code},Timeout=120,MemorySize=256,Architectures=["x86_64"],Description="theme second-wave layer")
    print("CREATED",r["FunctionArn"])
for _ in range(30):
    st=lam.get_function_configuration(FunctionName=FN)
    if st.get("State")=="Active" and st.get("LastUpdateStatus")!="InProgress": break
    time.sleep(2)
arn=lam.get_function_configuration(FunctionName=FN)["FunctionArn"]
ev.put_rule(Name=RULE,ScheduleExpression="cron(0 14 * * ? *)",State="ENABLED",Description="daily 14:00 UTC second-wave scan")
rarn=ev.describe_rule(Name=RULE)["Arn"]
try: lam.add_permission(FunctionName=FN,StatementId="evb-"+RULE,Action="lambda:InvokeFunction",Principal="events.amazonaws.com",SourceArn=rarn)
except Exception as e: print("perm:",str(e)[:70])
ev.put_targets(Rule=RULE,Targets=[{"Id":"1","Arn":arn}]); print("RULE wired (daily 14:00 UTC)")
r=lam.invoke(FunctionName=FN,InvocationType="RequestResponse"); print("INVOKE:",r["Payload"].read().decode()[:300])
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/theme-second-wave.json")["Body"].read())
sm=d.get("summary",{})
print("COUNTS:",{k:sm.get(k) for k in ("n_hot_themes","n_infrastructure","n_laggards","n_big_orders","n_smallcap_big_orders","n_top_picks")})
print("TOP PICKS:",json.dumps(sm.get("top_picks",[])[:6],indent=1)[:1100])
for t in d.get("hot_themes",[])[:3]:
    print("\nTHEME %s (%s) med=%s%% breadth=%s infra=%d lag=%d big=%d"%(t["etf"],t["name"],t["theme_median_ret20d_pct"],t["breadth_pct"],len(t["infrastructure"]),len(t["laggards"]),len(t["big_orders"])))
    for l in t["laggards"][:3]: print("   LAG %-6s gap=%spp r20=%s%% small=%s sigs=%s"%(l["symbol"],l["gap_vs_theme_pp"],l["ret_20d_pct"],l["is_small_cap"],l.get("big_order_signals")))
    for i in t["infrastructure"][:3]: print("   INF %-6s %-26s +%s%%"%(i["symbol"],(i["industry"] or "")[:26],i["ret_20d_pct"]))
    for b in t["big_orders"][:3]: print("   BIG %-6s small=%s sigs=%s"%(b["symbol"],b["is_small_cap"],[s["type"] for s in b["signals"]]))
