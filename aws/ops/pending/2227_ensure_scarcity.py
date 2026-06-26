import boto3, json, io, zipfile, time
lam=boto3.client("lambda","us-east-1"); ev=boto3.client("events","us-east-1")
FN="justhodl-scarcity-radar"
SRC=open("aws/lambdas/justhodl-scarcity-radar/source/lambda_function.py").read()
def zipsrc():
    b=io.BytesIO()
    with zipfile.ZipFile(b,"w",zipfile.ZIP_DEFLATED) as z: z.writestr("lambda_function.py",SRC)
    return b.getvalue()
try:
    lam.get_function(FunctionName=FN); print("exists via deploy"); lam.update_function_code(FunctionName=FN,ZipFile=zipsrc())
except lam.exceptions.ResourceNotFoundException:
    print("brand-new no-op -> boto3 create")
    lam.create_function(FunctionName=FN,Runtime="python3.12",Handler="lambda_function.lambda_handler",
        Role="arn:aws:iam::857687956942:role/lambda-execution-role",Code={"ZipFile":zipsrc()},Timeout=180,MemorySize=512)
for _ in range(40):
    c=lam.get_function(FunctionName=FN)["Configuration"]
    if c.get("State")=="Active" and c.get("LastUpdateStatus") in ("Successful",None): break
    time.sleep(3)
RULE="justhodl-scarcity-radar-daily"
try:
    ev.put_rule(Name=RULE,ScheduleExpression="cron(45 22 ? * MON-FRI *)",State="ENABLED",Description="Next-shortage radar")
    arn=lam.get_function(FunctionName=FN)["Configuration"]["FunctionArn"]
    ev.put_targets(Rule=RULE,Targets=[{"Id":"1","Arn":arn}])
    try: lam.add_permission(FunctionName=FN,StatementId="evt-scarcity",Action="lambda:InvokeFunction",Principal="events.amazonaws.com",SourceArn=f"arn:aws:events:us-east-1:857687956942:rule/{RULE}")
    except Exception as e: print("perm:",str(e)[:30])
    print("schedule OK")
except Exception as e: print("schedule FAIL:",str(e)[:70])
lam.invoke(FunctionName=FN,InvocationType="RequestResponse")
d=json.loads(boto3.client("s3","us-east-1").get_object(Bucket="justhodl-dashboard-live",Key="data/scarcity-radar.json")["Body"].read())
print("\ninputs_live:",json.dumps(d.get("inputs_live")))
print("counts:",json.dumps(d.get("counts")))
print("\n=== VERTICAL TIGHTNESS (which shortage is building) ===")
for v in (d.get("vertical_tightness") or [])[:6]:
    print(f"  {v['theme']:<5} tightness {v['tightness']:<5} phase={v.get('phase')} strong={v.get('n_strong_tightening')} signals={v.get('top_signals')} names={v.get('candidate_names')}")
print("\n=== HEADLINE BOARD (shortage building + nobody looking) ===")
for b in (d.get("headline_board") or [])[:10]:
    print(f"  {b['ticker']:<6} comp {b['composite']:<5} scarcity {b['scarcity']:<5} stealth {b['stealth']:<5} vert={b.get('vertical')} phase={b.get('theme_phase')} via {b['engines']}")
if not d.get("headline_board"):
    print("  (none cleared both thresholds — showing scarcity leaders)")
    for b in (d.get("scarcity_leaders") or [])[:6]:
        print(f"  {b['ticker']:<6} scarcity {b['scarcity']} stealth {b['stealth']} vert={b.get('vertical')}")
print("DONE 2227")
