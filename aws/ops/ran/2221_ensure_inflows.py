import boto3, json, io, zipfile, time
lam=boto3.client("lambda","us-east-1"); ev=boto3.client("events","us-east-1")
FN="justhodl-capital-inflows"
SRC=open("aws/lambdas/justhodl-capital-inflows/source/lambda_function.py").read()
# pull FRED key from an existing engine
fk=lam.get_function_configuration(FunctionName="justhodl-global-liquidity").get("Environment",{}).get("Variables",{}).get("FRED_API_KEY","")
print("have FRED key:", bool(fk))
def zipsrc():
    b=io.BytesIO()
    with zipfile.ZipFile(b,"w",zipfile.ZIP_DEFLATED) as z: z.writestr("lambda_function.py",SRC)
    return b.getvalue()
try:
    lam.get_function(FunctionName=FN); print("exists via deploy")
    lam.update_function_code(FunctionName=FN,ZipFile=zipsrc())
    time.sleep(4)
    lam.update_function_configuration(FunctionName=FN,Environment={"Variables":{"FRED_API_KEY":fk}})
except lam.exceptions.ResourceNotFoundException:
    print("brand-new no-op -> boto3 create")
    lam.create_function(FunctionName=FN,Runtime="python3.12",Handler="lambda_function.lambda_handler",
        Role="arn:aws:iam::857687956942:role/lambda-execution-role",Code={"ZipFile":zipsrc()},
        Timeout=120,MemorySize=256,Environment={"Variables":{"FRED_API_KEY":fk}})
for _ in range(40):
    c=lam.get_function(FunctionName=FN)["Configuration"]
    if c.get("State")=="Active" and c.get("LastUpdateStatus") in ("Successful",None): break
    time.sleep(3)
RULE="justhodl-capital-inflows-weekly"
try:
    ev.put_rule(Name=RULE,ScheduleExpression="cron(0 22 ? * THU *)",State="ENABLED",Description="US net capital inflows weekly")
    arn=lam.get_function(FunctionName=FN)["Configuration"]["FunctionArn"]
    ev.put_targets(Rule=RULE,Targets=[{"Id":"1","Arn":arn}])
    try: lam.add_permission(FunctionName=FN,StatementId="evt-capinflows",Action="lambda:InvokeFunction",Principal="events.amazonaws.com",SourceArn=f"arn:aws:events:us-east-1:857687956942:rule/{RULE}")
    except Exception as e: print("perm:",str(e)[:30])
    print("schedule OK")
except Exception as e: print("schedule FAIL:",str(e)[:70])
lam.invoke(FunctionName=FN,InvocationType="RequestResponse")
d=json.loads(boto3.client("s3","us-east-1").get_object(Bucket="justhodl-dashboard-live",Key="data/capital-inflows.json")["Body"].read())
print("\n=== US CAPITAL INFLOWS (data as of %s) ===" % d.get("data_asof"))
h=d.get("headline",{})
print(f"  Foreign net into US LT securities, 12mo: ${h.get('foreign_net_into_us_lt_12mo_b')}B")
print(f"  NET cross-border LT flow, 12mo:          ${h.get('net_cross_border_lt_12mo_b')}B")
print(f"  Latest month:                            ${h.get('latest_month_b')}B")
print(f"  3mo annualized run-rate:                 ${h.get('run_rate_3mo_annualized_b')}B")
print(f"  YoY change in 12mo sum:                  ${h.get('yoy_change_12mo_b')}B")
print(f"  Short-term Treasury 12mo:                ${h.get('short_term_treasury_12mo_b')}B")
print("  By asset class (12mo $B):", {k:v.get('rolling_12mo_b') for k,v in (d.get('by_asset_class') or {}).items()})
print(f"  REGIME: {d.get('regime')} — {d.get('regime_interpretation')}")
print("DONE 2221")
