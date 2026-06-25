import boto3, json, io, zipfile, time
lam=boto3.client("lambda","us-east-1")
ev=boto3.client("events","us-east-1")
FN="justhodl-accumulation-radar"
SRC=open("aws/lambdas/justhodl-accumulation-radar/source/lambda_function.py").read()
def zipsrc():
    b=io.BytesIO()
    with zipfile.ZipFile(b,"w",zipfile.ZIP_DEFLATED) as z: z.writestr("lambda_function.py",SRC)
    return b.getvalue()
try:
    c=lam.get_function(FunctionName=FN)
    print("function EXISTS via deploy-lambdas:",c["Configuration"]["State"])
    # ensure latest code
    lam.update_function_code(FunctionName=FN,ZipFile=zipsrc()); print("code refreshed")
except lam.exceptions.ResourceNotFoundException:
    print("brand-new-dir no-op confirmed -> creating via boto3")
    lam.create_function(FunctionName=FN,Runtime="python3.12",Handler="lambda_function.lambda_handler",
        Role="arn:aws:iam::857687956942:role/lambda-execution-role",Code={"ZipFile":zipsrc()},
        Timeout=900,MemorySize=1536,Environment={"Variables":{"POLYGON_KEY":"zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d"}})
    print("create initiated")
for _ in range(40):
    c=lam.get_function(FunctionName=FN)["Configuration"]
    if c.get("State")=="Active" and c.get("LastUpdateStatus") in ("Successful",None): break
    time.sleep(3)
print("ready:",c.get("State"),c.get("LastUpdateStatus"))
# ensure schedule rule (handle cap gracefully)
RULE="justhodl-accumulation-radar-daily"
try:
    ev.put_rule(Name=RULE,ScheduleExpression="cron(50 21 ? * MON-FRI *)",State="ENABLED",
                Description="Daily accumulation/distribution + tops/bottoms radar")
    arn=lam.get_function(FunctionName=FN)["Configuration"]["FunctionArn"]
    ev.put_targets(Rule=RULE,Targets=[{"Id":"1","Arn":arn}])
    try:
        lam.add_permission(FunctionName=FN,StatementId="evt-accum",Action="lambda:InvokeFunction",
            Principal="events.amazonaws.com",SourceArn=f"arn:aws:events:us-east-1:857687956942:rule/{RULE}")
    except Exception as e: print("perm:",str(e)[:40])
    print("schedule rule OK:",RULE)
except Exception as e:
    print("schedule rule FAILED (eventbridge cap?):",str(e)[:80])
print("DONE 2184")
