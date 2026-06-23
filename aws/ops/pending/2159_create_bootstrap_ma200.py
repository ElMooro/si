import boto3, json, time, io, zipfile
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=890,retries={"max_attempts":0}))
ev=boto3.client("events","us-east-1"); s3=boto3.client("s3","us-east-1")
FN="justhodl-ma200-reclaim"; ROLE="arn:aws:iam::857687956942:role/lambda-execution-role"
SRC="aws/lambdas/justhodl-ma200-reclaim/source/lambda_function.py"
try:
    lam.get_function(FunctionName=FN); print("exists — updating code")
    buf=io.BytesIO()
    with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z: z.writestr("lambda_function.py",open(SRC).read())
    lam.update_function_code(FunctionName=FN,ZipFile=buf.getvalue())
except lam.exceptions.ResourceNotFoundException:
    buf=io.BytesIO()
    with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z: z.writestr("lambda_function.py",open(SRC).read())
    lam.create_function(FunctionName=FN,Runtime="python3.12",Role=ROLE,Handler="lambda_function.lambda_handler",
        Code={"ZipFile":buf.getvalue()},Timeout=900,MemorySize=1536,Architectures=["x86_64"],
        Environment={"Variables":{"POLYGON_KEY":"zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d"}})
    for _ in range(25):
        if lam.get_function(FunctionName=FN)["Configuration"].get("State")=="Active": break
        time.sleep(3)
    rule="justhodl-ma200-reclaim-daily"
    ev.put_rule(Name=rule,ScheduleExpression="cron(30 21 ? * MON-FRI *)",State="ENABLED")
    try: lam.add_permission(FunctionName=FN,StatementId="ev-"+rule,Action="lambda:InvokeFunction",Principal="events.amazonaws.com",SourceArn=f"arn:aws:events:us-east-1:857687956942:rule/{rule}")
    except Exception as e: print("perm",str(e)[:40])
    ev.put_targets(Rule=rule,Targets=[{"Id":"1","Arn":lam.get_function(FunctionName=FN)["Configuration"]["FunctionArn"]}])
    print("created+scheduled")
# wait active
for _ in range(25):
    c=lam.get_function(FunctionName=FN)["Configuration"]
    if c.get("LastUpdateStatus")=="Successful" and c.get("State")=="Active": break
    time.sleep(3)
# bootstrap: invoke up to 3x to fill the buffer past 205 sessions
sess=0
for k in range(3):
    lam.invoke(FunctionName=FN,InvocationType="RequestResponse")
    d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/ma200-reclaim.json")["Body"].read())
    sess=d.get("buffer_sessions",0)
    print(f"invoke {k+1}: buffer_sessions={sess} status={d.get('status','READY')} fetched={d.get('fetched_this_run')}")
    if sess>=205: break
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/ma200-reclaim.json")["Body"].read())
print("\nuniverse:",d.get("universe"),"| counts:",json.dumps(d.get("counts",{})))
if d.get("retest_held"):
    print("\nRETEST-HELD (reclaimed 200-DMA, pulled back, held — the premium book):")
    for r in d["retest_held"][:10]:
        print(f"   {r['ticker']:<6} {r.get('state'):<16} px {r['price']} vs ma200 {r['ma200']} ({r['dist_pct']:+}%) slope {r.get('ma200_slope_pct')}% gc={r.get('ma50_above_ma200')} bars={r.get('bars_since_cross')}")
if d.get("fresh_breakouts_above"):
    print("\nFRESH BREAKOUTS ABOVE 200-DMA:")
    for r in d["fresh_breakouts_above"][:8]:
        print(f"   {r['ticker']:<6} {r['dist_pct']:+}% slope {r.get('ma200_slope_pct')}% bars={r.get('bars_since_cross')}")
if d.get("fresh_breakdowns_below"):
    print("\nFRESH BREAKDOWNS BELOW 200-DMA:")
    for r in d["fresh_breakdowns_below"][:8]:
        print(f"   {r['ticker']:<6} {r['dist_pct']:+}% slope {r.get('ma200_slope_pct')}% bars={r.get('bars_since_cross')}")
print("signals_logged:",d.get("signals_logged"))
print("DONE 2159")
