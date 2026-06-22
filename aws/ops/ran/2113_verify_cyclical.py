import boto3, json, time, io, zipfile
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=600,retries={"max_attempts":0}))
ev=boto3.client("events","us-east-1")
FN="justhodl-cyclical-bagger"; ROLE="arn:aws:iam::857687956942:role/lambda-execution-role"
try: lam.get_function(FunctionName=FN); ex=True; print("get_function: EXISTS")
except lam.exceptions.ResourceNotFoundException: ex=False; print("NotFound -> boto3-create")
if not ex:
    src=open("aws/lambdas/justhodl-cyclical-bagger/source/lambda_function.py").read()
    buf=io.BytesIO()
    with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z: z.writestr("lambda_function.py",src)
    lam.create_function(FunctionName=FN,Runtime="python3.12",Role=ROLE,Handler="lambda_function.lambda_handler",
        Code={"ZipFile":buf.getvalue()},Timeout=300,MemorySize=1024,Architectures=["x86_64"])
    for _ in range(20):
        if lam.get_function(FunctionName=FN)["Configuration"].get("State")=="Active": break
        time.sleep(3)
    rule="justhodl-cyclical-bagger-daily"
    ev.put_rule(Name=rule,ScheduleExpression="cron(0 15 * * ? *)",State="ENABLED")
    try: lam.add_permission(FunctionName=FN,StatementId="ev-"+rule,Action="lambda:InvokeFunction",Principal="events.amazonaws.com",SourceArn=f"arn:aws:events:us-east-1:857687956942:rule/{rule}")
    except Exception as e: print("perm:",str(e)[:50])
    ev.put_targets(Rule=rule,Targets=[{"Id":"1","Arn":lam.get_function(FunctionName=FN)["Configuration"]["FunctionArn"]}])
    print("created + scheduled.")
for _ in range(20):
    c=lam.get_function(FunctionName=FN)["Configuration"]
    if c.get("LastUpdateStatus")=="Successful" and c.get("State")=="Active": break
    time.sleep(3)
t=time.time()
r=lam.invoke(FunctionName=FN,InvocationType="RequestResponse")
print("invoke:",r["Payload"].read().decode()[:300],f"({time.time()-t:.0f}s)")
# verify output + MU/SNDK self-check
d=json.loads(boto3.client("s3","us-east-1").get_object(Bucket="justhodl-dashboard-live",Key="data/cyclical-bagger.json")["Body"].read())
print("mode:",d["mode"],"| stats:",d["stats"])
for r in d["all_ranked"]:
    if r["ticker"] in ("MU","SNDK"):
        print(f"  SELF-CHECK {r['ticker']}: 20x_shape={r['twenty_x_shape']} swing={r['om_swing_pp']}pp eps_n2p={r['eps_neg_to_pos']} stage={r['stage']} score={r['cyclical_20x_score']}")
print("  20x-shape book (early/confirming):")
for r in d["twenty_x_shape_book"][:10]:
    print(f"    {r['ticker']} {r['stage']} score={r['cyclical_20x_score']} swing={r['om_swing_pp']}pp run={r['run_from_trough_x']}x cap={r['cap_bucket']} {r.get('secular_themes')}")
print("DONE 2113")
