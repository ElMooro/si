import boto3, json, time, io, zipfile, urllib.request
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=600,retries={"max_attempts":0}))
ev=boto3.client("events","us-east-1")
FN="justhodl-chokepoint"; ROLE="arn:aws:iam::857687956942:role/lambda-execution-role"
try: lam.get_function(FunctionName=FN); ex=True; print("get_function: EXISTS")
except lam.exceptions.ResourceNotFoundException: ex=False; print("NotFound -> boto3-create")
if not ex:
    src=open("aws/lambdas/justhodl-chokepoint/source/lambda_function.py").read()
    buf=io.BytesIO()
    with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z: z.writestr("lambda_function.py",src)
    lam.create_function(FunctionName=FN,Runtime="python3.12",Role=ROLE,Handler="lambda_function.lambda_handler",
        Code={"ZipFile":buf.getvalue()},Timeout=300,MemorySize=1024,Architectures=["x86_64"])
    for _ in range(20):
        if lam.get_function(FunctionName=FN)["Configuration"].get("State")=="Active": break
        time.sleep(3)
    rule="justhodl-chokepoint-daily"
    ev.put_rule(Name=rule,ScheduleExpression="cron(30 15 * * ? *)",State="ENABLED")
    try: lam.add_permission(FunctionName=FN,StatementId="ev-"+rule,Action="lambda:InvokeFunction",Principal="events.amazonaws.com",SourceArn=f"arn:aws:events:us-east-1:857687956942:rule/{rule}")
    except Exception as e: print("perm:",str(e)[:50])
    ev.put_targets(Rule=rule,Targets=[{"Id":"1","Arn":lam.get_function(FunctionName=FN)["Configuration"]["FunctionArn"]}])
    print("created + scheduled.")
for _ in range(25):
    c=lam.get_function(FunctionName=FN)["Configuration"]
    if c.get("LastUpdateStatus")=="Successful" and c.get("State")=="Active": break
    time.sleep(3)
t=time.time()
r=lam.invoke(FunctionName=FN,InvocationType="RequestResponse")
print("invoke:",r["Payload"].read().decode()[:200],f"({time.time()-t:.0f}s)")
d=json.loads(boto3.client("s3","us-east-1").get_object(Bucket="justhodl-dashboard-live",Key="data/chokepoint.json")["Body"].read())
print("stats:",d["stats"],"| mode:",d["mode"])
print("\nTOP 12 by criticality:")
for r in d["all_chokepoints"][:12]:
    print(f"  {r['ticker']:<6}{r['criticality']:>6}  {r['gm_level']}%GM ±{r['gm_stability']} roic={r['roic']} hub={r['centrality']}  {(r.get('industry') or '')[:34]}")
print("\n💰 CHEAP chokepoints (the edge):")
for r in d.get("cheap_chokepoint_book",[])[:8]:
    print(f"  {r['ticker']:<6}{r['criticality']:>6}  {r.get('discount_to_fair_pct')}% below fair  {(r.get('name') or '')[:30]}")
print(d.get("cheap_chokepoint_book") or "  (none — chokepoints usually expensive)")
print("\n🔦 HIDDEN chokepoints (small/mid):")
for r in d.get("hidden_chokepoint_book",[])[:8]:
    print(f"  {r['ticker']:<6}{r['criticality']:>6}  {r['cap_bucket']}  {(r.get('industry') or '')[:30]}")
print("\n🏭 industry leaders (sample):", [(i['ticker'],i['criticality']) for i in d.get("industry_leaders",[])[:10]])
def chk(u):
    for _ in range(3):
        try:
            with urllib.request.urlopen(urllib.request.Request(u,headers={"User-Agent":"Mozilla/5.0 jh"}),timeout=20) as x:return x.getcode()
        except Exception:time.sleep(8)
    return None
print("\npage:",chk("https://justhodl.ai/equity-chokepoint.html?t="+str(int(time.time()))))
print("DONE 2118")
