import boto3, json, time, io, zipfile, glob, os
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=600,retries={"max_attempts":0}))
ev=boto3.client("events","us-east-1"); s3=boto3.client("s3","us-east-1")
FN="justhodl-page-ai"; ROLE="arn:aws:iam::857687956942:role/lambda-execution-role"

def build_zip():
    buf=io.BytesIO()
    with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z:
        z.writestr("lambda_function.py",open("aws/lambdas/justhodl-page-ai/source/lambda_function.py").read())
        for f in glob.glob("aws/shared/*.py"):
            z.writestr(os.path.basename(f),open(f).read())   # bundle llm_router etc at root
    return buf.getvalue()

try:
    lam.get_function(FunctionName=FN); ex=True; print("exists (deploy pipeline created it) — updating code to ensure shared bundled")
    lam.update_function_code(FunctionName=FN,ZipFile=build_zip())
except lam.exceptions.ResourceNotFoundException:
    ex=False; print("not found -> boto3 create with shared bundle")
    lam.create_function(FunctionName=FN,Runtime="python3.12",Role=ROLE,Handler="lambda_function.lambda_handler",
        Code={"ZipFile":build_zip()},Timeout=300,MemorySize=512,Architectures=["x86_64"])
    for _ in range(20):
        if lam.get_function(FunctionName=FN)["Configuration"].get("State")=="Active": break
        time.sleep(3)
    rule="justhodl-page-ai-wave"
    ev.put_rule(Name=rule,ScheduleExpression="cron(15 */3 * * ? *)",State="ENABLED")
    try: lam.add_permission(FunctionName=FN,StatementId="ev-"+rule,Action="lambda:InvokeFunction",Principal="events.amazonaws.com",SourceArn=f"arn:aws:events:us-east-1:857687956942:rule/{rule}")
    except Exception as e: print("perm:",str(e)[:40])
    ev.put_targets(Rule=rule,Targets=[{"Id":"1","Arn":lam.get_function(FunctionName=FN)["Configuration"]["FunctionArn"]}])
    print("created+scheduled")

for _ in range(30):
    c=lam.get_function(FunctionName=FN)["Configuration"]
    if c.get("LastUpdateStatus")=="Successful" and c.get("State")=="Active": break
    time.sleep(3)
t=time.time(); r=lam.invoke(FunctionName=FN,InvocationType="RequestResponse")
print("invoke:",r["Payload"].read().decode()[:240],f"({time.time()-t:.0f}s)")

def show(pg):
    try:
        d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key=f"data/page-ai/{pg}.json")["Body"].read())
        print(f"\n--- {pg} ---")
        print("  what_it_is:",(d.get('what_it_is') or '')[:150])
        print("  what_it_does:",(d.get('what_it_does') or '')[:150])
        print("  analysis:",(d.get('analysis') or '')[:200])
        print("  pick_read:",(d.get('pick_read') or '')[:160])
        o=d.get("outlook",{})
        print(f"  OUTLOOK: status={o.get('alpha_status')} grade={o.get('grade')} mean_excess_vs_spy={o.get('mean_excess_vs_spy_pct')}% hit={o.get('hit_rate_pct')}% n={o.get('n_graded')} matched={o.get('matched_key')}")
        print("           confidence:",o.get('confidence'))
    except Exception as e: print(f"  {pg}: not generated this wave ({str(e)[:40]})")
# show a few: which pages were in the first wave? cursor started at 0 -> first ~50 alphabetical
cur=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/_cache/page-ai-cursor.json")["Body"].read())
print("\ncursor now:",cur)
import re
man=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/page-ai-manifest.json")["Body"].read())
pages=list(man.keys())
for pg in pages[:3]+["ai-rerating","altseason","apex"]:
    show(pg)
print("\nDONE 2125")
