import boto3, json, io, zipfile, time
from botocore.config import Config
REGION="us-east-1"; FN="justhodl-money-flow-state"
ROLE="arn:aws:iam::857687956942:role/lambda-execution-role"
SRC="aws/lambdas/justhodl-money-flow-state/source/lambda_function.py"
lam=boto3.client("lambda",REGION,config=Config(read_timeout=290,retries={"max_attempts":0}))
events=boto3.client("events",REGION)
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z: z.writestr("lambda_function.py",open(SRC).read())
code=buf.getvalue()
try: lam.get_function(FunctionName=FN); exists=True
except lam.exceptions.ResourceNotFoundException: exists=False
if exists: lam.update_function_code(FunctionName=FN,ZipFile=code); print("updated")
else:
    lam.create_function(FunctionName=FN,Runtime="python3.12",Role=ROLE,Handler="lambda_function.lambda_handler",
        Code={"ZipFile":code},Timeout=120,MemorySize=1024,Architectures=["x86_64"],
        Description="Whole-market dollar money-flow -> data/money-flow-state.json"); print("created")
time.sleep(8)
rule="money-flow-state-daily"
events.put_rule(Name=rule,ScheduleExpression="cron(30 23 * * ? *)",State="ENABLED",Description="Daily money-flow")
arn=lam.get_function(FunctionName=FN)["Configuration"]["FunctionArn"]
try:
    lam.add_permission(FunctionName=FN,StatementId="mfs-evt",Action="lambda:InvokeFunction",
        Principal="events.amazonaws.com",SourceArn=f"arn:aws:events:{REGION}:857687956942:rule/{rule}"); print("perm ok")
except Exception as e: print("perm:",str(e)[:40])
events.put_targets(Rule=rule,Targets=[{"Id":"mfs","Arn":arn}]); print("scheduled")
r=lam.invoke(FunctionName=FN,InvocationType="RequestResponse",Payload=b"{}")
print("invoke status:",r["StatusCode"],"err:",r.get("FunctionError"))
print("payload:",r["Payload"].read().decode()[:200])
s3=boto3.client("s3",REGION)
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/money-flow-state.json")["Body"].read())
print("EMITTED as_of",d.get("as_of"),"n_stocks",d.get("n_stocks"),"n_days",d.get("n_days"))
def money(v):
    v=v or 0; a=abs(v); s="+" if v>=0 else "-"
    return s+"$"+(f"{a/1e9:.2f}B" if a>=1e9 else f"{a/1e6:.0f}M" if a>=1e6 else f"{a/1e3:.0f}K")
print("TOP STOCKS IN:",[(x["ticker"],x["industry"],money(x["flow_usd"])) for x in d.get("stocks_in",[])[:6]])
print("TOP STOCKS OUT:",[(x["ticker"],x["industry"],money(x["flow_usd"])) for x in d.get("stocks_out",[])[:6]])
print("INDUSTRIES IN:",[(x["industry"],money(x["net_flow_usd"])) for x in d.get("industries_in",[])[:6]])
print("SECTORS net:",[(x["sector"],money(x["net_flow_usd"])) for x in d.get("sectors",[])[:11]])
print("DONE 2506")
