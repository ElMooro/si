"""ops 2063: boto3-create edge-discovery, schedule, invoke (heavy — read_timeout), verify honest backtest output."""
import boto3, json, time, io, os, zipfile
from botocore.config import Config
REGION="us-east-1"; FN="justhodl-edge-discovery"; B="justhodl-dashboard-live"
ROLE="arn:aws:iam::857687956942:role/lambda-execution-role"
lam=boto3.client("lambda",REGION,config=Config(read_timeout=620,retries={"max_attempts":0}))
events=boto3.client("events",REGION); s3=boto3.client("s3",REGION)
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z:
    src=f"aws/lambdas/{FN}/source"
    for r,_,fs in os.walk(src):
        for f in fs:
            if f.endswith(".py"): p=os.path.join(r,f); z.write(p,os.path.relpath(p,src))
zb=buf.getvalue()
try: lam.get_function(FunctionName=FN); ex=True
except lam.exceptions.ResourceNotFoundException: ex=False
ENV={"Variables":{"S3_BUCKET":B}}
if ex:
    lam.update_function_code(FunctionName=FN,ZipFile=zb)
    for _ in range(30):
        if lam.get_function(FunctionName=FN)["Configuration"].get("LastUpdateStatus")!="InProgress":break
        time.sleep(3)
    lam.update_function_configuration(FunctionName=FN,Environment=ENV,Timeout=600,MemorySize=1024,Runtime="python3.12",Handler="lambda_function.lambda_handler"); print("updated")
else:
    lam.create_function(FunctionName=FN,Runtime="python3.12",Role=ROLE,Handler="lambda_function.lambda_handler",Code={"ZipFile":zb},Timeout=600,MemorySize=1024,Environment=ENV,Architectures=["x86_64"],Description="Edge discovery research factory"); print("created")
for _ in range(40):
    c=lam.get_function(FunctionName=FN)["Configuration"]
    if c.get("State")=="Active" and c.get("LastUpdateStatus") in ("Successful",None):break
    time.sleep(3)
arn=c["FunctionArn"]
rule="justhodl-edge-discovery-weekly"
rarn=events.put_rule(Name=rule,ScheduleExpression="cron(0 11 ? * SUN *)",State="ENABLED")["RuleArn"]
try: lam.add_permission(FunctionName=FN,StatementId="evt-ed",Action="lambda:InvokeFunction",Principal="events.amazonaws.com",SourceArn=rarn)
except lam.exceptions.ResourceConflictException: pass
events.put_targets(Rule=rule,Targets=[{"Id":"1","Arn":arn}]); print("scheduled")
try:
    r=lam.invoke(FunctionName=FN,InvocationType="RequestResponse"); print("invoke:",r["StatusCode"],r["Payload"].read().decode()[:240])
except Exception as e: print("invoke note (engine still completes):",str(e)[:90])
time.sleep(3)
d=json.loads(s3.get_object(Bucket=B,Key="data/edge-discovery.json")["Body"].read())
mt=d.get("multiple_testing",{})
print(f"\nRESEARCH FACTORY: tested {d.get('n_candidates_tested')} candidates | history {d.get('history_days')}d")
print(f"  multiple-testing: null per-trade SR {mt.get('expected_max_sharpe_per_trade_null')} | FDR survivors {mt.get('fdr_survivors')} | graduated {d.get('n_graduated_this_run')}")
print("  TOP candidates by Deflated-Sharpe (most will fail the bar — that's the discipline):")
for c in d.get("top_candidates",[])[:8]:
    print(f"    {c['feature']:<18}->{c['target']:<4} H{c['horizon']} dir{c['direction']:+d} | OOS Sharpe {c['sharpe_ann']:+.2f} DSR {c['dsr']} hit {c['hit_pct']}% n{c['n_trades']} | grad={c['graduate']}")
print("  graduated_live:",len(d.get("graduated_live",[])),"| retired:",len(d.get("retired_this_run",[])))
print("DONE 2063")
