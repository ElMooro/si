import boto3, json, zipfile, io, glob, time
from botocore.exceptions import ClientError
from collections import Counter
from boto3.dynamodb.conditions import Attr
lam=boto3.client("lambda","us-east-1"); s3=boto3.client("s3","us-east-1"); events=boto3.client("events","us-east-1")
B="justhodl-dashboard-live"; FN="justhodl-signal-harvester"; REGION="us-east-1"; ACCT="857687956942"
ROLE="arn:aws:iam::857687956942:role/lambda-execution-role"; RULE="signal-harvester-daily"
src=open(glob.glob("**/justhodl-signal-harvester/source/lambda_function.py",recursive=True)[0]).read()
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z: z.writestr("lambda_function.py",src)
code=buf.getvalue()
try:
    lam.create_function(FunctionName=FN,Runtime="python3.12",Role=ROLE,Handler="lambda_function.lambda_handler",
        Code={"ZipFile":code},Timeout=300,MemorySize=512,Architectures=["x86_64"],
        Description="Universal signal harvester -> justhodl-signals truth ledger (full-fleet coverage)")
    print("CREATED function")
except ClientError as e:
    if "already exist" in str(e).lower() or "ResourceConflict" in str(e):
        for _ in range(24):
            try: lam.update_function_code(FunctionName=FN,ZipFile=code); print("UPDATED existing"); break
            except ClientError as e2:
                if "ResourceConflict" in str(e2): time.sleep(5); continue
                raise
    else: raise
for _ in range(60):
    st=lam.get_function_configuration(FunctionName=FN)
    if st.get("State")=="Active" and st.get("LastUpdateStatus")!="InProgress": break
    time.sleep(3)
events.put_rule(Name=RULE,ScheduleExpression="cron(15 23 * * ? *)",State="ENABLED",Description="Daily universal signal harvest 23:15 UTC")
arn=lam.get_function(FunctionName=FN)["Configuration"]["FunctionArn"]
try:
    lam.add_permission(FunctionName=FN,StatementId="signal-harvester-daily-evt",Action="lambda:InvokeFunction",
        Principal="events.amazonaws.com",SourceArn="arn:aws:events:%s:%s:rule/%s"%(REGION,ACCT,RULE))
except ClientError as e:
    if "ResourceConflict" not in str(e): print("perm note:",str(e)[:50])
events.put_targets(Rule=RULE,Targets=[{"Id":"1","Arn":arn}])
print("SCHEDULED daily 23:15 UTC")
r=lam.invoke(FunctionName=FN,InvocationType="RequestResponse")
print("INVOKE:",r["Payload"].read().decode()[:260])
time.sleep(2)
summ=json.loads(s3.get_object(Bucket=B,Key="data/_harvest/last-run.json")["Body"].read())
print("\nSUMMARY:")
for k in ("n_engine_outputs_scanned","n_engines_with_picks","n_harvested","n_written","n_skipped_no_price","regime_at_log","elapsed_s"):
    print("  %-26s %s"%(k,summ.get(k)))
tbl=boto3.resource("dynamodb","us-east-1").Table("justhodl-signals")
resp=tbl.scan(FilterExpression=Attr("signal_type").begins_with("eng:") & Attr("status").eq("pending"),Limit=3000)
items=resp.get("Items",[])
c=Counter(i["signal_type"] for i in items)
print("\nLEDGER CHECK: harvested 'eng:' pending signals in sample scan=%d across %d distinct engines"%(len(items),len(c)))
for st,n in c.most_common(10): print("   %-34s %d"%(st,n))
if items:
    sx=items[0]
    print("sample:",{k:str(sx.get(k))[:26] for k in ("signal_type","measure_against","predicted_direction","baseline_price","confidence","regime_at_log","check_windows")})
