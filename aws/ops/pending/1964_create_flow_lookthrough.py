"""1964 — create justhodl-flow-lookthrough (new dir => deploy-lambdas no-ops),
attach daily schedule, invoke once, verify output."""
import boto3, json, io, zipfile, time, os
lam=boto3.client("lambda","us-east-1"); events=boto3.client("events","us-east-1")
s3=boto3.client("s3","us-east-1")
ACCT="857687956942"; ROLE=f"arn:aws:iam::{ACCT}:role/lambda-execution-role"
FN="justhodl-flow-lookthrough"; RULE="justhodl-flow-lookthrough-daily"
CRON="cron(15 23 * * ? *)"; REGION="us-east-1"

src=open("aws/lambdas/justhodl-flow-lookthrough/source/lambda_function.py","rb").read()
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z:
    zi=zipfile.ZipInfo("lambda_function.py"); zi.external_attr=0o644<<16
    z.writestr(zi,src)
zb=buf.getvalue()

# create or update
try:
    lam.get_function(FunctionName=FN)
    exists=True
except lam.exceptions.ResourceNotFoundException:
    exists=False

if not exists:
    lam.create_function(FunctionName=FN, Runtime="python3.12", Role=ROLE,
        Handler="lambda_function.lambda_handler", Code={"ZipFile": zb},
        Timeout=600, MemorySize=1024,
        Environment={"Variables":{"S3_BUCKET":"justhodl-dashboard-live"}},
        Description="Single-name flow-pressure board via ETF constituent look-through",
        Tags={"Project":"JustHodl","Component":"flow-intelligence","Tier":"lookthrough"})
    print("created", FN)
else:
    for _ in range(24):
        try:
            lam.update_function_code(FunctionName=FN, ZipFile=zb); break
        except lam.exceptions.ResourceConflictException:
            time.sleep(5)
    print("updated code", FN)

# wait active
for _ in range(30):
    c=lam.get_function_configuration(FunctionName=FN)
    if c.get("State")=="Active" and c.get("LastUpdateStatus")!="InProgress": break
    time.sleep(3)

# schedule
events.put_rule(Name=RULE, ScheduleExpression=CRON, State="ENABLED",
                Description="daily flow look-through")
events.put_targets(Rule=RULE, Targets=[{"Id":"1","Arn":f"arn:aws:lambda:{REGION}:{ACCT}:function:{FN}"}])
sid=f"{RULE}-invoke"
try: lam.remove_permission(FunctionName=FN, StatementId=sid)
except Exception: pass
lam.add_permission(FunctionName=FN, StatementId=sid, Action="lambda:InvokeFunction",
    Principal="events.amazonaws.com", SourceArn=f"arn:aws:events:{REGION}:{ACCT}:rule/{RULE}")
print("schedule wired:", CRON)

# invoke (sync) to verify
print("invoking (sync, may take ~1-2min for constituent fetches)...")
r=lam.invoke(FunctionName=FN, InvocationType="RequestResponse")
print("StatusCode:", r.get("StatusCode"), "FunctionError:", r.get("FunctionError"))
print("payload:", r["Payload"].read()[:400])

time.sleep(3)
try:
    j=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/flow-lookthrough.json")["Body"].read())
    print("\noutput: n_names=",j.get("n_names"),"n_etfs_used=",j.get("n_etfs_used"),"elapsed=",j.get("elapsed_s"))
    print("\nTOP INFLOW (ticker | net5d$M | type | bps_mcap | n_etfs | top driver):")
    for x in j.get("inflow_leaders",[])[:8]:
        d=x.get("drivers",[{}])[0]
        print(f"  {x['ticker']:<6} {x['net_flow_5d_usd']/1e6:>9.1f} {x['flow_type']:<17} {x.get('flow_bps_mcap')!s:<8} {x['n_etfs']} {d.get('etf')}")
    print("\nTHEMATIC ROTATION LEADERS (rotation, mcap-normalised):")
    for x in j.get("thematic_rotation_leaders",[])[:8]:
        print(f"  {x['ticker']:<6} net5d=${x['net_flow_5d_usd']/1e6:>8.1f}M bps_mcap={x.get('flow_bps_mcap')} thematic=${x['thematic_flow_5d_usd']/1e6:.1f}M")
except Exception as e:
    print("read output ERR:", e)
print("DONE 1964")
