"""1965 — idempotent: update flow-lookthrough code, ensure schedule, invoke, verify."""
import boto3, json, io, zipfile, time
lam=boto3.client("lambda","us-east-1"); events=boto3.client("events","us-east-1")
s3=boto3.client("s3","us-east-1")
ACCT="857687956942"; FN="justhodl-flow-lookthrough"; RULE="justhodl-flow-lookthrough-daily"
REGION="us-east-1"; CRON="cron(15 23 * * ? *)"

src=open("aws/lambdas/justhodl-flow-lookthrough/source/lambda_function.py","rb").read()
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z:
    zi=zipfile.ZipInfo("lambda_function.py"); zi.external_attr=0o644<<16; z.writestr(zi,src)
zb=buf.getvalue()
for _ in range(24):
    try: lam.update_function_code(FunctionName=FN, ZipFile=zb); break
    except lam.exceptions.ResourceConflictException: time.sleep(5)
# ensure config (mem/timeout)
for _ in range(24):
    try:
        lam.update_function_configuration(FunctionName=FN, Timeout=600, MemorySize=1024,
            Environment={"Variables":{"S3_BUCKET":"justhodl-dashboard-live"}}); break
    except lam.exceptions.ResourceConflictException: time.sleep(5)
for _ in range(30):
    c=lam.get_function_configuration(FunctionName=FN)
    if c.get("State")=="Active" and c.get("LastUpdateStatus")!="InProgress": break
    time.sleep(3)
print("code+config updated; mem", c.get("MemorySize"), "timeout", c.get("Timeout"))

events.put_rule(Name=RULE, ScheduleExpression=CRON, State="ENABLED", Description="daily flow look-through")
events.put_targets(Rule=RULE, Targets=[{"Id":"1","Arn":f"arn:aws:lambda:{REGION}:{ACCT}:function:{FN}"}])
sid=f"{RULE}-invoke"
try: lam.remove_permission(FunctionName=FN, StatementId=sid)
except Exception: pass
lam.add_permission(FunctionName=FN, StatementId=sid, Action="lambda:InvokeFunction",
    Principal="events.amazonaws.com", SourceArn=f"arn:aws:events:{REGION}:{ACCT}:rule/{RULE}")
d=events.describe_rule(Name=RULE); print("schedule:", d["State"], d["ScheduleExpression"])

print("invoking (sync)...")
r=lam.invoke(FunctionName=FN, InvocationType="RequestResponse")
print("StatusCode:", r.get("StatusCode"), "FunctionError:", r.get("FunctionError"))
print("payload:", r["Payload"].read()[:400])
time.sleep(3)
j=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/flow-lookthrough.json")["Body"].read())
print("\noutput: n_names=",j.get("n_names"),"n_etfs_used=",j.get("n_etfs_used"),"flows_asof=",j.get("flows_asof"),"elapsed=",j.get("elapsed_s"))
print("\nTOP INFLOW (ticker | net5d$M | type | bps_mcap | n_etfs | top driver):")
for x in j.get("inflow_leaders",[])[:8]:
    d0=(x.get("drivers") or [{}])[0]
    print(f"  {x['ticker']:<6} {x['net_flow_5d_usd']/1e6:>9.1f} {x['flow_type']:<17} {x.get('flow_bps_mcap')!s:<8} {x['n_etfs']} {d0.get('etf')}")
print("\nTOP OUTFLOW:")
for x in j.get("outflow_leaders",[])[:5]:
    print(f"  {x['ticker']:<6} {x['net_flow_5d_usd']/1e6:>9.1f} {x['flow_type']}")
print("\nTHEMATIC ROTATION LEADERS (mcap-normalised, the alpha view):")
for x in j.get("thematic_rotation_leaders",[])[:8]:
    print(f"  {x['ticker']:<6} net5d=${x['net_flow_5d_usd']/1e6:>8.1f}M bps_mcap={x.get('flow_bps_mcap')} thematic=${x['thematic_flow_5d_usd']/1e6:.1f}M")
print("DONE 1965")
