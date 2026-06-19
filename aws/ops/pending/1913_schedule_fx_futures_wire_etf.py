import boto3, json, zipfile, io, glob, time
from botocore.config import Config
from botocore.exceptions import ClientError
ev=boto3.client("events","us-east-1"); lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=180,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1"); B="justhodl-dashboard-live"; ACCT="857687956942"; REGION="us-east-1"
# A) schedule the FX + futures engines (they work, just had no rule)
for fn,rule,cron in [("justhodl-polygon-fx-regime","polygon-fx-regime-daily","cron(10 13 * * ? *)"),
                     ("justhodl-polygon-futures-curves","polygon-futures-curves-daily","cron(20 13 * * ? *)")]:
    ev.put_rule(Name=rule,ScheduleExpression=cron,State="ENABLED",Description="daily")
    arn=lam.get_function(FunctionName=fn)["Configuration"]["FunctionArn"]
    try:
        lam.add_permission(FunctionName=fn,StatementId=rule+"-evt",Action="lambda:InvokeFunction",
            Principal="events.amazonaws.com",SourceArn="arn:aws:events:%s:%s:rule/%s"%(REGION,ACCT,rule))
    except ClientError as e:
        if "ResourceConflict" not in str(e): print("perm",str(e)[:40])
    ev.put_targets(Rule=rule,Targets=[{"Id":"1","Arn":arn}])
    lam.invoke(FunctionName=fn,InvocationType="Event")
    print("SCHEDULED + refreshed:",fn,"->",cron)
# B) redeploy re-rating radar with ETF kicker
FN="justhodl-ai-rerating-radar"
src=open(glob.glob("**/justhodl-ai-rerating-radar/source/lambda_function.py",recursive=True)[0]).read()
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z: z.writestr("lambda_function.py",src)
for _ in range(40):
    st=lam.get_function_configuration(FunctionName=FN)
    if st.get("State")=="Active" and st.get("LastUpdateStatus")!="InProgress": break
    time.sleep(3)
for _ in range(24):
    try: lam.update_function_code(FunctionName=FN,ZipFile=buf.getvalue()); break
    except ClientError as e:
        if "ResourceConflict" in str(e): time.sleep(5); continue
        raise
for _ in range(40):
    st=lam.get_function_configuration(FunctionName=FN)
    if st.get("State")=="Active" and st.get("LastUpdateStatus")!="InProgress": break
    time.sleep(3)
r=lam.invoke(FunctionName=FN,InvocationType="RequestResponse"); print("\nRADAR INVOKE:",r["Payload"].read().decode()[:120])
time.sleep(2)
d=json.loads(s3.get_object(Bucket=B,Key="data/ai-rerating-radar.json")["Body"].read())
sets=(d.get("summary",{}) or {}).get("top_setups",[]) or []
print("IWM small-cap bid active:", any(x.get("smallcap_bid") for x in sets))
print("candidates with sector ETF inflow:", [x["symbol"] for x in sets if (x.get("etf_sector_flow_z") or 0)>=1.0][:8] or "none currently")
print("\nTOP 6 (note ETF flow in why):")
for x in sets[:6]:
    print("  %-6s comp=%-6s etf=%s(%s) | %s"%(x["symbol"],x["composite"],x.get("etf_sector"),x.get("etf_sector_flow_z"),(x.get("why") or "")[:78]))
