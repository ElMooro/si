"""1966 — redeploy flow-lookthrough w/ top_picks, regenerate, run harvester, confirm ledger pickup."""
import boto3, json, io, zipfile, time
lam=boto3.client("lambda","us-east-1"); s3=boto3.client("s3","us-east-1")
ddb=boto3.resource("dynamodb","us-east-1")
FN="justhodl-flow-lookthrough"
src=open("aws/lambdas/justhodl-flow-lookthrough/source/lambda_function.py","rb").read()
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z:
    zi=zipfile.ZipInfo("lambda_function.py"); zi.external_attr=0o644<<16; z.writestr(zi,src)
for _ in range(24):
    try: lam.update_function_code(FunctionName=FN, ZipFile=buf.getvalue()); break
    except lam.exceptions.ResourceConflictException: time.sleep(5)
for _ in range(30):
    c=lam.get_function_configuration(FunctionName=FN)
    if c.get("State")=="Active" and c.get("LastUpdateStatus")!="InProgress": break
    time.sleep(3)
print("redeployed flow-lookthrough")
lam.invoke(FunctionName=FN, InvocationType="RequestResponse")
time.sleep(2)
j=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/flow-lookthrough.json")["Body"].read())
tp=j.get("top_picks",[])
print("top_picks:", [(x["ticker"],x["score"]) for x in tp])

print("\ninvoking signal-harvester...")
r=lam.invoke(FunctionName="justhodl-signal-harvester", InvocationType="RequestResponse")
print("harvester StatusCode:", r.get("StatusCode"), "err:", r.get("FunctionError"))
print("payload:", r["Payload"].read()[:300])
time.sleep(2)
# confirm ledger pickup
try:
    summ=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/_harvest/last-run.json")["Body"].read())
    engines=summ.get("engines") or summ.get("per_engine") or {}
    hit=[e for e in (engines if isinstance(engines,list) else engines.keys()) if "flow-lookthrough" in str(e)]
    print("\nharvest summary keys:", list(summ.keys())[:10])
    print("flow-lookthrough in summary:", hit or "checking DDB...")
except Exception as e:
    print("summary read err:", e)
# scan DDB for eng:flow-lookthrough rows
try:
    t=ddb.Table("justhodl-signals")
    resp=t.scan(FilterExpression="signal_type = :s",
                ExpressionAttributeValues={":s":"eng:flow-lookthrough"}, Limit=200)
    items=resp.get("Items",[])
    print(f"\nDDB justhodl-signals rows for eng:flow-lookthrough: {len(items)}")
    for it in items[:6]:
        print("  ", it.get("signal_id"), it.get("predicted_direction"), "conf=",it.get("confidence"))
except Exception as e:
    print("DDB scan err:", e)
print("DONE 1966")
