import boto3, io, zipfile, json, time
REGION="us-east-1"; FN="justhodl-liquidity-inflection"; SRC=f"aws/lambdas/{FN}/source/lambda_function.py"
lam=boto3.client("lambda",region_name=REGION); s3=boto3.client("s3",region_name=REGION)
def wait():
    for _ in range(30):
        c=lam.get_function(FunctionName=FN)["Configuration"]
        if c.get("State")=="Active" and c.get("LastUpdateStatus")=="Successful": return
        time.sleep(4)
wait()
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z: z.writestr("lambda_function.py",open(SRC,"rb").read())
for _ in range(6):
    try: lam.update_function_code(FunctionName=FN,ZipFile=buf.getvalue()); print("deployed"); break
    except lam.exceptions.ResourceConflictException: time.sleep(12); wait()
wait()
r=lam.invoke(FunctionName=FN,InvocationType="RequestResponse",Payload=b"{}")
print("INVOKE:",r.get("StatusCode"),r.get("FunctionError"))
time.sleep(2)
j=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/liquidity-inflection.json")["Body"].read())
rr=j.get("reserve_runway") or {}
print("RUNWAY:",rr.get("read"))
print("  level $",rr.get("level_usd_bn"),"bn pace",rr.get("weekly_pace_bn"),"status",rr.get("status"),"wks_to_danger",rr.get("weeks_to_danger"))
fe=j.get("forward_expectation") or {}
print("FWD EXPECTATION: state",fe.get("state"),"z",fe.get("impulse_z"))
for a,v in (fe.get("assets") or {}).items():
    print(f"   {a}: 21d {v['mean']}% (excess {v['excess']}%, n{v['n']}, {v['hit_pct']}%✓{' *sig' if v['sig'] else ''})")
ts=j.get("tensions") or {}
print("TENSIONS:",ts.get("level"),"count",ts.get("count"))
for t in (ts.get("items") or []): print(f"   [{t['severity']}] {t['signal']}: {t['note'][:90]}")
print("DATA HEALTH:")
for dh in (j.get("data_health") or []): print(f"   {dh['feed']}: {dh['as_of']} ({dh['age_days']}d, {dh['status']})")
print("DONE 2628")
