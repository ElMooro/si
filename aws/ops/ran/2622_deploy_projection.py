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
print("INVOKE:",r.get("StatusCode"),r.get("FunctionError"),r["Payload"].read().decode()[:120])
time.sleep(2)
j=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/liquidity-inflection.json")["Body"].read())
p=j.get("projection") or {}
print("PROJECTION:", p.get("headline"))
print("  current $",p.get("current_net_liq_bn"),"bn → projected $",p.get("projected_net_liq_bn"),"bn (",p.get("projected_change_bn"),"bn)")
print("  weekly pace $",p.get("weekly_pace_bn"),"bn · primary:",p.get("primary_driver"))
print("  drivers/wk:",p.get("drivers_per_wk_bn"))
print("  hist pts:",len(p.get("history") or []),"path pts:",len(p.get("path") or []))
print("  path tail:",(p.get("path") or [])[-2:])
print("DONE 2622")
