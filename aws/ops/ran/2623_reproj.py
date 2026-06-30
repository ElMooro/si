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
lam.invoke(FunctionName=FN,InvocationType="RequestResponse",Payload=b"{}")
time.sleep(2)
p=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/liquidity-inflection.json")["Body"].read()).get("projection") or {}
print("HEADLINE:",p.get("headline"))
print("  cur $",p.get("current_net_liq_bn"),"→ proj $",p.get("projected_net_liq_bn"),"(",p.get("projected_change_bn"),"bn) pace",p.get("weekly_pace_bn"),"/wk")
print("  drivers/wk:",p.get("drivers_per_wk_bn"),"primary:",p.get("primary_driver"))
print("  hist0:",(p.get('history') or [{}])[0],"histN:",(p.get('history') or [{}])[-1])
print("  path1:",(p.get('path') or [{}])[0],"pathN:",(p.get('path') or [{}])[-1])
print("DONE 2623")
