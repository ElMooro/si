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
cc=j.get("composite_clock") or {}
print("\nCOMPOSITE CYCLE CLOCK:")
print("  PHASE:",cc.get("phase"),"| impulse",cc.get("impulse"),"accel",cc.get("acceleration"))
print("  rotation:",cc.get("rotation"),"| components:",cc.get("components_used"))
print("  orbit pts:",len(cc.get("orbit") or []))
cp=j.get("composite_projection") or {}
print("\nCOMPOSITE PROJECTION:")
print("  HEADLINE:",cp.get("headline"))
print("  cur z",cp.get("current_z"),"score",cp.get("current_score"),"→ proj z",cp.get("projected_z"),"score",cp.get("projected_score"),"(",cp.get("projected_change_z"),"z)")
print("  contributions:",cp.get("contributions"))
print("  primary:",cp.get("primary_driver"),"| components:",cp.get("components_used"))
print("  hist pts:",len(cp.get("history") or []),"path pts:",len(cp.get("path") or []))
print("DONE 2632")
