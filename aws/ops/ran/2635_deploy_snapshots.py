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
lam.invoke(FunctionName=FN,InvocationType="RequestResponse",Payload=b"{}"); time.sleep(2)
j=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/liquidity-inflection.json")["Body"].read())
sm=j.get("composite_snapshots") or {}
print("SNAPSHOT META:",sm)
cc=j.get("composite_clock") or {}; cp=j.get("composite_projection") or {}
print("composite_clock source:",cc.get("source"),"| phase",cc.get("phase"))
print("composite_projection source:",cp.get("source"),"| ",cp.get("current_score"),"->",cp.get("projected_score"))
# inspect snapshot file
try:
    snf=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/composite-snapshots.json")["Body"].read())
    print("SNAPSHOTS FILE: count",snf.get("count"),"first",snf.get("first"),"last",snf.get("last"))
    if snf.get("snapshots"):
        s0=snf["snapshots"][-1]
        print("  latest snap: date",s0["date"],"comp_z",s0["comp_z"],"score",s0["score"],"components",len(s0.get("components") or {}),"weights",len(s0.get("weights") or {}))
        print("  component names:",list((s0.get("components") or {}).keys()))
except Exception as e: print("snapshot file err:",e)
print("DONE 2635")
