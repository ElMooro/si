import boto3, io, zipfile, json, time
REGION="us-east-1"; FN="justhodl-attention-confluence"
SRC="aws/lambdas/justhodl-attention-confluence/source/lambda_function.py"
lam=boto3.client("lambda",region_name=REGION); s3=boto3.client("s3",region_name=REGION)
def wait():
    for _ in range(25):
        c=lam.get_function(FunctionName=FN)["Configuration"]
        if c.get("LastUpdateStatus")=="Successful" and c.get("State")=="Active": return
        time.sleep(4)
wait()
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z: z.writestr("lambda_function.py",open(SRC,"rb").read())
for a in range(6):
    try: lam.update_function_code(FunctionName=FN, ZipFile=buf.getvalue()); break
    except lam.exceptions.ResourceConflictException: time.sleep(12); wait()
wait()
r=lam.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=b"{}")
print("INVOKE:", r.get("StatusCode"), r.get("FunctionError"), r["Payload"].read().decode()[:200])
time.sleep(2)
j=json.loads(s3.get_object(Bucket="justhodl-dashboard-live", Key="data/attention-confluence.json")["Body"].read())
print("counts:", j.get("counts"))
# names where buyback family now fires
bbnames=[(v["symbol"],v["smart_score"],v["stage"],v["families_firing"]) for v in j.get("tickers",{}).values() if "buyback" in (v.get("families_firing") or [])]
print(f"buyback family firing on {len(bbnames)} names; e.g.:", bbnames[:6])
print("corporate_buybacks panel:", len(j.get("panels",{}).get("corporate_buybacks",[])), j.get("panels",{}).get("corporate_buybacks",[])[:3])
# show a stealth/igniting name that now includes buyback
for sec in ["stealth","igniting","undiscovered"]:
    for x in j.get("stages",{}).get(sec,[]):
        if "buyback" in (x.get("families_firing") or []):
            print(f"  {sec} w/ buyback:", x["symbol"], x["smart_score"], x.get("why")); break
print("DONE 2595")
