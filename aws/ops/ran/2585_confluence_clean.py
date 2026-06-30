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
print("INVOKE:", r.get("StatusCode"), r.get("FunctionError"), r["Payload"].read().decode()[:240])
time.sleep(2)
j=json.loads(s3.get_object(Bucket="justhodl-dashboard-live", Key="data/attention-confluence.json")["Body"].read())
print("counts:", j.get("counts"))
allt=[x["symbol"] for sec in j.get("stages",{}).values() for x in sec]
print("junk present?:", [t for t in allt if t in ("NONE","NULL") or t.isdigit()])
print("DONE 2585")
