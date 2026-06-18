import io, json, zipfile, time, boto3
lam=boto3.client("lambda",region_name="us-east-1"); s3=boto3.client("s3",region_name="us-east-1")
FN="justhodl-refining-stress"; SRC="aws/lambdas/justhodl-refining-stress/source/lambda_function.py"
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z: z.write(SRC,"lambda_function.py")
lam.update_function_code(FunctionName=FN, ZipFile=buf.getvalue())
lam.get_waiter("function_updated").wait(FunctionName=FN)
# ensure FMP_KEY/FRED_KEY in env
env={"Variables":{"FMP_KEY":"wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb","FRED_KEY":"2f057499936072679d8843d7fce99989","DASH_BUCKET":"justhodl-dashboard-live"}}
lam.update_function_configuration(FunctionName=FN, Environment=env)
lam.get_waiter("function_updated").wait(FunctionName=FN)
inv=lam.invoke(FunctionName=FN, InvocationType="RequestResponse")
print("invoke:", inv["Payload"].read().decode()[:300])
time.sleep(2)
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/refining-stress.json")["Body"].read())
print("REGIME=%s | %s"%(d.get("regime"),d.get("summary")))
print("data_quality:", json.dumps(d.get("data_quality")))
print("errors:", d.get("errors"))
for m in d.get("metrics",[]):
    print("  [%s] %-40s = %s%s  (pctile %s, asof %s)"%(m["status"],m["label"][:40],m["value"],m["unit"],m.get("percentile"),m.get("asof")))
