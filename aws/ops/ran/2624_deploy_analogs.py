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
a=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/liquidity-inflection.json")["Body"].read()).get("analogs") or {}
print("ANALOGS as_of:",a.get("as_of"),"features:",a.get("features"))
print("fingerprint now:",a.get("fingerprint_now"))
for an in (a.get("analogs") or []):
    print(f"  {an['date']} · sim {an['similarity_pct']}% (dist {an['distance']}) · SPX fwd 21d {an['spx_fwd_21d']}% / 63d {an['spx_fwd_63d']}% · fp {an['fingerprint']}")
print("DONE 2624")
