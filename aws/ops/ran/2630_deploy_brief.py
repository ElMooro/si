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
b=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/liquidity-inflection-decisive-call.json")["Body"].read())
print("\n=== DESK BRIEFING (decisive-call feed) ===")
print("HEADLINE:",b.get("headline"))
print("\nTHE CALL:",b.get("the_call"))
print("\nFWD EXP:",b.get("forward_expectation"))
print("\nHIDDEN RISKS:")
for r in b.get("hidden_risks") or []: print("  •",r)
print("\nANALOG:",b.get("nearest_analog"))
print("RUNWAY:",b.get("reserve_runway"))
print("TIMING:",b.get("does_timing_work"))
print("MODEL:",b.get("model"))
print("DONE 2630")
