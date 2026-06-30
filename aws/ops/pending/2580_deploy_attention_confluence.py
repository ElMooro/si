"""ops 2580 — create + invoke + verify justhodl-attention-confluence (brand-new fn)."""
import boto3, io, zipfile, json, time, os
REGION="us-east-1"; FN="justhodl-attention-confluence"
ROLE="arn:aws:iam::857687956942:role/lambda-execution-role"
SRC="aws/lambdas/justhodl-attention-confluence/source/lambda_function.py"
lam=boto3.client("lambda",region_name=REGION); s3=boto3.client("s3",region_name=REGION)

code=open(SRC,"rb").read()
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z: z.writestr("lambda_function.py",code)
zb=buf.getvalue()

# create or update
exists=False
try:
    lam.get_function(FunctionName=FN); exists=True
except lam.exceptions.ResourceNotFoundException:
    exists=False
if not exists:
    try:
        lam.create_function(FunctionName=FN, Runtime="python3.12", Role=ROLE,
            Handler="lambda_function.lambda_handler", Code={"ZipFile":zb},
            Timeout=120, MemorySize=512, Environment={"Variables":{"S3_BUCKET":"justhodl-dashboard-live"}},
            Description="Smart accumulation vs crowd attention fusion")
        print("CREATED", FN)
        time.sleep(8)
    except Exception as e:
        print("create err (maybe race):", str(e)[:120])
        time.sleep(8)
        lam.update_function_code(FunctionName=FN, ZipFile=zb); print("UPDATED after race")
else:
    lam.update_function_configuration(FunctionName=FN, Timeout=120, MemorySize=512,
        Environment={"Variables":{"S3_BUCKET":"justhodl-dashboard-live"}})
    time.sleep(5)
    lam.update_function_code(FunctionName=FN, ZipFile=zb); print("UPDATED", FN)
# wait for ready
for _ in range(15):
    st=lam.get_function(FunctionName=FN)["Configuration"].get("LastUpdateStatus")
    if st=="Successful": break
    time.sleep(3)

# invoke synchronously
r=lam.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=b"{}")
payload=r["Payload"].read().decode("utf-8","ignore")
print("INVOKE status:", r.get("StatusCode"), "| fnerr:", r.get("FunctionError"))
print("RESULT:", payload[:400])

# verify output written
time.sleep(2)
try:
    j=json.loads(s3.get_object(Bucket="justhodl-dashboard-live", Key="data/attention-confluence.json")["Body"].read())
    print("\nOUTPUT v", j.get("version"), "n_scored", j.get("n_scored"), "universe", j.get("universe_n"))
    print("counts:", j.get("counts"))
    for sec in ["stealth","igniting","crowded","distribution"]:
        rows=j.get("stages",{}).get(sec,[])
        ex=[(x["symbol"],x["smart_score"],x["crowd_score"],x["confluence_smart"]) for x in rows[:4]]
        print(f"  {sec}: {len(rows)} | {ex}")
    print("panels:", {k:len(v) for k,v in j.get("panels",{}).items()})
except Exception as e:
    print("verify err:", str(e)[:150])
print("DONE 2580")
