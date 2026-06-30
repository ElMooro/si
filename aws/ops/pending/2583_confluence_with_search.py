"""ops 2583 — redeploy attention-confluence with search-attention wired; verify STEALTH populates."""
import boto3, io, zipfile, json, time
REGION="us-east-1"; FN="justhodl-attention-confluence"
SRC="aws/lambdas/justhodl-attention-confluence/source/lambda_function.py"
lam=boto3.client("lambda",region_name=REGION); s3=boto3.client("s3",region_name=REGION)
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z: z.writestr("lambda_function.py",open(SRC,"rb").read())
lam.update_function_code(FunctionName=FN, ZipFile=buf.getvalue())
for _ in range(15):
    if lam.get_function(FunctionName=FN)["Configuration"].get("LastUpdateStatus")=="Successful": break
    time.sleep(3)
r=lam.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=b"{}")
print("INVOKE:", r.get("StatusCode"), r.get("FunctionError"), r["Payload"].read().decode()[:260])
time.sleep(2)
j=json.loads(s3.get_object(Bucket="justhodl-dashboard-live", Key="data/attention-confluence.json")["Body"].read())
print("counts:", j.get("counts"))
for sec in ["stealth","igniting","undiscovered","crowded","distribution"]:
    rows=j.get("stages",{}).get(sec,[])
    ex=[(x["symbol"],f"s{x['smart_score']}",f"c{x['crowd_score']}",f"cf{x['confluence_smart']}") for x in rows[:5]]
    print(f"  {sec} ({len(rows)}): {ex}")
for x in j.get("stages",{}).get("stealth",[])[:4]:
    print("   STEALTH:", x["symbol"], x.get("layer"), "->", x.get("why"))
print("DONE 2583")
