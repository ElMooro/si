"""ops 2584 — wait for any in-progress update, ensure latest code, invoke + verify STEALTH."""
import boto3, io, zipfile, json, time
REGION="us-east-1"; FN="justhodl-attention-confluence"
SRC="aws/lambdas/justhodl-attention-confluence/source/lambda_function.py"
lam=boto3.client("lambda",region_name=REGION); s3=boto3.client("s3",region_name=REGION)
def wait_ready(n=25):
    for _ in range(n):
        c=lam.get_function(FunctionName=FN)["Configuration"]
        if c.get("LastUpdateStatus")=="Successful" and c.get("State")=="Active": return True
        time.sleep(4)
    return False
wait_ready()
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z: z.writestr("lambda_function.py",open(SRC,"rb").read())
zb=buf.getvalue()
for attempt in range(6):
    try:
        lam.update_function_code(FunctionName=FN, ZipFile=zb); print("code updated"); break
    except lam.exceptions.ResourceConflictException:
        print(f"  conflict, wait ({attempt})"); time.sleep(12); wait_ready()
wait_ready()
r=lam.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=b"{}")
print("INVOKE:", r.get("StatusCode"), r.get("FunctionError"), r["Payload"].read().decode()[:260])
time.sleep(2)
j=json.loads(s3.get_object(Bucket="justhodl-dashboard-live", Key="data/attention-confluence.json")["Body"].read())
print("counts:", j.get("counts"))
for sec in ["stealth","igniting","undiscovered","crowded","distribution"]:
    rows=j.get("stages",{}).get(sec,[])
    ex=[(x["symbol"],f"s{x['smart_score']}",f"c{x['crowd_score']}",f"cf{x['confluence_smart']}") for x in rows[:5]]
    print(f"  {sec} ({len(rows)}): {ex}")
for x in j.get("stages",{}).get("stealth",[])[:5]:
    print("   STEALTH:", x["symbol"], x.get("layer"), "div", x.get("divergence"), "->", x.get("why"))
print("DONE 2584")
