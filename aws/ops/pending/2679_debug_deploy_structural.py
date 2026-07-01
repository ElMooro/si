"""ops 2679 — deploy diagnostic version to see exactly why restructuring returned 0
despite an identical manual query finding 7 hits."""
import boto3, io, zipfile, json, time
from botocore.config import Config
REGION="us-east-1"; FN="justhodl-structural-pre-signals"; SRC=f"aws/lambdas/{FN}/source/lambda_function.py"
lam=boto3.client("lambda",region_name=REGION, config=Config(read_timeout=150, connect_timeout=10, retries={"max_attempts":0}))
s3=boto3.client("s3",region_name=REGION)
def wait():
    for i in range(40):
        c=lam.get_function(FunctionName=FN)["Configuration"]
        if c.get("State")=="Active" and c.get("LastUpdateStatus")=="Successful": return
        time.sleep(5)
wait()
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z: z.writestr("lambda_function.py",open(SRC,"rb").read())
for _ in range(6):
    try: lam.update_function_code(FunctionName=FN,ZipFile=buf.getvalue()); print("deployed"); break
    except lam.exceptions.ResourceConflictException: time.sleep(10); wait()
wait()
r=lam.invoke(FunctionName=FN,InvocationType="RequestResponse",Payload=b"{}")
print("INVOKE:",r.get("StatusCode"),r.get("FunctionError"))
print("BODY:", r["Payload"].read().decode()[:400])
time.sleep(2)
j=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/structural-pre-signals.json")["Body"].read())
dbg = j.get("_debug", {})
print("\n=== DEBUG INFO ===")
print("restructuring_query_url:", dbg.get("restructuring_query_url"))
print("restructuring_error:", dbg.get("restructuring_error"))
print("restructuring_raw_hit_count:", dbg.get("restructuring_raw_hit_count"))
print("restructuring_after_filter:", dbg.get("restructuring_after_filter"))
print("\nbuildout_per_term:")
for bt in dbg.get("buildout_per_term", []):
    print(" ", bt)
print("\nrestructuring n:", j.get("restructuring",{}).get("n"))
print("buildout n:", j.get("buildout",{}).get("n"))
print("DONE 2679")
