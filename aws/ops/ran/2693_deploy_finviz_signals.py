"""ops 2693 — deploy finviz-signals with the corrected/expanded screen list, verify
the triangle/price-cross fix actually worked (result counts should NOT be ~11397 anymore)."""
import boto3, io, zipfile, json, time
from botocore.config import Config
REGION="us-east-1"; FN="justhodl-finviz-signals"; SRC=f"aws/lambdas/{FN}/source/lambda_function.py"
lam=boto3.client("lambda",region_name=REGION, config=Config(read_timeout=200, connect_timeout=10, retries={"max_attempts":0}))
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
print("BODY:", r["Payload"].read().decode()[:200])
time.sleep(2)
j = json.loads(s3.get_object(Bucket="justhodl-dashboard-live", Key="data/finviz-signals.json")["Body"].read())
counts = j.get("counts", {})
print("\nall screen counts:")
for name, n in counts.items():
    flag = " <-- SUSPICIOUS (near full universe, likely broken)" if n > 5000 else ""
    print(f"  {name:20s} {n}{flag}")
print("DONE 2693")
