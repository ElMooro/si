"""ops 2694 — redeploy finviz-signals with PROPER multi-file bundling (missed this
the first time - the Lambda needs aws/shared/finviz.py bundled alongside the entry
point, same pattern already established for master-ranker earlier this session)."""
import boto3, io, zipfile, json, time
from botocore.config import Config
REGION="us-east-1"; FN="justhodl-finviz-signals"; SRC_DIR=f"aws/lambdas/{FN}/source"
lam=boto3.client("lambda",region_name=REGION, config=Config(read_timeout=280, connect_timeout=10, retries={"max_attempts":0}))
s3=boto3.client("s3",region_name=REGION)
def wait():
    for i in range(40):
        c=lam.get_function(FunctionName=FN)["Configuration"]
        if c.get("State")=="Active" and c.get("LastUpdateStatus")=="Successful": return
        time.sleep(5)
wait()

buf=io.BytesIO()
added=set()
import os
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z:
    for root,_,files in os.walk(SRC_DIR):
        for f in files:
            if f.endswith(".pyc") or "__pycache__" in root: continue
            full=os.path.join(root,f)
            arc=os.path.relpath(full,SRC_DIR)
            z.write(full,arc); added.add(arc)
    for f in os.listdir("aws/shared"):
        if f.endswith(".py") and f not in added:
            z.write(os.path.join("aws/shared",f), f); added.add(f)
print("bundled files:", sorted(added))

for _ in range(6):
    try: lam.update_function_code(FunctionName=FN,ZipFile=buf.getvalue()); print("deployed"); break
    except lam.exceptions.ResourceConflictException: time.sleep(10); wait()
wait()

print("invoking (32 screens x ~4s spacing, this will take a few minutes)...")
r=lam.invoke(FunctionName=FN,InvocationType="RequestResponse",Payload=b"{}")
print("INVOKE:",r.get("StatusCode"),r.get("FunctionError"))
print("BODY:", r["Payload"].read().decode()[:300])
time.sleep(2)

j = json.loads(s3.get_object(Bucket="justhodl-dashboard-live", Key="data/finviz-signals.json")["Body"].read())
counts = j.get("counts", {})
print(f"\ntotal screens: {len(counts)}")
new_screens = {"triangle_asc","triangle_desc","wedge_up","wedge_down","sma20_cross50a","sma20_cross50b",
               "price_cross50a","price_cross200a","price_cross200b","new_high_20d","new_low_20d"}
print("\nNEW screens specifically (the ones just added):")
for name in new_screens:
    n = counts.get(name, "MISSING")
    flag = " <-- STILL SUSPICIOUS" if isinstance(n,int) and n > 5000 else ""
    print(f"  {name:20s} {n}{flag}")
print("\nall screen counts:")
for name, n in counts.items():
    print(f"  {name:20s} {n}")
print("DONE 2694")
