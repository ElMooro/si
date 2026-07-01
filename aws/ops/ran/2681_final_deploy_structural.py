"""ops 2681 — final deploy of block 2 with filing-level dedup fix (keeps the _debug
capture, it's cheap and useful for ongoing monitoring)."""
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
print("\nversion:", j.get("version"), "elapsed_s:", j.get("elapsed_s"))
rest = j.get("restructuring",{}); bo = j.get("buildout",{})
print(f"restructuring: {rest.get('n')} filings | buildout: {bo.get('n')} filings")

from collections import Counter
bo_items = bo.get("items", [])
dupe_check = Counter((r.get("ticker"), r.get("file_date")) for r in bo_items)
dupes = {k:v for k,v in dupe_check.items() if v>1}
print(f"remaining ticker+date duplicates in buildout: {len(dupes)} {dupes if dupes else '(clean)'}")

print("\nrestructuring filings:")
for r2 in rest.get("items") or []:
    print(f"  {r2.get('ticker') or '?':6s} {r2.get('company')} | {r2.get('file_date')} | {r2.get('sector')}")
print("\nbuildout filings (deduped):")
for r2 in bo_items[:15]:
    print(f"  {r2.get('ticker') or '?':6s} {r2.get('company')} | {r2.get('file_date')} | {r2.get('sector')}")
print("by_sector:", bo.get("by_sector"))
print("DONE 2681")
