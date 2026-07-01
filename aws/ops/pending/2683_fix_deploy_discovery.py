"""ops 2683 — redeploy with ETF filter + filing-level dedup fix."""
import boto3, io, zipfile, json, time
from botocore.config import Config
REGION="us-east-1"; FN="justhodl-universe-discovery"; SRC=f"aws/lambdas/{FN}/source/lambda_function.py"
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
j=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/universe-discovery.json")["Body"].read())
print("\nversion:", j.get("version"), "elapsed_s:", j.get("elapsed_s"))
ipo=j.get("ipo_calendar",{}); reg=j.get("new_registrants",{}); tc=j.get("threshold_crossers",{})
print(f"ipos: {ipo.get('n')} | new_registrants: {reg.get('n')} | threshold_crossers: {tc.get('n')}")

from collections import Counter
reg_items = reg.get("items", [])
dupe_check = Counter((r.get("ticker"), r.get("file_date"), r.get("registration_type")) for r in reg_items)
dupes = {k:v for k,v in dupe_check.items() if v>1}
print(f"remaining registrant duplicates: {len(dupes)} {dupes if dupes else '(clean)'}")

print("\nsample IPOs (ETF-filtered):")
for r2 in (ipo.get("items") or [])[:8]:
    print(f"  {r2.get('symbol') or '?':8s} {r2.get('company')} | {r2.get('date')} | {r2.get('exchange')}")
print("\nsample new registrants (deduped):")
for r2 in reg_items[:10]:
    print(f"  {r2.get('ticker') or '?':6s} {r2.get('company')} | {r2.get('registration_type')} | {r2.get('file_date')}")
print("\ncoverage:", j.get("coverage"))
print("crossers note:", tc.get("note"))
print("DONE 2683")
