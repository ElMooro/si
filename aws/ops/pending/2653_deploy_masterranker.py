"""ops 2653 — deploy master-ranker (multi-file bundle: lambda_function.py + aws/shared/*.py,
source/ wins on name collision — this is the established bundling gotcha for this Lambda)."""
import boto3, io, zipfile, json, time, os
REGION="us-east-1"; FN="justhodl-master-ranker"
SRC_DIR=f"aws/lambdas/{FN}/source"
lam=boto3.client("lambda",region_name=REGION); s3=boto3.client("s3",region_name=REGION)

def wait():
    for _ in range(30):
        c=lam.get_function(FunctionName=FN)["Configuration"]
        if c.get("State")=="Active" and c.get("LastUpdateStatus")=="Successful": return
        time.sleep(4)
wait()

buf=io.BytesIO()
added=set()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z:
    # source/ wins — add these first, track names
    for root,_,files in os.walk(SRC_DIR):
        for f in files:
            if f.endswith(".pyc") or "__pycache__" in root: continue
            full=os.path.join(root,f)
            arc=os.path.relpath(full,SRC_DIR)
            z.write(full,arc); added.add(arc)
    # then bundle aws/shared/*.py for anything not already present
    for f in os.listdir("aws/shared"):
        if f.endswith(".py") and f not in added:
            z.write(os.path.join("aws/shared",f), f); added.add(f)
print("bundled files:", sorted(added))

for _ in range(6):
    try: lam.update_function_code(FunctionName=FN,ZipFile=buf.getvalue()); print("deployed"); break
    except lam.exceptions.ResourceConflictException: time.sleep(12); wait()
wait()
r=lam.invoke(FunctionName=FN,InvocationType="RequestResponse",Payload=b"{}")
print("INVOKE:",r.get("StatusCode"),r.get("FunctionError"),r["Payload"].read().decode()[:300])
time.sleep(2)

j=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/master-ranker.json")["Body"].read())
tt=j.get("top_tickers") or []
print(f"\ntop_tickers: {len(tt)}")
new_fams={"institutional_13f","estimate_revisions","forward_orders","squeeze_setup","earnings_quality_hi"}
hit=[t for t in tt if new_fams & set(t.get("systems") or [])]
print(f"tickers in top-25 touched by a NEW family: {len(hit)}")
for t in hit[:8]:
    print(f"  {t['ticker']:6s} score={t['score']} systems={t['systems']}")
    print(f"    rationale: {t['rationale']}")
print("\n--- trust_mult sample from contributions ---")
if tt:
    for c in tt[0].get("contributions",[])[:4]:
        print(" ",c)
print("DONE 2653")
