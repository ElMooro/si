"""ops 2596 — rebuild signal-logger FULL package, invoke, verify buyback_* signals."""
import boto3, io, zipfile, glob, os, time
from datetime import datetime, timezone, timedelta
REGION="us-east-1"; FN="justhodl-signal-logger"; SRCDIR="aws/lambdas/justhodl-signal-logger/source"
lam=boto3.client("lambda",region_name=REGION); ddb=boto3.resource("dynamodb",region_name=REGION)
files={}
for p in glob.glob("aws/shared/*.py"):
    if "__pycache__" not in p: files[os.path.basename(p)]=p
for root,_,fs in os.walk(SRCDIR):
    if "__pycache__" in root: continue
    for fn in fs:
        if fn.endswith(".pyc"): continue
        files[os.path.relpath(os.path.join(root,fn),SRCDIR)]=os.path.join(root,fn)
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z:
    for arc,full in sorted(files.items()): z.write(full,arc)
def wait():
    for _ in range(25):
        c=lam.get_function(FunctionName=FN)["Configuration"]
        if c.get("LastUpdateStatus")=="Successful" and c.get("State")=="Active": return
        time.sleep(4)
wait()
for a in range(6):
    try: lam.update_function_code(FunctionName=FN, ZipFile=buf.getvalue()); print(f"full package deployed ({len(files)} files)"); break
    except lam.exceptions.ResourceConflictException: time.sleep(12); wait()
wait()
r=lam.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=b'{"action":"manual"}')
print("INVOKE:", r.get("StatusCode"), r.get("FunctionError"), r["Payload"].read().decode()[:200])
time.sleep(3)
tbl=ddb.Table("justhodl-signals")
cutoff=int((datetime.now(timezone.utc)-timedelta(minutes=8)).timestamp())
got={}
resp=tbl.scan(FilterExpression="logged_epoch > :c", ExpressionAttributeValues={":c":cutoff},
              ProjectionExpression="signal_type,signal_value,predicted_direction,confidence,baseline_price,baseline_benchmark_price")
for it in resp.get("Items",[]):
    stp=it.get("signal_type","")
    if stp.startswith("buyback_"):
        got.setdefault(stp,[]).append((it.get("signal_value"),it.get("predicted_direction"),float(it.get("confidence") or 0),it.get("baseline_price") is not None and it.get("baseline_benchmark_price") is not None))
for k in sorted(got):
    rows=got[k]; priced=sum(1 for x in rows if x[3])
    print(f"  {k}: {len(rows)} signals, {priced} fully-priced | e.g. {rows[:3]}")
if not got: print("  (no buyback_* signals found in window — may be page-paginated; check next run)")
print("DONE 2596")
