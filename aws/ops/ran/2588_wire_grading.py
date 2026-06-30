"""ops 2588 — deploy signal-logger w/ attention block, invoke, verify signals in DDB."""
import boto3, io, zipfile, time
from datetime import datetime, timezone, timedelta
REGION="us-east-1"; FN="justhodl-signal-logger"
SRC="aws/lambdas/justhodl-signal-logger/source/lambda_function.py"
lam=boto3.client("lambda",region_name=REGION); ddb=boto3.resource("dynamodb",region_name=REGION)
def wait():
    for _ in range(25):
        c=lam.get_function(FunctionName=FN)["Configuration"]
        if c.get("LastUpdateStatus")=="Successful" and c.get("State")=="Active": return
        time.sleep(4)
wait()
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z: z.writestr("lambda_function.py",open(SRC,"rb").read())
for a in range(6):
    try: lam.update_function_code(FunctionName=FN, ZipFile=buf.getvalue()); print("code updated"); break
    except lam.exceptions.ResourceConflictException: time.sleep(12); wait()
wait()
r=lam.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=b'{"action":"manual_attention_test"}')
print("INVOKE:", r.get("StatusCode"), r.get("FunctionError"), r["Payload"].read().decode()[:200])
# verify attention_* signals just written
time.sleep(3)
tbl=ddb.Table("justhodl-signals")
cutoff=int((datetime.now(timezone.utc)-timedelta(minutes=10)).timestamp())
got={}
resp=tbl.scan(FilterExpression="logged_epoch > :c", ExpressionAttributeValues={":c":cutoff}, ProjectionExpression="signal_type,signal_value,predicted_direction,confidence,baseline_price,baseline_benchmark_price")
for it in resp.get("Items",[]):
    stp=it.get("signal_type","")
    if stp.startswith("attention_"):
        got.setdefault(stp,[]).append((it.get("signal_value"),it.get("predicted_direction"),float(it.get("confidence") or 0),it.get("baseline_price") is not None))
for k in sorted(got):
    rows=got[k]; priced=sum(1 for x in rows if x[3])
    print(f"  {k}: {len(rows)} signals, {priced} with baseline price | e.g. {rows[:3]}")
if not got: print("  (no attention_* signals found yet — may need price backfill or rescan)")
print("DONE 2588")
