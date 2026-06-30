"""ops 2594 — wait for buyback-engine Active, ensure config+code, async-invoke, poll S3."""
import boto3, io, zipfile, json, time
REGION="us-east-1"; FN="justhodl-buyback-engine"
SRC="aws/lambdas/justhodl-buyback-engine/source/lambda_function.py"
ENV={"Variables":{"S3_BUCKET":"justhodl-dashboard-live","FMP_API_KEY":"wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"}}
lam=boto3.client("lambda",region_name=REGION); s3=boto3.client("s3",region_name=REGION)
def wait(n=40):
    for _ in range(n):
        try:
            c=lam.get_function(FunctionName=FN)["Configuration"]
            if c.get("State")=="Active" and c.get("LastUpdateStatus")=="Successful": return True
        except Exception: pass
        time.sleep(5)
    return False
wait()
# ensure config (timeout/env) — conflict tolerant
for a in range(6):
    try: lam.update_function_configuration(FunctionName=FN, Timeout=600, MemorySize=512, Environment=ENV); break
    except lam.exceptions.ResourceConflictException: time.sleep(10); wait()
wait()
# ensure latest code
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z: z.writestr("lambda_function.py",open(SRC,"rb").read())
for a in range(6):
    try: lam.update_function_code(FunctionName=FN, ZipFile=buf.getvalue()); print("code synced"); break
    except lam.exceptions.ResourceConflictException: time.sleep(10); wait()
wait()
prev=None
try: prev=s3.head_object(Bucket="justhodl-dashboard-live",Key="data/buyback-engine.json")["LastModified"]
except Exception: pass
lam.invoke(FunctionName=FN, InvocationType="Event", Payload=b"{}")
print("async invoked; polling S3 (FMP batch ~3min)...")
for i in range(12):
    time.sleep(20)
    try:
        h=s3.head_object(Bucket="justhodl-dashboard-live",Key="data/buyback-engine.json")
        if prev is None or h["LastModified"]>prev:
            j=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/buyback-engine.json")["Body"].read())
            print("OUTPUT v",j.get("version"),"scored",j.get("n_scored"),"fmp_ok",j.get("n_fmp_resolved"))
            print("counts:",j.get("counts"))
            for sec in ["high_conviction_pumps","fresh_authorizations","net_shrinkers","high_shareholder_yield","cheap_repurchasers","dilution_offset_warnings"]:
                r=j.get(sec,[]); ex=[(x["symbol"],f"sc{x.get('buyback_score')}",f"nbY{x.get('net_buyback_yield')}",f"sr{x.get('share_count_reduction_yoy')}",f"auth{x.get('auth_pct_mcap')}") for x in r[:4]]
                print(f"  {sec} ({len(r)}): {ex}")
            break
    except Exception as e: print(f"  poll {i}: {str(e)[:45]}")
    print(f"  poll {i}: not ready")
print("DONE 2594")
