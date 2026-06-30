"""ops 2599 — deploy buyback-engine w/ security-master exclusion, async invoke, verify noise gone."""
import boto3, io, zipfile, json, time
REGION="us-east-1"; FN="justhodl-buyback-engine"
SRC="aws/lambdas/justhodl-buyback-engine/source/lambda_function.py"
lam=boto3.client("lambda",region_name=REGION); s3=boto3.client("s3",region_name=REGION)
def wait():
    for _ in range(30):
        c=lam.get_function(FunctionName=FN)["Configuration"]
        if c.get("State")=="Active" and c.get("LastUpdateStatus")=="Successful": return
        time.sleep(4)
wait()
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z: z.writestr("lambda_function.py",open(SRC,"rb").read())
for a in range(6):
    try: lam.update_function_code(FunctionName=FN, ZipFile=buf.getvalue()); print("code synced"); break
    except lam.exceptions.ResourceConflictException: time.sleep(12); wait()
wait()
prev=None
try: prev=s3.head_object(Bucket="justhodl-dashboard-live",Key="data/buyback-engine.json")["LastModified"]
except Exception: pass
lam.invoke(FunctionName=FN, InvocationType="Event", Payload=b"{}")
print("async invoked; polling (4 FMP calls/ticker)...")
for i in range(14):
    time.sleep(20)
    try:
        h=s3.head_object(Bucket="justhodl-dashboard-live",Key="data/buyback-engine.json")
        if prev is None or h["LastModified"]>prev:
            j=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/buyback-engine.json")["Body"].read())
            print("scored",j.get("n_scored"),"fmp_ok",j.get("n_fmp_resolved"),"excluded",j.get("n_excluded"))
            print("counts:",j.get("counts"))
            tk=j.get("tickers",{})
            for noise in ["GLV","GLO","GLQ","CLSK","MARA","RIOT"]:
                print(f"  {noise} in board: {noise in tk} (should be False)")
            print("  excluded sample:", [e['ticker'] for e in j.get('excluded_sample',[])][:20])
            print("  high_shareholder_yield top:", [(x['symbol'],x.get('net_buyback_yield'),x.get('share_count_reduction_yoy')) for x in j.get('high_shareholder_yield',[])[:6]])
            print("  net_shrinkers top:", [(x['symbol'],x.get('share_count_reduction_yoy')) for x in j.get('net_shrinkers',[])[:6]])
            break
    except Exception as e: print(f"  poll {i}: {str(e)[:40]}")
    print(f"  poll {i}: not ready")
print("DONE 2599")
