"""ops 2600 — redeploy buyback-engine (tighter positive-list filters) + re-invoke."""
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
    try: lam.update_function_code(FunctionName=FN, ZipFile=buf.getvalue()); print("synced"); break
    except lam.exceptions.ResourceConflictException: time.sleep(12); wait()
wait()
prev=s3.head_object(Bucket="justhodl-dashboard-live",Key="data/buyback-engine.json")["LastModified"]
lam.invoke(FunctionName=FN, InvocationType="Event", Payload=b"{}")
for i in range(14):
    time.sleep(20)
    h=s3.head_object(Bucket="justhodl-dashboard-live",Key="data/buyback-engine.json")
    if h["LastModified"]>prev:
        j=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/buyback-engine.json")["Body"].read())
        print("scored",j.get("n_scored"),"excluded",j.get("n_excluded"),"counts",j.get("counts"))
        print("  high_shareholder_yield:", [(x['symbol'],x.get('net_buyback_yield'),x.get('dividend_yield')) for x in j.get('high_shareholder_yield',[])[:6]])
        print("  pumps:", [(x['symbol'],x.get('auth_pct_mcap')) for x in j.get('high_conviction_pumps',[])[:6]])
        print("  LIEN still in hsy:", any(x['symbol']=='LIEN' for x in j.get('high_shareholder_yield',[])))
        break
    print(f"  poll {i}: not ready")
print("DONE 2600")
