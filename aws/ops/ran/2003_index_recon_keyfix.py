"""ops 2003: redeploy index-recon w/ correct index_events key; verify; show flow-lookthrough event count."""
import boto3, json, time, io, os, zipfile
REGION="us-east-1"; FN="justhodl-index-recon"; B="justhodl-dashboard-live"
lam=boto3.client("lambda",REGION); s3=boto3.client("s3",REGION)
# how many real index events does flow-lookthrough currently publish?
try:
    fl=json.loads(s3.get_object(Bucket=B,Key="data/flow-lookthrough.json")["Body"].read())
    iev=fl.get("index_events") or []
    print("flow-lookthrough index_events:",len(iev),"n_index_events field:",fl.get("n_index_events"))
    for e in iev[:8]: print("   ",e)
except Exception as e: print("flow read err",e)
# redeploy
SRC=f"aws/lambdas/{FN}/source"; buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z:
    for r,_,fs in os.walk(SRC):
        for f in fs:
            if f.endswith(".py"): p=os.path.join(r,f); z.write(p,os.path.relpath(p,SRC))
buf.seek(0); lam.update_function_code(FunctionName=FN,ZipFile=buf.read())
for _ in range(30):
    c=lam.get_function(FunctionName=FN)["Configuration"]
    if c.get("LastUpdateStatus")=="Successful" and c.get("State")=="Active": break
    time.sleep(5)
lam.invoke(FunctionName=FN,InvocationType="RequestResponse"); time.sleep(2)
d=json.loads(s3.get_object(Bucket=B,Key="data/index-recon.json")["Body"].read())
cc=d.get("cross_confirm") or {}
print("\n=== CROSS-CONFIRM (after key fix) ===")
for k in ("n_events_observed","n_additions_confirmed","n_graduations_confirmed","n_deletions_confirmed","n_demotions_confirmed"): print(f"  {k}: {cc.get(k)}")
print("  confirmed_tickers:", (cc.get("confirmed_tickers") or [])[:20])
print("DONE 2003")
