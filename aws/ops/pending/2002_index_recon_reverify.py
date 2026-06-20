"""ops 2002: force-redeploy index-recon from repo source (no race - this push has no lambda src), verify cross_confirm."""
import boto3, json, time, io, os, zipfile
REGION="us-east-1"; FN="justhodl-index-recon"; B="justhodl-dashboard-live"
lam=boto3.client("lambda",REGION); s3=boto3.client("s3",REGION)
before=lam.get_function(FunctionName=FN)["Configuration"]["LastModified"]
print("LastModified BEFORE:",before)

# bundle source + any shared deps it imports (index-recon uses only stdlib+boto3)
SRC=f"aws/lambdas/{FN}/source"
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z:
    for r,_,fs in os.walk(SRC):
        for f in fs:
            if f.endswith(".py"):
                p=os.path.join(r,f); z.write(p,os.path.relpath(p,SRC))
buf.seek(0)
# verify our new code is in the bundle
zf=zipfile.ZipFile(io.BytesIO(buf.getvalue()))
src=zf.read([n for n in zf.namelist() if n.endswith("lambda_function.py")][0]).decode()
print("source has cross_confirm:", "cross_confirm" in src, "| flow_idx:", "flow_idx" in src)
buf.seek(0)
lam.update_function_code(FunctionName=FN,ZipFile=buf.read())
for _ in range(30):
    c=lam.get_function(FunctionName=FN)["Configuration"]
    if c.get("LastUpdateStatus")=="Successful" and c.get("State")=="Active": break
    time.sleep(5)
print("LastModified AFTER:",c["LastModified"])

r=lam.invoke(FunctionName=FN,InvocationType="RequestResponse")
print("invoke:",r["StatusCode"], r["Payload"].read().decode()[:220])
time.sleep(2)
d=json.loads(s3.get_object(Bucket=B,Key="data/index-recon.json")["Body"].read())
cc=d.get("cross_confirm") or {}
print("\n=== CROSS-CONFIRM ===")
for k in ("n_events_observed","n_additions_confirmed","n_graduations_confirmed","n_deletions_confirmed","n_demotions_confirmed"):
    print(f"  {k}: {cc.get(k)}")
print("  confirmed_tickers:", (cc.get("confirmed_tickers") or [])[:15])
adds=d.get("russell_2000_additions") or []
conf=[r for r in adds if r.get("flow_confirmed")]
print(f"\n  additions={len(adds)} flow_confirmed={len(conf)}; sample tags:")
for r in adds[:5]:
    print(f"    {r.get('symbol'):<6} conf={r.get('flow_confirmed')} etfs={r.get('confirming_etfs')}")
print("DONE 2002")
