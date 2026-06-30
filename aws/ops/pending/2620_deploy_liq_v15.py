"""ops 2620 — deploy liquidity-inflection v1.5.0 (regime-conditioned study + lead curve + flip log)."""
import boto3, io, zipfile, json, time
REGION="us-east-1"; FN="justhodl-liquidity-inflection"; SRC=f"aws/lambdas/{FN}/source/lambda_function.py"
lam=boto3.client("lambda",region_name=REGION); s3=boto3.client("s3",region_name=REGION)
def wait():
    for _ in range(30):
        c=lam.get_function(FunctionName=FN)["Configuration"]
        if c.get("State")=="Active" and c.get("LastUpdateStatus")=="Successful": return
        time.sleep(4)
wait()
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z: z.writestr("lambda_function.py",open(SRC,"rb").read())
for _ in range(6):
    try: lam.update_function_code(FunctionName=FN,ZipFile=buf.getvalue()); print("deployed"); break
    except lam.exceptions.ResourceConflictException: time.sleep(12); wait()
wait()
r=lam.invoke(FunctionName=FN,InvocationType="RequestResponse",Payload=b"{}")
print("INVOKE:",r.get("StatusCode"),r.get("FunctionError"),r["Payload"].read().decode()[:120])
time.sleep(2)
j=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/liquidity-inflection.json")["Body"].read())
print("version:",j.get("version"))
rr=j.get("regime_returns") or {}
for asset in ["SPX_proxy","BTC","HYG"]:
    a=rr.get(asset) or {}
    print(f"\n{asset}: n_total={a.get('n_total')} baseline={a.get('baseline')}")
    for st,row in (a.get("states") or {}).items():
        d21=row.get("d21") or {}
        print(f"   {st:18s} +21d mean {d21.get('mean')}% excess {d21.get('excess')}% (n{d21.get('n')}, hit {d21.get('hit_pct')}%, t {d21.get('t')}{' *' if d21.get('sig') else ''})")
lc=(j.get("lead_curves") or {}).get("SPX_proxy") or {}
print("\nSPX lead best:", lc.get("best"))
fl=(j.get("flip_log") or {}).get("BTC") or []
print("BTC flip_log:", fl)
print("DONE 2620")
