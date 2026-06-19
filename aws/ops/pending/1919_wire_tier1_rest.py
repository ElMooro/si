import boto3, json, zipfile, io, glob, time
from botocore.config import Config
from botocore.exceptions import ClientError
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=240,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1"); B="justhodl-dashboard-live"
def deploy(fn):
    src=open(glob.glob("**/%s/source/lambda_function.py"%fn,recursive=True)[0]).read()
    buf=io.BytesIO()
    with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z: z.writestr("lambda_function.py",src)
    for _ in range(40):
        st=lam.get_function_configuration(FunctionName=fn)
        if st.get("State")=="Active" and st.get("LastUpdateStatus")!="InProgress": break
        time.sleep(3)
    for _ in range(24):
        try: lam.update_function_code(FunctionName=fn,ZipFile=buf.getvalue()); break
        except ClientError as e:
            if "ResourceConflict" in str(e): time.sleep(5); continue
            raise
    for _ in range(40):
        st=lam.get_function_configuration(FunctionName=fn)
        if st.get("State")=="Active" and st.get("LastUpdateStatus")!="InProgress": break
        time.sleep(3)
    print("deployed",fn)
for fn in ["justhodl-pump-mechanics","justhodl-alpha-score","justhodl-convergence-radar"]: deploy(fn)

# verify convergence-radar (sync)
r=lam.invoke(FunctionName="justhodl-convergence-radar",InvocationType="RequestResponse"); print("conv:",r["Payload"].read().decode()[:90])
time.sleep(2)
cr=json.loads(s3.get_object(Bucket=B,Key="data/convergence-radar.json")["Body"].read())
raw=json.dumps(cr); has=raw.count('"massive-flow"')
tk=cr.get("tickers") or []
withm=[t.get("ticker") for t in tk if "massive-flow" in json.dumps(t)][:8]
print("convergence-radar: massive-flow source registered=%s | tickers flagged by massive-flow=%s %s"%(has>0,len(withm),withm))

# verify pump-mechanics (sync)
try:
    r=lam.invoke(FunctionName="justhodl-pump-mechanics",InvocationType="RequestResponse"); print("pump:",r["Payload"].read().decode()[:90])
except Exception as e: print("pump async:",str(e)[:40]); lam.invoke(FunctionName="justhodl-pump-mechanics",InvocationType="Event"); time.sleep(40)
time.sleep(2)
pm=json.loads(s3.get_object(Bucket=B,Key="data/pump-mechanics.json")["Body"].read())
cand=pm.get("candidates") or []
gz=[c.get("ticker") for c in cand if (c.get("squeeze_profile") or {}).get("massive_gamma_squeeze")]
print("pump-mechanics: n_candidates=%s | gamma-boosted=%s %s (dormant if squeeze list empty)"%(len(cand),len(gz),gz))

# verify alpha-score (async, then poll output freshness)
lam.invoke(FunctionName="justhodl-alpha-score",InvocationType="Event"); print("alpha-score: async triggered")
prev=None
try: prev=json.loads(s3.get_object(Bucket=B,Key="screener/alpha-score.json")["Body"].read()).get("generated_at")
except Exception: pass
for _ in range(9):
    time.sleep(13)
    try:
        a=json.loads(s3.get_object(Bucket=B,Key="screener/alpha-score.json")["Body"].read())
        if a.get("generated_at")!=prev:
            rows=a.get("stocks") or a.get("rows") or a.get("results") or []
            samp=[(r.get("symbol"),(r.get("components") or {}).get("options_flow")) for r in rows if r.get("symbol") in ("AVGO","AMD","GOOGL","FCX","BB")][:5]
            print("alpha-score: REFRESHED n=%s | options_flow comp for massive names=%s"%(len(rows),samp)); break
    except Exception as e: print("poll",str(e)[:40])
else: print("alpha-score: still running (full universe) — code deployed, will reflect next scheduled run")
