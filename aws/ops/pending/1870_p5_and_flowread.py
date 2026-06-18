import boto3, json, zipfile, io, glob, time
from botocore.exceptions import ClientError
lam=boto3.client("lambda","us-east-1"); s3=boto3.client("s3","us-east-1"); B="justhodl-dashboard-live"
FN="justhodl-deal-scanner"
src=open(glob.glob("**/justhodl-deal-scanner/source/lambda_function.py",recursive=True)[0]).read()
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z: z.writestr("lambda_function.py",src)
code=buf.getvalue()
def wait():
    for _ in range(90):
        st=lam.get_function_configuration(FunctionName=FN)
        if st.get("State")=="Active" and st.get("LastUpdateStatus")!="InProgress": return st
        time.sleep(3)
    return st
wait()
for _ in range(24):
    try: lam.update_function_code(FunctionName=FN,ZipFile=code); break
    except ClientError as e:
        if "ResourceConflict" in str(e): time.sleep(5); continue
        raise
wait(); print("deployed")
r=lam.invoke(FunctionName=FN,InvocationType="RequestResponse"); print("INVOKE:",r["Payload"].read().decode()[:200])
d=json.loads(s3.get_object(Bucket=B,Key="data/deal-scanner.json")["Body"].read()); sm=d["summary"]
print("DEAL-SCANNER: items=%s deals=%s ai=%s green=%s"%(sm["n_prs_scanned"],sm["n_deals"],sm.get("n_ai"),sm.get("n_green")))
noise={"SPCX","NFLX","AAPL","DAIO","REFI"}; present={x["symbol"] for x in d["deals"]}
print("opinion-noise still present:", sorted(noise & present) or "NONE (clean)")
for x in d["deals"]:
    print("   %-7s %-10s [%s%s] %s"%(x["symbol"],(x.get("publisher") or "")[:10],"AI" if x["ai_relevant"] else "",({"green":"G","yellow":"Y"}.get(x["highlight"],"")),x["title"][:46]))
# Part 2: show what existing sector-ETF flow engines flag RIGHT NOW
def peek(key):
    try: return json.loads(s3.get_object(Bucket=B,Key=key)["Body"].read())
    except Exception as e: return {"_err":str(e)[:40]}
print("\n=== ALREADY-BUILT: ETF-TRUE-FLOWS (data/etf-true-flows.json) ===")
tf=peek("data/etf-true-flows.json")
print("  keys:",list(tf.keys())[:12])
for lk in ("top_inflows","unusual","inflows","ranked","etfs","top","signals"):
    v=tf.get(lk)
    if isinstance(v,list) and v:
        print("  [%s] sample:"%lk)
        for it in v[:6]:
            if isinstance(it,dict): print("    ",{k:it.get(k) for k in list(it.keys())[:6]})
        break
print("\n=== ALREADY-BUILT: SECTOR-ROTATION (data/sector-rotation.json) ===")
sr=peek("data/sector-rotation.json")
print("  keys:",list(sr.keys())[:12])
for lk in ("early","early_signals","surges","anomalies","sectors","ranked","flows","leaders"):
    v=sr.get(lk)
    if isinstance(v,list) and v:
        print("  [%s] sample:"%lk)
        for it in v[:6]:
            if isinstance(it,dict): print("    ",{k:it.get(k) for k in list(it.keys())[:6]})
        break
