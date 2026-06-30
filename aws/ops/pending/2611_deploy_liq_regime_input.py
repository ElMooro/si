"""ops 2611 — deploy liquidity regime input into risk-regime (single-file) + master-ranker (full pkg)."""
import boto3, io, zipfile, json, time, os, glob
REGION="us-east-1"
lam=boto3.client("lambda",region_name=REGION); s3=boto3.client("s3",region_name=REGION)
def wait(fn):
    for _ in range(40):
        try:
            c=lam.get_function(FunctionName=fn)["Configuration"]
            if c.get("State")=="Active" and c.get("LastUpdateStatus")=="Successful": return
        except Exception: pass
        time.sleep(4)
def deploy_single(fn, src):
    wait(fn)
    buf=io.BytesIO()
    with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z: z.writestr("lambda_function.py",open(src,"rb").read())
    for _ in range(6):
        try: lam.update_function_code(FunctionName=fn, ZipFile=buf.getvalue()); print(f"  {fn}: deployed (single)"); break
        except lam.exceptions.ResourceConflictException: time.sleep(12); wait(fn)
    wait(fn)
def deploy_pkg(fn, srcdir):
    wait(fn)
    files={}
    for p in glob.glob("aws/shared/*.py"):
        if "__pycache__" in p: continue
        files[os.path.basename(p)]=p
    for root,_,fs in os.walk(srcdir):
        for f in fs:
            if f.endswith(".pyc") or "__pycache__" in root: continue
            full=os.path.join(root,f); arc=os.path.relpath(full,srcdir)
            files[arc]=full  # source wins
    buf=io.BytesIO()
    with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z:
        for arc,full in files.items(): z.writestr(arc, open(full,"rb").read())
    for _ in range(6):
        try: lam.update_function_code(FunctionName=fn, ZipFile=buf.getvalue()); print(f"  {fn}: deployed (pkg, {len(files)} files)"); break
        except lam.exceptions.ResourceConflictException: time.sleep(12); wait(fn)
    wait(fn)

deploy_single("justhodl-risk-regime","aws/lambdas/justhodl-risk-regime/source/lambda_function.py")
deploy_pkg("justhodl-master-ranker","aws/lambdas/justhodl-master-ranker/source")

# invoke risk-regime
r=lam.invoke(FunctionName="justhodl-risk-regime", InvocationType="RequestResponse", Payload=b"{}")
print("RR invoke:", r.get("StatusCode"), r.get("FunctionError"), r["Payload"].read().decode()[:120])
time.sleep(2)
rr=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/risk-regime.json")["Body"].read())
bu=rr.get("blocks_used") or []
print("  risk_regime_score:", rr.get("risk_regime_score"), rr.get("risk_regime"))
print("  blocks_used:", [(b['block'],b['weight'],b['score']) for b in bu])
lr=(rr.get("components") or {}).get("liquidity_regime")
print("  components.liquidity_regime:", lr)

# invoke master-ranker
r=lam.invoke(FunctionName="justhodl-master-ranker", InvocationType="RequestResponse", Payload=b"{}")
print("MR invoke:", r.get("StatusCode"), r.get("FunctionError"), r["Payload"].read().decode()[:120])
time.sleep(2)
mr=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/master-ranker.json")["Body"].read())
rc=mr.get("regime_context") or {}
print("  regime_context liquidity:", rc.get("liquidity_score"), rc.get("liquidity_regime"), "z", rc.get("liquidity_z"))
liqmac=[m for m in (mr.get("top_macro") or []) if m.get("type")=="liquidity_inflection"]
print("  top_macro liquidity entry:", liqmac[0] if liqmac else "NONE")
tilted=[t for t in (mr.get("top_tickers") or []) if (t.get("liquidity_regime_mult") or 1.0)!=1.0]
print("  tickers with liquidity tilt:", len(tilted), "e.g.", [(t['ticker'],t['liquidity_regime_mult']) for t in tilted[:5]])
print("DONE 2611")
