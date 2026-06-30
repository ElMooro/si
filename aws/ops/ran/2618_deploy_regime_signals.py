"""ops 2618 — push dollar-shortage + trajectory into risk-regime (single) + master-ranker (pkg)."""
import boto3, io, zipfile, json, time, os, glob
REGION="us-east-1"; lam=boto3.client("lambda",region_name=REGION); s3=boto3.client("s3",region_name=REGION)
def wait(fn):
    for _ in range(40):
        try:
            c=lam.get_function(FunctionName=fn)["Configuration"]
            if c.get("State")=="Active" and c.get("LastUpdateStatus")=="Successful": return
        except Exception: pass
        time.sleep(4)
def dep_single(fn,src):
    wait(fn); buf=io.BytesIO()
    with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z: z.writestr("lambda_function.py",open(src,"rb").read())
    for _ in range(6):
        try: lam.update_function_code(FunctionName=fn,ZipFile=buf.getvalue()); print(f"  {fn}: deployed"); break
        except lam.exceptions.ResourceConflictException: time.sleep(12); wait(fn)
    wait(fn)
def dep_pkg(fn,srcdir):
    wait(fn); files={}
    for p in glob.glob("aws/shared/*.py"):
        if "__pycache__" not in p: files[os.path.basename(p)]=p
    for root,_,fs in os.walk(srcdir):
        for f in fs:
            if f.endswith(".pyc") or "__pycache__" in root: continue
            files[os.path.relpath(os.path.join(root,f),srcdir)]=os.path.join(root,f)
    buf=io.BytesIO()
    with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z:
        for a,fl in files.items(): z.writestr(a,open(fl,"rb").read())
    for _ in range(6):
        try: lam.update_function_code(FunctionName=fn,ZipFile=buf.getvalue()); print(f"  {fn}: deployed ({len(files)}f)"); break
        except lam.exceptions.ResourceConflictException: time.sleep(12); wait(fn)
    wait(fn)
dep_single("justhodl-risk-regime","aws/lambdas/justhodl-risk-regime/source/lambda_function.py")
dep_pkg("justhodl-master-ranker","aws/lambdas/justhodl-master-ranker/source")
r=lam.invoke(FunctionName="justhodl-risk-regime",InvocationType="RequestResponse",Payload=b"{}")
print("RR:",r.get("StatusCode"),r.get("FunctionError"))
time.sleep(2)
rr=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/risk-regime.json")["Body"].read())
lrm=(rr.get("components") or {}).get("liquidity_regime") or {}
print("  risk_regime:",rr.get("risk_regime_score"),rr.get("risk_regime"),"| liq block score",lrm.get("score"),"traj",lrm.get("trajectory"),"ds",lrm.get("dollar_shortage"))
print("  liq tells:",lrm.get("tells"))
r=lam.invoke(FunctionName="justhodl-master-ranker",InvocationType="RequestResponse",Payload=b"{}")
print("MR:",r.get("StatusCode"),r.get("FunctionError"))
time.sleep(2)
mr=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/master-ranker.json")["Body"].read())
rc=mr.get("regime_context") or {}
print("  regime_context: liq",rc.get("liquidity_score"),rc.get("liquidity_regime"),"| traj",rc.get("liquidity_trajectory"),"| ds",rc.get("dollar_shortage"))
lm=[m for m in (mr.get("top_macro") or []) if m.get("type")=="liquidity_inflection"]
if lm: print("  macro signal:",lm[0].get("name"),"| act:",lm[0].get("action_hint"))
tl=[t for t in (mr.get("top_tickers") or []) if (t.get("liquidity_regime_mult") or 1.0)!=1.0]
print("  tickers w/ liq tilt:",len(tl),"e.g.",[(t['ticker'],t['liquidity_regime_mult']) for t in tl[:5]])
print("DONE 2618")
