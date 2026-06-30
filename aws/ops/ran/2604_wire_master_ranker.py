"""ops 2604 — full-package deploy master-ranker (buyback system), invoke, verify buyback contributes to rank."""
import boto3, io, zipfile, glob, os, json, time
REGION="us-east-1"; FN="justhodl-master-ranker"; SRCDIR=f"aws/lambdas/{FN}/source"
lam=boto3.client("lambda",region_name=REGION); s3=boto3.client("s3",region_name=REGION)
files={}
for p in glob.glob("aws/shared/*.py"):
    if "__pycache__" not in p: files[os.path.basename(p)]=p
for root,_,fs in os.walk(SRCDIR):
    if "__pycache__" in root: continue
    for fn in fs:
        if fn.endswith(".pyc"): continue
        files[os.path.relpath(os.path.join(root,fn),SRCDIR)]=os.path.join(root,fn)
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z:
    for arc,full in sorted(files.items()): z.write(full,arc)
def wait():
    for _ in range(30):
        c=lam.get_function(FunctionName=FN)["Configuration"]
        if c.get("State")=="Active" and c.get("LastUpdateStatus")=="Successful": return
        time.sleep(4)
wait()
for a in range(6):
    try: lam.update_function_code(FunctionName=FN, ZipFile=buf.getvalue()); print(f"deployed ({len(files)} files)"); break
    except lam.exceptions.ResourceConflictException: time.sleep(12); wait()
wait()
prev=None
try: prev=s3.head_object(Bucket="justhodl-dashboard-live",Key="data/master-ranker.json")["LastModified"]
except Exception: pass
r=lam.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=b"{}")
print("INVOKE:", r.get("StatusCode"), r.get("FunctionError"), r["Payload"].read().decode()[:160])
for i in range(8):
    time.sleep(15)
    h=s3.head_object(Bucket="justhodl-dashboard-live",Key="data/master-ranker.json")
    if prev is None or h["LastModified"]>prev:
        j=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/master-ranker.json")["Body"].read())
        ranks=j.get("rankings") or j.get("top_ranked") or j.get("ranked") or j.get("master_ranking") or []
        if isinstance(ranks,dict): ranks=list(ranks.values())
        print("total ranked:", len(ranks))
        withbb=[r for r in ranks if isinstance(r,dict) and ("buyback" in (r.get("systems") or r.get("systems_dict") or {}) or "buyback" in str(r.get("contributions") or "") or "buyback" in str(r.get("rationale") or "").lower() or "net shrinker" in str(r.get("rationale") or "").lower())]
        print("ranked names citing buyback:", len(withbb))
        for r in withbb[:8]:
            print(f"  {r.get('ticker')}: rank/score={r.get('conviction') or r.get('score') or r.get('master_score')} | {str(r.get('rationale'))[:90]}")
        break
    print(f"  poll {i}: not ready")
print("DONE 2604")
