"""ops 2597 — full-package deploy best-setups, invoke, verify BUYBACK signals fire."""
import boto3, io, zipfile, glob, os, json, time
REGION="us-east-1"; FN="justhodl-best-setups"; SRCDIR="aws/lambdas/justhodl-best-setups/source"
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
    for _ in range(25):
        c=lam.get_function(FunctionName=FN)["Configuration"]
        if c.get("LastUpdateStatus")=="Successful" and c.get("State")=="Active": return
        time.sleep(4)
wait()
for a in range(6):
    try: lam.update_function_code(FunctionName=FN, ZipFile=buf.getvalue()); print(f"deployed ({len(files)} files)"); break
    except lam.exceptions.ResourceConflictException: time.sleep(12); wait()
wait()
r=lam.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=b"{}")
print("INVOKE:", r.get("StatusCode"), r.get("FunctionError"), r["Payload"].read().decode()[:160])
time.sleep(2)
out=None
for key in ["data/best-setups.json","data/best-setups-board.json","data/setups.json"]:
    try: out=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key=key)["Body"].read()); print("read",key); break
    except Exception: pass
if out:
    setups=out.get("top_setups") or out.get("setups") or []
    bb=[s for s in setups if any((sg.get("key")=="BUYBACK") for sg in (s.get("signals") or []))]
    print(f"setups total {len(setups)}; with BUYBACK signal: {len(bb)}")
    for s in bb[:6]:
        bbsig=[sg for sg in s["signals"] if sg["key"]=="BUYBACK"][0]
        print(f"  {s.get('ticker')}: verdict={s.get('verdict')} conviction={s.get('conviction')} n_sig={len(s.get('signals'))} | BUYBACK str={bbsig.get('strength')} '{bbsig.get('detail')}'")
print("DONE 2597")
