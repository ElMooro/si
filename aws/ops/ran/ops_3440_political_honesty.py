"""ops 3440 — census #4: political desk honesty layer. Quiver key is 401
(Khalid-side renewal); until then feeds carry stale_cache flags and
political.html shows a loud banner. Gates: both engines settle + invoke ->
feeds carry source_status/stale_cache=true; page banner script live."""
import io, json, sys, time, urllib.request, zipfile
from datetime import datetime, timezone
from pathlib import Path
import boto3
from botocore.config import Config
from ops_report import report
LAM=boto3.client("lambda","us-east-1",config=Config(read_timeout=300,retries={"max_attempts":2}))
S3C=boto3.client("s3","us-east-1")
UA={"User-Agent":"Mozilla/5.0 (ops-3440)"}
def settled(fn,marker,tmax=360):
    dl=time.time()+tmax
    while time.time()<dl:
        try:
            if LAM.get_function_configuration(FunctionName=fn).get("LastUpdateStatus")=="Successful":
                info=LAM.get_function(FunctionName=fn)
                with urllib.request.urlopen(urllib.request.Request(info["Code"]["Location"],headers=UA),timeout=60) as r:
                    if marker in zipfile.ZipFile(io.BytesIO(r.read())).read("lambda_function.py").decode("utf-8","replace"):
                        return True
        except Exception: pass
        time.sleep(12)
    return False
with report("3440_political_honesty") as rep:
    rep.heading("ops 3440 — political honesty")
    out={"gates":{}}; fails=[]
    def gate(n,ok,d):
        out["gates"][n]={"ok":bool(ok),"detail":str(d)[:300]}
        line=("PASS  " if ok else "FAIL  ")+n+" — "+str(d)[:260]; print(line); rep.log(line)
        if not ok: fails.append(n)
    gate("G1_settled", settled("justhodl-lobbying-intel","STALE_CACHE") and
                       settled("justhodl-political-stocks","stale_cache"), "both stamps in zips")
    t0=datetime.now(timezone.utc).isoformat()
    for fn in ("justhodl-lobbying-intel","justhodl-political-stocks"):
        try: LAM.invoke(FunctionName=fn,InvocationType="RequestResponse",Payload=b"{}")
        except Exception as e: print("[invoke]",fn,str(e)[:70])
    ok2=False; det={}; dl=time.time()+180
    while time.time()<dl:
        try:
            j=json.loads(S3C.get_object(Bucket="justhodl-dashboard-live",Key="data/lobbying-intel.json")["Body"].read())
            if (j.get("generated_at") or "")>t0 or j.get("stale_cache") is not None:
                det={"stale":j.get("stale_cache"),"status":str(j.get("source_status"))[:120]}
                if j.get("stale_cache"): ok2=True; break
        except Exception: pass
        time.sleep(12)
    gate("G2_feed_honest", ok2, json.dumps(det))
    ok3=False; dl=time.time()+300
    while time.time()<dl:
        try:
            with urllib.request.urlopen(urllib.request.Request(f"https://justhodl.ai/political.html?t={int(time.time())}",headers=UA),timeout=25) as r:
                if "OPS3440" in r.read().decode("utf-8","replace"): ok3=True; break
        except Exception: pass
        time.sleep(15)
    gate("G3_banner_live", ok3, "OPS3440 on page")
    out["verdict"]="PASS_ALL" if not fails else "GAPS: "+",".join(fails)
    print("\nVERDICT:",out["verdict"]); rep.log("VERDICT: "+out["verdict"])
    Path("aws/ops/reports/3440.json").write_text(json.dumps(out,indent=2)); sys.exit(0)
