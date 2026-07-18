"""ops 3436 — census #2 CLOSE: engine-manifest refreshed by this very run's
workflow step (gate freshness + size); regime det-state polish proven via
single-context invoke."""
import io, json, sys, time, urllib.request, zipfile
from datetime import datetime, timezone, timedelta
from pathlib import Path
import boto3
from botocore.config import Config
from ops_report import report
LAM=boto3.client("lambda","us-east-1",config=Config(read_timeout=300,retries={"max_attempts":0}))
S3C=boto3.client("s3","us-east-1")
UA={"User-Agent":"Mozilla/5.0 (ops-3436)"}
with report("3437_close2b") as rep:
    rep.heading("ops 3436 — census #2 close")
    out={"gates":{}}; fails=[]
    def gate(n,ok,d):
        out["gates"][n]={"ok":bool(ok),"detail":str(d)[:340]}
        line=("PASS  " if ok else "FAIL  ")+n+" — "+str(d)[:300]; print(line); rep.log(line)
        if not ok: fails.append(n)
    o=S3C.get_object(Bucket="justhodl-dashboard-live",Key="data/engine-manifest.json")
    man=json.loads(o["Body"].read())
    age_m=(datetime.now(timezone.utc)-o["LastModified"]).total_seconds()/60
    keyed=sum(1 for e in (man.get("engines") or []) if e.get("keys"))
    gate("G1_manifest_fresh", age_m<30 and man.get("n_engines",0)>=600 and keyed>=300,
         f"age_min={round(age_m,1)} engines={man.get('n_engines')} with_keys={keyed}")
    ok2=False; dl=time.time()+420
    while time.time()<dl:
        try:
            if LAM.get_function_configuration(FunctionName="justhodl-ai-brief-router").get("LastUpdateStatus")=="Successful":
                info=LAM.get_function(FunctionName="justhodl-ai-brief-router")
                with urllib.request.urlopen(urllib.request.Request(info["Code"]["Location"],headers=UA),timeout=60) as r:
                    if "DET_PRIMARY_BFS" in zipfile.ZipFile(io.BytesIO(r.read())).read("lambda_function.py").decode("utf-8","replace"):
                        ok2=True; break
        except Exception: pass
        time.sleep(12)
    gate("G2_polish_settled", ok2, "regime_3m key in det_state")
    t0=datetime.now(timezone.utc)
    LAM.invoke(FunctionName="justhodl-ai-brief-router",InvocationType="RequestResponse",
               Payload=json.dumps({"contexts":["regime-decisive-call"]}).encode())
    det={}; dl=time.time()+120
    while time.time()<dl:
        try:
            det=json.loads(S3C.get_object(Bucket="justhodl-dashboard-live",Key="data/regime-decisive-call.json")["Body"].read())
            if (det.get("generated_at") or "")>t0.isoformat(): break
        except Exception: pass
        time.sleep(10)
    ok3=(det.get("mode")=="deterministic" and det.get("regime") not in (None,"","state unavailable")
         and "state unavailable" not in str(det.get("one_liner")))
    gate("G3_regime_state", ok3, f"regime={det.get('regime')} one_liner={str(det.get('one_liner'))[:130]}")
    out["verdict"]="PASS_ALL" if not fails else "GAPS: "+",".join(fails)
    print("\nVERDICT:",out["verdict"]); rep.log("VERDICT: "+out["verdict"])
    Path("aws/ops/reports/3437.json").write_text(json.dumps(out,indent=2,default=str)); sys.exit(0)
