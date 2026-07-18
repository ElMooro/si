"""ops 3435 — det rotation on PROVABLY-new code (DET_FALLBACK_V2 marker
settle before invoke; the 3432 rotation raced the parallel deploy and ran
old code). Gate: >=30/33 panels fresh + regime mode=deterministic."""
import io, json, sys, time, urllib.request, zipfile
from datetime import datetime, timezone
from pathlib import Path
import boto3
from botocore.config import Config
from ops_report import report
LAM=boto3.client("lambda","us-east-1",config=Config(read_timeout=600,retries={"max_attempts":0}))
S3C=boto3.client("s3","us-east-1")
UA={"User-Agent":"Mozilla/5.0 (ops-3435)"}
with report("3435_det_final") as rep:
    rep.heading("ops 3435 — det rotation final")
    out={"gates":{}}; fails=[]
    def gate(n,ok,d):
        out["gates"][n]={"ok":bool(ok),"detail":str(d)[:340]}
        line=("PASS  " if ok else "FAIL  ")+n+" — "+str(d)[:300]; print(line); rep.log(line)
        if not ok: fails.append(n)
    ok1=False; dl=time.time()+420
    while time.time()<dl:
        try:
            if LAM.get_function_configuration(FunctionName="justhodl-ai-brief-router").get("LastUpdateStatus")=="Successful":
                info=LAM.get_function(FunctionName="justhodl-ai-brief-router")
                with urllib.request.urlopen(urllib.request.Request(info["Code"]["Location"],headers=UA),timeout=60) as r:
                    if "DET_FALLBACK_V2" in zipfile.ZipFile(io.BytesIO(r.read())).read("lambda_function.py").decode("utf-8","replace"):
                        ok1=True; break
        except Exception: pass
        time.sleep(12)
    gate("G1_v2_settled", ok1, "DET_FALLBACK_V2 in zip")
    reg=json.loads(S3C.get_object(Bucket="justhodl-dashboard-live",Key="config/ai-brief-contexts.json")["Body"].read())
    panel=[c for c in (reg.get("contexts") or {}) if c.endswith("-decisive-call")]
    t0=datetime.now(timezone.utc)
    try:
        r=LAM.invoke(FunctionName="justhodl-ai-brief-router",InvocationType="RequestResponse",Payload=b"{}")
        print("[invoke]",r.get("StatusCode"),r.get("FunctionError"))
    except Exception as e:
        print("[invoke] sync fail -> Event:",str(e)[:70])
        LAM.invoke(FunctionName="justhodl-ai-brief-router",InvocationType="Event",Payload=b"{}")
    fresh=0; det=None; dl=time.time()+420
    while time.time()<dl:
        fresh=0
        for c in panel:
            try:
                if S3C.head_object(Bucket="justhodl-dashboard-live",Key=f"data/{c}.json")["LastModified"]>=t0: fresh+=1
            except Exception: pass
        if fresh>=30:
            try:
                det=json.loads(S3C.get_object(Bucket="justhodl-dashboard-live",Key="data/regime-decisive-call.json")["Body"].read())
                if (det.get("generated_at") or "")>t0.isoformat(): break
            except Exception: pass
        time.sleep(20)
    gate("G2_rotation_full", fresh>=30 and det and det.get("mode")=="deterministic",
         f"fresh={fresh}/{len(panel)} regime_mode={det and det.get('mode')} one_liner={det and str(det.get('one_liner'))[:120]}")
    out["verdict"]="PASS_ALL" if not fails else "GAPS: "+",".join(fails)
    print("\nVERDICT:",out["verdict"]); rep.log("VERDICT: "+out["verdict"])
    Path("aws/ops/reports/3435.json").write_text(json.dumps(out,indent=2,default=str)); sys.exit(0)
