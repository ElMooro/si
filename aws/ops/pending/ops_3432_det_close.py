"""ops 3431 — census #2 closure: deterministic rotation proof + orphan
retirements. Redeployed router falls back to engine-synthesis briefs when
LLM is cost-gated; invoke full rotation; gate >=12 panel feeds regenerated
TODAY incl regime-decisive-call with mode=deterministic. Publish
config/feed-retired.json (backtest-summary, market-internals-history,
finviz-signals-state: writerless orphans) for registry exemption."""
import io, json, sys, time, urllib.request, zipfile
from datetime import datetime, timezone
from pathlib import Path
import boto3
from botocore.config import Config
from ops_report import report
LAM=boto3.client("lambda","us-east-1",config=Config(read_timeout=600,retries={"max_attempts":0}))
S3C=boto3.client("s3","us-east-1")
UA={"User-Agent":"Mozilla/5.0 (ops-3431)"}
RETIRED=["data/backtest-summary.json","data/market-internals-history.json","data/finviz-signals-state.json"]
with report("3432_det_close") as rep:
    rep.heading("ops 3431 — deterministic rotation")
    out={"gates":{}}; fails=[]
    def gate(n,ok,d):
        out["gates"][n]={"ok":bool(ok),"detail":str(d)[:340]}
        line=("PASS  " if ok else "FAIL  ")+n+" — "+str(d)[:300]; print(line); rep.log(line)
        if not ok: fails.append(n)
    ok1=False; dl=time.time()+360
    while time.time()<dl:
        try:
            if LAM.get_function_configuration(FunctionName="justhodl-ai-brief-router").get("LastUpdateStatus")=="Successful":
                info=LAM.get_function(FunctionName="justhodl-ai-brief-router")
                with urllib.request.urlopen(urllib.request.Request(info["Code"]["Location"],headers=UA),timeout=60) as r:
                    if "_det_brief" in zipfile.ZipFile(io.BytesIO(r.read())).read("lambda_function.py").decode("utf-8","replace"):
                        ok1=True; break
        except Exception: pass
        time.sleep(12)
    gate("G1_router_v2", ok1, "det-fallback in zip")
    reg=json.loads(S3C.get_object(Bucket="justhodl-dashboard-live",Key="config/ai-brief-contexts.json")["Body"].read())
    panel=[c for c in (reg.get("contexts") or {}) if c.endswith("-decisive-call")]
    t0=datetime.now(timezone.utc)
    try:
        r=LAM.invoke(FunctionName="justhodl-ai-brief-router",InvocationType="RequestResponse",Payload=b"{}")
        print("[invoke]",r.get("StatusCode"),r.get("FunctionError"))
    except Exception as e:
        print("[invoke] sync fail -> Event:",str(e)[:70])
        LAM.invoke(FunctionName="justhodl-ai-brief-router",InvocationType="Event",Payload=b"{}")
    fresh=0; dl=time.time()+420; det=None
    while time.time()<dl:
        fresh=0
        for c in panel:
            try:
                if S3C.head_object(Bucket="justhodl-dashboard-live",Key=f"data/{c}.json")["LastModified"]>=t0: fresh+=1
            except Exception: pass
        if fresh>=26:
            try:
                det=json.loads(S3C.get_object(Bucket="justhodl-dashboard-live",Key="data/regime-decisive-call.json")["Body"].read())
                if (det.get("generated_at") or "")>t0.isoformat(): break
            except Exception: pass
        time.sleep(20)
    gate("G2_rotation_live", fresh>=26 and det and det.get("mode")=="deterministic",
         f"fresh={fresh}/{len(panel)} regime_mode={det and det.get('mode')} one_liner={det and str(det.get('one_liner'))[:110]}")
    rj=json.loads(S3C.get_object(Bucket="justhodl-dashboard-live",Key="config/feed-retired.json")["Body"].read())
    gate("G3_retired_list", set(rj.get("retired") or [])==set(RETIRED), f"{len(rj.get('retired') or [])} on file")
    out["verdict"]="PASS_ALL" if not fails else "GAPS: "+",".join(fails)
    print("\nVERDICT:",out["verdict"]); rep.log("VERDICT: "+out["verdict"])
    Path("aws/ops/reports/3432.json").write_text(json.dumps(out,indent=2,default=str)); sys.exit(0)
