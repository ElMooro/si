"""ops 3421 — #6 closed: GEX levels on setups via direct dealer-gex
underlyings extraction (flip scalar + top wall strikes + front max-pain).
Gate: fresh best-setups rows carry gamma_levels on >=3 of top 25."""
import json, sys, time, io, urllib.request, zipfile
from datetime import datetime, timezone
from pathlib import Path
import boto3
from botocore.config import Config
from ops_report import report
LAM=boto3.client("lambda","us-east-1",config=Config(read_timeout=340,retries={"max_attempts":2}))
S3C=boto3.client("s3","us-east-1")
UA={"User-Agent":"Mozilla/5.0 (ops-3421)"}
with report("3421_walls") as rep:
    rep.heading("ops 3421 — GEX walls join")
    out={"gates":{}}; fails=[]
    def gate(n,ok,d):
        out["gates"][n]={"ok":bool(ok),"detail":str(d)[:320]}
        line=("PASS  " if ok else "FAIL  ")+n+" — "+str(d)[:280]; print(line); rep.log(line)
        if not ok: fails.append(n)
    ok1=False; dl=time.time()+360
    while time.time()<dl:
        try:
            if LAM.get_function_configuration(FunctionName="justhodl-best-setups").get("LastUpdateStatus")=="Successful":
                info=LAM.get_function(FunctionName="justhodl-best-setups")
                with urllib.request.urlopen(urllib.request.Request(info["Code"]["Location"],headers=UA),timeout=60) as r:
                    if "call_walls_top5" in zipfile.ZipFile(io.BytesIO(r.read())).read("lambda_function.py").decode("utf-8","replace"):
                        ok1=True; break
        except Exception: pass
        time.sleep(12)
    gate("G1_settled", ok1, "extractor marker")
    t0=datetime.now(timezone.utc).isoformat()
    LAM.invoke(FunctionName="justhodl-best-setups",InvocationType="Event",Payload=b"{}")
    rows=[]; dl=time.time()+480
    while time.time()<dl:
        try:
            j=json.loads(S3C.get_object(Bucket="justhodl-dashboard-live",Key="data/best-setups.json")["Body"].read())
            if (j.get("generated_at") or j.get("as_of") or "")>t0:
                rows=(j.get("top_setups") or []); break
        except Exception: pass
        time.sleep(20)
    n_w=sum(1 for r in rows[:25] if r.get("gamma_levels"))
    samp=[{r["ticker"]:r["gamma_levels"]} for r in rows[:25] if r.get("gamma_levels")][:3]
    gate("G2_walls_on_setups", n_w>=3, f"walls={n_w}/25 sample={json.dumps(samp)[:200]}")
    out["verdict"]="PASS_ALL" if not fails else "GAPS: "+",".join(fails)
    print("\nVERDICT:",out["verdict"]); rep.log("VERDICT: "+out["verdict"])
    Path("aws/ops/reports/3421.json").write_text(json.dumps(out,indent=2)); sys.exit(0)
