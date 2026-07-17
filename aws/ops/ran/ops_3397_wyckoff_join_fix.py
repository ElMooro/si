"""ops 3397 — wyckoff join key fix (symbol|ticker) regate."""
import json, sys, time, urllib.request, io, zipfile
from datetime import datetime, timezone
from pathlib import Path
import boto3
from botocore.config import Config
from ops_report import report
LAM = boto3.client("lambda","us-east-1",config=Config(read_timeout=120,retries={"max_attempts":2}))
S3C = boto3.client("s3","us-east-1")
UA={"User-Agent":"Mozilla/5.0 (ops-3397)"}
def invoke_resilient(fn,tries=6):
    for k in range(tries):
        try: return LAM.invoke(FunctionName=fn,InvocationType="Event",Payload=b"{}")
        except Exception as e:
            if "TooManyRequests" in str(e) or "Rate Exceeded" in str(e): time.sleep(15*(k+1)); continue
            raise
    raise RuntimeError("throttled")
with report("3397_wyckoff_join_fix") as rep:
    rep.heading("ops 3397 — wyckoff join regate")
    out={"gates":{}}; fails=[]
    def gate(n,ok,d):
        out["gates"][n]={"ok":bool(ok),"detail":str(d)[:300]}
        line=("PASS  " if ok else "FAIL  ")+n+" — "+str(d)[:260]; print(line); rep.log(line)
        if not ok: fails.append(n)
    ok1=False; dl=time.time()+300
    while time.time()<dl:
        try:
            if LAM.get_function_configuration(FunctionName="justhodl-sector-capital-fusion").get("LastUpdateStatus")=="Successful":
                info=LAM.get_function(FunctionName="justhodl-sector-capital-fusion")
                with urllib.request.urlopen(urllib.request.Request(info["Code"]["Location"],headers=UA),timeout=60) as r:
                    if 'o.get("symbol") or o.get("ticker")' in zipfile.ZipFile(io.BytesIO(r.read())).read("lambda_function.py").decode("utf-8","replace"):
                        ok1=True; break
        except Exception: pass
        time.sleep(12)
    gate("G1_fix_deployed", ok1, "key-fix marker")
    t2=datetime.now(timezone.utc).isoformat(); invoke_resilient("justhodl-sector-capital-fusion")
    wy, ok2 = 0, False
    dl=time.time()+300
    while time.time()<dl:
        try:
            j=json.loads(S3C.get_object(Bucket="justhodl-dashboard-live",Key="data/sector-capital-fusion.json")["Body"].read())
            if (j.get("generated_at") or "")>t2:
                wy=sum(1 for r in (j.get("sectors") or []) if (r.get("technicals") or {}).get("wyckoff"))
                ok2 = wy>=6
                if ok2: break
        except Exception: pass
        time.sleep(15)
    gate("G2_wyckoff_joined", ok2, f"sectors_with_wyckoff={wy}")
    out["verdict"]="PASS_ALL" if not fails else "GAPS: "+",".join(fails)
    print("\nVERDICT:",out["verdict"]); rep.log("VERDICT: "+out["verdict"])
    Path("aws/ops/reports/3397.json").write_text(json.dumps(out,indent=2)); sys.exit(0)
