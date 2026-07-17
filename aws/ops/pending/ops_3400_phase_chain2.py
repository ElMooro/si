"""ops 3399 — deterministic chain: radar feed fresh w/ etf_phases -> fusion -> gate."""
import json, sys, time
from datetime import datetime, timezone
from pathlib import Path
import boto3
from botocore.config import Config
from ops_report import report
LAM = boto3.client("lambda","us-east-1",config=Config(read_timeout=120,retries={"max_attempts":2}))
S3C = boto3.client("s3","us-east-1")
def invoke_resilient(fn,tries=6):
    for k in range(tries):
        try: return LAM.invoke(FunctionName=fn,InvocationType="Event",Payload=b"{}")
        except Exception as e:
            if "TooManyRequests" in str(e) or "Rate Exceeded" in str(e): time.sleep(15*(k+1)); continue
            raise
    raise RuntimeError("throttled")
with report("3400_phase_chain2") as rep:
    rep.heading("ops 3399 — radar->fusion phase chain")
    out={"gates":{}}; fails=[]
    def gate(n,ok,d):
        out["gates"][n]={"ok":bool(ok),"detail":str(d)[:300]}
        line=("PASS  " if ok else "FAIL  ")+n+" — "+str(d)[:260]; print(line); rep.log(line)
        if not ok: fails.append(n)
    t0=datetime.now(timezone.utc).isoformat()
    invoke_resilient("justhodl-accumulation-radar")
    nph, ok1 = 0, False
    dl=time.time()+480
    while time.time()<dl:
        try:
            j=json.loads(S3C.get_object(Bucket="justhodl-dashboard-live",Key="data/accumulation-radar.json")["Body"].read())
            if (j.get("generated_at") or "")>t0 and j.get("etf_phases"):
                nph=len(j["etf_phases"]); xl=sum(1 for k in j["etf_phases"] if str(k).startswith("XL"))
                ok1 = xl>=10
                if ok1: break
        except Exception: pass
        time.sleep(20)
    gate("G1_radar_phases_fresh", ok1, f"etf_phases={nph} xl_count={xl if ok1 else '<10'}")
    t1=datetime.now(timezone.utc).isoformat()
    invoke_resilient("justhodl-sector-capital-fusion")
    wy, ok2 = 0, False
    dl=time.time()+300
    while time.time()<dl:
        try:
            j=json.loads(S3C.get_object(Bucket="justhodl-dashboard-live",Key="data/sector-capital-fusion.json")["Body"].read())
            if (j.get("generated_at") or "")>t1:
                wy=sum(1 for r in (j.get("sectors") or []) if (r.get("technicals") or {}).get("wyckoff"))
                ok2 = wy>=10
                if ok2: break
        except Exception: pass
        time.sleep(15)
    gate("G2_wyckoff_joined", ok2, f"sectors_with_wyckoff={wy}/11")
    out["verdict"]="PASS_ALL" if not fails else "GAPS: "+",".join(fails)
    print("\nVERDICT:",out["verdict"]); rep.log("VERDICT: "+out["verdict"])
    Path("aws/ops/reports/3400.json").write_text(json.dumps(out,indent=2)); sys.exit(0)
