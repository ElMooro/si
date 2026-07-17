"""ops 3395 — dormant true-YoY path (ledger) + hunt closure. Gate: v2.4.6
deployed with ledger-yoy marker; fresh run healthy with no ledger-yoy error
(dormant until ~2027-07 when rows>=300, then self-activates for SG/HK/TW/PE)."""
import io, json, sys, time, urllib.request, zipfile
from datetime import datetime, timezone
from pathlib import Path
import boto3
from botocore.config import Config
from ops_report import report
LAM = boto3.client("lambda", "us-east-1", config=Config(read_timeout=120, retries={"max_attempts": 2}))
S3C = boto3.client("s3", "us-east-1")
UA={"User-Agent":"Mozilla/5.0 (ops-3395)"}
FN="justhodl-sovereign-stress"
def invoke_resilient(fn, tries=6):
    for k in range(tries):
        try: return LAM.invoke(FunctionName=fn, InvocationType="Event", Payload=b"{}")
        except Exception as e:
            if "TooManyRequests" in str(e) or "Rate Exceeded" in str(e): time.sleep(15*(k+1)); continue
            raise
    raise RuntimeError("throttled")
with report("3395_ledger_yoy_dormant") as rep:
    rep.heading("ops 3395 — dormant ledger-YoY + closure")
    out={"gates":{}}; fails=[]
    def gate(n,ok,d):
        out["gates"][n]={"ok":bool(ok),"detail":str(d)[:300]}
        line=("PASS  " if ok else "FAIL  ")+n+" — "+str(d)[:260]; print(line); rep.log(line)
        if not ok: fails.append(n)
    ok1=False; dl=time.time()+300
    while time.time()<dl:
        try:
            if LAM.get_function_configuration(FunctionName=FN).get("LastUpdateStatus")=="Successful":
                info=LAM.get_function(FunctionName=FN)
                with urllib.request.urlopen(urllib.request.Request(info["Code"]["Location"],headers=UA),timeout=60) as r:
                    src=zipfile.ZipFile(io.BytesIO(r.read())).read("lambda_function.py").decode("utf-8","replace")
                if 'VERSION = "2.4.6"' in src and "true stress YoY" in src: ok1=True; break
        except Exception: pass
        time.sleep(12)
    gate("G1_246_settled", ok1, "ledger-yoy marker in zip")
    t_inv=datetime.now(timezone.utc).isoformat(); invoke_resilient(FN)
    ok2, det2=False, "no fresh feed"
    dl=time.time()+540
    while time.time()<dl:
        try:
            j=json.loads(S3C.get_object(Bucket="justhodl-dashboard-live",Key="data/sovereign-stress.json")["Body"].read())
            if (j.get("generated_at") or "")>t_inv:
                led_errs=[e for e in (j.get("errors") or []) if "ledger-yoy" in e]
                ok2 = j.get("version")=="2.4.6" and not led_errs
                det2=f"v={j.get('version')} ledger_yoy_errs={led_errs[:2]} (dormant, activates ~2027-07)"
                break
        except Exception: pass
        time.sleep(20)
    gate("G2_dormant_healthy", ok2, det2)
    out["verdict"]="PASS_ALL" if not fails else "GAPS: "+",".join(fails)
    print("\nVERDICT:",out["verdict"]); rep.log("VERDICT: "+out["verdict"])
    Path("aws/ops/reports/3395.json").write_text(json.dumps(out,indent=2)); sys.exit(0)
