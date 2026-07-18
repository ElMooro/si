"""ops 3458 — full filer names live (First+Last) through both engines."""
import io, json, sys, time, urllib.request, zipfile
from datetime import datetime, timezone
from pathlib import Path
import boto3
from botocore.config import Config
from ops_report import report
LAM=boto3.client("lambda","us-east-1",config=Config(read_timeout=300,retries={"max_attempts":0}))
S3C=boto3.client("s3","us-east-1")
UA={"User-Agent":"Mozilla/5.0 (ops-3458)"}
with report("3458_names_close") as rep:
    out={"gates":{}}; fails=[]
    def gate(n,ok,d):
        out["gates"][n]={"ok":bool(ok),"detail":str(d)[:340]}
        line=("PASS  " if ok else "FAIL  ")+n+" — "+str(d)[:300]; print(line); rep.log(line)
        if not ok: fails.append(n)
    ok1=False; dl=time.time()+420
    while time.time()<dl:
        try:
            if LAM.get_function_configuration(FunctionName="justhodl-congress-direct").get("LastUpdateStatus")=="Successful":
                info=LAM.get_function(FunctionName="justhodl-congress-direct")
                with urllib.request.urlopen(urllib.request.Request(info["Code"]["Location"],headers=UA),timeout=60) as r:
                    if 'VERSION = "1.0.2"' in zipfile.ZipFile(io.BytesIO(r.read())).read("lambda_function.py").decode("utf-8","replace"):
                        ok1=True; break
        except Exception: pass
        time.sleep(12)
    gate("G1_v102", ok1, "settled")
    t0=datetime.now(timezone.utc).isoformat()
    r=LAM.invoke(FunctionName="justhodl-congress-direct",InvocationType="RequestResponse",Payload=b"{}")
    fe=r.get("FunctionError")
    feed={}
    try: feed=json.loads(S3C.get_object(Bucket="justhodl-dashboard-live",Key="data/congress-direct.json")["Body"].read())
    except Exception: pass
    tx=((feed.get("senate") or {}).get("transactions")) or []
    named=[t.get("filer") for t in tx if t.get("filer") and " " in t.get("filer")]
    gate("G2_full_names", fe is None and (feed.get("generated_at") or "")>t0 and len(named)>=10,
         f"tx={len(tx)} full_named={len(named)} sample={sorted(set(named))[:4]}")
    LAM.invoke(FunctionName="justhodl-congress-alpha",InvocationType="Event",Payload=b"{}")
    out["verdict"]="PASS_ALL" if not fails else "GAPS: "+",".join(fails)
    print("\nVERDICT:",out["verdict"]); rep.log("VERDICT: "+out["verdict"])
    Path("aws/ops/reports/3458.json").write_text(json.dumps(out,indent=2,default=str)); sys.exit(0)
