"""ops 3451 — cannibals v1.1 close: feed-vocabulary rule live."""
import io, json, sys, time, urllib.request, zipfile
from datetime import datetime, timezone
from pathlib import Path
import boto3
from botocore.config import Config
from ops_report import report
LAM=boto3.client("lambda","us-east-1",config=Config(read_timeout=300,retries={"max_attempts":2}))
S3C=boto3.client("s3","us-east-1")
UA={"User-Agent":"Mozilla/5.0 (ops-3451)"}
with report("3451_cannibals_v11") as rep:
    out={"gates":{}}; fails=[]
    def gate(n,ok,d):
        out["gates"][n]={"ok":bool(ok),"detail":str(d)[:320]}
        line=("PASS  " if ok else "FAIL  ")+n+" — "+str(d)[:280]; print(line); rep.log(line)
        if not ok: fails.append(n)
    ok1=False; dl=time.time()+420
    while time.time()<dl:
        try:
            if LAM.get_function_configuration(FunctionName="justhodl-cannibals").get("LastUpdateStatus")=="Successful":
                info=LAM.get_function(FunctionName="justhodl-cannibals")
                with urllib.request.urlopen(urllib.request.Request(info["Code"]["Location"],headers=UA),timeout=60) as r:
                    if 'VERSION = "1.1.0"' in zipfile.ZipFile(io.BytesIO(r.read())).read("lambda_function.py").decode("utf-8","replace"):
                        ok1=True; break
        except Exception: pass
        time.sleep(12)
    gate("G1_v11", ok1, "settled")
    t0=datetime.now(timezone.utc).isoformat()
    LAM.invoke(FunctionName="justhodl-cannibals",InvocationType="RequestResponse",Payload=b"{}")
    time.sleep(3)
    feed={}
    try: feed=json.loads(S3C.get_object(Bucket="justhodl-dashboard-live",Key="data/cannibals.json")["Body"].read())
    except Exception: pass
    plans=feed.get("plans") or []
    gate("G2_live_book", (feed.get("generated_at") or "")>t0 and feed.get("n_rows_scanned",0)>=500 and len(plans)>=3,
         f"scanned={feed.get('n_rows_scanned')} logged={feed.get('logged')} plans={[(p.get('ticker'),p.get('net_buyback_yield_pct'),'CONV' if p.get('insider_conviction') else '') for p in plans][:8]}")
    out["plans"]=plans
    out["verdict"]="PASS_ALL" if not fails else "GAPS: "+",".join(fails)
    print("\nVERDICT:",out["verdict"]); rep.log("VERDICT: "+out["verdict"])
    Path("aws/ops/reports/3451.json").write_text(json.dumps(out,indent=2,default=str)); sys.exit(0)
