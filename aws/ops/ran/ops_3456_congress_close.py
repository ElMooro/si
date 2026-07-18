"""ops 3456 — congress-direct v1.0.1 close: tickers resolve."""
import io, json, sys, time, urllib.request, zipfile
from datetime import datetime, timezone
from pathlib import Path
import boto3
from botocore.config import Config
from ops_report import report
LAM=boto3.client("lambda","us-east-1",config=Config(read_timeout=300,retries={"max_attempts":0}))
S3C=boto3.client("s3","us-east-1")
UA={"User-Agent":"Mozilla/5.0 (ops-3456)"}
with report("3456_congress_close") as rep:
    out={"gates":{}}; fails=[]
    def gate(n,ok,d):
        out["gates"][n]={"ok":bool(ok),"detail":str(d)[:360]}
        line=("PASS  " if ok else "FAIL  ")+n+" — "+str(d)[:320]; print(line); rep.log(line)
        if not ok: fails.append(n)
    ok1=False; dl=time.time()+420
    while time.time()<dl:
        try:
            if LAM.get_function_configuration(FunctionName="justhodl-congress-direct").get("LastUpdateStatus")=="Successful":
                info=LAM.get_function(FunctionName="justhodl-congress-direct")
                with urllib.request.urlopen(urllib.request.Request(info["Code"]["Location"],headers=UA),timeout=60) as r:
                    if 'VERSION = "1.0.1"' in zipfile.ZipFile(io.BytesIO(r.read())).read("lambda_function.py").decode("utf-8","replace"):
                        ok1=True; break
        except Exception: pass
        time.sleep(12)
    gate("G1_v101", ok1, "settled")
    t0=datetime.now(timezone.utc).isoformat()
    r=LAM.invoke(FunctionName="justhodl-congress-direct",InvocationType="RequestResponse",Payload=b"{}")
    fe=r.get("FunctionError")
    feed={}
    try: feed=json.loads(S3C.get_object(Bucket="justhodl-dashboard-live",Key="data/congress-direct.json")["Body"].read())
    except Exception: pass
    sen=(feed.get("senate") or {})
    samp=[(t.get("filer"),t.get("ticker"),t.get("type"),t.get("amount")) for t in (sen.get("transactions") or []) if t.get("ticker")][:6]
    gate("G2_tickers", fe is None and (feed.get("generated_at") or "")>t0 and sen.get("n_with_ticker",0)>=5,
         f"tx={sen.get('n_transactions')} tickered={sen.get('n_with_ticker')} samp={samp}")
    out["sample"]=samp
    out["verdict"]="PASS_ALL" if not fails else "GAPS: "+",".join(fails)
    print("\nVERDICT:",out["verdict"]); rep.log("VERDICT: "+out["verdict"])
    Path("aws/ops/reports/3456.json").write_text(json.dumps(out,indent=2,default=str)); sys.exit(0)
