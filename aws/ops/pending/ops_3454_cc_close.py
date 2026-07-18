"""ops 3454 — credit-composite v1.0.1 live close."""
import io, json, sys, time, urllib.request, zipfile
from datetime import datetime, timezone
from pathlib import Path
import boto3
from botocore.config import Config
from ops_report import report
LAM=boto3.client("lambda","us-east-1",config=Config(read_timeout=300,retries={"max_attempts":2}))
S3C=boto3.client("s3","us-east-1")
UA={"User-Agent":"Mozilla/5.0 (ops-3454)"}
with report("3454_cc_close") as rep:
    out={"gates":{}}; fails=[]
    def gate(n,ok,d):
        out["gates"][n]={"ok":bool(ok),"detail":str(d)[:340]}
        line=("PASS  " if ok else "FAIL  ")+n+" — "+str(d)[:300]; print(line); rep.log(line)
        if not ok: fails.append(n)
    ok1=False; dl=time.time()+420
    while time.time()<dl:
        try:
            if LAM.get_function_configuration(FunctionName="justhodl-credit-composite").get("LastUpdateStatus")=="Successful":
                info=LAM.get_function(FunctionName="justhodl-credit-composite")
                with urllib.request.urlopen(urllib.request.Request(info["Code"]["Location"],headers=UA),timeout=60) as r:
                    if 'VERSION = "1.0.1"' in zipfile.ZipFile(io.BytesIO(r.read())).read("lambda_function.py").decode("utf-8","replace"):
                        ok1=True; break
        except Exception: pass
        time.sleep(12)
    gate("G1_v101", ok1, "settled")
    t0=datetime.now(timezone.utc).isoformat()
    r=LAM.invoke(FunctionName="justhodl-credit-composite",InvocationType="RequestResponse",Payload=b"{}")
    fe=r.get("FunctionError")
    time.sleep(3)
    feed={}
    try: feed=json.loads(S3C.get_object(Bucket="justhodl-dashboard-live",Key="data/credit-composite.json")["Body"].read())
    except Exception: pass
    lens=feed.get("lenses") or {}
    gate("G2_live", fe is None and (feed.get("generated_at") or "")>t0 and isinstance(feed.get("composite"),(int,float)),
         f"err={fe} composite={feed.get('composite')} lens={ {k:v.get('pts') for k,v in lens.items()} } "
         f"details={ {k:str(v.get('detail'))[:60] for k,v in lens.items()} } plans={[p.get('etf') for p in (feed.get('plans') or [])]} logged={feed.get('logged')}")
    out["live"]=feed.get("lenses")
    out["verdict"]="PASS_ALL" if not fails else "GAPS: "+",".join(fails)
    print("\nVERDICT:",out["verdict"]); rep.log("VERDICT: "+out["verdict"])
    Path("aws/ops/reports/3454.json").write_text(json.dumps(out,indent=2,default=str)); sys.exit(0)
