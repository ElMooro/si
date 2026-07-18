"""ops 3455 — creative #3: official-source congress rail live. Gates:
settle · schedule 15:30 · live invoke → senate transactions parsed with
tickers + house PTR filings + no fatal errors · page section live."""
import io, json, sys, time, urllib.request, zipfile
from datetime import datetime, timezone
from pathlib import Path
import boto3
from botocore.config import Config
from ops_report import report
LAM=boto3.client("lambda","us-east-1",config=Config(read_timeout=300,retries={"max_attempts":0}))
S3C=boto3.client("s3","us-east-1"); SCH=boto3.client("scheduler","us-east-1")
UA={"User-Agent":"Mozilla/5.0 (ops-3455)"}
FN="justhodl-congress-direct"
with report("3455_congress_direct") as rep:
    rep.heading("ops 3455 — congress direct")
    out={"gates":{}}; fails=[]
    def gate(n,ok,d):
        out["gates"][n]={"ok":bool(ok),"detail":str(d)[:360]}
        line=("PASS  " if ok else "FAIL  ")+n+" — "+str(d)[:320]; print(line); rep.log(line)
        if not ok: fails.append(n)
    ok1=False; dl=time.time()+420
    while time.time()<dl:
        try:
            if LAM.get_function_configuration(FunctionName=FN).get("LastUpdateStatus")=="Successful":
                info=LAM.get_function(FunctionName=FN)
                with urllib.request.urlopen(urllib.request.Request(info["Code"]["Location"],headers=UA),timeout=60) as r:
                    if 'VERSION = "1.0.0"' in zipfile.ZipFile(io.BytesIO(r.read())).read("lambda_function.py").decode("utf-8","replace"):
                        ok1=True; break
        except Exception: pass
        time.sleep(12)
    gate("G1_settled", ok1, "marker")
    try:
        arn=LAM.get_function_configuration(FunctionName=FN)["FunctionArn"]
        try: SCH.get_schedule(Name="justhodl-congress-direct-daily"); cr="exists"
        except Exception:
            SCH.create_schedule(Name="justhodl-congress-direct-daily",ScheduleExpression="cron(30 15 ? * * *)",
                FlexibleTimeWindow={"Mode":"OFF"},
                Target={"Arn":arn,"RoleArn":"arn:aws:iam::857687956942:role/justhodl-scheduler-role","Input":"{}"})
            cr="created"
    except Exception as e: cr=str(e)[:80]
    gate("G2_schedule", cr in ("exists","created"), cr)
    t0=datetime.now(timezone.utc).isoformat()
    r=LAM.invoke(FunctionName=FN,InvocationType="RequestResponse",Payload=b"{}")
    fe=r.get("FunctionError"); print("[invoke]",r.get("StatusCode"),fe, r["Payload"].read()[:200])
    feed={}
    try: feed=json.loads(S3C.get_object(Bucket="justhodl-dashboard-live",Key="data/congress-direct.json")["Body"].read())
    except Exception: pass
    sen=(feed.get("senate") or {}); hou=(feed.get("house") or {})
    samp=[(t.get("filer"),t.get("ticker"),t.get("type")) for t in (sen.get("transactions") or []) if t.get("ticker")][:5]
    gate("G3_live_data", fe is None and (feed.get("generated_at") or "")>t0
         and sen.get("n_transactions",0)>=1 and sen.get("n_with_ticker",0)>=1
         and hou.get("n_ptr_filings",0)>=1,
         f"senate tx={sen.get('n_transactions')} tickered={sen.get('n_with_ticker')} "
         f"house={hou.get('n_ptr_filings')} errs=({sen.get('error')},{hou.get('error')}) samp={samp}")
    ok4=False; dl=time.time()+300
    while time.time()<dl:
        try:
            with urllib.request.urlopen(urllib.request.Request(f"https://justhodl.ai/political.html?t={int(time.time())}",headers=UA),timeout=25) as r2:
                if "OPS3455" in r2.read().decode("utf-8","replace"): ok4=True; break
        except Exception: pass
        time.sleep(15)
    gate("G4_page_live", ok4, "OPS3455 on political.html")
    out["sample"]=samp
    out["verdict"]="PASS_ALL" if not fails else "GAPS: "+",".join(fails)
    print("\nVERDICT:",out["verdict"]); rep.log("VERDICT: "+out["verdict"])
    Path("aws/ops/reports/3455.json").write_text(json.dumps(out,indent=2,default=str)); sys.exit(0)
