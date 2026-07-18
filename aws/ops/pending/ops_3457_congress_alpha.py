"""ops 3457 — grade-the-filers live. Gates: settle · schedule 15:45 ·
runner probe (buys only, deduped) · live invoke → feeds written, purchases
counted, skill ledger initialized (empty-but-valid day one)."""
import io, json, sys, time, urllib.request, zipfile
from datetime import datetime, timezone
from pathlib import Path
import boto3
from botocore.config import Config
from ops_report import report
LAM=boto3.client("lambda","us-east-1",config=Config(read_timeout=300,retries={"max_attempts":0}))
S3C=boto3.client("s3","us-east-1"); SCH=boto3.client("scheduler","us-east-1")
UA={"User-Agent":"Mozilla/5.0 (ops-3457)"}
FN="justhodl-congress-alpha"
with report("3457_congress_alpha") as rep:
    rep.heading("ops 3457 — grade the filers")
    out={"gates":{}}; fails=[]
    def gate(n,ok,d):
        out["gates"][n]={"ok":bool(ok),"detail":str(d)[:340]}
        line=("PASS  " if ok else "FAIL  ")+n+" — "+str(d)[:300]; print(line); rep.log(line)
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
        try: SCH.get_schedule(Name="justhodl-congress-alpha-daily"); cr="exists"
        except Exception:
            SCH.create_schedule(Name="justhodl-congress-alpha-daily",ScheduleExpression="cron(45 15 ? * * *)",
                FlexibleTimeWindow={"Mode":"OFF"},
                Target={"Arn":arn,"RoleArn":"arn:aws:iam::857687956942:role/justhodl-scheduler-role","Input":"{}"})
            cr="created"
    except Exception as e: cr=str(e)[:80]
    gate("G2_schedule", cr in ("exists","created"), cr)
    probe={"_probe":{"transactions":[
      {"filer":"Sen A","ticker":"NVDA","type":"Purchase","amount":"$1,001 - $15,000","tx_date":"07/10/2026"},
      {"filer":"Sen B","ticker":"LMT","type":"Sale (Full)","amount":"x","tx_date":"07/09/2026"}]}}
    r=LAM.invoke(FunctionName=FN,InvocationType="RequestResponse",Payload=json.dumps(probe).encode())
    plans=json.loads(json.loads(r["Payload"].read()).get("body","{}")).get("plans") or []
    gate("G3_probe", [(p.get("filer"),p.get("ticker")) for p in plans]==[("Sen A","NVDA")],
         f"plans={[(p.get('filer'),p.get('ticker')) for p in plans]}")
    t0=datetime.now(timezone.utc).isoformat()
    r=LAM.invoke(FunctionName=FN,InvocationType="RequestResponse",Payload=b"{}")
    fe=r.get("FunctionError")
    feed={}; skill={}
    try: feed=json.loads(S3C.get_object(Bucket="justhodl-dashboard-live",Key="data/congress-alpha.json")["Body"].read())
    except Exception: pass
    try: skill=json.loads(S3C.get_object(Bucket="justhodl-dashboard-live",Key="data/congress-filer-skill.json")["Body"].read())
    except Exception: pass
    buys=feed.get("n_purchases")
    ok4=(fe is None and (feed.get("generated_at") or "")>t0 and isinstance(buys,int)
         and skill.get("ok") is True
         and (buys==0 or feed.get("logged",0)>=1))
    gate("G4_live", ok4,
         f"err={fe} src={feed.get('n_src_transactions')} buys={buys} logged={feed.get('logged')} "
         f"filers_tracked={feed.get('n_filers_tracked')} plans={[(p.get('filer'),p.get('ticker')) for p in (feed.get('plans') or [])][:5]}")
    out["live"]={"buys":buys,"plans":feed.get("plans")}
    out["verdict"]="PASS_ALL" if not fails else "GAPS: "+",".join(fails)
    print("\nVERDICT:",out["verdict"]); rep.log("VERDICT: "+out["verdict"])
    Path("aws/ops/reports/3457.json").write_text(json.dumps(out,indent=2,default=str)); sys.exit(0)
