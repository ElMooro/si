"""ops 3448 — cannibals family clean verification (3447's runner executed a
malformed clone). Gates: settle · schedule · rule probe · live feed."""
import io, json, sys, time, urllib.request, zipfile
from datetime import datetime, timezone
from pathlib import Path
import boto3
from botocore.config import Config
from ops_report import report
LAM=boto3.client("lambda","us-east-1",config=Config(read_timeout=300,retries={"max_attempts":2}))
S3C=boto3.client("s3","us-east-1"); SCH=boto3.client("scheduler","us-east-1")
UA={"User-Agent":"Mozilla/5.0 (ops-3448)"}
FN="justhodl-cannibals"
with report("3448_cannibals_verify") as rep:
    rep.heading("ops 3448 — cannibals verify")
    out={"gates":{}}; fails=[]
    def gate(n,ok,d):
        out["gates"][n]={"ok":bool(ok),"detail":str(d)[:320]}
        line=("PASS  " if ok else "FAIL  ")+n+" — "+str(d)[:280]; print(line); rep.log(line)
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
    gate("G1_settled", ok1, "cannibals v1.0.0 in zip")
    try:
        arn=LAM.get_function_configuration(FunctionName=FN)["FunctionArn"]
        try: SCH.get_schedule(Name="justhodl-cannibals-daily"); cr="exists"
        except Exception:
            SCH.create_schedule(Name="justhodl-cannibals-daily",ScheduleExpression="cron(55 21 ? * * *)",
                FlexibleTimeWindow={"Mode":"OFF"},
                Target={"Arn":arn,"RoleArn":"arn:aws:iam::857687956942:role/justhodl-scheduler-role","Input":"{}"})
            cr="created"
    except Exception as e: cr=str(e)[:80]
    gate("G2_schedule", cr in ("exists","created"), cr)
    probe={"_probe":{"rows":[
      {"ticker":"GOODCO","flags":["INSIDER_CONVICTION"],"sh_3y_cagr_pct":-4.5},
      {"ticker":"DIRTY","flags":["INSIDER_CONVICTION","SBC_WASH"],"sh_3y_cagr_pct":-6.0},
      {"ticker":"DILUTER","flags":["INSIDER_CONVICTION"],"sh_3y_cagr_pct":3.0}],
      "sec_of":{"GOODCO":"Technology"}}}
    r=LAM.invoke(FunctionName=FN,InvocationType="RequestResponse",Payload=json.dumps(probe).encode())
    plans=json.loads(json.loads(r["Payload"].read()).get("body","{}")).get("plans") or []
    gate("G3_rule_probe", len(plans)==1 and plans[0].get("ticker")=="GOODCO"
         and plans[0].get("pair_etf")=="XLK",
         f"plans={[(p.get('ticker'),p.get('pair_etf')) for p in plans]}")
    t0=datetime.now(timezone.utc).isoformat()
    LAM.invoke(FunctionName=FN,InvocationType="RequestResponse",Payload=b"{}")
    feed={}
    try: feed=json.loads(S3C.get_object(Bucket="justhodl-dashboard-live",Key="data/cannibals.json")["Body"].read())
    except Exception: pass
    gate("G4_feed_live", feed.get("ok") is True and (feed.get("generated_at") or "")>t0,
         f"scanned={feed.get('n_rows_scanned')} plans={[p.get('ticker') for p in (feed.get('plans') or [])][:8]} logged={feed.get('logged')}")
    out["plans"]=(feed.get("plans") or [])[:10]
    out["verdict"]="PASS_ALL" if not fails else "GAPS: "+",".join(fails)
    print("\nVERDICT:",out["verdict"]); rep.log("VERDICT: "+out["verdict"])
    Path("aws/ops/reports/3448.json").write_text(json.dumps(out,indent=2,default=str)); sys.exit(0)
