"""ops 3449 — creative #9: cannibals-with-conviction family. Gates: settle +
schedule 21:55 · invoke -> feed with n_universe>=300 (share-flows breadth
proves extraction) + screen rows carry pair_etf + ddb logged==feed.logged."""
import io, json, sys, time, urllib.request, zipfile
from datetime import datetime, timezone
from pathlib import Path
import boto3
from botocore.config import Config
from boto3.dynamodb.conditions import Attr
from ops_report import report
LAM=boto3.client("lambda","us-east-1",config=Config(read_timeout=300,retries={"max_attempts":2}))
S3C=boto3.client("s3","us-east-1"); DDB=boto3.resource("dynamodb","us-east-1")
SCH=boto3.client("scheduler","us-east-1")
UA={"User-Agent":"Mozilla/5.0 (ops-3449)"}
with report("3449_cannibals") as rep:
    rep.heading("ops 3449 — cannibals family")
    out={"gates":{}}; fails=[]
    def gate(n,ok,d):
        out["gates"][n]={"ok":bool(ok),"detail":str(d)[:330]}
        line=("PASS  " if ok else "FAIL  ")+n+" — "+str(d)[:290]; print(line); rep.log(line)
        if not ok: fails.append(n)
    ok1=False; dl=time.time()+400
    while time.time()<dl:
        try:
            if LAM.get_function_configuration(FunctionName="justhodl-cannibals").get("LastUpdateStatus")=="Successful":
                info=LAM.get_function(FunctionName="justhodl-cannibals")
                with urllib.request.urlopen(urllib.request.Request(info["Code"]["Location"],headers=UA),timeout=60) as r:
                    if 'VERSION = "1.0.0"' in zipfile.ZipFile(io.BytesIO(r.read())).read("lambda_function.py").decode():
                        ok1=True; break
        except Exception: pass
        time.sleep(12)
    try:
        arn=LAM.get_function_configuration(FunctionName="justhodl-cannibals")["FunctionArn"]
        try: SCH.get_schedule(Name="justhodl-cannibals-daily"); cr="exists"
        except Exception:
            SCH.create_schedule(Name="justhodl-cannibals-daily",ScheduleExpression="cron(55 21 ? * * *)",
                FlexibleTimeWindow={"Mode":"OFF"},
                Target={"Arn":arn,"RoleArn":"arn:aws:iam::857687956942:role/justhodl-scheduler-role","Input":"{}"})
            cr="created"
    except Exception as e: cr=str(e)[:80]
    gate("G1_deployed_scheduled", ok1 and cr in ("exists","created"), f"settled={ok1} sched={cr}")
    t0=datetime.now(timezone.utc).isoformat()
    LAM.invoke(FunctionName="justhodl-cannibals",InvocationType="RequestResponse",Payload=b"{}")
    feed={}
    try: feed=json.loads(S3C.get_object(Bucket="justhodl-dashboard-live",Key="data/cannibals.json")["Body"].read())
    except Exception: pass
    scr=feed.get("screen") or []
    ok2=(feed.get("generated_at") or "")>t0 and feed.get("n_universe",0)>=300 and all(x.get("pair_etf") for x in scr)
    gate("G2_screen_live", ok2,
         f"universe={feed.get('n_universe')} screen={feed.get('n_screen')} logged={feed.get('logged')} names={[(x['ticker'],x['sh_3y_cagr_pct']) for x in scr[:6]]}")
    n=0; lek=None; today=datetime.now(timezone.utc).date().isoformat()
    tbl=DDB.Table("justhodl-signals")
    while True:
        kw={"FilterExpression":Attr("signal_type").eq("cannibal-conviction")}
        if lek: kw["ExclusiveStartKey"]=lek
        rr=tbl.scan(**kw)
        n+=sum(1 for it in rr.get("Items",[]) if str(it.get("logged_at",""))[:10]==today)
        lek=rr.get("LastEvaluatedKey")
        if not lek: break
    gate("G3_signals_match", n==(feed.get("logged") or 0), f"ddb_today={n} feed.logged={feed.get('logged')}")
    out["screen"]=scr[:10]
    out["verdict"]="PASS_ALL" if not fails else "GAPS: "+",".join(fails)
    print("\nVERDICT:",out["verdict"]); rep.log("VERDICT: "+out["verdict"])
    Path("aws/ops/reports/3449.json").write_text(json.dumps(out,indent=2,default=str)); sys.exit(0)
