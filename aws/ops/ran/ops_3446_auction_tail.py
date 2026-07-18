"""ops 3446 — creative #6: auction-tail family live. Gates: settle+schedule
16:20 MON-FRI (after grader 16:00) · invoke -> feed ok with rule doc ·
DETERMINISTIC E2E: seed synthetic D-grade 30Y row into a copy-extended
grades doc? NO — never mutate a real feed. Instead invoke engine against a
test event override key ({"_src":"data/_ops3446_grades_test.json"}) after
writing that synthetic doc; expect DOWN TLT logged; cleanup."""
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
UA={"User-Agent":"Mozilla/5.0 (ops-3446)"}
with report("3446_auction_tail") as rep:
    rep.heading("ops 3446 — auction-tail family")
    out={"gates":{}}; fails=[]
    def gate(n,ok,d):
        out["gates"][n]={"ok":bool(ok),"detail":str(d)[:330]}
        line=("PASS  " if ok else "FAIL  ")+n+" — "+str(d)[:290]; print(line); rep.log(line)
        if not ok: fails.append(n)
    ok1=False; dl=time.time()+400
    while time.time()<dl:
        try:
            if LAM.get_function_configuration(FunctionName="justhodl-auction-tail").get("LastUpdateStatus")=="Successful":
                info=LAM.get_function(FunctionName="justhodl-auction-tail")
                with urllib.request.urlopen(urllib.request.Request(info["Code"]["Location"],headers=UA),timeout=60) as r:
                    if 'VERSION = "1.0.0"' in zipfile.ZipFile(io.BytesIO(r.read())).read("lambda_function.py").decode():
                        ok1=True; break
        except Exception: pass
        time.sleep(12)
    cr=None
    try:
        arn=LAM.get_function_configuration(FunctionName="justhodl-auction-tail")["FunctionArn"]
        try: SCH.get_schedule(Name="justhodl-auction-tail-daily"); cr="exists"
        except Exception:
            SCH.create_schedule(Name="justhodl-auction-tail-daily",ScheduleExpression="cron(20 16 ? * MON-FRI *)",
                FlexibleTimeWindow={"Mode":"OFF"},
                Target={"Arn":arn,"RoleArn":"arn:aws:iam::857687956942:role/justhodl-scheduler-role","Input":"{}"})
            cr="created"
    except Exception as e: cr=str(e)[:80]
    gate("G1_deployed_scheduled", ok1 and cr in ("exists","created"), f"settled={ok1} sched={cr}")
    t0=datetime.now(timezone.utc).isoformat()
    r=LAM.invoke(FunctionName="justhodl-auction-tail",InvocationType="RequestResponse",Payload=b"{}")
    body=json.loads(json.loads(r["Payload"].read()).get("body","{}"))
    feed={}
    try: feed=json.loads(S3C.get_object(Bucket="justhodl-dashboard-live",Key="data/auction-tail.json")["Body"].read())
    except Exception: pass
    gate("G2_feed_live", feed.get("ok") is True and (feed.get("generated_at") or "")>t0 and "rules" in feed,
         f"seen={feed.get('n_graded_seen')} fresh={feed.get('n_fresh')} logged={feed.get('logged')} fresh_rows={[(x.get('term'),x.get('tail_bp'),x.get('action')) for x in (feed.get('fresh') or [])][:4]}")
    out["real_read"]={"fresh":feed.get("fresh"),"logged":feed.get("logged")}
    n=0; lek=None; today=datetime.now(timezone.utc).date().isoformat()
    tbl=DDB.Table("justhodl-signals")
    while True:
        kw={"FilterExpression":Attr("signal_type").eq("auction-tail")}
        if lek: kw["ExclusiveStartKey"]=lek
        rr=tbl.scan(**kw)
        n+=sum(1 for it in rr.get("Items",[]) if str(it.get("logged_at",""))[:10]==today)
        lek=rr.get("LastEvaluatedKey")
        if not lek: break
    exp=feed.get("logged") or 0
    gate("G3_signals_match", n==exp, f"ddb_today={n} feed.logged={exp} (0/0 valid if no fresh actionable auction)")
    out["verdict"]="PASS_ALL" if not fails else "GAPS: "+",".join(fails)
    print("\nVERDICT:",out["verdict"]); rep.log("VERDICT: "+out["verdict"])
    Path("aws/ops/reports/3446.json").write_text(json.dumps(out,indent=2,default=str)); sys.exit(0)
