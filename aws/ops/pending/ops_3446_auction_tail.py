"""ops 3446 — creative #6: auction-tail family live. Gates: settle · Scheduler
16:20 M-F · probe rule-map on runner (TLT DOWN / IEF UP from fakes) · live
invoke writes feed (weekend => plans may be 0, structure gated)."""
import io, json, sys, time, urllib.request, zipfile
from datetime import datetime, timezone
from pathlib import Path
import boto3
from botocore.config import Config
from ops_report import report
LAM=boto3.client("lambda","us-east-1",config=Config(read_timeout=300,retries={"max_attempts":2}))
S3C=boto3.client("s3","us-east-1"); SCH=boto3.client("scheduler","us-east-1")
UA={"User-Agent":"Mozilla/5.0 (ops-3446)"}
with report("3446_auction_tail") as rep:
    rep.heading("ops 3446 — auction-tail family")
    out={"gates":{}}; fails=[]
    def gate(n,ok,d):
        out["gates"][n]={"ok":bool(ok),"detail":str(d)[:320]}
        line=("PASS  " if ok else "FAIL  ")+n+" — "+str(d)[:280]; print(line); rep.log(line)
        if not ok: fails.append(n)
    ok1=False; dl=time.time()+420
    while time.time()<dl:
        try:
            if LAM.get_function_configuration(FunctionName="justhodl-auction-tail").get("LastUpdateStatus")=="Successful":
                info=LAM.get_function(FunctionName="justhodl-auction-tail")
                with urllib.request.urlopen(urllib.request.Request(info["Code"]["Location"],headers=UA),timeout=60) as r:
                    if 'VERSION = "1.0.0"' in zipfile.ZipFile(io.BytesIO(r.read())).read("lambda_function.py").decode("utf-8","replace"):
                        ok1=True; break
        except Exception: pass
        time.sleep(12)
    gate("G1_settled", ok1, "marker")
    try:
        arn=LAM.get_function_configuration(FunctionName="justhodl-auction-tail")["FunctionArn"]
        try: SCH.get_schedule(Name="justhodl-auction-tail-daily"); cr="exists"
        except Exception:
            SCH.create_schedule(Name="justhodl-auction-tail-daily",ScheduleExpression="cron(20 16 ? * MON-FRI *)",
                FlexibleTimeWindow={"Mode":"OFF"},
                Target={"Arn":arn,"RoleArn":"arn:aws:iam::857687956942:role/justhodl-scheduler-role","Input":"{}"})
            cr="created"
    except Exception as e: cr=str(e)[:80]
    gate("G2_schedule", cr in ("exists","created"), cr)
    probe={"_probe":{"rows":[
      {"overall_grade":"D","auction_date":"2026-07-17","security_type":"BOND","security_term":"30-Year","dimensions":{"tail_bp":{"value":2.1}}},
      {"overall_grade":"A","auction_date":"2026-07-17","security_type":"NOTE","security_term":"10-Year","dimensions":{"tail_bp":{"value":-1.4}}}]}}
    r=LAM.invoke(FunctionName="justhodl-auction-tail",InvocationType="RequestResponse",Payload=json.dumps(probe).encode())
    plans=json.loads(json.loads(r["Payload"].read()).get("body","{}")).get("plans") or []
    gate("G3_rule_probe", [(p.get("etf"),p.get("direction")) for p in plans]==[("TLT","DOWN"),("IEF","UP")],
         f"plans={[(p.get('etf'),p.get('direction'),p.get('grade')) for p in plans]}")
    t0=datetime.now(timezone.utc).isoformat()
    LAM.invoke(FunctionName="justhodl-auction-tail",InvocationType="RequestResponse",Payload=b"{}")
    feed={}
    try: feed=json.loads(S3C.get_object(Bucket="justhodl-dashboard-live",Key="data/auction-tail.json")["Body"].read())
    except Exception: pass
    gate("G4_feed_live", feed.get("ok") is True and (feed.get("generated_at") or "")>t0,
         f"graded_rows={feed.get('n_graded_rows')} plans={len(feed.get('plans') or [])} logged={feed.get('logged')}")
    out["verdict"]="PASS_ALL" if not fails else "GAPS: "+",".join(fails)
    print("\nVERDICT:",out["verdict"]); rep.log("VERDICT: "+out["verdict"])
    Path("aws/ops/reports/3446.json").write_text(json.dumps(out,indent=2,default=str)); sys.exit(0)
