"""ops 3442 — creative-arc opener: [#7] REGIME_BRIDGE_V1 (checker copies
regime-at-log label onto every outcome, feeding scorecard.by_regime +
engine-trust conditioning) + signals_emit canonical label · [#2] stealth-flow
graded family live · [#1] inverse-harvester v1.1 self-updating from triage.
Gates on DATA with distinct markers + synthetic E2E."""
import io, json, sys, time, urllib.request, zipfile
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
import boto3
from botocore.config import Config
from ops_report import report
LAM=boto3.client("lambda","us-east-1",config=Config(read_timeout=340,retries={"max_attempts":2}))
S3C=boto3.client("s3","us-east-1"); DDB=boto3.resource("dynamodb","us-east-1")
SCH=boto3.client("scheduler","us-east-1")
UA={"User-Agent":"Mozilla/5.0 (ops-3442)"}
def settled(fn,marker,member="lambda_function.py",tmax=420):
    dl=time.time()+tmax
    while time.time()<dl:
        try:
            if LAM.get_function_configuration(FunctionName=fn).get("LastUpdateStatus")=="Successful":
                info=LAM.get_function(FunctionName=fn)
                with urllib.request.urlopen(urllib.request.Request(info["Code"]["Location"],headers=UA),timeout=60) as r:
                    if marker in zipfile.ZipFile(io.BytesIO(r.read())).read(member).decode("utf-8","replace"):
                        return True
        except Exception: pass
        time.sleep(12)
    return False
with report("3442_regime_bridge") as rep:
    rep.heading("ops 3442 — regime bridge + stealth + fade v1.1")
    out={"gates":{}}; fails=[]
    def gate(n,ok,d):
        out["gates"][n]={"ok":bool(ok),"detail":str(d)[:340]}
        line=("PASS  " if ok else "FAIL  ")+n+" — "+str(d)[:300]; print(line); rep.log(line)
        if not ok: fails.append(n)
    gate("G1_settled",
         settled("justhodl-outcome-checker","REGIME_BRIDGE_V1") and
         settled("justhodl-stealth-flow",'VERSION = "1.0.0"') and
         settled("justhodl-inverse-harvester",'VERSION = "1.1.0"') and
         settled("justhodl-stealth-flow","regime-map.json","signals_emit.py"),
         "4 markers incl label-stamp in bundled shared")
    tbl=DDB.Table("justhodl-signals")
    now=datetime.now(timezone.utc); t40=now-timedelta(days=40)
    sid=f"ops3442-bridge#{int(time.time())}"
    tbl.put_item(Item={"signal_id":sid,"signal_type":"ops3442-bridge","ticker":"SPY",
        "measure_against":"SPY","predicted_direction":"UP","logged_at":t40.isoformat(),
        "logged_epoch":int(t40.timestamp()),"schema_version":"2","status":"pending",
        "confidence":Decimal("0.5"),"baseline_price":Decimal("700"),
        "metadata":{"regime":{"label":"TESTREGIME3442","jsi_decile":3}},
        "check_timestamps":{"day_5":(t40+timedelta(days=5)).isoformat()},
        "outcomes":{}})
    LAM.invoke(FunctionName="justhodl-outcome-checker",InvocationType="RequestResponse",Payload=b"{}")
    it=tbl.get_item(Key={"signal_id":sid}).get("Item") or {}
    rl=((it.get("outcomes") or {}).get("day_5") or {}).get("regime_at_log")
    trow={}
    try:
        trow=DDB.Table("justhodl-outcomes").get_item(Key={"outcome_id":f"{sid}_day_5"}).get("Item") or {}
    except Exception as e: print("[otable]",str(e)[:60])
    gate("G2_bridge_e2e", rl=="TESTREGIME3442" and trow.get("regime_at_log")=="TESTREGIME3442",
         f"on_row={rl} table={trow.get('regime_at_log')}")
    try:
        tbl.delete_item(Key={"signal_id":sid})
        DDB.Table("justhodl-outcomes").delete_item(Key={"outcome_id":f"{sid}_day_5"})
        print("[cleanup] ok")
    except Exception: pass
    try:
        arn=LAM.get_function_configuration(FunctionName="justhodl-stealth-flow")["FunctionArn"]
        try: SCH.get_schedule(Name="justhodl-stealth-flow-daily"); cr="exists"
        except Exception:
            SCH.create_schedule(Name="justhodl-stealth-flow-daily",ScheduleExpression="cron(35 21 ? * * *)",
                FlexibleTimeWindow={"Mode":"OFF"},
                Target={"Arn":arn,"RoleArn":"arn:aws:iam::857687956942:role/justhodl-scheduler-role","Input":"{}"})
            cr="created"
    except Exception as e: cr=str(e)[:80]
    t0=now.isoformat()
    r=LAM.invoke(FunctionName="justhodl-stealth-flow",InvocationType="RequestResponse",Payload=b"{}")
    body=json.loads(json.loads(r["Payload"].read()).get("body","{}"))
    feed={}
    try: feed=json.loads(S3C.get_object(Bucket="justhodl-dashboard-live",Key="data/stealth-flow.json")["Body"].read())
    except Exception: pass
    gate("G3_stealth_live", cr in ("exists","created") and feed.get("ok") is True and (feed.get("generated_at") or "")>t0,
         f"sched={cr} recent={feed.get('n_recent')} logged={feed.get('logged')} rows={[h['ticker'] for h in (feed.get('recent_stealth') or [])][:6]}")
    r=LAM.invoke(FunctionName="justhodl-inverse-harvester",InvocationType="RequestResponse",
                 Payload=json.dumps({"_families_probe":1}).encode())
    fams=json.loads(json.loads(r["Payload"].read()).get("body","{}")).get("families") or []
    gate("G4_fade_selfupdating", set(fams)=={"eng:ai-infra-stack","eng:finnhub-signals"},
         f"families_from_triage={fams}")
    out["verdict"]="PASS_ALL" if not fails else "GAPS: "+",".join(fails)
    print("\nVERDICT:",out["verdict"]); rep.log("VERDICT: "+out["verdict"])
    Path("aws/ops/reports/3442.json").write_text(json.dumps(out,indent=2,default=str)); sys.exit(0)
