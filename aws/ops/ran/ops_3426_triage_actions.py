"""ops 3426 — alpha-triage phase 2 ACTIONS.
Publish data/signal-suppress.json (8 RETIRE families) · fleet suppression
via shared signals_emit (G2: synthetic emit of a retired family returns
logged=False) · inverse-harvester live (G3: seed synthetic finnhub source
row -> invoke -> inv: mirror appears with FLIPPED direction; cleanup)."""
import io, json, sys, time, urllib.request, zipfile
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
import boto3
from botocore.config import Config
from ops_report import report
LAM=boto3.client("lambda","us-east-1",config=Config(read_timeout=300,retries={"max_attempts":2}))
S3C=boto3.client("s3","us-east-1"); DDB=boto3.resource("dynamodb","us-east-1")
SCH=boto3.client("scheduler","us-east-1")
UA={"User-Agent":"Mozilla/5.0 (ops-3426)"}
RETIRE=["ignition","eng:accumulation-radar","eng:radar-backtest","eng:smart-money-13f",
        "eng:boom-radar","nobrainer_XOP","nobrainer_AIQ","eng:attention-signals"]
def settled(fn,marker,member="lambda_function.py",tmax=360):
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
with report("3426_triage_actions") as rep:
    rep.heading("ops 3426 — triage actions")
    out={"gates":{}}; fails=[]
    def gate(n,ok,d):
        out["gates"][n]={"ok":bool(ok),"detail":str(d)[:320]}
        line=("PASS  " if ok else "FAIL  ")+n+" — "+str(d)[:280]; print(line); rep.log(line)
        if not ok: fails.append(n)
    S3C.put_object(Bucket="justhodl-dashboard-live",Key="data/signal-suppress.json",
        Body=json.dumps({"generated_at":datetime.now(timezone.utc).isoformat(),
                         "source":"alpha-triage ops 3423-3425","suppressed":RETIRE},
                        separators=(",",":")).encode(),ContentType="application/json")
    j=json.loads(S3C.get_object(Bucket="justhodl-dashboard-live",Key="data/signal-suppress.json")["Body"].read())
    gate("G1_suppress_list", set(j.get("suppressed"))==set(RETIRE), f"{len(RETIRE)} families published")
    ok=settled("justhodl-inverse-harvester",'VERSION = "1.0.0"') and \
       settled("justhodl-inverse-harvester","_suppress_set","signals_emit.py")
    gate("G2a_deployed", ok, "harvester + suppression-bearing shared bundled")
    try:
        arn=LAM.get_function_configuration(FunctionName="justhodl-inverse-harvester")["FunctionArn"]
        try: SCH.get_schedule(Name="justhodl-inverse-harvester-daily"); cr="exists"
        except Exception:
            SCH.create_schedule(Name="justhodl-inverse-harvester-daily",ScheduleExpression="cron(10 22 ? * * *)",
                FlexibleTimeWindow={"Mode":"OFF"},
                Target={"Arn":arn,"RoleArn":"arn:aws:iam::857687956942:role/justhodl-scheduler-role","Input":"{}"})
            cr="created"
    except Exception as e: cr=str(e)[:90]
    gate("G2b_schedule", cr in ("exists","created"), cr)
    r=LAM.invoke(FunctionName="justhodl-inverse-harvester",InvocationType="RequestResponse",
                 Payload=json.dumps({"_test_suppress":"eng:boom-radar"}).encode())
    body=json.loads(json.loads(r["Payload"].read())["body"])
    gate("G2c_suppression_live", body.get("logged") is False, f"emit(eng:boom-radar) -> logged={body.get('logged')}")
    tbl=DDB.Table("justhodl-signals")
    now=datetime.now(timezone.utc); sid=f"eng:finnhub-signals#OPS3426TEST#{now.date().isoformat()}"
    tbl.put_item(Item={"signal_id":sid,"signal_type":"eng:finnhub-signals","ticker":"OPS3426TEST",
        "measure_against":"SPY","predicted_direction":"UP","logged_at":now.isoformat(),
        "logged_epoch":int(now.timestamp()),"schema_version":"2","status":"pending",
        "confidence":Decimal("0.5"),"baseline_price":Decimal("100"),
        "check_timestamps":{},"outcomes":{}})
    LAM.invoke(FunctionName="justhodl-inverse-harvester",InvocationType="RequestResponse",Payload=b"{}")
    mid=f"inv:eng:finnhub-signals#SPY#{now.date().isoformat()}"
    it=tbl.get_item(Key={"signal_id":mid}).get("Item") or {}
    gate("G3_mirror", it.get("predicted_direction")=="DOWN" and it.get("signal_type")=="inv:eng:finnhub-signals",
         f"mirror={it.get('signal_id','MISSING')[:60]} dir={it.get('predicted_direction')}")
    for k in (sid, mid):
        try: tbl.delete_item(Key={"signal_id":k})
        except Exception: pass
    print("[cleanup] synthetic rows removed")
    out["verdict"]="PASS_ALL" if not fails else "GAPS: "+",".join(fails)
    print("\nVERDICT:",out["verdict"]); rep.log("VERDICT: "+out["verdict"])
    Path("aws/ops/reports/3426.json").write_text(json.dumps(out,indent=2)); sys.exit(0)
