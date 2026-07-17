"""ops 3411 — checker-v3 historical pricing (#10a) + synthetic backdated E2E.
Insert a schema-v2 SPY signal backdated 40d (baseline = real close then),
run the checker, and require: day_5 AND day_21 outcomes exist, their returns
DIFFER (impossible under check-time pricing), and CW shows 'priced as-of'.
Cleanup deletes the synthetic row."""
import io, json, sys, time, urllib.request, zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
import boto3
from botocore.config import Config
from ops_report import report
LAM=boto3.client("lambda","us-east-1",config=Config(read_timeout=340))
DDB=boto3.resource("dynamodb","us-east-1"); LOGS=boto3.client("logs","us-east-1")
UA={"User-Agent":"Mozilla/5.0 (ops-3411)"}

def ychart_close(sym, date_iso):
    u=f"https://query1.finance.yahoo.com/v8/finance/chart/{sym}?range=3mo&interval=1d"
    with urllib.request.urlopen(urllib.request.Request(u,headers=UA),timeout=15) as r:
        j=json.loads(r.read())
    res=j["chart"]["result"][0]; ts=res["timestamp"]; cl=res["indicators"]["quote"][0]["close"]
    best=None
    for i,t in enumerate(ts):
        d=datetime.fromtimestamp(t,tz=timezone.utc).date().isoformat()
        if d<=date_iso and cl[i] is not None: best=float(cl[i])
        elif d>date_iso: break
    return best

with report("3411_checker_v3") as rep:
    rep.heading("ops 3411 — checker-v3 hist pricing E2E")
    out={"gates":{}}; fails=[]
    def gate(n,ok,d):
        out["gates"][n]={"ok":bool(ok),"detail":str(d)[:340]}
        line=("PASS  " if ok else "FAIL  ")+n+" — "+str(d)[:290]; print(line); rep.log(line)
        if not ok: fails.append(n)
    ok1=False; dl=time.time()+360
    while time.time()<dl:
        try:
            c=LAM.get_function_configuration(FunctionName="justhodl-outcome-checker")
            if c.get("LastUpdateStatus")=="Successful":
                info=LAM.get_function(FunctionName="justhodl-outcome-checker")
                with urllib.request.urlopen(urllib.request.Request(info["Code"]["Location"],headers=UA),timeout=60) as r:
                    if "get_price_at" in zipfile.ZipFile(io.BytesIO(r.read())).read("lambda_function.py").decode():
                        ok1=True; break
        except Exception: pass
        time.sleep(12)
    gate("G1_v3_deployed", ok1, "get_price_at in zip")

    t40=(datetime.now(timezone.utc)-timedelta(days=40))
    logged=t40.isoformat(); d40=t40.date().isoformat()
    base=ychart_close("SPY", d40)
    sid=f"ops3411-e2e#{int(time.time())}"
    tbl=DDB.Table("justhodl-signals")
    from decimal import Decimal
    row={"signal_id":sid,"signal_type":"ops3411-e2e","ticker":"SPY","measure_against":"SPY",
         "predicted_direction":"UP","logged_at":logged,"logged_epoch":int(t40.timestamp()),
         "schema_version":"2","status":"pending","confidence":Decimal("0.5"),
         "baseline_price":Decimal(str(round(base,2))),
         "check_timestamps":{"day_5":(t40+timedelta(days=5)).isoformat(),
                              "day_21":(t40+timedelta(days=21)).isoformat()},
         "outcomes":{}}
    tbl.put_item(Item=row)
    print(f"[e2e] synthetic in: baseline {base} @ {d40}")
    LAM.invoke(FunctionName="justhodl-outcome-checker",InvocationType="RequestResponse",Payload=b"{}")
    it=tbl.get_item(Key={"signal_id":sid}).get("Item") or {}
    oc=it.get("outcomes") or {}
    r5=(oc.get("day_5") or {}); r21=(oc.get("day_21") or {})
    v5=r5.get("return_pct") or r5.get("actual_return_pct") or r5.get("excess_return_pct")
    v21=r21.get("return_pct") or r21.get("actual_return_pct") or r21.get("excess_return_pct")
    differ=(v5 is not None and v21 is not None and float(v5)!=float(v21))
    ev=LOGS.filter_log_events(logGroupName="/aws/lambda/justhodl-outcome-checker",
        startTime=int((time.time()-900)*1000), filterPattern='"priced as-of"')
    asof=len(ev.get("events",[]))
    gate("G2_historical_grading", bool(oc.get("day_5")) and bool(oc.get("day_21")) and differ and asof>=2,
         f"day5={json.dumps(r5,default=str)[:110]} day21={json.dumps(r21,default=str)[:110]} asof_prints={asof}")
    try: tbl.delete_item(Key={"signal_id":sid}); print("[e2e] synthetic cleaned")
    except Exception as e: print("[e2e] cleanup:",str(e)[:60])
    out["verdict"]="PASS_ALL" if not fails else "GAPS: "+",".join(fails)
    print("\nVERDICT:",out["verdict"]); rep.log("VERDICT: "+out["verdict"])
    Path("aws/ops/reports/3411.json").write_text(json.dumps(out,indent=2,default=str)); sys.exit(0)
