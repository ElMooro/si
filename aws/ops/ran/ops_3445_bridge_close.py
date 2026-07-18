"""ops 3443 — bridge table-leg verification with the table's REAL key schema."""
import json, sys, time
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
import boto3
from boto3.dynamodb.conditions import Attr
from ops_report import report
LAM=boto3.client("lambda","us-east-1"); DDB=boto3.resource("dynamodb","us-east-1")
DDBC=boto3.client("dynamodb","us-east-1")
with report("3445_bridge_close") as rep:
    import io as _io, urllib.request as _ur, zipfile as _zf
    dl=time.time()+420; okz=False
    while time.time()<dl:
        try:
            if LAM.get_function_configuration(FunctionName="justhodl-outcome-checker").get("LastUpdateStatus")=="Successful":
                info=LAM.get_function(FunctionName="justhodl-outcome-checker")
                with _ur.urlopen(_ur.Request(info["Code"]["Location"],headers={"User-Agent":"Mozilla/5.0"}),timeout=60) as r:
                    if "V1b" in _zf.ZipFile(_io.BytesIO(r.read())).read("lambda_function.py").decode("utf-8","replace"):
                        okz=True; break
        except Exception: pass
        time.sleep(12)
    print(("PASS  " if okz else "FAIL  ")+"G0_v1b_settled"); rep.log("G0_v1b_settled "+str(okz))
    if not okz:
        Path("aws/ops/reports/3445.json").write_text(json.dumps({"verdict":"GAPS: G0"})); sys.exit(0)
    out={"gates":{}}; fails=[]
    def gate(n,ok,d):
        out["gates"][n]={"ok":bool(ok),"detail":str(d)[:320]}
        line=("PASS  " if ok else "FAIL  ")+n+" — "+str(d)[:280]; print(line); rep.log(line)
        if not ok: fails.append(n)
    ks=DDBC.describe_table(TableName="justhodl-outcomes")["Table"]["KeySchema"]
    line="key schema: "+json.dumps(ks); print(line); rep.log(line)
    tbl=DDB.Table("justhodl-signals"); otb=DDB.Table("justhodl-outcomes")
    now=datetime.now(timezone.utc); t40=now-timedelta(days=40)
    sid=f"ops3445-bridge#{int(time.time())}"
    tbl.put_item(Item={"signal_id":sid,"signal_type":"ops3445-bridge","ticker":"SPY",
        "measure_against":"SPY","predicted_direction":"UP","logged_at":t40.isoformat(),
        "logged_epoch":int(t40.timestamp()),"schema_version":"2","status":"pending",
        "confidence":Decimal("0.5"),"baseline_price":Decimal("700"),
        "metadata":{"regime":{"label":"TESTREGIME3445"}},
        "check_timestamps":{"day_5":(t40+timedelta(days=5)).isoformat()},"outcomes":{}})
    LAM.invoke(FunctionName="justhodl-outcome-checker",InvocationType="RequestResponse",Payload=b"{}")
    rows=[]; lek=None
    while True:
        kw={"FilterExpression":Attr("signal_id").eq(sid)}
        if lek: kw["ExclusiveStartKey"]=lek
        r=otb.scan(**kw); rows+=r.get("Items") or []
        lek=r.get("LastEvaluatedKey")
        if not lek: break
    tr=(rows[0] if rows else {})
    gate("G1_table_leg", tr.get("regime_at_log")=="TESTREGIME3445",
         f"rows={len(rows)} regime_at_log={tr.get('regime_at_log')} keys={sorted(list(tr.keys()))[:8]}")
    try:
        tbl.delete_item(Key={"signal_id":sid})
        if rows:
            k={x["AttributeName"]:tr[x["AttributeName"]] for x in ks}
            otb.delete_item(Key=k)
        print("[cleanup] ok")
    except Exception as e: print("[cleanup]",str(e)[:60])
    out["verdict"]="PASS_ALL" if not fails else "GAPS: "+",".join(fails)
    print("\nVERDICT:",out["verdict"]); rep.log("VERDICT: "+out["verdict"])
    Path("aws/ops/reports/3445.json").write_text(json.dumps(out,indent=2,default=str)); sys.exit(0)
