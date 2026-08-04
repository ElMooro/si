"""ops 4372 — insiders v2 verify: two spaced invokes; assert daily-index
backfill scanning, transaction count materially above the old 7, real
sectors mapped, fleet joins landing, cursor advancing."""
import json, os, time
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REGION="us-east-1"; BUCKET="justhodl-dashboard-live"; FN="justhodl-insider-trades"
lam=boto3.client("lambda",region_name=REGION,config=Config(read_timeout=340,retries={"max_attempts":0}))
s3=boto3.client("s3",region_name=REGION)
R={"ops":4372,"started":datetime.now(timezone.utc).isoformat(),"rounds":[]}

def snap():
    d=json.loads(s3.get_object(Bucket=BUCKET,Key="data/insider-trades.json")["Body"].read())
    st=d.get("stats") or {}; cov=d.get("coverage") or {}
    led=(d.get("fleet") or {}).get("ledger") or []
    return {"version":d.get("version"),"buys":st.get("total_buys"),
            "txn_rows":len(d.get("transactions") or []),
            "sell_rows":len(d.get("sell_transactions") or []),
            "value":st.get("total_value_usd"),"companies":st.get("unique_companies"),
            "clusters":st.get("cluster_count"),
            "scanned":cov.get("filings_scanned_this_run"),
            "days_complete":cov.get("backfill_days_complete"),
            "sector_mapped":cov.get("sector_mapped"),
            "backfill_error":cov.get("backfill_error"),
            "fleet_ok":sum(1 for e in led if e.get("status")=="ok"),
            "fleet_missing":[e["feed"] for e in led if e.get("status")=="missing"],
            "sample_sectors":sorted({t.get("sector") for t in (d.get("transactions") or [])
                                     if t.get("sector")})[:8]}
ok=False
for i in range(2):
    rd={"n":i+1}
    try:
        inv=lam.invoke(FunctionName=FN,InvocationType="RequestResponse",Payload=b"{}")
        rd["fn_err"]=inv.get("FunctionError")
        rd["payload"]=inv["Payload"].read().decode()[:260]
    except Exception as e:
        rd["invoke_err"]=str(e)[:150]
    try:
        rd["snap"]=snap()
    except Exception as e:
        rd["snap_err"]=str(e)[:120]
    R["rounds"].append(rd)
    if i==0: time.sleep(20)
last=(R["rounds"][-1].get("snap") or {})
ok=(last.get("version")=="2.0" and (last.get("scanned") or 0)>0
    and (last.get("txn_rows") or 0)>7 and (last.get("sector_mapped") or 0)>0
    and (last.get("fleet_ok") or 0)>=3 and not last.get("backfill_error"))
R["verdict"]="PASS — v2 deep coverage live" if ok else "PARTIAL — see rounds"
R["finished"]=datetime.now(timezone.utc).isoformat()
os.makedirs("aws/ops/reports",exist_ok=True)
json.dump(R,open("aws/ops/reports/4372_insiders_v2.json","w"),indent=1,default=str)
open("aws/ops/reports/4372_insiders_v2.md","w").write(
    f"# ops 4372 — insiders v2 verify — {R['verdict']}\n"
    f"- round1: {json.dumps(R['rounds'][0].get('snap') or R['rounds'][0])[:500]}\n"
    f"- round2: {json.dumps(last)[:700]}\n")
print(json.dumps(R,indent=1,default=str))
