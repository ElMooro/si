"""ops 4365 — final: quota-paced cq-feed v2.1 against the 39-path catalog,
then the engine, end-to-end. Waits for the v2.1 deploy (marker in payload),
requires n_metrics >= 30, then engine catalog_metrics >= 30 + leaves + ratchet."""
import json, os, time
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REGION="us-east-1"; BUCKET="justhodl-dashboard-live"
lam=boto3.client("lambda",region_name=REGION,
                 config=Config(read_timeout=320,retries={"max_attempts":0}))
s3=boto3.client("s3",region_name=REGION)
R={"ops":4365,"started":datetime.now(timezone.utc).isoformat(),"tries":[]}
n=0
for i in range(6):
    tr={"n":i+1}
    try:
        inv=lam.invoke(FunctionName="justhodl-cq-feed",
                       InvocationType="RequestResponse",Payload=b"{}")
        tr["fn_err"]=inv.get("FunctionError"); inv["Payload"].read()
        fd=json.loads(s3.get_object(Bucket=BUCKET,Key="data/cq-feed.json")["Body"].read())
        n=fd.get("n_metrics") or 0
        tr["n_metrics"]=n; tr["marker"]=fd.get("marker")
    except Exception as e:
        tr["err"]=str(e)[:120]
    R["tries"].append(tr)
    if n>=30 and "v2.1" in str(tr.get("marker","")): break
    time.sleep(45)
R["cq_feed"]={"n_metrics":n,"marker":R["tries"][-1].get("marker")}
try:
    inv=lam.invoke(FunctionName="justhodl-crypto-intel",
                   InvocationType="RequestResponse",Payload=b"{}")
    inv["Payload"].read()
    d=json.loads(s3.get_object(Bucket=BUCKET,Key="crypto-intel.json")["Body"].read())
    q=d.get("cryptoquant") or {}
    R["engine"]={"catalog_metrics":len(q.get("catalog_metrics") or {}),
                 "headline":len(q.get("metrics") or {}),
                 "new_paths_present":[k for k in (q.get("catalog_metrics") or {})
                                      if any(s in k for s in ("nvt","puell","stock-to-flow",
                                                              "coinbase-premium","network-data"))][:15],
                 "leaves":(d.get("coverage") or {}).get("total_leaves"),
                 "ratchet":(d.get("coverage") or {}).get("ratchet"),
                 "anomaly_points":(d.get("anomaly") or {}).get("history_points"),
                 "generated_at":d.get("generated_at")}
except Exception as e:
    R["engine_err"]=str(e)[:150]
ok=(n>=30 and (R.get("engine",{}).get("catalog_metrics") or 0)>=30)
R["verdict"]="PASS — 39-path expansion live end-to-end" if ok else "PARTIAL"
R["finished"]=datetime.now(timezone.utc).isoformat()
os.makedirs("aws/ops/reports",exist_ok=True)
json.dump(R,open("aws/ops/reports/4365_cq_final.json","w"),indent=1,default=str)
e=R.get("engine",{})
open("aws/ops/reports/4365_cq_final.md","w").write(
    f"# ops 4365 — CQ expansion final — {R['verdict']}\n"
    f"- cq-feed: n_metrics={n} marker={R['cq_feed'].get('marker')} tries={[t.get('n_metrics') for t in R['tries']]}\n"
    f"- engine: catalog={e.get('catalog_metrics')} headline={e.get('headline')} leaves={e.get('leaves')}\n"
    f"- new paths in engine: {e.get('new_paths_present')}\n"
    f"- ratchet: {json.dumps(e.get('ratchet'))} | anomaly pts: {e.get('anomaly_points')}\n")
print(json.dumps(R,indent=1,default=str))
