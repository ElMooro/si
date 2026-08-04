"""ops 4364 — settle after 4362's probe burst rate-limited CQ during the
immediate cq-feed re-fire (n_metrics collapsed 22 -> 2 on that one run).
Minutes have passed; re-fire cq-feed, require n_metrics >= 28, then invoke
crypto-intel and require catalog_metrics >= 28 and leaves recovered. Retries
with backoff if CQ still throttling."""
import json, os, time
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REGION="us-east-1"; BUCKET="justhodl-dashboard-live"
lam=boto3.client("lambda",region_name=REGION,
                 config=Config(read_timeout=320,retries={"max_attempts":0}))
s3=boto3.client("s3",region_name=REGION)
R={"ops":4364,"started":datetime.now(timezone.utc).isoformat(),"tries":[]}
n=0
for i in range(3):
    tr={"n":i+1}
    try:
        inv=lam.invoke(FunctionName="justhodl-cq-feed",
                       InvocationType="RequestResponse",Payload=b"{}")
        tr["invoke"]=inv.get("StatusCode"); tr["fn_err"]=inv.get("FunctionError")
        inv["Payload"].read()
        fd=json.loads(s3.get_object(Bucket=BUCKET,Key="data/cq-feed.json")["Body"].read())
        n=fd.get("n_metrics") or 0
        tr["n_metrics"]=n; tr["generated_at"]=fd.get("generated_at")
    except Exception as e:
        tr["err"]=str(e)[:120]
    R["tries"].append(tr)
    if n>=28: break
    time.sleep(90)
R["cq_feed_n_metrics"]=n
if n>=28:
    try:
        inv=lam.invoke(FunctionName="justhodl-crypto-intel",
                       InvocationType="RequestResponse",Payload=b"{}")
        inv["Payload"].read()
        d=json.loads(s3.get_object(Bucket=BUCKET,Key="crypto-intel.json")["Body"].read())
        R["engine"]={"catalog_metrics":len((d.get("cryptoquant") or {}).get("catalog_metrics") or {}),
                     "headline":len((d.get("cryptoquant") or {}).get("metrics") or {}),
                     "leaves":(d.get("coverage") or {}).get("total_leaves"),
                     "ratchet":(d.get("coverage") or {}).get("ratchet"),
                     "generated_at":d.get("generated_at")}
    except Exception as e:
        R["engine_err"]=str(e)[:150]
ok=(n>=28 and (R.get("engine",{}).get("catalog_metrics") or 0)>=28)
R["verdict"]="PASS — expansion flows end-to-end" if ok else "PARTIAL"
R["finished"]=datetime.now(timezone.utc).isoformat()
os.makedirs("aws/ops/reports",exist_ok=True)
json.dump(R,open("aws/ops/reports/4364_cq_settle.json","w"),indent=1,default=str)
open("aws/ops/reports/4364_cq_settle.md","w").write(
    f"# ops 4364 — CQ expansion settle — {R['verdict']}\n"
    f"- cq-feed n_metrics: {n} (tries: {[t.get('n_metrics') for t in R['tries']]})\n"
    f"- engine: {json.dumps(R.get('engine') or R.get('engine_err'))}\n")
print(json.dumps(R,indent=1,default=str))
