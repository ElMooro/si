"""ops 4465 — APR-0003 REJECTED by Khalid (chat verbatim). Ledger decided,
registry closed as accepted-risk, audit posted. Zero code changes — which
is exactly the point: rejection means the system stays byte-identical."""
import json,os
from datetime import datetime,timezone
import boto3
from botocore.config import Config
REGION="us-east-1"; BUCKET="justhodl-dashboard-live"; BUS="justhodl-a2a-bus"
s3=boto3.client("s3",region_name=REGION)
lam=boto3.client("lambda",region_name=REGION,config=Config(read_timeout=200,retries={"max_attempts":0}))
R={"ops":4465,"started":datetime.now(timezone.utc).isoformat()}
doc=json.loads(s3.get_object(Bucket=BUCKET,Key="data/audit/approvals.json")["Body"].read())
hit=next((x for x in doc.get("pending",[]) if x.get("id")=="APR-0003"),None)
if hit:
    doc["pending"]=[x for x in doc["pending"] if x.get("id")!="APR-0003"]
    hit.update({"decision":"rejected",
        "decided_by":"khalid (chat, verbatim: 'Reject it -> everything stays exactly as-is')",
        "decided_at":R["started"],"source":"chat",
        "reason":"retain FRED/Yahoo/CoinMetrics unchanged; license risk documented-and-accepted"})
    doc.setdefault("decided",[]).append(hit); doc["as_of"]=R["started"]
    s3.put_object(Bucket=BUCKET,Key="data/audit/approvals.json",
        Body=json.dumps(doc,indent=1,default=str).encode(),ContentType="application/json",CacheControl="no-cache")
    R["apr0003"]="rejected+archived"
s3.put_object(Bucket=BUCKET,Key="data/audit/provider-expansion-registry.json",
 Body=open("aws/infra/provider-expansion-registry.json","rb").read(),
 ContentType="application/json",CacheControl="no-cache")
def bus(p):
    i=lam.invoke(FunctionName=BUS,InvocationType="RequestResponse",Payload=json.dumps(p).encode())
    bb=json.loads(i["Payload"].read().decode())
    return json.loads(bb["body"]) if isinstance(bb,dict) and "body" in bb else bb
msg=("APPROVAL REJECTED: APR-0003 (license migrations) — Khalid, chat verbatim: 'Reject it "
 "-> everything stays exactly as-is'. FRED/Yahoo/CoinMetrics retained unchanged; risks "
 "documented-and-accepted in the registry. Zero code changes executed — the F6 loop closed "
 "with the arbiter's word, twice-proven (one approve, one reject).")
bus({"action":"post_turn","thread_id":"0805201645","from":"khalid","to":"*","kind":"agree","content":msg})
bus({"action":"fanout_pending"})
R["verdict"]=f"PASS — {R.get('apr0003','already-decided')}; registry closed as accepted-risk"
R["finished"]=datetime.now(timezone.utc).isoformat()
os.makedirs("aws/ops/reports",exist_ok=True)
json.dump(R,open("aws/ops/reports/4465_reject.json","w"),indent=1,default=str)
open("aws/ops/reports/4465_reject.md","w").write(f"# ops 4465 — APR-0003 rejected — {R['verdict']}\n")
print(R["verdict"])
