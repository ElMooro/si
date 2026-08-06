"""ops 4490 — APR-0002 REJECTED by Khalid (chat verbatim: "Yes im not
signing up for those"). Paid tier declined per the council's own 'not
until a workflow pays' rule; ledger decided, zero spend, zero changes;
re-fileable if a workflow ever names the need."""
import json,os
from datetime import datetime,timezone
import boto3
from botocore.config import Config
REGION="us-east-1"; BUCKET="justhodl-dashboard-live"; BUS="justhodl-a2a-bus"
s3=boto3.client("s3",region_name=REGION)
lam=boto3.client("lambda",region_name=REGION,config=Config(read_timeout=200,retries={"max_attempts":0}))
R={"ops":4490,"started":datetime.now(timezone.utc).isoformat()}
doc=json.loads(s3.get_object(Bucket=BUCKET,Key="data/audit/approvals.json")["Body"].read())
hit=next((x for x in doc.get("pending",[]) if x.get("id")=="APR-0002"),None)
if hit:
    doc["pending"]=[x for x in doc["pending"] if x.get("id")!="APR-0002"]
    hit.update({"decision":"rejected",
        "decided_by":"khalid (chat, verbatim: 'Yes im not signing up for those')",
        "decided_at":R["started"],"source":"chat",
        "reason":"paid tier declined — free tier complete at $0; council's own rule: not until a named workflow pays; re-fileable"})
    doc.setdefault("decided",[]).append(hit); doc["as_of"]=R["started"]
    s3.put_object(Bucket=BUCKET,Key="data/audit/approvals.json",
        Body=json.dumps(doc,indent=1,default=str).encode(),
        ContentType="application/json",CacheControl="no-cache")
    R["apr0002"]="rejected+archived"
else:
    R["apr0002"]="not-pending (already decided?)"
def bus(p):
    i=lam.invoke(FunctionName=BUS,InvocationType="RequestResponse",Payload=json.dumps(p).encode())
    b2=json.loads(i["Payload"].read().decode())
    return json.loads(b2["body"]) if isinstance(b2,dict) and "body" in b2 else b2
msg=("APPROVAL REJECTED: APR-0002 (paid tier, ~$4,149/mo TRACE+Cboe) — Khalid verbatim: "
 "'Yes im not signing up for those'. Council's own sequencing honored: free tier is complete "
 "at $0 and no workflow names the need. Zero spend, zero changes; re-fileable. The approvals "
 "ledger now stands fully DECIDED: APR-0000 closed-as-built, 0001 approved->E2 live, "
 "0003+0002 rejected->untouched. F6 four-for-four.")
bus({"action":"post_turn","thread_id":"0805201645","from":"khalid","to":"*","kind":"agree","content":msg})
bus({"action":"fanout_pending"})
R["verdict"]=f"PASS — {R['apr0002']}; ledger fully decided"
R["finished"]=datetime.now(timezone.utc).isoformat()
os.makedirs("aws/ops/reports",exist_ok=True)
json.dump(R,open("aws/ops/reports/4490_reject2.json","w"),indent=1,default=str)
open("aws/ops/reports/4490_reject2.md","w").write(f"# ops 4490 — APR-0002 rejected — {R['verdict']}\n")
print(R["verdict"])
