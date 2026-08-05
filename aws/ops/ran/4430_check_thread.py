"""ops 4430 — quick pull of master thread + verify state before next build pass."""
import json,os
from datetime import datetime,timezone
import boto3
from botocore.config import Config
REGION="us-east-1"; BUCKET="justhodl-dashboard-live"; BUS="justhodl-a2a-bus"
lam=boto3.client("lambda",region_name=REGION,config=Config(read_timeout=200,retries={"max_attempts":0}))
s3=boto3.client("s3",region_name=REGION)
R={"ops":4430,"started":datetime.now(timezone.utc).isoformat()}
def bus(p):
    i=lam.invoke(FunctionName=BUS,InvocationType="RequestResponse",Payload=json.dumps(p).encode())
    b=json.loads(i["Payload"].read().decode())
    return json.loads(b["body"]) if isinstance(b,dict) and "body" in b else b
for tid in ("0805201645","0805174350"):
    th=bus({"action":"get_thread","thread_id":tid}).get("thread") or {}
    R[tid]=[{"from":x.get("from"),"kind":x.get("kind"),"verdict":x.get("verdict"),
             "ts":x.get("ts"),"content":(x.get("content") or "")[:1200]}
            for x in (th.get("turns") or [])[-3:]]
R["board"]=bus({"action":"get_tasks"})
R["verdict"]="READ"
os.makedirs("aws/ops/reports",exist_ok=True)
json.dump(R,open("aws/ops/reports/4430_check.json","w"),indent=1,default=str)
md=[f"# ops 4430 — thread check"]
for tid in ("0805201645","0805174350"):
    md.append(f"\n## {tid}")
    for x in R[tid]:
        md.append(f"\n### {x['from']} [{x['kind']}{'/'+str(x['verdict']) if x.get('verdict') else ''}] {x['ts']}\n{x['content']}")
open("aws/ops/reports/4430_check.md","w").write("\n".join(md)+"\n")
print("done")
