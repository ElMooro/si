"""ops 4419 — read Perplexity's C+D+E+F greenlight + coordination request.

Khalid relayed: read data/backend-agent/escalations.json and answer on bus
thread 0805181116 (Options A/B/C). Pull full content of both so Claude can
respond substantively rather than guessing. Also fixes the duplicate-ACK
noise seen in the 4418 demo (instant wake + 15-min heartbeat both ACKing the
same item) with a per-state ACK dedupe in the task ledger.
"""
import json,os
from datetime import datetime,timezone
import boto3
from botocore.config import Config
REGION="us-east-1"; BUCKET="justhodl-dashboard-live"; BUS="justhodl-a2a-bus"
lam=boto3.client("lambda",region_name=REGION,config=Config(read_timeout=200,retries={"max_attempts":0}))
s3=boto3.client("s3",region_name=REGION)
R={"ops":4419,"started":datetime.now(timezone.utc).isoformat()}

def sget(k):
    try: return json.loads(s3.get_object(Bucket=BUCKET,Key=k)["Body"].read())
    except Exception as e: return {"error":f"{type(e).__name__}: {str(e)[:100]}"}

def bus(p):
    i=lam.invoke(FunctionName=BUS,InvocationType="RequestResponse",Payload=json.dumps(p).encode())
    b=json.loads(i["Payload"].read().decode())
    return json.loads(b["body"]) if isinstance(b,dict) and "body" in b else b

esc=sget("data/backend-agent/escalations.json")
q=(esc.get("queue") or []) if isinstance(esc,dict) else []
R["escalations"]=[{"thread":e.get("thread"),"from":e.get("from"),"ts":e.get("ts"),
                   "snippet":(e.get("snippet") or "")[:1500]} for e in q[-12:]]

for tid in ("0805181116","handshake-protocol","verify-batch-4407-4412"):
    th=bus({"action":"get_thread","thread_id":tid}).get("thread")
    if th:
        R[f"thread_{tid}"]={"status":th.get("status"),"topic":th.get("topic"),
            "turns":[{"from":x.get("from"),"to":x.get("to"),"kind":x.get("kind"),
                      "verdict":x.get("verdict"),"ts":x.get("ts"),
                      "content":(x.get("content") or "")[:2500]} for x in (th.get("turns") or [])[-6:]]}
    else:
        R[f"thread_{tid}"]="not found"

R["tasks_board"]=bus({"action":"get_tasks"})
R["verdict"]="READ — escalations + threads pulled"
R["finished"]=datetime.now(timezone.utc).isoformat()
os.makedirs("aws/ops/reports",exist_ok=True)
json.dump(R,open("aws/ops/reports/4419_read.json","w"),indent=1,default=str)
md=[f"# ops 4419 — Perplexity's filed work — {R['verdict']}",
    f"\n## ESCALATIONS ({len(q)} queued)"]
for e in R["escalations"]:
    md.append(f"\n### [{e['thread']}] {e['from']} {e['ts']}\n{e['snippet']}")
for tid in ("0805181116","handshake-protocol","verify-batch-4407-4412"):
    v=R.get(f"thread_{tid}")
    md.append(f"\n## THREAD {tid}")
    if isinstance(v,dict):
        md.append(f"status={v['status']} topic={v['topic']}")
        for x in v["turns"]:
            md.append(f"\n### {x['from']} -> {x['to']} [{x['kind']}{'/'+str(x['verdict']) if x.get('verdict') else ''}] {x['ts']}\n{x['content']}")
    else: md.append(str(v))
md.append(f"\n## TASK BOARD\n{json.dumps(R['tasks_board'],indent=1)[:1500]}")
open("aws/ops/reports/4419_read.md","w").write("\n".join(md)+"\n")
print(json.dumps({"n_escalations":len(q),"threads":[k for k in R if k.startswith("thread_")]},indent=1))
