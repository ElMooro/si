"""ops 4379 — Perplexity activation: key -> SSM, breaker clear, registry
heal, fan-out delivers the 5 queued threads to Perplexity's API, its turns
post back through invariant A. The human leaves the loop here."""
import json, os, time
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REGION="us-east-1"; BUCKET="justhodl-dashboard-live"; BUS="justhodl-a2a-bus"
lam=boto3.client("lambda",region_name=REGION,config=Config(read_timeout=280,retries={"max_attempts":0}))
s3=boto3.client("s3",region_name=REGION); ssm=boto3.client("ssm",region_name=REGION)
R={"ops":4379,"started":datetime.now(timezone.utc).isoformat()}

key=os.environ.get("PPLX_API_KEY","").strip()
R["key_in_env"]=bool(key)
if key:
    ssm.put_parameter(Name="/justhodl/perplexity/api-key",Value=key,
                      Type="SecureString",Overwrite=True)
    R["ssm_written"]=True
try:
    ssm.delete_parameter(Name="/justhodl/a2a/breaker/perplexity")
    R["breaker_cleared"]=True
except Exception:
    R["breaker_cleared"]="none existed"

def call(payload):
    inv=lam.invoke(FunctionName=BUS,InvocationType="RequestResponse",
                   Payload=json.dumps(payload).encode())
    b=json.loads(inv["Payload"].read().decode())
    return json.loads(b["body"]) if isinstance(b,dict) and "body" in b else b

# registry heal
try:
    reg=json.loads(s3.get_object(Bucket=BUCKET,Key="data/a2a/registry.json")["Body"].read())
    reg["providers"]["perplexity"]["status"]="key_present"
    reg["providers"]["perplexity"]["note"]="SSM key live (ops 4379); health confirmed on first successful turn"
    reg["updated"]=datetime.now(timezone.utc).isoformat()
    s3.put_object(Bucket=BUCKET,Key="data/a2a/registry.json",
                  Body=json.dumps(reg).encode(),ContentType="application/json")
    R["registry"]="healed"
except Exception as e:
    R["registry_err"]=str(e)[:100]

# deliver all queued threads (1 thread/provider/invoke -> loop)
R["fanout"]=[]
for i in range(6):
    r=call({"action":"fanout_pending"})
    R["fanout"].append(r.get("fanout"))
    try:
        box=json.loads(s3.get_object(Bucket=BUCKET,
             Key="data/a2a/inbox/perplexity.json")["Body"].read())
        if not box.get("threads"): break
    except Exception: break
    time.sleep(3)

try:
    R["inbox_after"]=json.loads(s3.get_object(Bucket=BUCKET,
        Key="data/a2a/inbox/perplexity.json")["Body"].read()).get("threads")
except Exception as e:
    R["inbox_after"]=f"err {str(e)[:60]}"

def thread(tid):
    try:
        return call({"action":"get_thread","thread_id":tid}).get("thread")
    except Exception as e:
        return {"err":str(e)[:80]}
R["t0001"]=thread("0001-build-the-bus")
R["t0002"]=thread("0002-xss-uniformity")
px_turns=[x for t in (R["t0001"],R["t0002"]) if isinstance(t,dict)
          for x in (t.get("turns") or []) if x.get("from")=="perplexity"
          and not x.get("delivered_via")]
ok=R.get("ssm_written") and len(px_turns)>=1
R["perplexity_autonomous_turns"]=len(px_turns)
R["verdict"]=("PASS — Perplexity live on the bus, %d autonomous turns"
              % len(px_turns)) if ok else "PARTIAL — see fanout/threads"
R["finished"]=datetime.now(timezone.utc).isoformat()
os.makedirs("aws/ops/reports",exist_ok=True)
json.dump(R,open("aws/ops/reports/4379_perplexity_activation.json","w"),indent=1,default=str)
md=[f"# ops 4379 — Perplexity activation — {R['verdict']}",
    f"- key_in_env={R['key_in_env']} ssm_written={R.get('ssm_written')} "
    f"breaker={R.get('breaker_cleared')} registry={R.get('registry')}",
    f"- fanout rounds: {json.dumps(R['fanout'])[:600]}",
    f"- inbox after: {R.get('inbox_after')}"]
for name in ("t0001","t0002"):
    t=R.get(name) or {}
    md.append(f"\n## THREAD {t.get('thread_id')} (status {t.get('status')})")
    for x in (t.get("turns") or []):
        md.append(f"\n### {x['from']} -> {x['to']} [{x['kind']}"
                  f"{'/'+x['verdict'] if x.get('verdict') else ''}] {x['ts']}"
                  f"{' (via '+x['delivered_via']+')' if x.get('delivered_via') else ''}")
        md.append((x.get("content") or "")[:1800])
        if x.get("evidence"):
            md.append("evidence: "+json.dumps([{k:e.get(k) for k in
                      ("kind","ref","resolved")} for e in x["evidence"]]))
    for x in (t.get("rejected") or [])[-2:]:
        md.append(f"\n### REJECTED {x['from']} [{x['kind']}] — {x.get('status')}")
        md.append((x.get("content") or "")[:400])
open("aws/ops/reports/4379_perplexity_activation.md","w").write("\n".join(md)+"\n")
print(json.dumps({k:v for k,v in R.items() if k not in ("t0001","t0002")},
                 indent=1,default=str)[:2500])
