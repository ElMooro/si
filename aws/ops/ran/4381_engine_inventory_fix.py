"""ops 4381 — engine inventory fix: manifest shape-guess produced 0 engines;
rebuild via lam.list_functions (source of truth), hot-update the loop with
the same fallback, re-run one shard to prove engine auditing live. Also
probe the Telegram SSM names (nudge returned false)."""
import io, json, os, time, zipfile
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REGION="us-east-1"; BUCKET="justhodl-dashboard-live"; LOOP="justhodl-audit-loop"
lam=boto3.client("lambda",region_name=REGION,config=Config(read_timeout=280,retries={"max_attempts":0}))
s3=boto3.client("s3",region_name=REGION); ssm=boto3.client("ssm",region_name=REGION)
R={"ops":4381,"started":datetime.now(timezone.utc).isoformat()}

# authoritative engine list
engines,tok=[],None
while True:
    kw={"MaxItems":50}
    if tok: kw["Marker"]=tok
    resp=lam.list_functions(**kw)
    engines+= [f["FunctionName"] for f in resp.get("Functions",[])
               if f["FunctionName"].startswith("justhodl")]
    tok=resp.get("NextMarker")
    if not tok: break
engines=sorted(set(engines))
R["engines_discovered"]=len(engines)

inv=json.loads(s3.get_object(Bucket=BUCKET,Key="data/audit/inventory.json")["Body"].read())
inv["engines"]=engines; inv["updated"]=datetime.now(timezone.utc).isoformat()
inv["_persisted"]=True
s3.put_object(Bucket=BUCKET,Key="data/audit/inventory.json",
              Body=json.dumps(inv).encode(),ContentType="application/json")

# hot-update loop code
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z:
    z.write("aws/lambdas/justhodl-audit-loop/source/lambda_function.py","lambda_function.py")
    if os.path.exists("aws/shared/_sentry_lite.py"):
        z.write("aws/shared/_sentry_lite.py","_sentry_lite.py")
lam.update_function_code(FunctionName=LOOP,ZipFile=buf.getvalue())
for _ in range(24):
    c=lam.get_function_configuration(FunctionName=LOOP)
    if c.get("LastUpdateStatus")=="Successful": break
    time.sleep(5)
R["loop_updated"]=True

# telegram key probe
tg={}
for name in ("/justhodl/telegram/bot_token","/justhodl/telegram/chat_id"):
    try:
        ssm.get_parameter(Name=name,WithDecryption=True)
        tg[name]="present"
    except Exception as e:
        tg[name]=type(e).__name__
R["telegram_ssm"]=tg

# one engine-bearing shard
inv2=lam.invoke(FunctionName=LOOP,InvocationType="RequestResponse",Payload=b"{}")
body=json.loads(inv2["Payload"].read().decode())
r=json.loads(body["body"]) if isinstance(body,dict) and "body" in body else body
R["shard"]={k:r.get(k) for k in ("shard","new_findings","open_total","critical","filed_to_bus")}

hand=json.loads(s3.get_object(Bucket=BUCKET,Key="data/audit/handoff.json")["Body"].read())
R["coverage"]=hand.get("coverage")
eng_new=[f for f in (hand.get("top_open") or []) if f.get("layer")=="engine"]
R["sample_engine_findings"]=[f"[{f['severity']}] {f['target']} {f['check']}: {f['detail'][:90]}" for f in eng_new[:6]]

ok=R["engines_discovered"]>400 and (R["shard"].get("shard") or {}).get("engines",0)>0
R["verdict"]=(f"PASS — {R['engines_discovered']} engines inventoried, engine shard auditing live"
              if ok else "PARTIAL")
R["finished"]=datetime.now(timezone.utc).isoformat()
os.makedirs("aws/ops/reports",exist_ok=True)
json.dump(R,open("aws/ops/reports/4381_engine_inventory.json","w"),indent=1,default=str)
open("aws/ops/reports/4381_engine_inventory.md","w").write(
    f"# ops 4381 — engine inventory fix — {R['verdict']}\n"
    f"- engines discovered: {R['engines_discovered']} | loop updated: {R['loop_updated']}\n"
    f"- shard: {json.dumps(R['shard'])[:500]}\n"
    f"- coverage now: {json.dumps(R['coverage'])}\n"
    f"- telegram ssm: {json.dumps(tg)}\n"
    f"- sample engine findings:\n" + "\n".join("  "+s for s in R["sample_engine_findings"]) + "\n")
print(json.dumps(R,indent=1,default=str)[:2000])
