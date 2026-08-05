"""ops 4424 — shard the health sweep (it timed out; the watchdog caught it).

The first sweep scanned every thread each run and hit the 180s Lambda
timeout — a real defect in my own self-supervision code, found by running it
rather than by assuming it worked. Fixed: rotating cursor, 12 newest-first
threads per run, so the whole bus is covered continuously without any single
invocation blowing its budget. Also raises the function timeout headroom.
"""
import io,json,os,time,zipfile
from datetime import datetime,timezone
import boto3
from botocore.config import Config
REGION="us-east-1"; BUCKET="justhodl-dashboard-live"; AGENT="justhodl-backend-agent"; BUS="justhodl-a2a-bus"
lam=boto3.client("lambda",region_name=REGION,config=Config(read_timeout=280,retries={"max_attempts":0}))
s3=boto3.client("s3",region_name=REGION)
R={"ops":4424,"started":datetime.now(timezone.utc).isoformat()}
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z:
    z.write(f"aws/lambdas/{AGENT}/source/lambda_function.py","lambda_function.py")
    if os.path.exists("aws/shared/_sentry_lite.py"): z.write("aws/shared/_sentry_lite.py","_sentry_lite.py")
for _ in range(20):
    c=lam.get_function_configuration(FunctionName=AGENT)
    if c.get("LastUpdateStatus") in (None,"Successful") and c.get("State")=="Active": break
    time.sleep(6)
for _ in range(5):
    try: lam.update_function_code(FunctionName=AGENT,ZipFile=buf.getvalue()); break
    except lam.exceptions.ResourceConflictException: time.sleep(12)
for _ in range(24):
    if lam.get_function_configuration(FunctionName=AGENT).get("LastUpdateStatus")=="Successful": break
    time.sleep(5)
try:
    lam.update_function_configuration(FunctionName=AGENT,Timeout=300)
    R["timeout"]=300
    time.sleep(8)
except Exception as e: R["timeout_err"]=str(e)[:100]
t0=time.time()
inv=lam.invoke(FunctionName=AGENT,InvocationType="RequestResponse",Payload=b"{}")
R["elapsed_s"]=round(time.time()-t0,1)
b=json.loads(inv["Payload"].read().decode())
try: R["result"]=json.loads(b["body"]) if isinstance(b,dict) and "body" in b else b
except Exception: R["result"]=str(b)[:300]
time.sleep(3)
for k,label in (("data/backend-agent/bus-health.json","health"),
                ("data/backend-agent/sweep-cursor.json","cursor")):
    try: R[label]=json.loads(s3.get_object(Bucket=BUCKET,Key=k)["Body"].read())
    except Exception as e: R[label+"_err"]=str(e)[:80]
h=R.get("health") or {}
issues={}
for f in (h.get("findings") or []): issues[f.get("issue")]=issues.get(f.get("issue"),0)+1
R["issues"]=issues
ok=R.get("elapsed_s",999)<170 and isinstance(h,dict) and "findings" in h
R["verdict"]=(f"PASS — sweep completed in {R['elapsed_s']}s, "
              f"{h.get('n_findings')} findings, {h.get('n_repairs')} repairs, "
              f"cursor {((R.get('cursor') or {}).get('i'))}/{((R.get('cursor') or {}).get('total_threads'))}"
              if ok else f"PARTIAL — elapsed {R.get('elapsed_s')}s")
R["finished"]=datetime.now(timezone.utc).isoformat()
os.makedirs("aws/ops/reports",exist_ok=True)
json.dump(R,open("aws/ops/reports/4424_shard.json","w"),indent=1,default=str)
open("aws/ops/reports/4424_shard.md","w").write(
 f"# ops 4424 — sharded health sweep — {R['verdict']}\n"
 f"- elapsed: {R.get('elapsed_s')}s | timeout now {R.get('timeout')}\n"
 f"- issues: {json.dumps(issues)}\n"
 f"- repairs: {json.dumps((h.get('repairs') or [])[:6],indent=1)[:700]}\n"
 f"- findings: {json.dumps((h.get('findings') or [])[:8],indent=1)[:900]}\n"
 f"- cursor: {json.dumps(R.get('cursor'))[:200]}\n")
print(json.dumps({"elapsed":R.get("elapsed_s"),"issues":issues,"repairs":h.get("n_repairs")},indent=1)[:500])
