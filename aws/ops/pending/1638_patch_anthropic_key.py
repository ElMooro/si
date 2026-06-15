"""Root cause: bottleneck-research (new fn) lacks ANTHROPIC_API_KEY, so Haiku theses
fail fast. Copy the key from an existing Claude engine, patch env, regenerate."""
import json, time, boto3
from datetime import datetime, timezone
lam=boto3.client("lambda",region_name="us-east-1"); s3=boto3.client("s3",region_name="us-east-1")
B="justhodl-dashboard-live"; K="data/bottleneck-boom-research.json"
TGT="justhodl-bottleneck-research"

# 1) find ANTHROPIC_API_KEY from a known Claude engine
key=None
for src in ("justhodl-research-critique","justhodl-khalid-metrics","justhodl-ai-chat","justhodl-financial-secretary"):
    try:
        env=lam.get_function_configuration(FunctionName=src).get("Environment",{}).get("Variables",{})
        if env.get("ANTHROPIC_API_KEY"):
            key=env["ANTHROPIC_API_KEY"]; print(f"found ANTHROPIC_API_KEY in {src} (len {len(key)})"); break
    except Exception as e:
        print(f"  {src}: {str(e)[:60]}")
if not key:
    print("NO anthropic key found anywhere"); raise SystemExit

# 2) merge into target env (preserve existing)
cur=lam.get_function_configuration(FunctionName=TGT).get("Environment",{}).get("Variables",{})
cur["ANTHROPIC_API_KEY"]=key
lam.update_function_configuration(FunctionName=TGT, Environment={"Variables":cur})
print("patched env; waiting for update...")
for _ in range(15):
    time.sleep(4)
    st=lam.get_function_configuration(FunctionName=TGT).get("LastUpdateStatus")
    if st=="Successful": print("update successful"); break
    if st=="Failed": print("update FAILED"); raise SystemExit

# 3) wipe cache + regenerate
try: s3.delete_object(Bucket=B,Key=K)
except Exception: pass
n0=datetime.now(timezone.utc)
lam.invoke(FunctionName=TGT, InvocationType="Event"); print("invoked fresh")
POLL=("Draft","Critique","Analyze the Request","**","Deconstruct","Final Polish","Input Data","Role:")
for i in range(12):
    time.sleep(20)
    try:
        d=json.loads(s3.get_object(Bucket=B,Key=K)["Body"].read())
        if datetime.fromisoformat(d.get("generated_at"))>=n0.replace(microsecond=0) and d.get("new_theses",0)>0:
            bt=d.get("by_ticker",{})
            clean=sum(1 for v in bt.values() if v.get("thesis") and not any(m in v["thesis"] for m in POLL))
            print(f"\nREADY {(i+1)*20}s: new={d.get('new_theses')} CLEAN={clean}/{len(bt)} dur={d.get('duration_s')}s")
            for t in ("VST","MU"):
                v=bt.get(t,{}); print(f"\n{t}: {v.get('thesis')}")
            break
    except Exception: pass
    print(f"  ...{(i+1)*20}s")
else: print("not ready in window")
