import json, time, boto3
from datetime import datetime, timezone
s3=boto3.client("s3",region_name="us-east-1"); lam=boto3.client("lambda",region_name="us-east-1")
B="justhodl-dashboard-live"; K="data/bottleneck-boom-research.json"
# 1) wipe cache so every thesis regenerates via Haiku
try: s3.delete_object(Bucket=B,Key=K); print("cache deleted")
except Exception as e: print("delete err",str(e)[:80])
time.sleep(2)
n0=datetime.now(timezone.utc)
lam.invoke(FunctionName="justhodl-bottleneck-research", InvocationType="Event"); print("invoked fresh")
for i in range(12):
    time.sleep(20)
    try:
        d=json.loads(s3.get_object(Bucket=B,Key=K)["Body"].read())
        ga=datetime.fromisoformat(d.get("generated_at"))
        if ga>=n0.replace(microsecond=0) and d.get("new_theses",0)>0:
            bt=d.get("by_ticker",{})
            POLL=("Draft","Critique","Analyze the Request","**","Deconstruct","Final Polish","Input Data")
            clean=sum(1 for v in bt.values() if v.get("thesis") and not any(m in v["thesis"] for m in POLL))
            wth=sum(1 for v in bt.values() if v.get("thesis"))
            print(f"\nREADY {(i+1)*20}s: n={len(bt)} new={d.get('new_theses')} thesis={wth}/{len(bt)} CLEAN={clean}/{len(bt)} dur={d.get('duration_s')}s")
            for t in ("VST","MU"):
                v=bt.get(t,{})
                print(f"\n=== {t} P/E {round(v.get('pe') or 0,1)} vs ind {v.get('industry_pe')} | {len(v.get('financials') or [])}yr ===\n{(v.get('thesis') or '(none)')[:600]}")
            break
    except Exception: pass
    print(f"  ...{(i+1)*20}s")
else: print("not fully ready")
