import json, time, boto3
from datetime import datetime, timezone
s3=boto3.client("s3",region_name="us-east-1"); lam=boto3.client("lambda",region_name="us-east-1")
B="justhodl-dashboard-live"; K="data/bottleneck-boom-research.json"
n0=datetime.now(timezone.utc)
lam.invoke(FunctionName="justhodl-bottleneck-research", InvocationType="Event"); print("invoked")
for i in range(11):
    time.sleep(20)
    try:
        d=json.loads(s3.get_object(Bucket=B,Key=K)["Body"].read())
        if datetime.fromisoformat(d.get("generated_at"))>=n0.replace(microsecond=0):
            bt=d.get("by_ticker",{})
            clean=sum(1 for v in bt.values() if v.get("thesis") and not any(m in v["thesis"] for m in ("Draft","Critique","Analyze the Request","**")))
            print(f"READY {(i+1)*20}s: n={len(bt)} new={d.get('new_theses')} clean_theses={clean}/{len(bt)} dur={d.get('duration_s')}s")
            for t in ("VST","MU","DELL"):
                v=bt.get(t,{})
                if v.get("thesis"): print(f"\n=== {t} ({v.get('name')}) P/E {round(v.get('pe') or 0,1)} vs ind {v.get('industry_pe')} ===\n{v['thesis']}")
            break
    except Exception as e: pass
    print(f"  ...{(i+1)*20}s")
else: print("not ready")
