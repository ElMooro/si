import json, boto3
from datetime import datetime, timezone
s3=boto3.client("s3",region_name="us-east-1")
B="justhodl-dashboard-live"; K="data/bottleneck-boom-research.json"
try:
    head=s3.head_object(Bucket=B,Key=K)
    d=json.loads(s3.get_object(Bucket=B,Key=K)["Body"].read())
except Exception as e:
    print("read err / no file:", str(e)[:100]); raise SystemExit
bt=d.get("by_ticker",{})
POLL=("Draft","Critique","Analyze the Request","**","Deconstruct","Final Polish","Input Data","Role:")
clean=[t for t,v in bt.items() if v.get("thesis") and not any(m in v["thesis"] for m in POLL)]
poll=[t for t,v in bt.items() if v.get("thesis") and any(m in v["thesis"] for m in POLL)]
noth=[t for t,v in bt.items() if not v.get("thesis")]
print(f"generated_at: {d.get('generated_at')} | new_theses(last run): {d.get('new_theses')} | dur: {d.get('duration_s')}s")
print(f"n={len(bt)} | CLEAN={len(clean)} | polluted={len(poll)} | no_thesis={len(noth)}")
print("polluted tickers:", poll[:20])
print("no-thesis tickers:", noth[:20])
# show one clean sample fully
if clean:
    v=bt[clean[0]]
    print(f"\nclean sample {clean[0]}: thesis_ver={v.get('thesis_ver')}\n{v.get('thesis')}")
