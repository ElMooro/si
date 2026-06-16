import json, time, boto3
from botocore.config import Config
s3=boto3.client("s3",region_name="us-east-1")
lam=boto3.client("lambda",region_name="us-east-1",config=Config(read_timeout=300,retries={"max_attempts":0}))
try: before=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/ciss-stress.json")["Body"].read()).get("generated_at")
except: before=None
lam.invoke(FunctionName="justhodl-ciss-stress",InvocationType="Event")
print("invoked ciss-stress (async); polling...")
d=None
for i in range(14):
    time.sleep(20)
    try:
        d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/ciss-stress.json")["Body"].read())
        if d.get("generated_at")!=before: break
    except: pass
if not d or d.get("generated_at")==before: print("no refresh yet"); raise SystemExit
print(f"elapsed={d.get('elapsed_s')}s | n_series={d.get('n_series')} | EA regime={d.get('ea_regime')} composite={d.get('ea_composite')} @ {d.get('ea_composite_date')}")
print("categories:", d.get("categories"))
ser=d.get("series",[])
print("\nsample per category (history depth + latest):")
seen=set()
for s in ser:
    if s["category"] not in seen:
        seen.add(s["category"])
        print(f"  [{s['category']:18}] {s['label'][:38]:38} {s['start_date']}->{s['latest_date']} n={s['n_obs']} stored={len(s['points'])} latest={s.get('latest')} pctile={s.get('pctile')} z={s.get('zscore')}")
# size check
import sys
sz=len(json.dumps(d))
print(f"\npayload size: {sz/1e6:.2f} MB")
