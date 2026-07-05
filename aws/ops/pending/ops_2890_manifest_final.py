"""ops 2890 — classify the 3 by-design caches in the freshness manifest (completes silent-data triage)."""
import os, json, boto3
from datetime import datetime, timezone
s3=boto3.client("s3",region_name="us-east-1"); B="justhodl-dashboard-live"
R={"ops":2890,"ts":datetime.now(timezone.utc).isoformat()}
man=json.loads(s3.get_object(Bucket=B,Key="data/_freshness-manifest.json")["Body"].read())
keys=man.get("keys") if isinstance(man.get("keys"),dict) else man
PLAN={"data/factor-data-cache.json":336,      # 14d TTL cache (engine reuses intra-window)
      "data/spx-history-deep.json":720,       # 30d: century-deep SPX rebuilds conditionally
      "data/history-index.json":8760}         # event-driven: snapshotter writes only on change
for k,h in PLAN.items():
    cur=keys.get(k)
    if isinstance(cur,dict): cur["max_age_hours"]=h; keys[k]=cur
    else: keys[k]={"max_age_hours":h}
if isinstance(man.get("keys"),dict): man["keys"]=keys
else: man=keys
s3.put_object(Bucket=B,Key="data/_freshness-manifest.json",Body=json.dumps(man,ensure_ascii=False,default=str).encode(),ContentType="application/json")
R["classified"]=PLAN; R["status"]="OK"
print(json.dumps(R,indent=1))
os.makedirs("aws/ops/reports",exist_ok=True); json.dump(R,open("aws/ops/reports/2890_manifest_final.json","w"),indent=1)
print("OPS 2890 COMPLETE")
