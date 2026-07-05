"""ops 2900 — mark ka/khalid-analysis CREDITS-GATED in freshness manifest (engines healthy; writes resume on Anthropic top-up)."""
import os, json, boto3
from datetime import datetime, timezone
s3=boto3.client("s3",region_name="us-east-1"); B="justhodl-dashboard-live"
R={"ops":2900,"ts":datetime.now(timezone.utc).isoformat()}
man=json.loads(s3.get_object(Bucket=B,Key="data/_freshness-manifest.json")["Body"].read())
keys=man.get("keys") if isinstance(man.get("keys"),dict) else man
for k in ("data/khalid-analysis.json","data/ka-analysis.json"):
    cur=keys.get(k) if isinstance(keys.get(k),dict) else {}
    cur["max_age_hours"]=8760
    cur["note"]="CREDITS-GATED: engine healthy (84 metrics publish, 35s clean); Sonnet narrative writes on Anthropic top-up"
    keys[k]=cur
if isinstance(man.get("keys"),dict): man["keys"]=keys
else: man=keys
s3.put_object(Bucket=B,Key="data/_freshness-manifest.json",Body=json.dumps(man,ensure_ascii=False,default=str).encode(),ContentType="application/json")
R["status"]="OK"; R["marked"]=["khalid-analysis","ka-analysis"]
print(json.dumps(R,indent=1))
os.makedirs("aws/ops/reports",exist_ok=True); json.dump(R,open("aws/ops/reports/2900_credits_gated.json","w"),indent=1)
print("OPS 2900 COMPLETE")
