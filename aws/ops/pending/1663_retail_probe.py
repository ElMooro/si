import json, boto3
from datetime import datetime, timezone
s3=boto3.client("s3",region_name="us-east-1"); B="justhodl-dashboard-live"
d=json.loads(s3.get_object(Bucket=B,Key="data/retail-sentiment.json")["Body"].read())
print("top-level keys:", list(d.keys()))
print("generated_at:", d.get("generated_at"), "| now:", datetime.now(timezone.utc).isoformat()[:19])
rk=d.get("ranked") or {}
print("ranked keys:", list(rk.keys()) if isinstance(rk,dict) else type(rk))
surges=(rk.get("biggest_velocity_surges") or [])
print("biggest_velocity_surges n:", len(surges))
if surges: print("  sample surge:", json.dumps(surges[0]))
t30=d.get("top_30_by_mentions") or []
print("top_30_by_mentions n:", len(t30))
if t30: print("  sample t30:", json.dumps(t30[0]))
print("market_regime_data:", json.dumps(d.get("market_regime_data") or {}))
# also check the lambda schedule/last run
lam=boto3.client("lambda",region_name="us-east-1")
try:
    import datetime as _dt
    cfg=lam.get_function_configuration(FunctionName="justhodl-retail-sentiment")
    print("lambda LastModified:", cfg.get("LastModified"))
except Exception as e: print("lambda check:", str(e)[:80])
