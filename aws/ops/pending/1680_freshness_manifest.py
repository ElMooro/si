import json, boto3, time
from datetime import datetime, timezone
s3=boto3.client("s3",region_name="us-east-1"); ev=boto3.client("events",region_name="us-east-1"); lam=boto3.client("lambda",region_name="us-east-1")
BUCKET="justhodl-dashboard-live"; MK="data/_freshness-manifest.json"
# 1) read existing manifest (preserve), merge per-feed SLAs
try:
    man=json.loads(s3.get_object(Bucket=BUCKET,Key=MK)["Body"].read())
except Exception:
    man={"rules":[{"prefix":"data/","default_max_age_h":26.0}]}
ov=man.get("key_overrides") or {}
# realistic SLAs by known cadence (hours). Only feeds whose cadence is known → avoid false alarms.
new={
  "data/retail-sentiment.json":2.0,          # every 30min
  "data/retail-attention-history.json":2.0,  # same engine
  "data/retail-alerts.json":2.0,             # same engine
  "data/ticker-trends.json":28.0,            # 2x daily 13/21 UTC
  "data/google-trends.json":1000000.0,       # RETIRED ops547 — mute (intentionally dead)
}
ov.update(new); man["key_overrides"]=ov
s3.put_object(Bucket=BUCKET,Key=MK,Body=json.dumps(man,indent=2).encode(),ContentType="application/json")
print("manifest key_overrides now:", json.dumps(man.get("key_overrides"),indent=0)[:400])
# 2) ensure monitor schedule enabled
rn="fleet-freshness-monitor-30min"
try:
    r=ev.describe_rule(Name=rn); st=r.get("State")
    if st!="ENABLED": ev.enable_rule(Name=rn); print(f"rule {rn}: was {st} -> ENABLED")
    else: print(f"rule {rn}: already {st}")
except Exception as e: print(f"rule {rn}: {str(e)[:120]}")
# 3) run the monitor now to regenerate data/_freshness-monitor.json with new SLAs
lam.invoke(FunctionName="justhodl-fleet-freshness-monitor", InvocationType="RequestResponse")
time.sleep(2)
st=json.loads(s3.get_object(Bucket=BUCKET,Key="data/_freshness-monitor.json")["Body"].read())
print(f"\nmonitor: tracked={st.get('n_keys_tracked')} fresh={st.get('n_fresh')} stale={st.get('n_stale')} alerts={st.get('n_alerts_raised')}")
# show how our key feeds are tracked
watch={"data/ticker-trends.json","data/retail-sentiment.json","data/options-flow.json","data/news-velocity.json","data/short-interest.json","data/retail-attention-history.json"}
stale_keys={r["key"]:r for r in st.get("stale_top_50",[])}
for k in sorted(watch):
    if k in stale_keys: print(f"  STALE {k}: {stale_keys[k]['age_h']}h (SLA {stale_keys[k]['max_age_h']}h)")
    else: print(f"  ok    {k}: fresh (within SLA)")
print("\ntop stale overall:", [(r['key'],f\"{r['age_h']}h/SLA{r['max_age_h']}h\") for r in st.get('stale_top_50',[])[:8]])
