import json, boto3
from botocore.config import Config
from datetime import datetime, timezone
s3=boto3.client("s3",region_name="us-east-1")
# 1) did the prior invoke already write the feed?
try:
    h=s3.head_object(Bucket="justhodl-dashboard-live",Key="data/finviz-signals.json")
    age=(datetime.now(timezone.utc)-h["LastModified"]).total_seconds()/60
    print(f"finviz-signals.json EXISTS age={age:.1f}min")
    fresh = age < 30
except Exception:
    print("finviz-signals.json not present yet"); fresh=False
# 2) if stale/missing, invoke with a long client read timeout
if not fresh:
    lam=boto3.client("lambda",region_name="us-east-1",config=Config(read_timeout=240,retries={"max_attempts":0}))
    print("invoking justhodl-finviz-signals (long timeout)...")
    r=lam.invoke(FunctionName="justhodl-finviz-signals",InvocationType="RequestResponse")
    print("invoke status:",r["StatusCode"],"payload:",r["Payload"].read().decode()[:300])
# 3) read + report
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/finviz-signals.json")["Body"].read())
print("generated_at:",d.get("generated_at"))
c=d.get("counts",{})
for k in sorted(c,key=lambda x:-c[x]): print(f"  {k:16} {c[k]}")
gc=d["signals"].get("golden_cross",[])[:5]
print("golden_cross:", [(x.get("ticker"),x.get("perf_m")) for x in gc])
dc=d["signals"].get("death_cross",[])[:5]
print("death_cross:", [x.get("ticker") for x in dc])
