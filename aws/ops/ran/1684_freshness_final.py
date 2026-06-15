import json, boto3
s3=boto3.client("s3",region_name="us-east-1"); lam=boto3.client("lambda",region_name="us-east-1")
BUCKET="justhodl-dashboard-live"; MK="data/_freshness-manifest.json"
man=json.loads(s3.get_object(Bucket=BUCKET,Key=MK)["Body"].read())
excl=set(man.get("exclude_prefixes") or [])
excl.add("data/_")   # all underscore-prefixed internal/cache/admin namespaces (askdesk/ledger/cache/internals/freshness/alerts)
man["exclude_prefixes"]=sorted(excl)
s3.put_object(Bucket=BUCKET,Key=MK,Body=json.dumps(man,indent=2).encode(),ContentType="application/json")
lam.invoke(FunctionName="justhodl-fleet-freshness-monitor", InvocationType="RequestResponse")
import time; time.sleep(2)
stt=json.loads(s3.get_object(Bucket=BUCKET,Key="data/_freshness-monitor.json")["Body"].read())
print("FINAL: tracked={} fresh={} stale={} alerts={}".format(stt.get("n_keys_tracked"),stt.get("n_fresh"),stt.get("n_stale"),stt.get("n_alerts_raised")))
print("\nremaining stale LIVE feeds (key | age | SLA):")
for r in sorted(stt.get("stale_top_50",[]), key=lambda x:-x.get("age_h",0)):
    print("  {} | {:.0f}h | SLA {}h".format(r["key"], r.get("age_h",0), r.get("max_age_h")))
print("\nexclude_prefixes now:", man["exclude_prefixes"])
