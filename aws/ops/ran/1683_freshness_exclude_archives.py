import json, boto3, re
from collections import defaultdict
s3=boto3.client("s3",region_name="us-east-1"); lam=boto3.client("lambda",region_name="us-east-1")
BUCKET="justhodl-dashboard-live"; MK="data/_freshness-manifest.json"
man=json.loads(s3.get_object(Bucket=BUCKET,Key=MK)["Body"].read())
excl=set(man.get("exclude_prefixes") or [])
add=[
  "data/activity-nowcast/snapshots/",
  "data/_alerts/",
  "data/ai-commentary/history/",
  "data/track-record/snapshots/",
  "data/track-record/",
  "data/estimate-revisions/",
  "data/digest-trends-ai-history/",
  "data/regime-history/",
]
before=len(excl); excl.update(add); man["exclude_prefixes"]=sorted(excl)
s3.put_object(Bucket=BUCKET,Key=MK,Body=json.dumps(man,indent=2).encode(),ContentType="application/json")
print("exclude_prefixes: {} -> {}".format(before,len(excl)))
lam.invoke(FunctionName="justhodl-fleet-freshness-monitor", InvocationType="RequestResponse")
import time; time.sleep(2)
stt=json.loads(s3.get_object(Bucket=BUCKET,Key="data/_freshness-monitor.json")["Body"].read())
print("AFTER mute: tracked={} fresh={} stale={} alerts={}".format(stt.get("n_keys_tracked"),stt.get("n_fresh"),stt.get("n_stale"),stt.get("n_alerts_raised")))
date_re=re.compile(r'\d{4}-\d{2}-\d{2}')
groups=defaultdict(lambda:{"n":0,"dated":0,"sample":None,"max_age":0})
for r in stt.get("stale_top_50",[]):
    k=r["key"]; pref="/".join(k.split("/")[:-1])+"/"
    g=groups[pref]; g["n"]+=1
    if date_re.search(k.split("/")[-1]): g["dated"]+=1
    if g["sample"] is None: g["sample"]=k.split("/")[-1]
    g["max_age"]=max(g["max_age"], r.get("age_h",0))
print("\nREMAINING stale by dir (these are the REAL live-feed failures now):")
for pref,g in sorted(groups.items(), key=lambda x:-x[1]["n"]):
    tag="archive?" if g["dated"]>=max(g["n"]*0.6,1) else "LIVE-FEED"
    print("  [{}] {} n={} dated={} maxage={:.0f}h e.g.={}".format(tag,pref,g["n"],g["dated"],g["max_age"],g["sample"]))
