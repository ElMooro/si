import json, boto3, re
from collections import defaultdict
s3=boto3.client("s3",region_name="us-east-1")
stt=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/_freshness-monitor.json")["Body"].read())
stale=stt.get("stale_top_50",[])
print("n_stale total:", stt.get("n_stale"), "| in snapshot:", len(stale))
date_re=re.compile(r'\d{4}-\d{2}-\d{2}')
groups=defaultdict(lambda:{"n":0,"dated":0,"sample":None,"max_age":0})
for r in stale:
    k=r["key"]; pref="/".join(k.split("/")[:-1])+"/"
    g=groups[pref]; g["n"]+=1
    if date_re.search(k.split("/")[-1]): g["dated"]+=1
    if g["sample"] is None: g["sample"]=k.split("/")[-1]
    g["max_age"]=max(g["max_age"], r.get("age_h",0))
print("\nstale grouped by directory (dated = filename has YYYY-MM-DD → immutable archive):")
for pref,g in sorted(groups.items(), key=lambda x:-x[1]["n"]):
    tag="ARCHIVE(mute)" if g["dated"]>=g["n"]*0.6 else "LIVE-FEED(keep)"
    print(f"  [{tag}] {pref}  n={g['n']} dated={g['dated']} maxage={g['max_age']:.0f}h e.g.={g['sample']}")
