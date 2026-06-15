import json, boto3
s3=boto3.client("s3",region_name="us-east-1")
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/bottleneck-boom-research.json")["Body"].read())
bt=d.get("by_ticker",{})
print("n:",len(bt),"new_theses:",d.get("new_theses"))
for t,v in list(bt.items())[:3]:
    th=v.get("thesis") or ""
    poll = any(x in th for x in ("Draft","Critique","mechanism constraint","Let's check","*"))
    print(f"\n{t}: len={len(th)} draft_polluted={poll}")
    print(th[:500])
