import json, boto3
s3=boto3.client("s3",region_name="us-east-1")
for k in ["data/index-inclusion.json","data/index-membership.json","data/finviz-index-changes.json"]:
    try:
        h=s3.head_object(Bucket="justhodl-dashboard-live",Key=k); print(f"  {k} EXISTS {h['ContentLength']}b")
    except: print(f"  {k} —")
# does a finviz index-inclusion engine output exist?
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/finviz-universe.json")["Body"].read())
bt=d.get("by_ticker",{})
from collections import Counter
c=Counter()
for r in bt.values():
    im=r.get("index_membership")
    if im:
        for tok in str(im).replace(",", " ").split():
            c[tok]+=1
print("index membership token counts:", dict(c))
