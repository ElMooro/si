import boto3, json
s3=boto3.client("s3","us-east-1")
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/flow-confluence.json")["Body"].read())
tm=d.get("ticker_map") or {}
print("ticker_map size:", len(tm))
# count engine occurrences across all names
from collections import Counter
c=Counter()
for tk,a in tm.items():
    for e in (a.get("engines") or []): c[e]+=1
print("engine occurrence counts:", dict(c))
# show a few insider/buyback names
for tk,a in tm.items():
    eng=a.get("engines") or []
    if any(e in {"insider","buyback","insider-buyback"} for e in eng):
        print(f"  {tk}: engines={eng} score={a.get('score')} posture={a.get('posture')}")
print("DONE 2205")
