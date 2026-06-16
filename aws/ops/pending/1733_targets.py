import json, boto3
s3=boto3.client("s3",region_name="us-east-1")
v=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/stock-valuations.json")["Body"].read())
print("valuations top-level keys:", list(v.keys()))
for k in v:
    if isinstance(v[k],list) and v[k] and isinstance(v[k][0],dict) and ("t" in v[k][0] or "ticker" in v[k][0]):
        print(f"  list '{k}': {len(v[k])} rows, sample t={v[k][0].get('t') or v[k][0].get('ticker')}")
try:
    o=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/opportunities-research.json")["Body"].read())
    bt=o.get("by_ticker",{}); print("opportunities-research by_ticker:", len(bt), "sample:", list(bt)[:8])
except Exception as e: print("opp feed:", str(e)[:60])
