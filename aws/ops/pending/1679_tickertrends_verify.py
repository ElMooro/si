import json, boto3
from datetime import datetime, timezone
s3=boto3.client("s3",region_name="us-east-1"); lam=boto3.client("lambda",region_name="us-east-1")
h=s3.head_object(Bucket="justhodl-dashboard-live",Key="data/ticker-trends.json")
age=(datetime.now(timezone.utc)-h["LastModified"]).total_seconds()/3600
tt=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/ticker-trends.json")["Body"].read())
allr=tt.get("all_results",[])
rising=[r for r in allr if isinstance(r,dict) and (r.get("stealth") or (r.get("velocity") or 0)>=1.5)]
print(f"ticker-trends.json age={age:.2f}h  generated_at={tt.get('generated_at')}  n_results={len(allr)}  n_ok={tt.get('n_ok')}")
print(f"rising/stealth now: {len(rising)} -> {[r['ticker'] for r in rising[:12]]}")
# refresh retail feed so Search corroboration reflects fresh trends
lam.invoke(FunctionName="justhodl-retail-sentiment", InvocationType="RequestResponse")
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/retail-sentiment.json")["Body"].read())
t30=d.get("top_30_by_mentions",[])
ws=[e["ticker"] for e in t30 if "Search" in (e.get("corroboration") or [])]
print(f"retail Search corroboration now: {len(ws)} names {ws[:12]}")
print(f"multi_venue_confirmed: {len((d.get('ranked',{}) or {}).get('multi_venue_confirmed',[]))}")
