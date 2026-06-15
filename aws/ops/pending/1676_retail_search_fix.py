import json, boto3, base64
from datetime import datetime, timezone
lam=boto3.client("lambda",region_name="us-east-1"); s3=boto3.client("s3",region_name="us-east-1")
# freshness of ticker-trends.json
try:
    h=s3.head_object(Bucket="justhodl-dashboard-live",Key="data/ticker-trends.json")
    age=(datetime.now(timezone.utc)-h["LastModified"]).total_seconds()/3600
    tt=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/ticker-trends.json")["Body"].read())
    allr=tt.get("all_results",[])
    rising=[r for r in allr if isinstance(r,dict) and (r.get("stealth") or (r.get("velocity") or 0)>=1.5)]
    print(f"ticker-trends.json age={age:.1f}h n_results={len(allr)} rising/stealth={len(rising)} sample={[r['ticker'] for r in rising[:8]]}")
except Exception as e: print("ticker-trends:",str(e)[:120])
r=lam.invoke(FunctionName="justhodl-retail-sentiment", InvocationType="RequestResponse", LogType="Tail")
log=base64.b64decode(r.get("LogResult","")).decode("utf-8","ignore")
for ln in log.splitlines():
    if "corroboration:" in ln: print("LOG:",ln.strip()[:160])
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/retail-sentiment.json")["Body"].read())
t30=d.get("top_30_by_mentions",[])
with_search=[e["ticker"] for e in t30 if "Search" in (e.get("corroboration") or [])]
print("names with Search corroboration:", len(with_search), with_search[:10])
print("multi_venue_confirmed:", len((d.get("ranked",{}) or {}).get("multi_venue_confirmed",[])))
