import json, boto3, base64
from datetime import datetime, timezone
s3=boto3.client("s3",region_name="us-east-1"); lam=boto3.client("lambda",region_name="us-east-1")
# finviz feed freshness + coverage
for key in ["data/finviz-short.json","data/finviz-universe.json"]:
    try:
        h=s3.head_object(Bucket="justhodl-dashboard-live",Key=key)
        age=(datetime.now(timezone.utc)-h["LastModified"]).total_seconds()/3600
        d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key=key)["Body"].read())
        bt=d.get("by_ticker",{})
        print(f"{key}: age={age:.1f}h tickers={len(bt)} n_short={d.get('n_with_short_float',d.get('n_short','?'))}")
    except Exception as e: print(f"{key}: {str(e)[:90]}")
# invoke retail-sentiment, capture finviz log + squeeze coverage
r=lam.invoke(FunctionName="justhodl-retail-sentiment", InvocationType="RequestResponse", LogType="Tail")
log=base64.b64decode(r.get("LogResult","")).decode("utf-8","ignore")
for ln in log.splitlines():
    if "finviz short coverage" in ln: print("LOG:",ln.strip())
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/retail-sentiment.json")["Body"].read())
t30=d.get("top_30_by_mentions",[])
cov=[e for e in t30 if e.get("short_pct") is not None]
fv=[e for e in t30 if e.get("short_src")=="finviz"]
print(f"squeeze short_pct coverage: {len(cov)}/{len(t30)} (was 13/30) | from finviz: {len(fv)}")
print("squeeze_radar:", len((d.get('ranked',{}) or {}).get('squeeze_radar',[])))
for e in t30[:6]:
    print(f"  {e.get('ticker'):6} short={e.get('short_pct')} src={e.get('short_src')} sqz={e.get('squeeze_score')} relvol={e.get('rel_volume')}")
