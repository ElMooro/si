import json, urllib.request, boto3
from datetime import datetime, timezone
s3=boto3.client("s3",region_name="us-east-1")
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/retail-sentiment.json")["Body"].read())
surges=(d.get("ranked") or {}).get("biggest_velocity_surges") or []
ga=d.get("generated_at","")
ageH=(datetime.now(timezone.utc)-datetime.fromisoformat(ga)).total_seconds()/3600 if ga else 999
print(f"FEED: generated_at {ga[:19]} ({ageH:.1f}h old) | surges {len(surges)} | sample {surges[0]['ticker']} +{surges[0]['velocity_pct']}% / {surges[0]['mentions']} mentions" if surges else "no surges")
p=urllib.request.urlopen(urllib.request.Request("https://justhodl.ai/digest-trends.html",headers={"User-Agent":"Mozilla/5.0"}),timeout=20).read().decode("utf-8","ignore")
print("PAGE reads correct key:", "biggest_velocity_surges" in p)
print("PAGE stale-guard present:", "feed refreshing" in p)
