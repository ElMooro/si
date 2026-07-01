"""ops 2654 — full shape of alerts/feed_health/missing_feeds + a ticker with red_flags/cycle_warning/structural_chokepoint populated, so the page rebuild uses real field names."""
import boto3, json
s3 = boto3.client("s3", region_name="us-east-1")
j = json.loads(s3.get_object(Bucket="justhodl-dashboard-live", Key="data/master-ranker.json")["Body"].read())
print("alerts:", json.dumps(j.get("alerts"), indent=1))
print("\nfeed_health:", json.dumps(j.get("feed_health"), indent=1)[:500])
print("\nfeed_freshness (sample keys):", list((j.get("feed_freshness") or {}).keys())[:8])
print("\nstale_feeds_excluded:", j.get("stale_feeds_excluded"))
print("\nmissing_feeds:", j.get("missing_feeds"))
tt = j.get("top_tickers") or []
for t in tt:
    if t.get("red_flags") or t.get("cycle_warning") or t.get("structural_chokepoint"):
        print(f"\n{t['ticker']}: red_flags={t.get('red_flags')} cycle_warning={t.get('cycle_warning')} structural={t.get('structural_chokepoint')} chokepoint_crit={t.get('chokepoint_criticality')}")
print("DONE 2654")
