import boto3, datetime
s3=boto3.client("s3","us-east-1"); now=datetime.datetime.now(datetime.timezone.utc); B="justhodl-dashboard-live"
keys=["data/polygon-fx-regime.json","data/polygon-futures-curves.json","data/polygon-options-flow.json",
 "data/dealer-gex.json","data/options-flow.json","data/options-gamma.json",
 "etf-flows/composite.json","etf-flows/rotation.json","etf-flows/per-ticker-context.json","etf-flows/daily.json",
 "data/activist-13d.json","data/insider-clusters.json","data/squeeze-pretrigger.json"]
print("MASSIVE-DATA / CONSUMER OUTPUT FRESHNESS:")
for k in keys:
    try:
        h=s3.head_object(Bucket=B,Key=k); age=(now-h["LastModified"]).total_seconds()/3600
        print("  %-44s %6.1fKB  %s"%(k,h["ContentLength"]/1024,"FRESH" if age<24 else "STALE %.0fh"%age))
    except Exception: print("  %-44s MISSING"%k)
# does etf-flows per-ticker-context have real sector flows?
import json
try:
    d=json.loads(s3.get_object(Bucket=B,Key="etf-flows/per-ticker-context.json")["Body"].read())
    bs=d.get("by_sector",{})
    print("\nETF per-ticker-context: %d sectors, regime=%s"%(len(bs),d.get("global_regime")))
    for sec,v in list(bs.items())[:6]:
        print("  %-22s %-14s z=%s"%(sec,v.get("flow_label"),v.get("flow_zscore_90d")))
except Exception as e: print("etf ctx err:",str(e)[:60])
