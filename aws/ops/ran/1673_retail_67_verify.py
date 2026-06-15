import json, boto3
lam=boto3.client("lambda",region_name="us-east-1"); s3=boto3.client("s3",region_name="us-east-1")
lam.invoke(FunctionName="justhodl-retail-sentiment", InvocationType="RequestResponse")
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/retail-sentiment.json")["Body"].read())
r=d.get("ranked",{}); t30=d.get("top_30_by_mentions",[])
print("#6 multi_venue_confirmed:", len(r.get("multi_venue_confirmed",[])))
cc=[e for e in t30 if e.get("corroboration_count")]
print("  corroboration coverage:", len(cc),"/",len(t30))
mv=r.get("multi_venue_confirmed",[])
for e in mv[:4]: print("   ",e.get("ticker"),e.get("corroboration_count"),"×",e.get("corroboration"))
print("#7 flow_leaders:", len(r.get("flow_leaders",[])))
fl=[e for e in t30 if e.get("flow_signal")]
print("  flow_signal coverage:", len(fl),"/",len(t30), "| opt_cpr:", sum(1 for e in t30 if e.get("opt_cpr") is not None))
for e in (r.get("flow_leaders",[]) or [])[:4]: print("   ",e.get("ticker"),e.get("flow_signal"),"cpr",e.get("opt_cpr"),"surge",e.get("opt_call_surge"))
wl=[e for e in t30 if e.get("watchlist_chg_pct") is not None]
print("  watchlist_chg coverage:", len(wl),"/",len(t30),"(needs 2+ days, sparse at day-1)")
