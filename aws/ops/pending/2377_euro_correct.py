import boto3, json
s3=boto3.client("s3","us-east-1")
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/eurodollar-plumbing.json")["Body"].read())
print("plumbing_health:",d.get("plumbing_health"),"| verdict:",d.get("verdict"))
fx=(d.get("layers") or {}).get("fx") or {}
print("fx layer title:",fx.get("title"))
print("fx metric ids:",[m.get("id") for m in fx.get("metrics",[])])
for m in fx.get("metrics",[]):
    if m.get("id")=="stablecoin_offshore_usd":
        print("\n\u2713 STABLECOIN OFFSHORE-$ METRIC LIVE:")
        print("   ",m.get("label"),"=",m.get("value"),m.get("unit"),"["+str(m.get("status"))+"]")
        print("   ",(m.get("detail") or ""))
print("DONE 2377")
