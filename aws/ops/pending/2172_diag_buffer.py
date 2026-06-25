import boto3, json
s3=boto3.client("s3","us-east-1")
buf=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/_ma200/crypto-closes.json")["Body"].read())
dates=buf["dates"]
print("len:",len(dates))
print("min:",min(dates),"max:",max(dates))
print("first5:",dates[:5])
print("last5:",dates[-5:])
print("is_sorted_ascending:",dates==sorted(dates))
print("has_duplicates:",len(dates)!=len(set(dates)))
# how many in 2026 vs 2025
y26=sum(1 for d in dates if d>="2026-01-01"); y25=len(dates)-y26
print("2026 dates:",y26,"| 2025 dates:",y25)
# sample series length vs dates
btc=buf["series"].get("BTC")
print("BTC series len:",len(btc) if btc else None,"vs dates len:",len(dates))
print("BTC non-null:",sum(1 for x in btc if x is not None) if btc else None)
print("DONE 2172")
