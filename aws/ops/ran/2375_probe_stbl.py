import boto3, json
s3=boto3.client("s3","us-east-1")
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/crypto-liquidity.json")["Body"].read())
ss=d.get("stablecoin_supply") or {}
print("stablecoin_supply:",json.dumps(ss)[:400])
# also check for any 30d momentum field elsewhere
for k in d.keys():
    if "supply" in k.lower() or "stbl" in k.lower(): print("key:",k,"->",json.dumps(d[k])[:200])
print("DONE 2375")
