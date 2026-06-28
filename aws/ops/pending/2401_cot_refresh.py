import boto3, json, time
lam=boto3.client("lambda","us-east-1"); s3=boto3.client("s3","us-east-1")
lam.invoke(FunctionName="justhodl-crypto-cot",InvocationType="RequestResponse",Payload=b"{}")
time.sleep(3)
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/crypto-cot.json")["Body"].read())
am=(d.get("btc") or {}).get("asset_mgr") or {}
print("COT btc asset_mgr: net",am.get("net"),"| read",am.get("read"),"| pctile",am.get("net_pctile_3y"),"| extreme:",am.get("extreme"))
print("DONE 2401")
