import boto3, json, time
lam=boto3.client("lambda","us-east-1"); s3=boto3.client("s3","us-east-1")
c=lam.get_function_configuration(FunctionName="justhodl-crypto-cot")
print("deployed LastModified:",c["LastModified"])
# re-invoke after confirming
lam.invoke(FunctionName="justhodl-crypto-cot",InvocationType="RequestResponse",Payload=b"{}")
time.sleep(3)
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/crypto-cot.json")["Body"].read())
am=(d.get("btc") or {}).get("asset_mgr") or {}
print("extreme label now:",am.get("extreme"))
print("DONE 2402")
