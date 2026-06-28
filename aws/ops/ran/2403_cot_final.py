import boto3, json, time
lam=boto3.client("lambda","us-east-1"); s3=boto3.client("s3","us-east-1")
c=lam.get_function_configuration(FunctionName="justhodl-crypto-cot")
print("deployed LastModified:",c["LastModified"])
lam.invoke(FunctionName="justhodl-crypto-cot",InvocationType="RequestResponse",Payload=b"{}")
time.sleep(3)
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/crypto-cot.json")["Body"].read())
for a in ("btc","eth"):
    am=(d.get(a) or {}).get("asset_mgr") or {}; lf=(d.get(a) or {}).get("lev_funds") or {}
    print(f"  {a.upper()}: AsstMgr {am.get('read')} net {am.get('net')} ({am.get('net_pctile_3y')}th, extreme={am.get('extreme')}) | LevFund {lf.get('read')} net {lf.get('net')} ({lf.get('net_pctile_3y')}th, extreme={lf.get('extreme')})")
print("DONE 2403")
