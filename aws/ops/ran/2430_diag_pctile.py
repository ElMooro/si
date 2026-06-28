import boto3, json, time
lam=boto3.client("lambda","us-east-1"); s3=boto3.client("s3","us-east-1")
# raw file value + type
ef=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/crypto-etf-flows.json")["Body"].read())
p=(ef.get("btc_etf") or {}).get("cum_30d_pctile")
print("crypto-etf-flows.json btc_etf.cum_30d_pctile =",repr(p),type(p).__name__)
print("  keys in btc_etf:",sorted((ef.get('btc_etf') or {}).keys()))
# fresh cycle-clock invoke
lam.invoke(FunctionName="justhodl-cycle-clock",InvocationType="RequestResponse",Payload=b"{}"); time.sleep(3)
cc=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/cycle-clock.json")["Body"].read())
cr=cc.get("crypto") or {}
print("cycle-clock crypto.etf_flow_btc_pctile =",repr(cr.get("etf_flow_btc_pctile")),"| regime",cr.get("etf_flow_btc_regime"))
syn=cc.get("synthesis") or {}
print("crypto bearish drivers:",[x.get("label") for x in (syn.get("bearish_drivers") or []) if "Crypto" in (x.get("label") or "")])
print("all bearish driver labels:",[x.get("label") for x in (syn.get("bearish_drivers") or [])])
print("DONE 2430")
