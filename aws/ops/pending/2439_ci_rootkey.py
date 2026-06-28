import boto3, json, time
lam=boto3.client("lambda","us-east-1"); s3=boto3.client("s3","us-east-1")
# find the actual key crypto-intel writes
import re
src=open("aws/lambdas/justhodl-crypto-intel/source/lambda_function.py").read()
keys=sorted(set(re.findall(r"Key=['\"]([^'\"]*crypto-intel[^'\"]*\.json)['\"]", src)))
print("crypto-intel put keys in source:",keys)
lam.invoke(FunctionName="justhodl-crypto-intel",InvocationType="Event",Payload=b"{}")
print("waiting 150s..."); time.sleep(150)
ci=None
for k in keys+["crypto-intel.json","data/crypto-intel.json"]:
    try: ci=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key=k)["Body"].read()); print("FOUND at key:",k); break
    except Exception: pass
if ci:
    hl=ci.get("hyperliquid") or {}; ef=ci.get("etf_flows") or {}
    print("version:",ci.get("version"))
    print("hyperliquid: OI $%.1fB | funding %s%%/yr | regime %s | liq %s"%((hl.get("total_oi_usd") or 0)/1e9,hl.get("btc_funding_ann_pct"),hl.get("leverage_regime"),hl.get("liq_pressure_proxy")))
    print("etf_flows: BTC",ef.get("btc_regime"),"($%.1fB, %s ES)"%((ef.get("btc_flow_30d_usd") or 0)/1e9,ef.get("btc_event_study")))
else: print("crypto-intel.json not found at any key")
print("DONE 2439")
