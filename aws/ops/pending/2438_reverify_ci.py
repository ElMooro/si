import boto3, json, time
lam=boto3.client("lambda","us-east-1"); s3=boto3.client("s3","us-east-1")
lam.invoke(FunctionName="justhodl-crypto-intel",InvocationType="Event",Payload=b"{}")
print("crypto-intel async; waiting 150s for heavy aggregate..."); time.sleep(150)
ci=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/crypto-intel.json")["Body"].read())
hl=ci.get("hyperliquid") or {}; ef=ci.get("etf_flows") or {}
print("crypto-intel version:",ci.get("version"))
print("  hyperliquid: OI $%.1fB | BTC funding %s%%/yr | regime %s | liq %s | chg24h %s"%(
    (hl.get("total_oi_usd") or 0)/1e9,hl.get("btc_funding_ann_pct"),hl.get("leverage_regime"),hl.get("liq_pressure_proxy"),hl.get("total_oi_chg_24h_pct")))
print("  hyperliquid top_oi:",[(x["coin"],"$%.1fB"%(x["oi_usd"]/1e9)) for x in (hl.get("top_oi") or [])[:3]])
print("  etf_flows still present: BTC",ef.get("btc_regime"),"($%.1fB)"%((ef.get("btc_flow_30d_usd") or 0)/1e9))
print("DONE 2438")
