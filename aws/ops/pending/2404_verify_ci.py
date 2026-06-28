import boto3, json, time
lam=boto3.client("lambda","us-east-1"); s3=boto3.client("s3","us-east-1")
lam.invoke(FunctionName="justhodl-crypto-intel",InvocationType="Event",Payload=b"{}")
print("invoked async, waiting 75s for heavy run...")
time.sleep(75)
# crypto-intel.json is at bucket ROOT
key=None
for k in ["crypto-intel.json","data/crypto-intel.json"]:
    try:
        d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key=k)["Body"].read()); key=k; break
    except Exception: pass
if not key: print("could not find crypto-intel.json"); print("DONE 2404"); raise SystemExit
print("key:",key,"| version:",d.get("version"),"| generated:",d.get("generated_at"))
xf=d.get("exchange_flows") or {}; ct=d.get("cot") or {}; cp=d.get("coinbase_premium") or {}; sp=d.get("stablecoin_peg") or {}; onr=d.get("onchain_ratios") or {}
print("exchange_flows:",xf.get("btc_regime"),"30d",xf.get("btc_netflow_30d"),"pctile",xf.get("btc_netflow_30d_pctile"),"| ES",xf.get("event_study"))
print("cot:",ct.get("btc_asset_mgr_read"),"net",ct.get("btc_asset_mgr_net"),"("+str(ct.get("btc_asset_mgr_pctile"))+"th,",ct.get("btc_asset_mgr_extreme"),") | LevFund",ct.get("btc_lev_funds_read"))
print("coinbase_premium:",cp.get("btc_pct"),"%",cp.get("btc_read"))
print("stablecoin_peg:",sp.get("status"),"worst",sp.get("worst_coin"),sp.get("worst_depeg_pct"))
print("onchain realized_price:",onr.get("realized_price"),"| price_vs_realized",onr.get("price_vs_realized_pct"),"| NUPL",onr.get("nupl"),onr.get("nupl_zone"))
print("DONE 2404")
