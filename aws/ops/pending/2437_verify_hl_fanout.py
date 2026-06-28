import boto3, json, time
lam=boto3.client("lambda","us-east-1"); s3=boto3.client("s3","us-east-1")
def rd(k):
    try: return json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key=k)["Body"].read())
    except Exception: return {}
lam.invoke(FunctionName="justhodl-crypto-intel",InvocationType="Event",Payload=b"{}")
print("crypto-intel async; wait 75s..."); time.sleep(75)
ci=rd("data/crypto-intel.json"); hl=ci.get("hyperliquid") or {}
print("crypto-intel v%s hyperliquid: OI $%.1fB | BTC funding %s%%/yr | regime %s | liq %s"%(
    ci.get("version"),(hl.get("total_oi_usd") or 0)/1e9,hl.get("btc_funding_ann_pct"),hl.get("leverage_regime"),hl.get("liq_pressure_proxy")))
lam.invoke(FunctionName="justhodl-cycle-clock",InvocationType="RequestResponse",Payload=b"{}");time.sleep(3)
cc=rd("data/cycle-clock.json"); cr=cc.get("crypto") or {}
print("cycle-clock crypto: hl_total_oi $%.1fB | hl_regime %s | hl_funding %s | hl_liq %s"%(
    (cr.get("hl_total_oi_usd") or 0)/1e9,cr.get("hl_leverage_regime"),cr.get("hl_btc_funding_ann_pct"),cr.get("hl_liq_pressure")))
syn=cc.get("synthesis") or {}
print("  synthesis:",syn.get("posture"),syn.get("score"),"| any perp contributor:",[x.get("label") for x in (syn.get("bearish_drivers") or []) if "perp" in (x.get("label") or "").lower()])
lam.invoke(FunctionName="justhodl-crypto-confluence",InvocationType="RequestResponse",Payload=b"{}");time.sleep(2)
mc=rd("data/crypto-confluence.json").get("market_context") or {}
print("confluence: regime",mc.get("regime"),"tilt",mc.get("tilt"),"| hl_regime",mc.get("hl_leverage_regime"),"| hl_oi $%.1fB"%((mc.get("hl_total_oi_usd") or 0)/1e9))
print("DONE 2437")
