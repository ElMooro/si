import boto3, json, time
lam=boto3.client("lambda","us-east-1"); s3=boto3.client("s3","us-east-1")
def doc(): return json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/cycle-clock.json")["Body"].read())
b4=doc().get("generated_at")
lam.invoke(FunctionName="justhodl-cycle-clock",InvocationType="Event",Payload=b"{}")
d=None
for i in range(18):
    time.sleep(12); cur=doc()
    if cur.get("generated_at")!=b4 and cur.get("version")=="3.1": d=cur; print(f"wrote v3.1 dur {cur.get('duration_s')}s"); break
if not d: print("NO v3.1:",doc().get("version")); d=doc()
cr=d.get("crypto") or {}
print("\n=== CRYPTO BLOCK ===")
print("  DVOL btc:",cr.get("dvol_btc"),cr.get("dvol_btc_regime"),cr.get("dvol_btc_pctile"),"pctile",cr.get("dvol_btc_trend"),"| eth:",cr.get("dvol_eth"))
print("  funding regime:",cr.get("funding_regime"),"| signal:",cr.get("funding_signal"),"| composite:",cr.get("funding_composite"))
print("  squeeze candidates:",[(c["coin"],c["regime"],c["z"]) for c in (cr.get("squeeze_candidates") or [])][:5])
print("  liquidity:",cr.get("liquidity_regime"),"| SSR:",cr.get("ssr"),"@",cr.get("ssr_pctile"),"pctile | F&G:",cr.get("fear_greed"),cr.get("fear_greed_class"))
sy=d.get("synthesis") or {}
print("\n=== SYNTHESIS (crypto contributors) ===")
print("  posture:",sy.get("posture"),sy.get("score"),"|",sy.get("n_risk_off"),"off vs",sy.get("n_risk_on"),"on")
print("  bullish:",[c["label"] for c in (sy.get("bullish_drivers") or [])])
print("  bearish:",[c["label"] for c in (sy.get("bearish_drivers") or [])])
print("  rates DVOL:",(d.get("rates_fed_vol") or {}).get("crypto_dvol"),(d.get("rates_fed_vol") or {}).get("crypto_dvol_regime"))
print("  crypto divergence present:", any("CRYPTO" in x for x in (d.get("divergences") or [])))
print("DONE 2364")
