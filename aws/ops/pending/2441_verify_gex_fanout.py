import boto3, json, time
lam=boto3.client("lambda","us-east-1"); s3=boto3.client("s3","us-east-1")
def rd(k):
    try: return json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key=k)["Body"].read())
    except Exception: return {}
# re-run gex (max-pain fix) and check near-term
lam.invoke(FunctionName="justhodl-crypto-gex",InvocationType="RequestResponse",Payload=b"{}"); time.sleep(3)
g=rd("data/crypto-gex.json").get("btc") or {}
print("GEX btc: %s | net $%.0fM | flip $%s | call wall $%s | put wall $%s | MAX-PAIN $%s (%s) <- near-term now"%(
    g.get("regime"),(g.get("net_gex_usd") or 0)/1e6,g.get("gamma_flip"),g.get("call_wall"),g.get("put_wall"),g.get("max_pain"),g.get("max_pain_exp")))
# crypto-intel (root key)
lam.invoke(FunctionName="justhodl-crypto-intel",InvocationType="Event",Payload=b"{}")
print("crypto-intel async; wait 150s..."); time.sleep(150)
ci=rd("crypto-intel.json"); gx=ci.get("gex") or {}
print("crypto-intel v%s gex: %s | flip $%s | call/put wall $%s/$%s | max-pain $%s"%(
    ci.get("version"),gx.get("btc_regime"),gx.get("btc_gamma_flip"),gx.get("btc_call_wall"),gx.get("btc_put_wall"),gx.get("btc_max_pain")))
# cycle-clock
lam.invoke(FunctionName="justhodl-cycle-clock",InvocationType="RequestResponse",Payload=b"{}"); time.sleep(3)
cc=rd("data/cycle-clock.json"); cr=cc.get("crypto") or {}; syn=cc.get("synthesis") or {}
print("cycle-clock crypto: gex_regime %s | flip $%s | spot_vs_flip %s | call/put wall $%s/$%s"%(
    cr.get("gex_btc_regime"),cr.get("gex_btc_flip"),cr.get("gex_btc_spot_vs_flip"),cr.get("gex_btc_call_wall"),cr.get("gex_btc_put_wall")))
print("  synthesis:",syn.get("posture"),syn.get("score"),"n_risk_off",syn.get("n_risk_off"))
# confluence
lam.invoke(FunctionName="justhodl-crypto-confluence",InvocationType="RequestResponse",Payload=b"{}"); time.sleep(2)
mc=rd("data/crypto-confluence.json").get("market_context") or {}
print("confluence: regime",mc.get("regime"),"tilt",mc.get("tilt"),"| gex_regime",mc.get("gex_btc_regime"),"max-pain $%s"%mc.get("gex_btc_max_pain"))
print("DONE 2441")
