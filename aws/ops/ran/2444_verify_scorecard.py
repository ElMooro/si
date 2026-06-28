import boto3, json, time
lam=boto3.client("lambda","us-east-1"); s3=boto3.client("s3","us-east-1")
# confirm ANTHROPIC key inherited
env=lam.get_function_configuration(FunctionName="justhodl-signal-backtest").get("Environment",{}).get("Variables",{})
print("has ANTHROPIC_API_KEY:",bool(env.get("ANTHROPIC_API_KEY")),"| has FMP_KEY:",bool(env.get("FMP_KEY")))
lam.invoke(FunctionName="justhodl-signal-backtest",InvocationType="Event",Payload=b"{}")
print("invoked async; waiting 180s (heavy: snapshots+quotes+Claude)..."); time.sleep(180)
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/signal-backtest.json")["Body"].read())
print("version:",d.get("version"),"| n_obs:",d.get("n_observations"),"| maturity:",d.get("maturity"))
bvs=d.get("by_verdict_stocks") or {}
print("\nby_verdict_stocks present:",list(bvs.keys()))
for v in ["STRONG OPPORTUNITY","HIGH RISK","EXPENSIVE"]:
    st=bvs.get(v)
    if not st: continue
    print("\n%s (%d tickers):"%(v,st["n_tickers"]))
    print("  WINNERS:",[(x["ticker"],"%s%%"%x["ret"],"n%d"%x["n"],"w%d%%"%x["win_rate"]) for x in st["leaders"][:6]])
    print("  LOSERS :",[(x["ticker"],"%s%%"%x["ret"],"n%d"%x["n"],"w%d%%"%x["win_rate"]) for x in st["laggards"][:6]])
ai=d.get("ai_analysis") or {}
print("\n=== AI ANALYSIS ===")
if ai.get("_skip") or ai.get("_error"): print("AI:",ai)
else:
    print("headline:",ai.get("headline"))
    print("diagnosis:",ai.get("diagnosis"))
    print("patterns:",ai.get("patterns"))
    print("recommendations:",ai.get("recommendations"))
    vn=ai.get("verdict_notes") or {}
    print("verdict_notes keys:",list(vn.keys())[:6])
print("DONE 2444")
