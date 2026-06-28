import boto3, json
s3=boto3.client("s3","us-east-1")
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/signal-backtest.json")["Body"].read())
print("top-level keys:",sorted(d.keys()))
print("n_observations:",d.get("n_observations"),"| maturity:",d.get("maturity"),"| generated:",d.get("generated_at"))
bvs=d.get("by_verdict_stocks") or {}
print("\nby_verdict_stocks present:",bool(bvs),"| verdicts:",list(bvs.keys())[:8])
if bvs:
    for v,st in list(bvs.items())[:2]:
        L=st.get("leaders") or []; G=st.get("laggards") or []
        print("  %s: %d leaders, %d laggards | n_tickers %s"%(v,len(L),len(G),st.get("n_tickers")))
        print("    top leaders:",[(x["ticker"],x["ret"],"n%s"%x["n"]) for x in L[:4]])
        print("    top laggards:",[(x["ticker"],x["ret"],"n%s"%x["n"]) for x in G[:4]])
ai=d.get("ai_analysis") or d.get("ai") or {}
print("\nai_analysis present:",bool(ai),"| keys:",list(ai.keys())[:10] if isinstance(ai,dict) else type(ai).__name__)
if isinstance(ai,dict):
    print("  headline:",str(ai.get("headline") or ai.get("diagnosis") or "")[:160])
print("DONE 2444")
