import boto3, json, time
lam=boto3.client("lambda","us-east-1"); s3=boto3.client("s3","us-east-1")
FN="justhodl-deal-scanner"
for _ in range(60):  # wait out any in-progress deploy-lambdas update
    st=lam.get_function_configuration(FunctionName=FN)
    if st.get("State")=="Active" and st.get("LastUpdateStatus")!="InProgress": break
    time.sleep(3)
print("fn active; last_modified=%s"%st.get("LastModified"))
r=lam.invoke(FunctionName=FN,InvocationType="RequestResponse"); print("INVOKE:",r["Payload"].read().decode()[:240])
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/deal-scanner.json")["Body"].read())
sm=d["summary"]
print("SUMMARY: items=%s deals=%s green=%s ai=%s ai_mega=%s elapsed=%s"%(sm["n_prs_scanned"],sm["n_deals"],sm.get("n_green"),sm.get("n_ai"),sm.get("n_ai_mega"),d.get("elapsed_s")))
print("\n=== AI MEGA-DEALS (AI + big vs mkt-cap / billions) ===")
for x in sm.get("ai_megadeals",[]) or []:
    print("  %-7s %-6s val=%-13s mc=%-7s bil=%s kw=%s\n        %s"%(x["symbol"],x["cap_bucket"] or "?",x["deal_value_str"],
          ("%.0f%%"%x["vs_market_cap_pct"]) if x["vs_market_cap_pct"] is not None else "n/a",x["is_billion"],x["ai_keywords"][:3],x["title"][:74]))
if not sm.get("ai_megadeals"): print("  (none in current rolling window)")
print("\n=== AI-RELEVANT (top 10) ===")
for x in (sm.get("ai_deals",[]) or [])[:10]:
    print("  %-7s %-6s val=%-13s mc=%-7s kw=%s | %s"%(x["symbol"],x["cap_bucket"] or "?",x["deal_value_str"],
          ("%.0f%%"%x["vs_market_cap_pct"]) if x["vs_market_cap_pct"] is not None else "n/a",x["ai_keywords"][:2],x["title"][:46]))
if not sm.get("ai_deals"): print("  (none)")
print("\n=== TOP OVERALL ===")
for x in d["deals"][:8]:
    tags="".join([t for t in [("AI" if x["ai_relevant"] else ""),("$B" if x["is_billion"] else ""),({"green":"G","yellow":"Y"}.get(x["highlight"],""))] if t]) or "-"
    print("  %-7s sc=%-6s [%s] %s"%(x["symbol"],x["score"],tags,x["title"][:52]))
