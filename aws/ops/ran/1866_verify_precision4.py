import boto3, json, time
lam=boto3.client("lambda","us-east-1"); s3=boto3.client("s3","us-east-1")
FN="justhodl-deal-scanner"
for _ in range(70):  # wait for deploy-lambdas (source commit) to finish, then invoke (no code update here)
    st=lam.get_function_configuration(FunctionName=FN)
    if st.get("State")=="Active" and st.get("LastUpdateStatus")!="InProgress": break
    time.sleep(3)
print("active; last_modified=%s"%st.get("LastModified"))
r=lam.invoke(FunctionName=FN,InvocationType="RequestResponse"); print("INVOKE:",r["Payload"].read().decode()[:200])
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/deal-scanner.json")["Body"].read())
sm=d["summary"]
print("SUMMARY: items=%s deals=%s green=%s ai=%s ai_mega=%s"%(sm["n_prs_scanned"],sm["n_deals"],sm.get("n_green"),sm.get("n_ai"),sm.get("n_ai_mega")))
print("opinion-noise check (NBIS/GSK present?):", sorted({"NBIS","GSK"} & {x["symbol"] for x in d["deals"]}) or "NONE (clean)")
print("\n=== ALL DEALS ===")
for x in d["deals"]:
    tags="".join([t for t in [("AI" if x["ai_relevant"] else ""),("$B" if x["is_billion"] else ""),({"green":"G","yellow":"Y"}.get(x["highlight"],""))] if t]) or "-"
    print("  %-7s %-6s mc=%-7s val=%-13s [%s] %s"%(x["symbol"],x["cap_bucket"] or "?",
          ("%.0f%%"%x["vs_market_cap_pct"]) if x["vs_market_cap_pct"] is not None else "n/a",x["deal_value_str"],tags,x["title"][:48]))
print("\nAI MEGA:",[(x["symbol"],x["deal_value_str"]) for x in sm.get("ai_megadeals",[])] or "none in window")
