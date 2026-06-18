import boto3, json, zipfile, io, glob, time
lam=boto3.client("lambda","us-east-1"); s3=boto3.client("s3","us-east-1")
FN="justhodl-deal-scanner"
src=open(glob.glob("**/justhodl-deal-scanner/source/lambda_function.py",recursive=True)[0]).read()
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z: z.writestr("lambda_function.py",src)
lam.update_function_code(FunctionName=FN,ZipFile=buf.getvalue())
for _ in range(40):
    st=lam.get_function_configuration(FunctionName=FN)
    if st.get("State")=="Active" and st.get("LastUpdateStatus")!="InProgress": break
    time.sleep(2)
print("redeployed")
r=lam.invoke(FunctionName=FN,InvocationType="RequestResponse"); print("INVOKE:",r["Payload"].read().decode()[:240])
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/deal-scanner.json")["Body"].read())
sm=d["summary"]
print("SUMMARY: prs=%s deals=%s green=%s ai=%s ai_mega=%s elapsed=%s"%(sm["n_prs_scanned"],sm["n_deals"],sm.get("n_green"),sm.get("n_ai"),sm.get("n_ai_mega"),d.get("elapsed_s")))
print("\n=== AI MEGA-DEALS (AI + big vs market cap / billions) ===")
for x in sm.get("ai_megadeals",[]):
    print("  %-7s %-6s val=%-13s mc=%-8s bil=%s kw=%s\n        %s"%(x["symbol"],x["cap_bucket"] or "?",x["deal_value_str"],
          ("%.0f%%"%x["vs_market_cap_pct"]) if x["vs_market_cap_pct"] is not None else "n/a",x["is_billion"],x["ai_keywords"][:3],x["title"][:74]))
print("\n=== AI-RELEVANT DEALS (top 8) ===")
for x in sm.get("ai_deals",[])[:8]:
    print("  %-7s %-6s val=%-13s mc=%-8s kw=%s | %s"%(x["symbol"],x["cap_bucket"] or "?",x["deal_value_str"],
          ("%.0f%%"%x["vs_market_cap_pct"]) if x["vs_market_cap_pct"] is not None else "n/a",x["ai_keywords"][:2],x["title"][:50]))
print("\n=== TOP OVERALL (AI/billion floats up) ===")
for x in d["deals"][:8]:
    tags="".join([t for t in [("🤖" if x["ai_relevant"] else ""),("💰" if x["is_billion"] else ""),({"green":"🟢","yellow":"🟡"}.get(x["highlight"],""))] if t])
    print("  %-7s sc=%-6s %s %s"%(x["symbol"],x["score"],tags,x["title"][:52]))
