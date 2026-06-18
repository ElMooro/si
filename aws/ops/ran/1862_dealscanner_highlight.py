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
print("SUMMARY: deals=%s green=%s yellow=%s small=%s"%(sm["n_deals"],sm.get("n_green"),sm.get("n_yellow"),sm["n_small_cap"]))
print("\n=== GREEN HIGHLIGHTS (big vs market cap / revenue) ===")
for x in sm.get("green_highlights",[]):
    print("  %-7s %-7s val=%-12s rev=%-7s mc=%-7s | %s"%(x["symbol"],x["cap_bucket"] or "?",x["deal_value_str"],
          ("%.0f%%"%x["materiality_pct"]) if x["materiality_pct"] not in (None,9999.0) else ("PRE-REV" if x["materiality_pct"]==9999.0 else "n/a"),
          ("%.1f%%"%x["vs_market_cap_pct"]) if x["vs_market_cap_pct"] is not None else "n/a", x["title"][:52]))
print("\n=== YELLOW ===")
for x in sm.get("yellow_highlights",[]):
    print("  %-7s %-7s val=%-12s rev=%-7s mc=%-7s | %s"%(x["symbol"],x["cap_bucket"] or "?",x["deal_value_str"],
          ("%.0f%%"%x["materiality_pct"]) if x["materiality_pct"] not in (None,9999.0) else "n/a",
          ("%.1f%%"%x["vs_market_cap_pct"]) if x["vs_market_cap_pct"] is not None else "n/a", x["title"][:52]))
print("\n=== TOP (green floats up via focus boost) ===")
for x in d["deals"][:8]:
    print("  [%s] %-7s sc=%-6s %s"%((x["highlight"] or "----")[:5].ljust(5),x["symbol"],x["score"],x["title"][:50]))
