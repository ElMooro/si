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
print("SUMMARY: prs=%s deals=%s sized=%s small=%s highmat=%s"%(sm["n_prs_scanned"],sm["n_deals"],sm["n_with_size"],sm["n_small_cap"],sm["n_high_materiality"]))
print("\nTOP DEALS (post-tighten):")
for x in sm["top_deals"][:10]:
    print("  %-6s %-7s mat=%s%% val=%s age=%sh score=%s"%(x["symbol"],x["cap_bucket"] or "?",x["materiality_pct"],x["deal_value_str"],x["age_h"],x["score"]))
    print("        \"%s\""%(x["title"][:92]))
