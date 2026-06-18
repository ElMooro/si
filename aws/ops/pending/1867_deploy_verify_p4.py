import boto3, json, zipfile, io, glob, time
from botocore.exceptions import ClientError
lam=boto3.client("lambda","us-east-1"); s3=boto3.client("s3","us-east-1")
FN="justhodl-deal-scanner"
src=open(glob.glob("**/justhodl-deal-scanner/source/lambda_function.py",recursive=True)[0]).read()
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z: z.writestr("lambda_function.py",src)
code=buf.getvalue()
def wait():
    for _ in range(90):
        st=lam.get_function_configuration(FunctionName=FN)
        if st.get("State")=="Active" and st.get("LastUpdateStatus")!="InProgress": return st
        time.sleep(3)
    return st
wait()
for _ in range(24):  # deploy MY code, retry through any deploy-lambdas race
    try: lam.update_function_code(FunctionName=FN,ZipFile=code); break
    except ClientError as e:
        if "ResourceConflict" in str(e): time.sleep(5); continue
        raise
st=wait(); print("deployed; last_modified=%s codesize=%s"%(st.get("LastModified"),st.get("CodeSize")))
r=lam.invoke(FunctionName=FN,InvocationType="RequestResponse"); print("INVOKE:",r["Payload"].read().decode()[:200])
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/deal-scanner.json")["Body"].read())
sm=d["summary"]
print("SUMMARY: items=%s deals=%s green=%s ai=%s ai_mega=%s"%(sm["n_prs_scanned"],sm["n_deals"],sm.get("n_green"),sm.get("n_ai"),sm.get("n_ai_mega")))
print("opinion-noise (NBIS/GSK present?):", sorted({"NBIS","GSK"} & {x["symbol"] for x in d["deals"]}) or "NONE (clean)")
print("\n=== ALL DEALS ===")
for x in d["deals"]:
    tags="".join([t for t in [("AI" if x["ai_relevant"] else ""),("$B" if x["is_billion"] else ""),({"green":"G","yellow":"Y"}.get(x["highlight"],""))] if t]) or "-"
    print("  %-7s %-6s mc=%-7s val=%-13s [%-4s] %s"%(x["symbol"],x["cap_bucket"] or "?",
          ("%.0f%%"%x["vs_market_cap_pct"]) if x["vs_market_cap_pct"] is not None else "n/a",x["deal_value_str"] or "—",tags,x["title"][:46]))
print("\nAI-RELEVANT:",[(x["symbol"],x["deal_value_str"],x["ai_keywords"][:2]) for x in sm.get("ai_deals",[])] or "none")
print("AI MEGA:",[(x["symbol"],x["deal_value_str"]) for x in sm.get("ai_megadeals",[])] or "none in window")
