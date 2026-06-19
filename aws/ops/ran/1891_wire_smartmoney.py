import boto3, json, zipfile, io, glob, time
from botocore.exceptions import ClientError
lam=boto3.client("lambda","us-east-1"); s3=boto3.client("s3","us-east-1"); B="justhodl-dashboard-live"
def redeploy(fn, path):
    src=open(glob.glob("**/%s/source/lambda_function.py"%path,recursive=True)[0]).read()
    buf=io.BytesIO()
    with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z: z.writestr("lambda_function.py",src)
    for _ in range(60):
        st=lam.get_function_configuration(FunctionName=fn)
        if st.get("State")=="Active" and st.get("LastUpdateStatus")!="InProgress": break
        time.sleep(3)
    for _ in range(24):
        try: lam.update_function_code(FunctionName=fn,ZipFile=buf.getvalue()); break
        except ClientError as e:
            if "ResourceConflict" in str(e): time.sleep(5); continue
            raise
    for _ in range(60):
        st=lam.get_function_configuration(FunctionName=fn)
        if st.get("State")=="Active" and st.get("LastUpdateStatus")!="InProgress": break
        time.sleep(3)
for fn,p in [("justhodl-ai-rerating-radar","justhodl-ai-rerating-radar"),("justhodl-deal-scanner","justhodl-deal-scanner")]:
    redeploy(fn,p)
    r=lam.invoke(FunctionName=fn,InvocationType="RequestResponse"); print(fn,"INVOKE:",r["Payload"].read().decode()[:130])
time.sleep(2)
rr=json.loads(s3.get_object(Bucket=B,Key="data/ai-rerating-radar.json")["Body"].read())
sets=(rr.get("summary",{}) or {}).get("top_setups",[]) or []
print("\nRE-RATING universe=%s candidates=%s"%(rr.get("summary",{}).get("n_universe"),rr.get("summary",{}).get("n_candidates")))
backed=[r for r in sets if r.get("smart_money_backed")]
print("smart-money-backed setups:",[(r["symbol"],r.get("layer"),r.get("discount_to_implied_pct")) for r in backed][:10] or "none in top_setups")
mem=[r for r in sets if r.get("layer") in ("memory","miners_to_ai","neocloud")]
print("NEW-layer names now scored:",[(r["symbol"],r["layer"],r.get("discount_to_implied_pct")) for r in mem][:12] or "none surfaced as candidates")
dd=json.loads(s3.get_object(Bucket=B,Key="data/deal-scanner.json")["Body"].read())
db=[x["symbol"] for x in (dd.get("summary",{}) or {}).get("top_deals",[]) if x.get("smart_money_backed")]
print("\nDEAL-SCANNER smart-money-backed deals:",db[:10] or "none in current deals")
