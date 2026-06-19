import boto3, json, zipfile, io, glob, time, urllib.request
from botocore.exceptions import ClientError
lam=boto3.client("lambda","us-east-1"); s3=boto3.client("s3","us-east-1"); B="justhodl-dashboard-live"
FN="justhodl-ai-rerating-radar"; FMP="wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
# quick confirm: single analyst-estimates works sequentially
try:
    d=json.loads(urllib.request.urlopen("https://financialmodelingprep.com/stable/analyst-estimates?symbol=MU&limit=3&apikey=%s"%FMP,timeout=15).read())
    print("single analyst-estimates(MU): %s n=%s"%(type(d).__name__, len(d) if isinstance(d,list) else d))
except Exception as e: print("single est err:",str(e)[:80])
src=open(glob.glob("**/justhodl-ai-rerating-radar/source/lambda_function.py",recursive=True)[0]).read()
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
for _ in range(24):
    try: lam.update_function_code(FunctionName=FN,ZipFile=code); break
    except ClientError as e:
        if "ResourceConflict" in str(e): time.sleep(5); continue
        raise
wait(); print("redeployed (throttled)")
r=lam.invoke(FunctionName=FN,InvocationType="RequestResponse"); print("INVOKE:",r["Payload"].read().decode()[:200])
d=json.loads(s3.get_object(Bucket=B,Key="data/ai-rerating-radar.json")["Body"].read()); sm=d["summary"]; reg=d["regression"]
print("REGRESSION: EV/Sales = %.2f + %.3f*growth (n=%s) elapsed=%s"%(reg["intercept"] or 0,reg["slope_evsales_per_growth"] or 0,reg["n_points"],d.get("elapsed_s")))
print("priced=%s candidates=%s small_mid=%s"%(sm["n_priced"],sm["n_candidates"],sm["n_small_mid_candidates"]))
print("\n=== TOP RE-RATING SETUPS ===")
for x in sm.get("top_setups",[])[:12]:
    print("  %-6s %-6s %-9s fwdG=%-5s%% EV/S=%-5s impl=%-5s disc=%-6s lag=%-7s %s%s"%(x["symbol"],x["cap_bucket"],(x["layer"] or "")[:9],x["fwd_growth_pct"],x["ev_sales"],x["ev_sales_implied"],("%s%%"%x["discount_to_implied_pct"]) if x["discount_to_implied_pct"] is not None else "-",("%spp"%x["laggard_gap_pp"]) if x["laggard_gap_pp"] is not None else "-","AC" if x["accelerating"] else "","BN" if x["bottleneck"] else ""))
print("\n=== DEEPEST DISCOUNTS (most underpriced for growth) ===")
for x in sm.get("deepest_discounts",[])[:8]:
    print("  %-6s %-6s %s"%(x["symbol"],x["cap_bucket"],x["why"][:96]))
