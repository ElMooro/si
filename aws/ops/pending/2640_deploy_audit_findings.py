import boto3, io, zipfile, json, time
REGION="us-east-1"; FN="justhodl-liquidity-inflection"; SRC=f"aws/lambdas/{FN}/source/lambda_function.py"
lam=boto3.client("lambda",region_name=REGION); s3=boto3.client("s3",region_name=REGION)
def wait():
    for _ in range(30):
        c=lam.get_function(FunctionName=FN)["Configuration"]
        if c.get("State")=="Active" and c.get("LastUpdateStatus")=="Successful": return
        time.sleep(4)
wait()
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z: z.writestr("lambda_function.py",open(SRC,"rb").read())
for _ in range(6):
    try: lam.update_function_code(FunctionName=FN,ZipFile=buf.getvalue()); print("deployed"); break
    except lam.exceptions.ResourceConflictException: time.sleep(12); wait()
wait()
r=lam.invoke(FunctionName=FN,InvocationType="RequestResponse",Payload=b"{}")
print("INVOKE:",r.get("StatusCode"),r.get("FunctionError"),r["Payload"].read().decode()[:200])
time.sleep(2)
j=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/liquidity-inflection.json")["Body"].read())
print("\nversion:",j.get("version"))
sl=j.get("swap_lines") or {}
print("\nSRF/BACKSTOPS:",sl)
ta=j.get("treasury_auctions") or {}
print("\nTREASURY AUCTIONS:")
print(" regime",ta.get("regime"),"score",ta.get("composite_score"),"|",ta.get("interpretation"))
print(" issuance_anomaly:",ta.get("issuance_anomaly"))
print(" tenor_highlights:",ta.get("tenor_highlights"))
print(" near_term_calendar:",ta.get("near_term_calendar"))
sf=j.get("stablecoin_full") or {}
print("\nSTABLECOIN FULL:")
print(" state",sf.get("state"),"| strength",sf.get("signal_strength"),"| eff_z",sf.get("eff_z"))
print(" desc:",sf.get("state_description"))
print(" deltas 24h/7d/30d:",sf.get("delta_24h_pct"),sf.get("delta_7d_pct"),sf.get("delta_30d_pct"))
print(" total_usd_bn:",sf.get("total_usd_bn"))
print(" top_minters:",sf.get("top_minters_30d"))
comp=j.get("composite") or {}
print("\nCOMPOSITE now:",comp.get("liquidity_score"),comp.get("regime"),"| n_components",comp.get("n_components"))
for c in (comp.get("components") or []):
    if c["name"] in ("m2_growth","stablecoin_flow"): print("  NEW COMPONENT:",c)
ts=j.get("tensions") or {}
print("\nTENSIONS:",ts.get("level"),ts.get("count"))
for t in (ts.get("items") or []): print("  [",t["severity"],"]",t["signal"],":",t["note"][:100])
print("\nDONE 2640")
