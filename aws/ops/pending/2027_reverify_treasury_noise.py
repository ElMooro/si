"""ops 2027: re-invoke treasury-noise after 20Y exclusion; check calm-day percentile sanity."""
import boto3, json, time
lam=boto3.client("lambda","us-east-1"); s3=boto3.client("s3","us-east-1"); B="justhodl-dashboard-live"
# clear stale-scale history trail
try: s3.put_object(Bucket=B,Key="data/treasury-noise-history.json",Body=b"[]",ContentType="application/json")
except Exception as e: print("hist clear:",e)
r=lam.invoke(FunctionName="justhodl-treasury-noise",InvocationType="RequestResponse")
print("invoke:",r["StatusCode"],"|",r["Payload"].read().decode()[:400])
time.sleep(2)
d=json.loads(s3.get_object(Bucket=B,Key="data/treasury-noise.json")["Body"].read())
print("\nstress:",d.get("treasury_stress"),"regime:",d.get("regime"))
print("curve_noise:",d.get("curve_noise_bps"),"bps | PCTILE:",d.get("curve_noise_pctile"),"| z:",d.get("curve_noise_z"))
print("bill-SOFR:",d.get("bill_sofr_spread_bps"),"bps | funding stress pct:",d.get("funding_stress_pctile"))
print("highest noise days (stress eras should rank high):",[(x['date'],x['noise_bps']) for x in d.get("highest_noise_days",[])])
tr=d.get("noise_trail_60d",[])
if tr:
    vals=[x["noise_bps"] for x in tr]
    print(f"60d trail: min={min(vals)} max={max(vals)} mean={round(sum(vals)/len(vals),2)} latest={vals[-1]}")
print("DONE 2027")
