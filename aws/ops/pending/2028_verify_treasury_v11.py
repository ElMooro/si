"""ops 2028: verify treasury-noise v1.1 funding-led composite reads calm today; spreads sane."""
import boto3, json, time
lam=boto3.client("lambda","us-east-1"); s3=boto3.client("s3","us-east-1"); B="justhodl-dashboard-live"
r=lam.invoke(FunctionName="justhodl-treasury-noise",InvocationType="RequestResponse")
print("invoke:",r["StatusCode"],"|",r["Payload"].read().decode()[:400])
time.sleep(2)
d=json.loads(s3.get_object(Bucket=B,Key="data/treasury-noise.json")["Body"].read())
print("\nversion:",d.get("version"),"treasury_stress:",d.get("treasury_stress"),"regime:",d.get("regime"),"(expect CALM/WATCH on a calm day)")
sp=d.get("spreads",{})
print("SPREADS:")
print("  bill-SOFR:",sp.get("bill_sofr_bps"),"bps  stress pctile:",sp.get("bill_sofr_stress_pctile"))
print("  SOFR-EFFR:",sp.get("sofr_effr_bps"),"bps  stress pctile:",sp.get("sofr_effr_stress_pctile"))
print("  CP-bill:",sp.get("cp_bill_bps"),"bps  stress pctile:",sp.get("cp_bill_stress_pctile"))
diag=d.get("curve_shape_diagnostic",{})
print("NS diagnostic (experimental, NOT in score):",diag.get("ns_residual_bps"),"bps pctile",diag.get("pctile"))
print("DONE 2028")
