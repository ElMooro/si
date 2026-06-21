import boto3, json, time
lam=boto3.client("lambda","us-east-1"); s3=boto3.client("s3","us-east-1"); B="justhodl-dashboard-live"
for _ in range(20):
    c=lam.get_function(FunctionName="justhodl-engine-trust")["Configuration"]
    if c.get("LastUpdateStatus")=="Successful": break
    time.sleep(3)
print("invoke:",lam.invoke(FunctionName="justhodl-engine-trust",InvocationType="RequestResponse")["Payload"].read().decode()[:200])
time.sleep(2)
d=json.loads(s3.get_object(Bucket=B,Key="data/engine-trust.json")["Body"].read())
print("\ncurrent_regime in engine-trust:",d.get("current_regime"))
engs=d.get("engines") or d.get("registry") or []
if isinstance(engs,dict): engs=list(engs.values())
disp=[e for e in engs if e.get("regime_source")=="risk-map-dispersion"]
print("engines now conditioned on Risk Map dispersion:",len(disp),"/",len(engs))
print("examples (effective_trust reflects regime fit):")
for e in sorted(disp,key=lambda x:-(x.get("regime_factor") or 0))[:5]+sorted(disp,key=lambda x:(x.get("regime_factor") or 1))[:5]:
    print(f"   {e['signal_type']:<30} regime_factor {e.get('regime_factor')} eff_trust {e.get('effective_trust')} (n_regime {e.get('regime_n')})")
print("DONE 2066")
