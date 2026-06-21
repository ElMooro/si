import boto3, json, time
lam=boto3.client("lambda","us-east-1"); s3=boto3.client("s3","us-east-1"); B="justhodl-dashboard-live"
for _ in range(20):
    c=lam.get_function(FunctionName="justhodl-crypto-emergence")["Configuration"]
    if c.get("LastUpdateStatus")=="Successful": break
    time.sleep(3)
print("invoke:",lam.invoke(FunctionName="justhodl-crypto-emergence",InvocationType="RequestResponse")["Payload"].read().decode()[:160])
time.sleep(2)
d=json.loads(s3.get_object(Bucket=B,Key="data/crypto-emergence.json")["Body"].read())
print("\ncomplex:",d["complex_stage"])
print("read:",d["complex_read"])
print(f"\ncontext now populated → MVRV {d.get('mvrv')} | dump-risk {d.get('cycle_risk')} ({d.get('risk_level')}) | market funding ann {d.get('market_funding_annualized_pct')}%")
print("accumulation_context:",d.get("accumulation_context"))
print("\nper-coin funding sample:")
for o in d["coins"][:6]:
    print(f"  {o['name']:<12} {o['stage']:<11} funding {o.get('funding_annualized_pct')}% | {', '.join(o['signals'][:3])}")
print("DONE 2073")
