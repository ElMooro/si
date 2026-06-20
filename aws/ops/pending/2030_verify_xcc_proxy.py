"""ops 2030: verify crisis-plumbing's existing XCC basis proxy is live + fresh (so #4 isn't duplicated)."""
import boto3, json, time
lam=boto3.client("lambda","us-east-1"); s3=boto3.client("s3","us-east-1"); B="justhodl-dashboard-live"
# invoke to refresh
try:
    r=lam.invoke(FunctionName="justhodl-crisis-plumbing",InvocationType="RequestResponse")
    print("crisis-plumbing invoke:",r["StatusCode"],"|",r["Payload"].read().decode()[:200])
except Exception as e: print("invoke err (may be diff name):",str(e)[:120])
time.sleep(2)
try:
    d=json.loads(s3.get_object(Bucket=B,Key="data/crisis-plumbing.json")["Body"].read())
    print("\ngenerated_at:",d.get("generated_at") or d.get("as_of") or d.get("timestamp"))
    print("xcc_basis_3m_eur:",d.get("xcc_basis_3m_eur"))
    print("xcc_basis_3m_jpy:",d.get("xcc_basis_3m_jpy"))
    # surface any basis-related keys
    for k,v in d.items():
        if "basis" in k.lower() or "xcc" in k.lower(): print("  ",k,"=",v)
except Exception as e: print("read err:",str(e)[:150])
print("DONE 2030")
