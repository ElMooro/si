import json, boto3
lam=boto3.client("lambda",region_name="us-east-1"); s3=boto3.client("s3",region_name="us-east-1")
B="justhodl-dashboard-live"
r=lam.invoke(FunctionName="justhodl-eurodollar-plumbing",InvocationType="RequestResponse")
print("invoke:",r["Payload"].read().decode()[:120])
d=json.loads(s3.get_object(Bucket=B,Key="data/eurodollar-plumbing.json")["Body"].read())
print("health=%s verdict=%s composite_score=%s reds=%s yellows=%s"%(d.get("plumbing_health"),d.get("verdict"),d.get("composite_score"),d.get("red_flags"),d.get("yellow_flags")))
L=d.get("layers") or {}
want={"ofr_fsi_funding":"bank_funding","fima_repo":"backstops","fed_repo_srf":"backstops","net_due_foreign":"fx"}
for mid,lk in want.items():
    ms=(L.get(lk) or {}).get("metrics") or []
    hit=[m for m in ms if m.get("id")==mid]
    if hit: m=hit[0]; print("  ✓ [%s] %s = %s%s"%(m.get("status"),m.get("label"),m.get("value"),m.get("unit","")))
    else: print("  ✗ MISSING %s in %s"%(mid,lk))
