import json, boto3
lam=boto3.client("lambda",region_name="us-east-1"); s3=boto3.client("s3",region_name="us-east-1")
B="justhodl-dashboard-live"
r=lam.invoke(FunctionName="justhodl-eurodollar-plumbing",InvocationType="RequestResponse")
print("invoke:",r["Payload"].read().decode()[:120])
d=json.loads(s3.get_object(Bucket=B,Key="data/eurodollar-plumbing.json")["Body"].read())
print("health=%s verdict=%s composite_score=%s"%(d.get("plumbing_health"),d.get("verdict"),d.get("composite_score")))
hubs=((d.get("layers") or {}).get("hubs") or {}).get("metrics") or []
print("country-hub metrics:")
for m in hubs:
    print("  [%s] %s = %s%s — %s"%(m.get("status"),m.get("label"),m.get("value"),m.get("unit",""),(m.get("detail") or "")[:60]))
cnh=[m for m in hubs if m.get("id") in ("cnh_cny","cnh")]
print("CNH escape-valve present:", bool(cnh))
