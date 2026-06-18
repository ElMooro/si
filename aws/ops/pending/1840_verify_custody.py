import json, boto3
lam=boto3.client("lambda",region_name="us-east-1"); s3=boto3.client("s3",region_name="us-east-1")
B="justhodl-dashboard-live"
r=lam.invoke(FunctionName="justhodl-eurodollar-plumbing",InvocationType="RequestResponse")
print("invoke:",r["Payload"].read().decode()[:120])
d=json.loads(s3.get_object(Bucket=B,Key="data/eurodollar-plumbing.json")["Body"].read())
print("health=%s verdict=%s composite_score=%s"%(d.get("plumbing_health"),d.get("verdict"),d.get("composite_score")))
fx=((d.get("layers") or {}).get("fx") or {}).get("metrics") or []
print("FX/offshore-strain metrics:")
for m in fx:
    print("  [%s] %s = %s%s — %s"%(m.get("status"),m.get("label"),m.get("value"),m.get("unit",""),(m.get("detail") or "")[:55]))
cu=[m for m in fx if m.get("id")=="foreign_custody"]
print("foreign-custody indicator present:", bool(cu), "->", (cu[0].get("value") if cu else "MISSING"))
