"""ops 2031: verify treasury-noise registers as a crisis-composite component."""
import boto3, json, time
lam=boto3.client("lambda","us-east-1"); s3=boto3.client("s3","us-east-1"); B="justhodl-dashboard-live"
r=lam.invoke(FunctionName="justhodl-crisis-composite",InvocationType="RequestResponse")
print("crisis-composite:",r["StatusCode"],"|",r["Payload"].read().decode()[:200])
time.sleep(2)
d=json.loads(s3.get_object(Bucket=B,Key="data/crisis-composite.json")["Body"].read())
comps=d.get("components") or d.get("component_detail") or []
tre=[c for c in comps if isinstance(c,dict) and "treasury" in json.dumps(c).lower()]
print("master:",d.get("master_score") or d.get("composite_score") or d.get("score"),"defcon/level:",d.get("defcon") or d.get("level") or d.get("regime"))
print("treasury-noise component present:",bool(tre),"->",tre[:1])
print("DONE 2031")
