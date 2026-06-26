import boto3, json
s3=boto3.client("s3","us-east-1")
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/tic-flows.json")["Body"].read())
print("regime:", d.get("regime"), "| composite_tic_stress:", d.get("composite_tic_stress"))
print("total_foreign_holdings:", json.dumps(d.get("total_foreign_holdings"))[:300])
print("net_purchases:", json.dumps(d.get("net_purchases"))[:300])
print("top_reasons:", d.get("top_reasons"))
print("interpretation:", json.dumps(d.get("interpretation"))[:400])
ind=d.get("individual") or {}
print("individual keys:", list(ind.keys())[:12] if isinstance(ind,dict) else type(ind).__name__)
print("DONE 2218")
