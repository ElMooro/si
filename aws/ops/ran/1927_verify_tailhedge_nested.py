import boto3, json
s3=boto3.client("s3","us-east-1"); B="justhodl-dashboard-live"
d=json.loads(s3.get_object(Bucket=B,Key="data/tail-hedge.json")["Body"].read())
rg=d.get("regime") or {}
print("regime.stance:",rg.get("stance"))
print("regime.vol_cost_context:",json.dumps(rg.get("vol_cost_context") or {}))
print("cost_benefit:",json.dumps(d.get("cost_benefit") or {})[:260])
print("generated_at:",d.get("generated_at"))
