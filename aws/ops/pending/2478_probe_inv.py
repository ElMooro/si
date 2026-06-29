import boto3, json
s3=boto3.client("s3","us-east-1")
try:
    d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/inventory-drawdown.json")["Body"].read())
    print("gen:",d.get("generated_at"),"counts:",json.dumps(d.get("counts")))
    print("sector sample:",json.dumps((d.get("sector_drawdown") or [])[:3]))
    print("boom_setup sample:",json.dumps((d.get("boom_setups") or [])[:3]))
except Exception as e: print("ERR",str(e)[:80])
print("DONE 2478")
