import json, boto3
s3=boto3.client("s3",region_name="us-east-1"); K="data/_freshness-manifest.json"
m=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key=K)["Body"].read())
m.setdefault("key_overrides",{})["data/finviz-etf-flows.json"]=14
s3.put_object(Bucket="justhodl-dashboard-live",Key=K,Body=json.dumps(m,indent=2).encode(),ContentType="application/json")
print("etf-flows SLA=14h added")
