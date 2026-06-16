import json, boto3
s3=boto3.client("s3",region_name="us-east-1"); K="data/_freshness-manifest.json"
m=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key=K)["Body"].read())
m.setdefault("key_overrides",{})["data/finviz-earnings-calendar.json"]=14
s3.put_object(Bucket="justhodl-dashboard-live",Key=K,Body=json.dumps(m,indent=2).encode(),ContentType="application/json")
print("earnings-calendar SLA=14h | total finviz overrides:", len([k for k in m["key_overrides"] if "finviz" in k]))
