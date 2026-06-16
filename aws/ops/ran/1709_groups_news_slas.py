import json, boto3
s3=boto3.client("s3",region_name="us-east-1")
K="data/_freshness-manifest.json"
m=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key=K)["Body"].read())
ko=m.setdefault("key_overrides",{})
ko["data/finviz-groups.json"]=14
ko["data/finviz-news.json"]=8
s3.put_object(Bucket="justhodl-dashboard-live",Key=K,Body=json.dumps(m,indent=2).encode(),ContentType="application/json")
print("added groups=14h, news=8h | total overrides:", len(ko))
print("all finviz overrides:", {k:v for k,v in ko.items() if "finviz" in k})
