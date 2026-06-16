import json, boto3
s3=boto3.client("s3",region_name="us-east-1")
K="data/_freshness-manifest.json"
m=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key=K)["Body"].read())
ko=m.setdefault("key_overrides",{})
# 2x/day feeds → 14h SLA; 3x/day signals → 10h SLA
slas={"data/finviz-universe.json":14,"data/finviz-short.json":14,
      "data/finviz-heatmap.json":14,"data/finviz-signals.json":10}
for k,v in slas.items(): ko[k]=v
s3.put_object(Bucket="justhodl-dashboard-live",Key=K,
              Body=json.dumps(m,indent=2).encode(),ContentType="application/json")
print("finviz SLAs written:", slas)
print("total key_overrides now:", len(ko))
print("finviz overrides present:", {k:ko[k] for k in slas})
