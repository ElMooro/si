import json, boto3
s3=boto3.client("s3",region_name="us-east-1")
try:
    m=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/_freshness-manifest.json")["Body"].read())
    print("manifest keys:", list(m.keys()))
    print("key_overrides count:", len(m.get("key_overrides",{})))
    print("sample overrides:", json.dumps(dict(list(m.get("key_overrides",{}).items())[:6]),indent=0)[:400])
    print("has finviz overrides?:", [k for k in m.get("key_overrides",{}) if "finviz" in k])
except Exception as e: print("manifest err:",str(e)[:120])
