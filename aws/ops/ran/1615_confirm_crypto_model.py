"""Read crypto-intel.json from S3 and surface any 'model' field to confirm GLM."""
import json
import boto3

s3 = boto3.client("s3", region_name="us-east-1")
obj = json.loads(s3.get_object(Bucket="justhodl-dashboard-live", Key="crypto-intel.json")["Body"].read())


def find_models(o, hits):
    if isinstance(o, dict):
        for k, v in o.items():
            if k == "model" and isinstance(v, str):
                hits.append(v)
            else:
                find_models(v, hits)
    elif isinstance(o, list):
        for x in o:
            find_models(x, hits)


hits = []
find_models(obj, hits)
print("model fields found in crypto-intel.json:", hits)
print("generated_at:", obj.get("generated_at") or obj.get("time"))
if any("glm" in m for m in hits):
    print("\n✅ CONFIRMED: crypto-intel AI narrative served by GLM-5.1")
elif hits:
    print(f"\nℹ️  served by: {hits} (GLM may not have run yet / fell back)")
else:
    print("\n(no model field present in output)")
