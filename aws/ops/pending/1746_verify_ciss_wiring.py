import json, boto3
from botocore.config import Config
s3=boto3.client("s3",region_name="us-east-1")
lam=boto3.client("lambda",region_name="us-east-1",config=Config(read_timeout=120,retries={"max_attempts":0}))
try:
    c=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/ecb-hist/ciss_ea.json")["Body"].read())
    pts=c.get("points",[]); print(f"ciss_ea.json: {len(pts)} points, latest={pts[-1] if pts else '—'}")
except Exception as e: print("ciss_ea read err:", str(e)[:80])
r=lam.invoke(FunctionName="justhodl-crisis-composite",InvocationType="RequestResponse")
print("invoke status:", r.get("StatusCode"))
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/crisis-composite.json")["Body"].read())
comps=d.get("components",[])
score=d.get("crisis_score") or d.get("master") or d.get("master_score") or d.get("score")
print(f"master crisis score={score} | components={len(comps)}")
for c in comps:
    if "CISS" in str(c.get("label","")):
        print("  CISS component ->", json.dumps(c))
