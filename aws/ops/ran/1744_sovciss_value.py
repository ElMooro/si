import json, boto3, re
s3=boto3.client("s3",region_name="us-east-1")
src=open("aws/lambdas/ecb-auto-updater/source/lambda_function.py").read()
m=re.search(r"BUCKET\s*=\s*['\"]([^'\"]+)", src)
bucket=m.group(1) if m else "justhodl-dashboard-live"
print("engine bucket:", bucket)
d=json.loads(s3.get_object(Bucket=bucket,Key="ecb_data.json")["Body"].read())
sov=d.get("ECB.SOVCISS",{})
print(f"\nECB.SOVCISS -> value={sov.get('value')} date={sov.get('date')} freq={sov.get('frequency')} obs={sov.get('observations')}")
print(f"  name: {sov.get('name')}")
print(f"  last_updated: {sov.get('last_updated')}")
print("\nCISS subindices refreshed today:")
import datetime; today=datetime.date.today().isoformat()
for k,v in d.items():
    if isinstance(v,dict) and "CISS" in k.upper() and "SOV" not in k.upper() and isinstance(v.get('value'),(int,float)):
        lu=str(v.get('last_updated',''))[:10]; fresh="✓today" if lu>="2026-06-16" else lu
        print(f"  {k:26} {round(v['value'],4)} @ {v.get('date')}  upd={fresh}")
