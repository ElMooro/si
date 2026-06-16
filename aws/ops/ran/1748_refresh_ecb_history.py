import json, time, boto3
s3=boto3.client("s3",region_name="us-east-1")
lam=boto3.client("lambda",region_name="us-east-1")
lam.invoke(FunctionName="justhodl-ecb-history",InvocationType="Event")
print("ecb-history invoked (async, full-history pull); polling sub-index files...")
time.sleep(75)
for fid in ["ciss_ea","ciss_fi","ciss_bo","ciss_eq","ciss_mm","ciss_fx"]:
    try:
        c=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key=f"data/ecb-hist/{fid}.json")["Body"].read())
        pts=c.get("points",[]); print(f"  {fid:10} {len(pts):5} pts  latest={pts[-1] if pts else '—'}")
    except Exception as e: print(f"  {fid}: {str(e)[:50]}")
