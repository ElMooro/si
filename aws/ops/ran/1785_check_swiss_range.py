import json, boto3
s3=boto3.client("s3",region_name="us-east-1")
try:
    d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/switzerland.json")["Body"].read())
    print("generated_at:", d.get("generated_at","?")[:19], "| n_series:", d.get("n_series"))
    for s in d.get("series",[]):
        pts=s.get("points",[])
        print(f"  {s['id']:24} {s.get('start_date')}→{s.get('latest_date')}  n={len(pts)}  first_pt={pts[0] if pts else '-'}")
except Exception as e: print("ERR", e)
