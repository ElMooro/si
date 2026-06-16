import json, boto3
s3=boto3.client("s3",region_name="us-east-1")
v=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/stock-valuations.json")["Body"].read())
sp=v.get("sp_table",[])
for f in ["ev_ebitda","ev_s","ev_sales"]:
    c=sum(1 for r in sp if r.get(f) is not None)
    print(f"  {f:10} {c}/{len(sp)}  sample={[r.get(f) for r in sp[:3]]}")
