import boto3
s3=boto3.client("s3",region_name="us-east-1")
for k in ["data/edgar-authority.json","data/edgar-crosscheck.json","data/net-nets.json","data/ncav.json","data/edgar-financials.json"]:
    try: s3.head_object(Bucket="justhodl-dashboard-live",Key=k); print(f"  {k} EXISTS")
    except: print(f"  {k} —")
