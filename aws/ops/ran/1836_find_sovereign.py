import boto3
lam=boto3.client("lambda",region_name="us-east-1")
p=lam.get_paginator("list_functions"); names=[]
for pg in p.paginate():
    for f in pg["Functions"]:
        n=f["FunctionName"].lower()
        if "sovereign" in n or "systemic" in n:
            names.append((f["FunctionName"], f["LastModified"][:19]))
print("matching functions:", names or "NONE")
# also check who is scheduled to write data/sovereign-stress.json by checking recent S3 obj
import json,datetime
s3=boto3.client("s3",region_name="us-east-1")
try:
    h=s3.head_object(Bucket="justhodl-dashboard-live",Key="data/sovereign-stress.json")
    print("data/sovereign-stress.json last modified:", h["LastModified"])
except Exception as e: print("no sovereign-stress.json:",str(e)[:80])
