import boto3, json
s3=boto3.client("s3","us-east-1")
for k in ["crypto-intel.json","data/crypto-intel.json","crypto-dvol.json","data/crypto-dvol.json"]:
    try:
        s3.head_object(Bucket="justhodl-dashboard-live",Key=k); print(k,"EXISTS")
    except Exception: print(k,"-- missing")
print("DONE 2368")
