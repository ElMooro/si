import boto3, json
s3=boto3.client("s3","us-east-1")
try:
    d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/cftc-all-cache.json")["Body"].read())
    print("top type:",type(d).__name__,"| keys:",list(d.keys())[:12] if isinstance(d,dict) else "list")
    # find the per-contract data structure
    body=d.get("data") or d.get("contracts") or d.get("results") or d
    if isinstance(body,dict):
        sample_k=list(body.keys())[:5]
        print("body keys sample:",sample_k)
        for k in sample_k[:2]:
            print(" ",k,"->",json.dumps(body[k])[:300])
    elif isinstance(body,list):
        print("list len",len(body),"sample:",json.dumps(body[0])[:400])
except Exception as e: print("ERR",str(e)[:100])
print("DONE 2482")
