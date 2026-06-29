import boto3, json
s3=boto3.client("s3","us-east-1")
for key in ["data/supply-chain-linkage.json","data/supply-chain-linkages.json"]:
    try:
        d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key=key)["Body"].read())
        print("KEY",key,"| top:",list(d.keys())[:12] if isinstance(d,dict) else type(d).__name__)
        print("  gen:",d.get("generated_at") if isinstance(d,dict) else "")
        # find per-company concentration flags
        body=d.get("companies") or d.get("by_ticker") or d.get("linkages") or d
        if isinstance(body,dict):
            ks=list(body.keys())[:3]
            for k in ks:
                if isinstance(body[k],dict): print("  ",k,"->",json.dumps(body[k])[:300])
        elif isinstance(body,list):
            print("  list len",len(body),"sample:",json.dumps(body[0])[:300])
        break
    except Exception as e: print("KEY",key,"ERR",str(e)[:60])
print("DONE 2487")
