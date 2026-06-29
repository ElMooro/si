import boto3, json
s3=boto3.client("s3","us-east-1")
try:
    d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/supply-inflection.json")["Body"].read())
    print("top keys:",list(d.keys())[:25])
    # find where signals live + a PPI sample
    for k,v in d.items():
        if isinstance(v,list) and v and isinstance(v[0],dict):
            ppi=[x for x in v if 'ppi' in json.dumps(x).lower()][:2]
            if ppi:
                print(f"list '{k}' has PPI items; sample:",json.dumps(ppi[0])[:400]);break
    else:
        print("no obvious PPI list; full sample:",json.dumps(d)[:600])
except Exception as e: print("ERR",str(e)[:90])
print("DONE 2480")
