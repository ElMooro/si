import boto3, json
s3=boto3.client("s3","us-east-1"); B="justhodl-dashboard-live"
for k in ["data/polygon-fx-regime.json","data/polygon-futures-curves.json"]:
    try:
        d=json.loads(s3.get_object(Bucket=B,Key=k)["Body"].read())
        print("\n%s\n  keys: %s"%(k,list(d.keys())))
        print("  generated_at:",d.get("generated_at"))
        print("  signals:",d.get("signals"))
        for kk in ["pairs","regimes","dxy","majors","curves","contracts","data"]:
            if kk in d:
                v=d[kk]; print("  %s sample: %s"%(kk, json.dumps(v)[:300]))
    except Exception as e: print(k,"ERR",str(e)[:60])
