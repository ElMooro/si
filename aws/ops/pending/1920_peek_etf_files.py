import boto3, json
s3=boto3.client("s3","us-east-1"); B="justhodl-dashboard-live"
for k in ["data/etf-flows.json","data/etf-fund-flows.json","data/etf-true-flows.json","etf-flows/daily.json"]:
    try:
        d=json.loads(s3.get_object(Bucket=B,Key=k)["Body"].read())
        ga=d.get("generated_at") or d.get("as_of") or d.get("date") or "?"
        print("%-28s keys=%s gen=%s"%(k,list(d.keys())[:8],str(ga)[:19]))
    except Exception as e: print("%-28s MISSING/ERR %s"%(k,str(e)[:40]))
