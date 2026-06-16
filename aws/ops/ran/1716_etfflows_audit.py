import json, boto3
s3=boto3.client("s3",region_name="us-east-1")
try:
    d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/etf-flows.json")["Body"].read())
    print("etf-flows.json keys:", list(d.keys())[:12])
    s=d.get("by_ticker") or d.get("etfs") or d.get("sectors") or d
    if isinstance(s,dict):
        k=list(s)[0] if s else None
        print("sample entry", k, ":", json.dumps(s.get(k),default=str)[:300] if k else "")
except Exception as e: print("etf-flows.json:", str(e)[:100])
