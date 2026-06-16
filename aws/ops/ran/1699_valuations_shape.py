import json, boto3
s3=boto3.client("s3",region_name="us-east-1")
try:
    d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/stock-valuations.json")["Body"].read())
    print("top-level keys:", list(d.keys())[:12])
    bt=d.get("by_ticker") or d.get("valuations") or d.get("stocks") or {}
    if isinstance(bt,dict) and bt:
        k=list(bt)[0]; print("sample ticker",k,"fields:",list(bt[k].keys())[:25])
    elif isinstance(bt,list) and bt:
        print("list len",len(bt),"sample fields:",list(bt[0].keys())[:25])
    print("has target/recom already?:", "target" in json.dumps(d)[:5000], "recom" in json.dumps(d)[:5000])
except Exception as e: print("shape err:",str(e)[:150])
