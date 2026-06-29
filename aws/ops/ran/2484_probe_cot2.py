import boto3, json
s3=boto3.client("s3","us-east-1")
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/cftc-all-cache.json")["Body"].read())
body=d.get("data") or {}
print("ALL contract keys:",sorted(body.keys()))
for c in ["CL","NG","HG"]:
    cc=body.get(c)
    if not cc: print(c,"MISSING"); continue
    wr=cc.get("weekly_reports") or []
    print(f"{c} report_type={cc.get('report_type')} n_reports={len(wr)}")
    if wr: print("   latest report keys:",sorted(wr[-1].keys()))
    if wr: print("   latest sample:",json.dumps(wr[-1])[:400])
print("DONE 2484")
