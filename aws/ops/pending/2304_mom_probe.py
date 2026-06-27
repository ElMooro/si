import boto3, json
s3=boto3.client("s3","us-east-1")
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/bottleneck-boom-research.json")["Body"].read())
bt=d.get("by_ticker") or {}
print("fields on NVDA:", sorted([k for k in (bt.get('NVDA') or {}).keys() if any(s in k for s in ['52','ret_','mom','range','price'])]))
for tk in ["NVDA","VST","CEG","LDOS","MU","ARM"]:
    r=bt.get(tk) or {}
    print(f"{tk}: off_52w_high={r.get('off_52w_high')} ret_1m={r.get('ret_1m')} ret_3m={r.get('ret_3m')} range_52w={r.get('range_52w')} price={r.get('price')}")
print("DONE 2304")
