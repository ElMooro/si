import boto3, json
s3=boto3.client("s3","us-east-1")
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/bottleneck-boom-research.json")["Body"].read())
bt=d.get("by_ticker") or {}
for tk in ["VST","CEG","NRG","GEV","TLN","OKLO"]:
    r=bt.get(tk)
    if r: print(f"{tk}: acq_driven={r.get('acq_driven')} rev_growth={r.get('rev_growth_yoy')} pe={r.get('pe')} ps={r.get('ps')} fwd_val.growth_src={(r.get('fwd_val') or {}).get('growth_source')}")
# distribution of pe to pick a 'distorted' threshold
pes=sorted([(tk,r.get('pe')) for tk,r in bt.items() if r.get('pe') is not None],key=lambda x:-(x[1] or 0))
print("highest P/E:", pes[:6])
print("negative/zero P/E:", [(tk,p) for tk,p in pes if p is not None and p<=0])
print("DONE 2303")
