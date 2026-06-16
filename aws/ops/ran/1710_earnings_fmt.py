import json, boto3
from collections import Counter
s3=boto3.client("s3",region_name="us-east-1")
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/finviz-universe.json")["Body"].read())
bt=d.get("by_ticker",{})
ed=[(tk,r.get("earnings_date")) for tk,r in bt.items() if r.get("earnings_date")]
print("tickers with earnings_date:", len(ed))
print("sample raw values:")
for tk,v in ed[:20]: print(f"  {tk:6} {v!r}")
# distribution of formats
fmts=Counter()
for _,v in ed:
    s=str(v); fmts[len(s.split())]+=1
print("token-count distribution:", dict(fmts))
