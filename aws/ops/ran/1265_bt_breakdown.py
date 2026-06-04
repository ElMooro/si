import json, boto3
from botocore.config import Config
s3=boto3.client("s3",region_name="us-east-1",config=Config(read_timeout=60))
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/signal-backtest.json")["Body"].read())
out={"maturity":d.get("maturity"),"n":d.get("n_observations"),"overall":d.get("overall"),
     "by_verdict":d.get("by_verdict"),"by_compounder":d.get("by_compounder_bucket"),
     "by_cap":d.get("by_cap_bucket"),"by_revision":d.get("by_revision")}
open("aws/ops/reports/1265_bt.json","w").write(json.dumps(out,indent=2,default=str))
print(json.dumps(out,indent=2,default=str))
