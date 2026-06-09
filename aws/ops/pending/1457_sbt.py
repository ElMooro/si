import json, boto3
from botocore.config import Config
s3=boto3.client("s3",region_name="us-east-1",config=Config(read_timeout=60))
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/signal-backtest.json")["Body"].read())
out={"n_observations":d.get("n_observations"),"snapshots_used":d.get("snapshots_used"),"maturity":d.get("maturity"),
     "overall":d.get("overall"),"by_verdict":d.get("by_verdict"),"by_compounder_bucket":d.get("by_compounder_bucket")}
open("aws/ops/reports/1457_sbt.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
