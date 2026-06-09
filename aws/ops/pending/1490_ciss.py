import json, boto3
from botocore.config import Config
s3=boto3.client("s3",region_name="us-east-1",config=Config(read_timeout=60))
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/ecb-hist/ciss_ea.json")["Body"].read())
out={"latest":d.get("latest"),"min":d.get("min"),"max":d.get("max"),"first_date":d.get("first_date"),"latest_date":d.get("latest_date"),
     "first_3_points":d.get("points",[])[:3],"last_3_points":d.get("points",[])[-3:]}
open("aws/ops/reports/1490_c.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
