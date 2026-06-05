"""1289 — find Lambdas in the repo that are NOT deployed to AWS (built-not-shipped)."""
import json, os, boto3
from botocore.config import Config
cfg=Config(read_timeout=120,retries={"max_attempts":2})
lam=boto3.client("lambda",region_name="us-east-1",config=cfg)
# all deployed function names
deployed=set(); paginator=lam.get_paginator("list_functions")
for pg in paginator.paginate():
    for fn in pg["Functions"]: deployed.add(fn["FunctionName"])
# all repo lambdas
repo=[d for d in os.listdir("aws/lambdas") if os.path.isfile(f"aws/lambdas/{d}/source/lambda_function.py")]
missing=sorted([d for d in repo if d not in deployed])
out={"n_deployed":len(deployed),"n_repo":len(repo),"in_repo_not_deployed":missing}
open("aws/ops/reports/1289_deploy_audit.json","w").write(json.dumps(out,indent=2,default=str))
print(json.dumps(out,indent=2)[:1500])
