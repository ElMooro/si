import json, boto3, time
from botocore.config import Config
cfg=Config(read_timeout=120,retries={"max_attempts":1})
lam=boto3.client("lambda",region_name="us-east-1",config=cfg)
s3=boto3.client("s3",region_name="us-east-1",config=cfg)
out={}
# run recalibrator + capture FULL response
try:
    r=lam.invoke(FunctionName="justhodl-cascade-recalibrator",InvocationType="RequestResponse",Payload=b"{}")
    out["full_response"]=r["Payload"].read().decode()[:500]
except Exception as e: out["err"]=str(e)[:90]
# what does it READ to recalibrate? check the source for its input + write condition
src=open("aws/lambdas/justhodl-cascade-recalibrator/source/lambda_function.py").read() if __import__('os').path.exists("aws/lambdas/justhodl-cascade-recalibrator/source/lambda_function.py") else ""
import re
out["reads"]=sorted(set(re.findall(r'data/[a-z0-9_-]+\.json',src)))[:12]
out["min_obs_gate"]=re.findall(r'(MIN_\w+\s*=\s*\d+|n_obs\w*\s*[<>]=?\s*\d+|< ?\d+ ?:.*return|insufficient)',src)[:5]
open("aws/ops/reports/1461_why.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
