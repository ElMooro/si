import boto3, json, time
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=240,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1")
for _ in range(20):
    c=lam.get_function(FunctionName="justhodl-risk-regime")["Configuration"]
    if c.get("LastUpdateStatus")=="Successful": break
    time.sleep(3)
lam.invoke(FunctionName="justhodl-risk-regime",InvocationType="RequestResponse")
rr=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/risk-regime.json")["Body"].read())
print("risk_regime:",rr.get("risk_regime"),"score",rr.get("risk_regime_score"))
print("posture:",json.dumps(rr.get("posture",{})))
print("participation:",json.dumps(rr.get("participation",{})))
print("DONE 2158")
