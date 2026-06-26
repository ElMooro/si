import boto3, json, time
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=300,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1")
for _ in range(25):
    c=lam.get_function(FunctionName="justhodl-risk-regime")["Configuration"]
    if c.get("State")=="Active" and c.get("LastUpdateStatus") in ("Successful",None): break
    time.sleep(3)
lam.invoke(FunctionName="justhodl-risk-regime",InvocationType="RequestResponse")
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/risk-regime.json")["Body"].read())
ci=d.get("capital_inflows")
print("regime:",d.get("risk_regime"),"score",d.get("risk_regime_score"))
if ci:
    print(f"CAPITAL-INFLOWS OVERLAY: regime={ci['regime']} confirmation={ci['confirmation']} into12=${ci['foreign_net_into_us_lt_12mo_b']}B")
    print(f"  note: {ci['note']}")
print("all macro overlays now:", [k for k in ("cross_border","systemic_stress","liquidity","capital_inflows") if d.get(k)])
print("DONE 2222")
