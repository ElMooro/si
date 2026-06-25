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
print("regime:",d.get("risk_regime"),"score",d.get("risk_regime_score"))
ss=d.get("systemic_stress")
if ss:
    print("SYSTEMIC-STRESS OVERLAY (7 islands wired in):")
    print(f"  level={ss['level']} confirmation={ss['confirmation']} elevated={ss['n_elevated']}/{ss['n_gauges']}")
    print(f"  readings: {json.dumps(ss['readings'])}")
    print(f"  note: {ss['note']}")
    print("  size_mult:",d.get("posture",{}).get("size_mult"),"stress_warning:",d.get("posture",{}).get("stress_warning"))
else: print("systemic_stress: None")
print("tells:",[t for t in (d.get('tells') or []) if 'stress' in t.lower() or 'Cross-border' in t])
print("DONE 2193")
