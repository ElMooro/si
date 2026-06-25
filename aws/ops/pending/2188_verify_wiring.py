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
cb=d.get("cross_border")
if cb:
    print("CROSS-BORDER OVERLAY (hot-money wired in):")
    print(f"  confirmation={cb['confirmation']} flow_state={cb['flow_state']}")
    print(f"  EM inflow countries={cb['em_inflow_countries']} outflow={cb['em_outflow_countries']} debt={cb['em_debt_signal']}")
    print(f"  top inflows={cb['top_inflows']} top outflows={cb['top_outflows']}")
    print(f"  note: {cb['note'][:90]}")
    print("  size_mult:",d.get("posture",{}).get("size_mult"),"warn:",d.get("posture",{}).get("crossborder_warning"))
else: print("cross_border: None")
print("DONE 2188")
