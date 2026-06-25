import boto3, json, time
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=820,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1")
for _ in range(30):
    c=lam.get_function(FunctionName="justhodl-hot-money")["Configuration"]
    if c.get("State")=="Active" and c.get("LastUpdateStatus") in ("Successful",None): break
    time.sleep(3)
lam.invoke(FunctionName="justhodl-hot-money",InvocationType="RequestResponse")
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/hot-money.json")["Body"].read())
print("v",d.get("version"),"| risk_regime:",json.dumps(d.get("risk_regime",{}) or {})[:90])
print("\nINFLOW LEADERS (with conviction + velocity):")
for c in (d.get("inflow_leaders") or [])[:10]:
    print(f"  #{c.get('rank'):<2} {c['country']:<13} score {c['hot_money_score']:+.2f}  {c.get('conviction',''):<18} {c.get('flow_velocity') or '':<13} mom {c.get('rel_mom_20d')} flow5d ${c.get('net_flow_5d_usd')} fx {c.get('fx_strength')}")
print("\nOUTFLOW LEADERS:")
for c in (d.get("outflow_leaders") or [])[:5]:
    print(f"  {c['country']:<13} score {c['hot_money_score']:+.2f} {c.get('conviction','')}")
print("DONE 2178")
