import boto3, json, time
from botocore.config import Config
from datetime import datetime, timezone
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=200,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1"); B="justhodl-dashboard-live"
# 1) wait for best-setups fresh deploy
for _ in range(40):
    lm=lam.get_function_configuration(FunctionName="justhodl-best-setups")["LastModified"]
    age=(datetime.now(timezone.utc)-datetime.fromisoformat(lm.replace("Z","+00:00"))).total_seconds()
    if age < 200 and lam.get_function_configuration(FunctionName="justhodl-best-setups")["LastUpdateStatus"]!="InProgress": break
    time.sleep(6)
print("best-setups deployed %.0fs ago"%age)
# 2) invoke best-setups
r=lam.invoke(FunctionName="justhodl-best-setups",InvocationType="RequestResponse"); print("best-setups:",r["Payload"].read().decode()[:120])
time.sleep(2)
bs=json.loads(s3.get_object(Bucket=B,Key="data/best-setups.json")["Body"].read())
setups=bs.get("setups") or bs.get("top_setups") or bs.get("best_setups") or []
if not setups:
    for k,v in bs.items():
        if isinstance(v,list) and v and isinstance(v[0],dict): setups=v; print("(setups under key '%s')"%k); break
scf=[]
for st in setups:
    for s in (st.get("signals") or []):
        if s.get("key")=="SECTOR_CAPITAL_FLOW": scf.append((st.get("ticker"),s.get("detail")))
print("SECTOR_CAPITAL_FLOW signals in best-setups: %d"%len(scf))
for t,d in scf[:10]: print("   %-6s %s"%(t,d))
# 3) re-run master-ranker (v2 overlay)
r=lam.invoke(FunctionName="justhodl-master-ranker",InvocationType="RequestResponse"); print("\nmaster-ranker:",r["Payload"].read().decode()[:120])
time.sleep(2)
mr=json.loads(s3.get_object(Bucket=B,Key="data/master-ranker.json")["Body"].read())
tops=mr.get("top_tickers") or []
moved=[t for t in tops if abs((t.get("capital_flow_mult") or 1.0)-1.0)>0.001]
print("\nTOP-25: %d repriced by capital-flow overlay (v2):"%len(moved))
for t in moved[:12]:
    print("   #%-2s %-6s score=%-6s cf_mult=%-5s %s"%(tops.index(t)+1,t["ticker"],t["score"],t.get("capital_flow_mult"),
        (t.get("rationale") or "")[-70:]))
