import boto3, json, time
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=500,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1")
def ready(fn):
    for _ in range(25):
        c=lam.get_function(FunctionName=fn)["Configuration"]
        if c.get("State")=="Active" and c.get("LastUpdateStatus") in ("Successful",None): return
        time.sleep(3)
# risk-regime secondary risk
ready("justhodl-risk-regime"); lam.invoke(FunctionName="justhodl-risk-regime",InvocationType="RequestResponse")
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/risk-regime.json")["Body"].read())
sr=d.get("secondary_risk") or {}
print(f"risk-regime: regime={d.get('risk_regime')} score={d.get('risk_regime_score')}")
print(f"  SECONDARY RISK: {sr.get('confirmation')} {sr.get('n_firing')}/{sr.get('n_available')} firing: {sr.get('firing')}")
# best-setups red flags
ready("justhodl-best-setups"); lam.invoke(FunctionName="justhodl-best-setups",InvocationType="RequestResponse")
b=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/best-setups.json")["Body"].read())
rf=[s for s in (b.get("top_setups") or []) if s.get("red_flags")]
print(f"\nbest-setups: {len(b.get('top_setups') or [])} setups, {len(rf)} red-flagged")
for s in rf[:5]: print(f"  {s['ticker']:<6} conv {s['conviction']} flags={s['red_flags']}")
# master-ranker red flags
ready("justhodl-master-ranker"); lam.invoke(FunctionName="justhodl-master-ranker",InvocationType="RequestResponse")
m=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/master-ranker.json")["Body"].read())
mrf=[t for t in (m.get("top_tickers") or []) if t.get("red_flags")]
print(f"\nmaster-ranker: {len(m.get('top_tickers') or [])} ranks, {len(mrf)} red-flagged")
for t in mrf[:5]: print(f"  {t['ticker']:<6} score {t['score']} flags={t['red_flags']}")
print("DONE 2192")
