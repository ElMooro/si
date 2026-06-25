import boto3, json, time
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=890,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1")
for _ in range(30):
    c=lam.get_function(FunctionName="justhodl-hot-money")["Configuration"]
    if c.get("State")=="Active" and c.get("LastUpdateStatus") in ("Successful",None): break
    time.sleep(3)
lam.invoke(FunctionName="justhodl-hot-money",InvocationType="RequestResponse")
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/hot-money.json")["Body"].read())
print("v",d.get("version"),"dur",d.get("duration_s"),"s")
# 1) FX coverage
allc=d.get("all_countries",[])
withfx=[c for c in allc if c.get("fx_strength") is not None]
print(f"\nFX COVERAGE: {len(withfx)}/{len(allc)} countries now have currency strength")
for c in (d.get("inflow_leaders") or [])[:8]:
    print(f"  {c['country']:<13} score {c['hot_money_score']:+.2f} {c.get('conviction',''):<18} fx {c.get('fx_strength')}")
# 2) drill foreign momentum
dr=d.get("drilldowns") or {}
for cn,v in list(dr.items())[:2]:
    st=", ".join(f"{h['ticker']}({h.get('day_chg_pct','?')}%)" for h in (v.get('top_holdings') or [])[:6])
    print(f"\n  DRILL {cn} stocks w/ momentum: {st}")
# 3) EM-debt
em=d.get("em_debt_flows") or {}
print(f"\nEM-DEBT: net_flow_5d ${em.get('net_flow_5d_usd')} signal={em.get('signal')}")
for e in (em.get("by_etf") or []):
    print(f"  {e['etf']:<6} {e.get('name','')[:18]:<18} ret20 {e.get('ret_20d_pct')}% flow5d ${e.get('flow_5d_usd')}")
print("DONE 2181")
