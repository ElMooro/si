import json, boto3
s3=boto3.client("s3",region_name="us-east-1")
uni=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/finviz-universe.json")["Body"].read()).get("by_ticker",{})
val=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/stock-valuations.json")["Body"].read())
sp=val.get("sp_table",[]); hp=val.get("hp_table",[]) or val.get("hp",[])
def cov(rows, field):
    n=len(rows); c=sum(1 for r in rows if uni.get(r.get("t",r.get("ticker","")),{}).get(field) is not None)
    return f"{c}/{n} ({100*c//max(n,1)}%)"
print("EV/EBITDA coverage on sp_table:", cov(sp,"ev_ebitda"))
print("EV/Sales  coverage on sp_table:", cov(sp,"ev_sales"))
print("EV/EBITDA coverage on hp_table:", cov(hp,"ev_ebitda"))
# sample values
for t in ["AAPL","MSFT","JPM","XOM","KO"]:
    r=uni.get(t,{}); print(f"  {t}: ev_ebitda={r.get('ev_ebitda')} ev_sales={r.get('ev_sales')} sector={r.get('sector')}")
# sector EV/EBITDA medians for a sector-relative tilt (large caps only >= 10B)
from statistics import median
bys={}
for t,r in uni.items():
    ev=r.get("ev_ebitda"); sec=r.get("sector"); mc=r.get("market_cap") or 0
    if ev is not None and 0<ev<200 and sec and mc>=2000:
        bys.setdefault(sec,[]).append(ev)
print("\nsector median EV/EBITDA (mcap>=2B):")
for sec,v in sorted(bys.items()):
    if len(v)>=5: print(f"  {sec:26} med={median(v):.1f}  n={len(v)}")
