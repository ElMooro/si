import boto3, json, time
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=840,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1")
try: lam.get_function(FunctionName="justhodl-hot-money"); print("hot-money EXISTS")
except lam.exceptions.ResourceNotFoundException: print("hot-money NOT DEPLOYED"); raise SystemExit
for _ in range(30):
    c=lam.get_function(FunctionName="justhodl-hot-money")["Configuration"]
    if c.get("State")=="Active" and c.get("LastUpdateStatus") in ("Successful",None): break
    time.sleep(3)
lam.invoke(FunctionName="justhodl-hot-money",InvocationType="RequestResponse")
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/hot-money.json")["Body"].read())
print("generated:",d.get("generated_at"),"countries:",d.get("n_countries"),"dur:",d.get("duration_s"))
print("world ret20:",d.get("world_ret_20d_pct"),"| signals_logged:",d.get("signals_logged"))
print("\nINFLOW LEADERS (country | score | rel_mom20 | net_flow_5d_$):")
for c in d.get("inflow_leaders",[])[:10]:
    nf=c.get("net_flow_5d_usd"); nf=f"${nf/1e6:.0f}M" if isinstance(nf,(int,float)) else "n/a"
    print(f"   #{c.get('rank'):<2} {c['country']:<14} score {c['hot_money_score']:+.2f}  relmom {c.get('rel_mom_20d')}  flow {nf}  etfs {c.get('etfs')}")
print("\nOUTFLOW LEADERS:")
for c in d.get("outflow_leaders",[])[:6]:
    nf=c.get("net_flow_5d_usd"); nf=f"${nf/1e6:.0f}M" if isinstance(nf,(int,float)) else "n/a"
    print(f"   {c['country']:<14} score {c['hot_money_score']:+.2f}  flow {nf}")
print("\nDRILLDOWNS (sector/stock funnel):")
for ctry,dr in list(d.get("drilldowns",{}).items())[:3]:
    print(f"   {ctry} via {dr.get('etf')}: sectors={[s['sector'] for s in dr.get('top_sectors',[])[:4]]}")
    print(f"      top holdings: {[(n['ticker'],n.get('day_chg_pct')) for n in dr.get('top_holdings',[])[:5]]}")
# how many countries have real $ flow vs null
nf_ok=sum(1 for c in d.get("all_countries",[]) if isinstance(c.get('net_flow_5d_usd'),(int,float)))
print(f"\ncountries with real $ flow: {nf_ok}/{d.get('n_countries')}")
print("DONE 2177")
