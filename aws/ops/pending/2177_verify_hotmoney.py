import boto3, json, urllib.request, time
from datetime import datetime, timezone
s3=boto3.client("s3","us-east-1")
def age(ts):
    try: return round((datetime.now(timezone.utc)-datetime.fromisoformat(ts.replace("Z","+00:00"))).total_seconds()/3600,1)
    except: return "?"
# 1) live hot-money output
try:
    d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/hot-money.json")["Body"].read())
    print(f"hot-money.json: generated {d.get('generated_at','?')[:16]} ({age(d.get('generated_at',''))}h ago)")
    print(f"  n_countries={d.get('n_countries')} method={d.get('method','')[:60]} signals_logged={d.get('signals_logged')}")
    print("  INFLOW LEADERS:")
    for c in (d.get("inflow_leaders") or [])[:8]:
        print(f"    #{c.get('rank')} {c['country']:<13} score {c['hot_money_score']:+} rel_mom {c.get('rel_mom_20d')} flow5d ${c.get('net_flow_5d_usd')} etfs={c.get('etfs')}")
    print("  OUTFLOW LEADERS:")
    for c in (d.get("outflow_leaders") or [])[:5]:
        print(f"    {c['country']:<13} score {c['hot_money_score']:+} rel_mom {c.get('rel_mom_20d')}")
    dr=d.get("drilldowns") or {}
    print(f"  DRILLDOWNS: {list(dr.keys())}")
    for cn,v in list(dr.items())[:2]:
        secs=", ".join(f"{s['sector']} {s['weight_pct']}%" for s in (v.get('top_sectors') or [])[:4])
        hold=", ".join(f"{h['ticker']}({h.get('day_chg_pct','?')}%)" for h in (v.get('top_holdings') or [])[:6])
        print(f"    {cn}: sectors=[{secs}]")
        print(f"          stocks=[{hold}]")
except Exception as e: print("hot-money.json ERR:",str(e)[:80])
# 2) input freshness
for k in ["data/etf-true-flows.json","data/polygon-fx-regime.json"]:
    try:
        x=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key=k)["Body"].read())
        print(f"  input {k}: {age(x.get('generated_at',''))}h ago  keys={list(x.keys())[:6]}")
    except Exception as e: print(f"  input {k}: {str(e)[:50]}")
# 3) Polygon ETF-global fund-flows entitlement test (EWZ)
KEY="zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d"
try:
    j=json.loads(urllib.request.urlopen(f"https://api.polygon.io/etf-global/v1/fund-flows?composite_ticker=EWZ&apiKey={KEY}",timeout=30).read())
    res=j.get("results") or j.get("data") or []
    print(f"\nPolygon ETF-global fund-flows EWZ: status={j.get('status')} n={len(res) if isinstance(res,list) else 'n/a'} sample={str(res[:1])[:120] if res else 'EMPTY'}")
except Exception as e: print("\nPolygon ETF-global fund-flows:",str(e)[:80])
print("DONE 2177")
