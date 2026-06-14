"""Confirm the revenueGrowth fix yields real values for tail small/mid names."""
import json, time, urllib.request, boto3
s3 = boto3.client("s3", region_name="us-east-1")
B="justhodl-dashboard-live"; FMP="wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
base="https://financialmodelingprep.com/stable"
def get(u):
    try:
        r=urllib.request.Request(u,headers={"User-Agent":"JustHodl/1.0"})
        with urllib.request.urlopen(r,timeout=20) as x: return json.loads(x.read().decode())
    except Exception: return None
def num(v):
    try: return float(v)
    except Exception: return None
def bucket(m):
    if not m: return "?"
    if m<300e6: return "micro/nano"
    if m<2e9: return "small"
    if m<10e9: return "mid"
    return "large+"

stocks=json.loads(s3.get_object(Bucket=B,Key="data/universe.json")["Body"].read()).get("stocks") or []
# screener names are large; pick tradeable tail = small + mid
sample=[s for s in stocks if bucket(num(s.get("market_cap"))) in ("small","mid")][:8]
got=0
for s in sample:
    sym=(s.get("symbol") or "").upper()
    inc=get(f"{base}/income-statement?symbol={sym}&period=annual&limit=2&apikey={FMP}")
    rg=None
    if isinstance(inc,list) and len(inc)>=2:
        rn,ro=num(inc[0].get("revenue")),num(inc[1].get("revenue"))
        if rn and ro and ro>0: rg=round((rn/ro-1)*100,1)
    if rg is not None: got+=1
    print(f"  {sym:>6} ({bucket(num(s.get('market_cap'))):>5}): revenueGrowth = {rg}%")
    time.sleep(0.15)
print(f"\npopulated {got}/{len(sample)} sampled tail names (was 0/all before — hardcoded None)")
