"""Decisive: is the micro/nano gap a fixable cap-limit, or an FMP data desert?
(1) check universe.json ordering; (2) probe real FMP coverage on a small-cap sample."""
import json, time, urllib.request, boto3, collections
s3 = boto3.client("s3", region_name="us-east-1")
B = "justhodl-dashboard-live"; FMP = "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
base = "https://financialmodelingprep.com/stable"
def load(k): return json.loads(s3.get_object(Bucket=B, Key=k)["Body"].read())
def get(u):
    try:
        r = urllib.request.Request(u, headers={"User-Agent":"JustHodl/1.0"})
        with urllib.request.urlopen(r, timeout=20) as x: return json.loads(x.read().decode())
    except Exception: return None
def num(v):
    try: return float(v)
    except Exception: return None
def bucket(m):
    if not m: return "no_mcap"
    if m<50e6: return "nano"
    if m<300e6: return "micro"
    if m<2e9: return "small"
    if m<10e9: return "mid"
    if m<200e9: return "large"
    return "mega"

stocks = load("data/universe.json").get("stocks") or []
# (1) ordering: are first 1500 cap-sorted (truncating small)?
caps = [num(s.get("market_cap")) for s in stocks if num(s.get("market_cap"))]
first1500 = [bucket(c) for c in caps[:1500]]
last = [bucket(c) for c in caps[1500:]]
print("first 1500 by bucket:", dict(collections.Counter(first1500)))
print("beyond 1500 by bucket:", dict(collections.Counter(last)))

# (2) FMP coverage on micro/nano sample
sample = [s for s in stocks if bucket(num(s.get("market_cap"))) in ("micro","nano")][:30]
have_pe=have_ps=have_rev=have_fpe=0; n=0
for s in sample:
    sym=(s.get("symbol") or "").upper()
    if not sym: continue
    n+=1
    km=get(f"{base}/key-metrics-ttm?symbol={sym}&apikey={FMP}")
    rt=get(f"{base}/ratios-ttm?symbol={sym}&apikey={FMP}")
    km=(km[0] if isinstance(km,list) and km else km) or {}
    rt=(rt[0] if isinstance(rt,list) and rt else rt) or {}
    pe = rt.get("priceToEarningsRatioTTM") or km.get("peRatioTTM")
    ps = rt.get("priceToSalesRatioTTM") or km.get("priceToSalesRatioTTM")
    rev = km.get("revenuePerShareTTM") or rt.get("priceToSalesRatioTTM")
    if pe is not None: have_pe+=1
    if ps is not None: have_ps+=1
    if rev is not None: have_rev+=1
    time.sleep(0.12)
print(f"\nFMP coverage on {n} micro/nano names:")
print(f"  has P/E: {have_pe}/{n} ({round(have_pe/n*100) if n else 0}%)")
print(f"  has P/S: {have_ps}/{n} ({round(have_ps/n*100) if n else 0}%)")
print(f"  has revenue data: {have_rev}/{n} ({round(have_rev/n*100) if n else 0}%)")
print(f"  -> survives 'P/E or P/S' filter: {sum(1 for _ in range(0))}", 
      f"{round(max(have_pe,have_ps)/n*100) if n else 0}% approx")
