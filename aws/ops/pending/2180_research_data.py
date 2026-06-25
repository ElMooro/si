import urllib.request, json, datetime
PK="zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d"; FK="wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
import boto3
s3=boto3.client("s3","us-east-1")
def g(u):
    try: return json.loads(urllib.request.urlopen(u,timeout=30).read())
    except Exception as e: return {"_err":str(e)[:50]}
# 1) what FX pairs does fx-regime provide?
fx=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/polygon-fx-regime.json")["Body"].read())
print("fx-regime pairs:",sorted((fx.get("pair_data") or {}).keys()))
# 2) Polygon FX aggs for EM crosses
frm=(datetime.date.today()-datetime.timedelta(days=30)).isoformat(); to=(datetime.date.today()-datetime.timedelta(days=1)).isoformat()
print("\nPolygon FX EM crosses (25d bars):")
for cc in ["C:USDIDR","C:USDTWD","C:USDPHP","C:USDINR","C:USDKRW","C:USDTHB","C:USDMYR","C:USDZAR"]:
    j=g(f"https://api.polygon.io/v2/aggs/ticker/{cc}/range/1/day/{frm}/{to}?adjusted=true&apiKey={PK}")
    print(f"  {cc}: status={j.get('status')} bars={j.get('resultsCount')}")
# 3) FMP quote for foreign-listed drill holdings
print("\nFMP quote foreign listings:")
for sym in ["BBCA.JK","BHP.AX","BMRI.JK","TLKM.JK"]:
    j=g(f"https://financialmodelingprep.com/stable/quote?symbol={sym}&apikey={FK}")
    if isinstance(j,list) and j: print(f"  {sym}: price={j[0].get('price')} chg%={j[0].get('changePercentage')}")
    else: print(f"  {sym}: {str(j)[:60]}")
# 4) etf-global fund flows for EM-debt ETFs
print("\nPolygon etf-global flows EM-debt:")
for tk in ["EMB","EMLC","PCY","EMHY","VWOB"]:
    j=g(f"https://api.polygon.io/etf-global/v1/fund-flows?composite_ticker={tk}&apiKey={PK}")
    res=j.get("results") or []
    print(f"  {tk}: status={j.get('status')} n={len(res) if isinstance(res,list) else 'na'}")
print("DONE 2180")
