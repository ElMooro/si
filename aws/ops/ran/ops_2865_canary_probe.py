"""ops 2865 — verify 6 new leading-canary FRED series (rates/credit/labor/housing/monetary)."""
import os, json, urllib.request, boto3
from datetime import datetime, timezone
R={"ops":2865,"ts":datetime.now(timezone.utc).isoformat()}
lam=boto3.client("lambda",region_name="us-east-1")
FRED=lam.get_function_configuration(FunctionName="justhodl-china-liquidity").get("Environment",{}).get("Variables",{}).get("FRED_API_KEY","")
def fred(sid,n=60):
    try:
        u="https://api.stlouisfed.org/fred/series/observations?series_id=%s&api_key=%s&file_type=json&sort_order=desc&limit=%d"%(sid,FRED,n)
        d=json.loads(urllib.request.urlopen(u,timeout=15).read())
        o=[x for x in d.get("observations",[]) if x.get("value") not in(".","",None)]
        if not o: return {"empty":True}
        vals=[float(x["value"]) for x in o]
        cur=vals[0]; mu=sum(vals)/len(vals); sd=(sum((v-mu)**2 for v in vals)/len(vals))**0.5
        return {"latest":round(cur,3),"date":o[0]["date"],"z_vs_recent":round((cur-mu)/sd,2) if sd else None,"n":len(o)}
    except Exception as e: return {"err":str(e)[:60]}
cands={
 "yield_curve_10y2y":"T10Y2Y",           # rates: inversion=recession lead (dir=fall)
 "yield_curve_10y3m":"T10Y3M",
 "hy_credit_oas":"BAMLH0A0HYM2",         # credit: widening=stress (dir=rise)
 "initial_claims_4wk":"IC4WSA",          # labor: rising=stress (dir=rise)
 "building_permits":"PERMIT",            # housing lead: falling=stress (dir=fall)
 "real_m2":"M2REAL",                     # monetary: contracting=stress (dir=fall)
 "lending_standards_ci":"DRTSCILM",      # SLOOS: tightening=stress (dir=rise), quarterly
}
for name,sid in cands.items():
    R.setdefault("fred",{})[name]={"sid":sid, **fred(sid)}
print(json.dumps(R,ensure_ascii=False,indent=1,default=str)[:2600])
os.makedirs("aws/ops/reports",exist_ok=True); json.dump(R,open("aws/ops/reports/2865_canary_probe.json","w"),ensure_ascii=False,indent=1,default=str)
print("OPS 2865 COMPLETE")
