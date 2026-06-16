import json, urllib.request, boto3
s3=boto3.client("s3",region_name="us-east-1")
def get(url,t=30,hdr=None):
    h={"User-Agent":"JustHodl raafouis@gmail.com"}; h.update(hdr or {})
    try:
        with urllib.request.urlopen(urllib.request.Request(url,headers=h),timeout=t) as r: return r.status,r.read().decode("utf-8","ignore")
    except Exception as e: return getattr(e,'code',type(e).__name__),str(e)[:80]

print("=== 1) der.indicators (all dump signals) ===")
try:
    d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/ecb-derived.json")["Body"].read())
    for k,v in (d.get("indicators") or {}).items():
        if isinstance(v,dict): print(f"  {k:30} signal={v.get('signal')} fields={[x for x in v.keys() if x not in ('signal','interpretation')][:6]}")
except Exception as e: print("  ERR",e)

print("\n=== 2) ecb-hist manifest series ids ===")
try:
    m=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/ecb-hist/_manifest.json")["Body"].read())
    ids=[s["id"] for s in m.get("series",[])]
    print("  ",len(ids),"series:",", ".join(sorted(ids)))
except Exception as e: print("  ERR",e)

print("\n=== 3) Eurostat ei_bssi_m_r2 — confidence indicators available (EA20) ===")
st,body=get("https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/ei_bssi_m_r2?format=JSON&geo=EA20&s_adj=SA&lastTimePeriod=1")
try:
    j=json.loads(body); dim=j["dimension"]["indic"]["category"]
    for code,lab in dim["label"].items(): print(f"  {code:14} {lab[:60]}")
except Exception as e: print("  ERR",st,str(body)[:120])

print("\n=== 4) Eurostat sts_inpr_m — IP YoY (PCH_SM) NACE breakdowns ===")
st,body=get("https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/sts_inpr_m?format=JSON&geo=EA20&unit=PCH_SM&s_adj=SCA&lastTimePeriod=1")
try:
    j=json.loads(body); nace=j["dimension"]["nace_r2"]["category"]["label"]
    print("  NACE codes:", ", ".join(list(nace.keys())[:25]))
    # sample value
    print("  sample (first cell):", list(j["value"].items())[:1])
except Exception as e: print("  ERR",st,str(body)[:140])

print("\n=== 5) ECB IT-DE 10y spread underliers (govt bond yields) ===")
for nm,fk in {"IT 10y":"IRS/M.IT.L.L40.CI.0000.EUR.N.Z","DE 10y":"IRS/M.DE.L.L40.CI.0000.EUR.N.Z","IT 10y conv":"FM/M.IT.EUR.RT.GBY.GBRT10Y.HSTA"}.items():
    st,body=get(f"https://data-api.ecb.europa.eu/service/data/{fk}?format=csvdata&lastNObservations=1")
    head=body.splitlines()[1][:70] if st==200 and len(body.splitlines())>1 else f"http={st}"
    print(f"  {nm:12} {fk:34} -> {head}")
