import urllib.request, json
def get(url,t=30,hdr=None):
    h={"User-Agent":"JustHodl raafouis@gmail.com"}; h.update(hdr or {})
    try:
        with urllib.request.urlopen(urllib.request.Request(url,headers=h),timeout=t) as r:
            return r.status, r.read().decode("utf-8","ignore")
    except Exception as e: return getattr(e,'code',type(e).__name__), str(e)[:80]

print("=== A) SNB API (data.snb.ch) — confirm + find cubes ===")
for cid in ["snbpolrat","rendoblid","devkua","devkum","capchstocki","capchstockm","snbbipo","zimoma","plkoba"]:
    st,body=get(f"https://data.snb.ch/api/cube/{cid}/data/csv/en")
    head=body.split("\n")[0][:60] if isinstance(body,str) else ""
    print(f"  cube {cid:14} -> http={st} | {head}")

print("\n=== B) Yahoo — SMI / SPI indices ===")
for sym in ["%5ESSMI","%5ESSHI","SPI.SW","%5ESPI"]:
    st,body=get(f"https://query1.finance.yahoo.com/v8/finance/chart/{sym}?range=5d&interval=1d",hdr={"User-Agent":"Mozilla/5.0"})
    try:
        j=json.loads(body); r=j["chart"]["result"][0]; px=r["meta"].get("regularMarketPrice"); nm=r["meta"].get("symbol")
        print(f"  {sym:10} -> http={st} price={px} ({nm})")
    except Exception: print(f"  {sym:10} -> http={st} (no data)")

print("\n=== C) FRED — Swiss macro (check freshness) ===")
FRED="2f057499936072679d8843d7fce99989"
fred={"unemployment LRHU":"LRHUTTTTCHM156S","unemp SECO":"LMUNRRTTCHM156S","IP OECD":"CHEPROINDMISMEI",
      "IP prod PRINTO":"PRINTO01CHQ661S","mfg PRMNTO":"PRMNTO01CHQ661S","BCI OECD":"BSCICP02CHM460S",
      "CCI OECD":"CSCICP02CHM460S","CLI":"CHELOLITONOSTSAM"}
for nm,sid in fred.items():
    st,body=get(f"https://api.stlouisfed.org/fred/series/observations?series_id={sid}&api_key={FRED}&file_type=json&sort_order=desc&limit=1")
    try:
        o=json.loads(body)["observations"][0]; print(f"  {nm:18} {sid:18} -> {o['value']} ({o['date']})")
    except Exception: print(f"  {nm:18} {sid:18} -> http={st} (none)")

print("\n=== D) KOF barometer (datenservice.kof.ethz.ch) ===")
st,body=get("https://datenservice.kof.ethz.ch/api/v1/public/series?keys=ch.kof.barometer&mime=json&start=2025-01")
print(f"  KOF barometer -> http={st} | {str(body)[:120]}")

print("\n=== E) ECB business/industrial confidence + production YoY ===")
def ecb(fk):
    st,body=get(f"https://data-api.ecb.europa.eu/service/data/{fk}?format=csvdata&lastNObservations=1")
    if st==200:
        ls=body.splitlines(); h={x:i for i,x in enumerate(ls[0].split(","))}; c=ls[1].split(",") if len(ls)>1 else []
        return f"{c[h.get('OBS_VALUE',6)]} ({c[h.get('TIME_PERIOD',5)]}) {c[h.get('TITLE',99)][:40] if 'TITLE' in h and len(c)>h['TITLE'] else ''}"
    return f"http={st}"
for nm,fk in {"ESI sentiment":"RTD/M.S0.S.Y_ESI.LEVEL","ind confidence BCS":"RTD/M.S0.S.Y_BCS_BCI.LEVEL",
              "ind conf ESI":"RTD/M.S0.S.B_ICI.LEVEL","IP YoY (compute from idx)":"STS/M.I9.Y.PROD.NS0020.4.000"}.items():
    print(f"  {nm:26} {fk:34} -> {ecb(fk)}")
