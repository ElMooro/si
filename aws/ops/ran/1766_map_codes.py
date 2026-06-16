import urllib.request
def one(fk,t=35):
    url="https://data-api.ecb.europa.eu/service/data/"+fk+"?format=csvdata&lastNObservations=1"
    try:
        with urllib.request.urlopen(urllib.request.Request(url,headers={"User-Agent":"JustHodl raafouis@gmail.com"}),timeout=t) as r:
            lines=r.read().decode("utf-8","ignore").splitlines()
            if len(lines)<2: return "EMPTY","",""
            h={x:i for i,x in enumerate(lines[0].split(","))}; c=lines[1].split(",")
            ti=h.get("TITLE"); tpi=h.get("TIME_PERIOD"); ovi=h.get("OBS_VALUE")
            return (c[ti][:58] if ti is not None and len(c)>ti else ""), (c[tpi] if tpi and len(c)>tpi else ""), (c[ovi] if ovi and len(c)>ovi else "")
    except Exception as e: return f"ERR {getattr(e,'code',type(e).__name__)}","",""
print("=== UNEMPLOYMENT (LFSI) — EA youth + member states ===")
un={"EA total":"LFSI/M.I9.S.UNEHRT.TOTAL0.15_74.T","EA youth<25":"LFSI/M.I9.S.UNEHRT.TOTAL0.15_24.T"}
for cc in ["DE","FR","IT","ES","NL","GR","PT","IE","AT","BE","FI"]:
    un[cc]=f"LFSI/M.{cc}.S.UNEHRT.TOTAL0.15_74.T"
for nm,fk in un.items():
    tt=one(fk); print(f"  {nm:10} {fk:38} -> {tt[2]} ({tt[1]}) {tt[0]}")
print("\n=== STS PRODUCTION activity codes (map manufacturing/MIGs/construction) ===")
for code in ["NS0010","NS0011","NS0020","NS0021","NS0022","NS0023","NS0030","NS0040","NS0050","NS0060","MIG_CAG","MIG_ING","MIG_COG","MIG_DCOG","MIG_NDCOG","MIG_NRG"]:
    fk=f"STS/M.I9.Y.PROD.{code}.4.000"; tt=one(fk); print(f"  {code:10} -> {tt[2]} ({tt[1]}) {tt[0]}")
# construction production + turnover/manufacturing variants
print("\n--- other STS families ---")
for nm,fk in {"construction PROD":"STS/M.I9.Y.PRCO.NS0030.4.000","retail turnover":"STS/M.I9.Y.TOVT.NS0040.4.000","manufacturing TOVT":"STS/M.I9.Y.TOVT.NS0020.4.000"}.items():
    tt=one(fk); print(f"  {nm:18} {fk:34} -> {tt[2]} ({tt[1]}) {tt[0]}")
