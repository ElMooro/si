import urllib.request
def one(fk,t=25):
    url="https://data-api.ecb.europa.eu/service/data/"+fk+"?format=csvdata&lastNObservations=1"
    try:
        with urllib.request.urlopen(urllib.request.Request(url,headers={"User-Agent":"JustHodl raafouis@gmail.com"}),timeout=t) as r:
            lines=r.read().decode("utf-8","ignore").splitlines()
            if len(lines)<2: return None
            h={x:i for i,x in enumerate(lines[0].split(","))}; c=lines[1].split(",")
            return (c[h.get("TITLE",99)][:52] if "TITLE" in h and len(c)>h["TITLE"] else ""), (c[h.get("TIME_PERIOD",5)] if len(c)>h.get("TIME_PERIOD",5) else ""), (c[h.get("OBS_VALUE",6)] if len(c)>h.get("OBS_VALUE",6) else "")
    except Exception as e: return None
print("=== PPI (producer prices) candidates ===")
ppi=["STS/M.I9.N.PRIN.0000.3.000","STS/M.I8.N.PRIN.0000.3.000","STS/M.I9.N.PRIN.NS0020.3.000",
     "STS/M.I9.N.PRON.NS0020.3.000","STS/M.I9.Y.PRIN.0000.4.000","STS/M.I9.N.PRIN.N000.3.000",
     "STS/M.I9.N.PRIN.NSTO.3.000","STS/M.I9.N.PRIN.NS0010.3.000","STS/M.I9.N.PRIN.NS0030.3.000"]
for fk in ppi:
    r=one(fk); print(f"  {fk:40} -> {(str(r[2])+' ('+r[1]+') '+r[0]) if r else 'no'}")
print("\n=== 5y5y inflation-linked swap candidates ===")
ils=["FM/B.U2.EUR.RT.MM.EURIRS5X5Y.HSTA","FM/M.U2.EUR.RT.IL.EUR5Y5Y.HSTA",
     "FM/B.U2.EUR.RT.IL.ILS_5Y5Y.HSTA","FM/B.U2.EUR.4F.IL.5Y5Y.HSTA",
     "FM/B.U2.EUR.RT.MM.EURILS5Y5Y.HSTA","FM/M.U2.EUR.4F.ILS.5Y5Y.HSTA",
     "FM/B.U2.EUR.RT.IL.EUR_5Y5Y.HSTA","FM/D.U2.EUR.RT.MM.EUR5Y5Y.HSTA"]
for fk in ils:
    r=one(fk); print(f"  {fk:40} -> {(str(r[2])+' ('+r[1]+') '+r[0]) if r else 'no'}")
# also try inflation swap simple tenors (5Y, 10Y) to derive 5y5y
print("\n=== inflation swap tenors (to derive 5y5y if needed) ===")
for fk in ["FM/B.U2.EUR.RT.MM.EUR5YILS.HSTA","FM/B.U2.EUR.RT.MM.EUR10YILS.HSTA","ILS/M.U2.EUR.5Y","FM/B.U2.EUR.RT.IL.5Y.HSTA"]:
    r=one(fk); print(f"  {fk:40} -> {(str(r[2])+' ('+r[1]+') '+r[0]) if r else 'no'}")
