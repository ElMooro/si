import urllib.request, re
def get(url,t=60,acc="application/xml"):
    try:
        with urllib.request.urlopen(urllib.request.Request(url,headers={"User-Agent":"JustHodl raafouis@gmail.com","Accept":acc}),timeout=t) as r:
            return r.status, r.read().decode("utf-8","ignore")
    except Exception as e: return getattr(e,'code',type(e).__name__), str(e)[:90]

st,body=get("https://data-api.ecb.europa.eu/service/datastructure/ECB/ECB_SUP1?references=all")
print("structure http",st,"bytes",len(body))
if st==200:
    # generic tag discovery
    print("Dimension tags:", sorted(set(re.findall(r'<(\w+:Dimension) id',body)))[:5])
    print("Code tags:", sorted(set(re.findall(r'<(\w+:Code) id',body)))[:5])
    dims=re.findall(r':Dimension id="([^"]+)"',body); print("dimensions:",dims)
    # find codelist for the indicator/series concept with confidence-ish names
    codes=re.findall(r':Code id="([^"]+)">.*?:Name[^>]*>([^<]+)<',body,re.S)
    pat=re.compile(r'confiden|sentiment|industr|servic|consumer|retail|construct',re.I)
    hits=[(i,n) for i,n in codes if pat.search(n)][:40]
    print(f"{len(codes)} codes; survey hits:")
    for i,n in hits: print(f"   {i:16} {n[:55]}")

print("\n=== try data queries (find working flowRef + key) ===")
for fk in ["ECB_SUP1/M.U2.S.ESI.....","SUP/M.U2...","RTD/M.S0.S.Y_ESI_.LEVEL",
           "ECB_SUP1/.....","ESI/M.U2.....","ICS/M.U2....."]:
    st,body=get(f"https://data-api.ecb.europa.eu/service/data/{fk}?lastNObservations=1&format=csvdata","application/vnd.sdmx.data+csv")
    print(f"   {fk:28} -> http={st} {('| '+body.splitlines()[1][:50]) if st==200 and len(body.splitlines())>1 else ''}")

print("\n=== FRED/OECD EA confidence components (fallback availability) ===")
FRED="2f057499936072679d8843d7fce99989"
for nm,sid in {"ESI proxy BCI":"BSCICP02EZM460S","consumer CCI":"CSCICP02EZM460S",
               "ind conf":"BSCURT02EZM460S","ind conf2":"BSCICP03EZM665S","serv conf":"BVCICP02EZM460S"}.items():
    st,body=get(f"https://api.stlouisfed.org/fred/series/observations?series_id={sid}&api_key={FRED}&file_type=json&sort_order=desc&limit=1","application/json")
    import json as J
    try: o=J.loads(body)["observations"][0]; print(f"   {nm:16} {sid:18} {o['value']} ({o['date']})")
    except Exception: print(f"   {nm:16} {sid:18} none")
