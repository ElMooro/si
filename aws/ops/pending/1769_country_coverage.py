import urllib.request
def has(fk,t=25):
    url="https://data-api.ecb.europa.eu/service/data/"+fk+"?format=csvdata&lastNObservations=1"
    try:
        with urllib.request.urlopen(urllib.request.Request(url,headers={"User-Agent":"JustHodl raafouis@gmail.com"}),timeout=t) as r:
            lines=r.read().decode("utf-8","ignore").splitlines()
            if len(lines)<2: return None
            h={x:i for i,x in enumerate(lines[0].split(","))}; c=lines[1].split(",")
            return (c[h.get("OBS_VALUE",6)], c[h.get("TIME_PERIOD",5)])
    except Exception: return None
EU=["AT","BE","BG","HR","CY","CZ","DK","EE","FI","FR","DE","GR","HU","IE","IT","LV","LT","LU","MT","NL","PL","PT","RO","SK","SI","ES","SE","I8","I9"]
print("country | unemployment(LFSI) | indprod(STS excl-constr)")
u_ok=[]; p_ok=[]
for cc in EU:
    u=has(f"LFSI/M.{cc}.S.UNEHRT.TOTAL0.15_74.T")
    p=has(f"STS/M.{cc}.Y.PROD.NS0020.4.000")
    if u: u_ok.append(cc)
    if p: p_ok.append(cc)
    print(f"  {cc:4} | {('%s (%s)'%u) if u else '—':16} | {('%s (%s)'%p) if p else '—'}")
print(f"\nUNEMPLOYMENT available ({len(u_ok)}): {u_ok}")
print(f"INDPROD available ({len(p_ok)}): {p_ok}")
