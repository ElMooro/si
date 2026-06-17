import urllib.request, json
def get(url,t=40):
    try:
        with urllib.request.urlopen(urllib.request.Request(url,headers={"User-Agent":"JustHodl raafouis@gmail.com"}),timeout=t) as r:
            return r.status, r.read().decode("utf-8","ignore")
    except Exception as e: return getattr(e,'code',type(e).__name__), str(e)[:90]

BASE="https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/ei_bssi_m_r2"
codes={"ESI":"BS-ESI-I","industrial":"BS-ICI-BAL","services":"BS-SCI-BAL",
       "consumer":"BS-CSMCI-BAL","retail":"BS-RCI-BAL","construction":"BS-CCI-BAL"}
print("=== Eurostat ei_bssi_m_r2 — EA20 confidence suite (latest) ===")
ok={}
for nm,ic in codes.items():
    st,body=get(f"{BASE}?format=JSON&lang=EN&geo=EA20&indic={ic}&s_adj=SA&sinceTimePeriod=2025-10")
    if st==200:
        try:
            j=json.loads(body); per=j["dimension"]["time"]["category"]["index"]; vals=j["value"]
            inv={v:k for k,v in per.items()}; last=max(int(i) for i in vals.keys())
            print(f"  {nm:13} {ic:14} -> {vals[str(last)]} ({inv[last]})  start? n_periods={len(per)}")
            ok[nm]=ic
        except Exception as e: print(f"  {nm:13} {ic:14} -> parse {e}")
    else: print(f"  {nm:13} {ic:14} -> http={st} {str(body)[:50]}")
# history depth check (ESI)
st,body=get(f"{BASE}?format=JSON&lang=EN&geo=EA20&indic=BS-ESI-I&s_adj=SA")
if st==200:
    j=json.loads(body); per=j["dimension"]["time"]["category"]["index"]
    ks=sorted(per.keys()); print(f"\n  ESI history: {ks[0]} -> {ks[-1]} ({len(ks)} months)")
print("\nworking:",list(ok.values()))
