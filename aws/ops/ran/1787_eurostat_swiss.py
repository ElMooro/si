import urllib.request, json
def get(url,t=40):
    try:
        with urllib.request.urlopen(urllib.request.Request(url,headers={"User-Agent":"JustHodl raafouis@gmail.com"}),timeout=t) as r:
            return r.status, r.read().decode("utf-8","ignore")
    except Exception as e: return getattr(e,'code',type(e).__name__), str(e)[:80]
def latest(url):
    st,b=get(url)
    if st!=200: return f"http={st} {str(b)[:50]}"
    try:
        j=json.loads(b); idx=j["dimension"]["time"]["category"]["index"]; vals=j["value"]
        inv={p:per for per,p in idx.items()}; mx=max(int(k) for k in vals.keys())
        return f"{vals[str(mx)]} ({inv[mx]})  n={len(idx)} start={sorted(idx)[0]}"
    except Exception as e: return f"parse {e} | {b[:80]}"
B="https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data"
print("CH unemployment (une_rt_m, SA, total):")
print("  ", latest(f"{B}/une_rt_m?format=JSON&lang=EN&geo=CH&s_adj=SA&sex=T&age=TOTAL&unit=PC_ACT&sinceTimePeriod=2025-09"))
print("CH industrial production (sts_inpr_m, NACE B-D, SCA, index):")
print("  ", latest(f"{B}/sts_inpr_m?format=JSON&lang=EN&geo=CH&nace_r2=B-D&s_adj=SCA&unit=I21&sinceTimePeriod=2025-06"))
print("CH manufacturing production (sts_inpr_m, NACE C, SCA, index):")
print("  ", latest(f"{B}/sts_inpr_m?format=JSON&lang=EN&geo=CH&nace_r2=C&s_adj=SCA&unit=I21&sinceTimePeriod=2025-06"))
print("EA business confidence (ei_bssi_m_r2 industrial BS-ICI-BAL):")
print("  ", latest(f"{B}/ei_bssi_m_r2?format=JSON&lang=EN&geo=EA20&indic=BS-ICI-BAL&s_adj=SA&sinceTimePeriod=2025-12"))
print("EA consumer confidence (ei_bssi_m_r2 BS-CSMCI-BAL):")
print("  ", latest(f"{B}/ei_bssi_m_r2?format=JSON&lang=EN&geo=EA20&indic=BS-CSMCI-BAL&s_adj=SA&sinceTimePeriod=2025-12"))
