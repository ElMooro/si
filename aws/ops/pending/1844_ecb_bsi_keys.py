import urllib.request, re
def get(url, accept="application/vnd.sdmx.structure+xml;version=2.1"):
    req=urllib.request.Request(url, headers={"User-Agent":"Mozilla/5.0 (jh-probe)","Accept":accept})
    try: return urllib.request.urlopen(req, timeout=60).read().decode("utf-8","replace")
    except Exception as e:
        b=""
        try: b=e.read().decode("utf-8","replace")[:150]
        except Exception: pass
        return "ERR %s %s %s"%(type(e).__name__,str(e)[:120],b)

# 1) DSD dimension order
dsd=get("https://data-api.ecb.europa.eu/service/datastructure/ECB/ECB_BSI1")
print("=== DSD len=%d ==="%len(dsd))
dims=re.findall(r'<str:Dimension id="([^"]+)"',dsd)
print("dimension order:", dims)

# 2) serieskeysonly wildcard for external assets/liabilities, USD, built by dim order
def buildkey(item, ccy):
    # 11-dim BSI order: FREQ REF_AREA ADJUSTMENT BS_REP_SECTOR BS_ITEM MATURITY_ORIG DATA_TYPE COUNT_AREA BS_COUNT_SECTOR CURRENCY_TRANS BS_SUFFIX
    p=["M","U2","","","",item,"","","","",ccy,""]  # placeholder len 12 -> trim
    return None
def csv(url): return get(url,"text/csv")

trials=[
 "M.U2...AXG.....USD.","M.U2...LXG.....USD.",
 "M.U2..A.AXG....USD.","M.U2..U.AXG....USD.",
 "M.U2...AXG.....USD","M.U2.N.A.AXG.A.1.U4..USD.E",
 "M.U2...AXG....USD.","M.U2...AXG......USD.",
]
print("=== serieskeysonly trials ===")
good=[]
for k in trials:
    r=csv("https://data-api.ecb.europa.eu/service/data/BSI/%s?detail=serieskeysonly&format=csvdata"%k)
    ok=not r.startswith("ERR") and "KEY" in r.upper() or (not r.startswith("ERR") and len(r)>20)
    print("  [%s] -> %s"%(k, r[:160].replace("\n"," | ")))
    if not r.startswith("ERR"): good.append(k)

# 3) For first good wildcard, pull actual keys list (full)
if good:
    r=csv("https://data-api.ecb.europa.eu/service/data/BSI/%s?detail=serieskeysonly&format=csvdata"%good[0])
    print("=== KEYS for %s ==="%good[0])
    print(r[:1500])
