import urllib.request, re
def csv(url):
    req=urllib.request.Request(url, headers={"User-Agent":"Mozilla/5.0 (jh)","Accept":"text/csv"})
    try: return urllib.request.urlopen(req, timeout=60).read().decode("utf-8","replace")
    except Exception as e:
        b=""
        try: b=e.read().decode("utf-8","replace")[:120]
        except Exception: pass
        return "ERR %s %s"%(str(e)[:60],b)

def key(item, ccy=""):
    # 11 dims: FREQ REF_AREA ADJ BS_REP_SECTOR BS_ITEM MATURITY DATA_TYPE COUNT_AREA BS_COUNT_SECTOR CCY SUFFIX
    p=["M","U2","","",item,"","","","",ccy,""]
    return ".".join(p)

for item in ["AXG","LXG","A80"]:
    k=key(item)
    r=csv("https://data-api.ecb.europa.eu/service/data/BSI/%s?detail=serieskeysonly&format=csvdata"%k)
    if r.startswith("ERR"):
        print("== %s (%s) -> %s"%(item,k,r)); continue
    keys=[ln.split(",")[0] for ln in r.splitlines()[1:] if ln.strip()]
    print("\n== %s: %d series (key=%s) =="%(item,len(keys),k))
    usd=[x for x in keys if ".USD." in x]
    print("  USD-denominated:", len(usd))
    for x in usd[:25]: print("    ",x)
    if not usd:
        # show currency codes present (dim index 9)
        ccys=sorted(set(x.split(".")[9] for x in keys if len(x.split("."))>=11))
        print("  currencies present (dim10):", ccys)
        areas=sorted(set(x.split(".")[7] for x in keys if len(x.split("."))>=11))
        print("  counterpart areas (dim8):", areas[:20])
