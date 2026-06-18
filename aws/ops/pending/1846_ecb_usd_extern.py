import urllib.request
def csv(url):
    req=urllib.request.Request(url, headers={"User-Agent":"Mozilla/5.0 (jh)","Accept":"text/csv"})
    try: return urllib.request.urlopen(req, timeout=70).read().decode("utf-8","replace")
    except Exception as e:
        b=""
        try: b=e.read().decode("utf-8","replace")[:120]
        except Exception: pass
        return "ERR %s %s"%(str(e)[:60],b)

def probe(label, idx):  # idx: dict position->value, rest wildcard (FREQ=M,REF_AREA=U2 fixed)
    p=["M","U2","","","","","","","","",""]
    for i,v in idx.items(): p[i]=v
    k=".".join(p)
    r=csv("https://data-api.ecb.europa.eu/service/data/BSI/%s?detail=serieskeysonly&format=csvdata"%k)
    print("\n== %s  key=%s =="%(label,k))
    if r.startswith("ERR"): print("  ",r); return []
    keys=[ln.split(",")[0] for ln in r.splitlines()[1:] if ln.strip()]
    print("  total series:",len(keys))
    return keys

# USD (dim9) + extra-euro-area counterpart U4 (dim7), all items
for area in ["U4","W1"]:
    ks=probe("USD x counterpart %s"%area, {7:area, 9:"USD"})
    items=sorted(set(k.split(".")[4] for k in ks if len(k.split("."))>=11))
    print("  BS_ITEMs available in USD vs %s:"%area, items)
    for x in ks[:30]: print("    ",x)
