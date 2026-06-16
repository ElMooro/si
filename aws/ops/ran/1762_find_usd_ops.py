import urllib.request
def fetch(url,t=55):
    try:
        with urllib.request.urlopen(urllib.request.Request(url,headers={"User-Agent":"JustHodl raafouis@gmail.com"}),timeout=t) as r:
            return r.read().decode("utf-8","ignore")
    except Exception as e: return f"ERR {getattr(e,'code',type(e).__name__)}"
# Pull whole ILM weekly flow, grep titles for USD/dollar/foreign/swap
body=fetch("https://data-api.ecb.europa.eu/service/data/ILM/W?format=csvdata&lastNObservations=1")
hits=[]
if not body.startswith("ERR"):
    lines=body.splitlines(); hdr=lines[0].split(",")
    ki=hdr.index("KEY") if "KEY" in hdr else 0
    ti=hdr.index("TITLE") if "TITLE" in hdr else None
    tpi=hdr.index("TIME_PERIOD") if "TIME_PERIOD" in hdr else None
    ovi=hdr.index("OBS_VALUE") if "OBS_VALUE" in hdr else None
    seen=set()
    for ln in lines[1:]:
        c=ln.split(",")
        if len(c)<=ki: continue
        key=c[ki]; tit=c[ti] if ti is not None and len(c)>ti else ""
        blob=(key+" "+tit).lower()
        if any(w in blob for w in ["dollar","usd","foreign currency","fx swap","us dollar"]):
            if key not in seen:
                seen.add(key)
                hits.append((key, tit[:70], c[tpi] if tpi and len(c)>tpi else "", c[ovi] if ovi and len(c)>ovi else ""))
    print(f"ILM total rows: {len(lines)-1} | USD/dollar/foreign hits: {len(hits)}")
    for k,t,tp,v in hits[:25]: print(f"  {k:40} | {t} | {tp} {v}")
else:
    print("ILM flow fetch:", body)
