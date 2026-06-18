import urllib.request
def get(url, t=40):
    try:
        req=urllib.request.Request(url, headers={"User-Agent":"Mozilla/5.0 jh"})
        return urllib.request.urlopen(req,timeout=t).read().decode("utf-8","ignore")
    except Exception as e:
        return "ERR: %s"%(str(e)[:80])
B="https://data-api.ecb.europa.eu/service/data/"
cands = {
 "A020000.U4.Z06 (claims on NON-residents, FX)":"ILM/W.U2.C.A020000.U4.Z06",
 "A020000.U4.EUR (claims on NON-residents, EUR)":"ILM/W.U2.C.A020000.U4.EUR",
 "A030000.U2.Z06 (claims on EA-residents, FX) [known]":"ILM/W.U2.C.A030000.U2.Z06",
 "L060000.U4.Z06 (liab to NON-residents, FX)":"ILM/W.U2.C.L060000.U4.Z06",
 "L060000.U4.EUR (liab to NON-residents, EUR) [known]":"ILM/W.U2.C.L060000.U4.EUR",
 "L080000.U4.Z06 (ext liab FX, maybe discont.)":"ILM/W.U2.C.L080000.U4.Z06",
}
for label,key in cands.items():
    r=get(B+key+"?format=csvdata&lastNObservations=2")
    if r.startswith("ERR"): print("  [%s]\n     %s -> %s"%(label,key,r)); continue
    lines=[l for l in r.splitlines() if l.strip()]
    # csv header + rows; print header once-ish + last row's title+date+value
    last=lines[-1].split(",") if len(lines)>1 else []
    # try to find TITLE column from header
    hdr=lines[0].split(",")
    ti = hdr.index("TITLE") if "TITLE" in hdr else (hdr.index("TITLE_COMPL") if "TITLE_COMPL" in hdr else None)
    di = hdr.index("TIME_PERIOD") if "TIME_PERIOD" in hdr else None
    vi = hdr.index("OBS_VALUE") if "OBS_VALUE" in hdr else None
    title = last[ti] if (ti is not None and len(last)>ti) else "?"
    date = last[di] if (di is not None and len(last)>di) else "?"
    val = last[vi] if (vi is not None and len(last)>vi) else "?"
    print("  [%s]\n     %s | rows=%d | %s=%s | title=%s"%(label,key,len(lines)-1,date,val,title[:75]))
print("\n--- WILDCARD: all ILM weekly items, NON-resident (U4) foreign-currency (Z06) ---")
r=get(B+"ILM/W.U2.C..U4.Z06?detail=serieskeysonly&format=csvdata")
print(r[:600] if not r.startswith("ERR") else r)
