import urllib.request, re
def get(url, accept="text/csv"):
    req=urllib.request.Request(url, headers={"User-Agent":"Mozilla/5.0 (jh-probe)","Accept":accept})
    try:
        return urllib.request.urlopen(req, timeout=50).read().decode("utf-8","replace")
    except Exception as e:
        body=""
        try: body=e.read().decode("utf-8","replace")[:200]
        except Exception: pass
        return "ERR %s :: %s :: %s"%(type(e).__name__, str(e)[:160], body)

# 1) BS_ITEM codelist — context windows around 'external'
cl = get("https://data-api.ecb.europa.eu/service/codelist/ECB/CL_BS_ITEM","application/vnd.sdmx.structure+xml;version=2.1")
print("=== CL_BS_ITEM len=%d ==="%len(cl))
if not cl.startswith("ERR"):
    seen=set()
    for m in re.finditer(r'id="([A-Z0-9]{2,6})"[^>]*?>\s*<[^>]*Name[^>]*>([^<]{3,70})', cl):
        cid,nm=m.group(1),m.group(2)
        if re.search(r'extern|non-resid|rest of the world|claims on|deposit liab', nm, re.I) and cid not in seen:
            seen.add(cid); print("  ITEM %-6s = %s"%(cid, nm))
else:
    print(cl)

# 2) AREA codelist — non-resident / rest of world
cla = get("https://data-api.ecb.europa.eu/service/codelist/ECB/CL_AREA","application/vnd.sdmx.structure+xml;version=2.1")
print("=== CL_AREA len=%d (rest-of-world candidates) ==="%len(cla))
if not cla.startswith("ERR"):
    seen=set()
    for m in re.finditer(r'id="([A-Z0-9]{2,5})"[^>]*?>\s*<[^>]*Name[^>]*>([^<]{3,70})', cla):
        cid,nm=m.group(1),m.group(2)
        if re.search(r'rest of the world|non-resid|extra|world not|outside', nm, re.I) and cid not in seen:
            seen.add(cid); print("  AREA %-5s = %s"%(cid, nm))

# 3) serieskeysonly probes (try several BS_ITEM/area/currency combos)
print("=== serieskeysonly probes ===")
for key in ["M.U2.N.A.A80.A.1.U4.0000.USD.E","M.U2.N.A.L80.A.1.U4.0000.USD.E",
            "M.U2.N.A.A80.A.1.W0.0000.USD.E","M.U2.N.A.A80.A.4.U4.0000.USD.E"]:
    r=get("https://data-api.ecb.europa.eu/service/data/BSI/%s?detail=serieskeysonly&format=csvdata"%key)
    print("  [%s] -> %s"%(key, (r[:180] if not r.startswith("ERR") else r[:180])))
