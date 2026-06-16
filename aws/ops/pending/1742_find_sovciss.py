import urllib.request, re
def get(url, accept=None, t=40):
    h={"User-Agent":"JustHodl Research raafouis@gmail.com"}
    if accept: h["Accept"]=accept
    try:
        with urllib.request.urlopen(urllib.request.Request(url,headers=h),timeout=t) as r:
            return r.status, r.read().decode("utf-8","ignore")
    except Exception as e: return getattr(e,'code',type(e).__name__), str(e)[:80]

print("=== (1) ALL live CISS-flow series keys (csvdata) — scan for sovereign ===")
st,body=get("https://data-api.ecb.europa.eu/service/data/CISS?format=csvdata&lastNObservations=1")
print(f"  CISS flow http={st} bytes={len(body)}")
keys=set()
if st==200:
    lines=body.splitlines()
    hdr=lines[0].split(",") if lines else []
    ki=hdr.index("KEY") if "KEY" in hdr else 0
    for ln in lines[1:]:
        c=ln.split(",")
        if len(c)>ki: keys.add(c[ki])
    print(f"  distinct series in CISS flow: {len(keys)}")
    sov=[k for k in keys if "SOV" in k.upper()]
    print(f"  SOVEREIGN series ({len(sov)}):")
    for k in sorted(sov)[:25]: print("    ", k)
    # show indicator-dimension variety to understand structure
    print("  sample non-sov keys:", sorted(k for k in keys if "SOV" not in k.upper())[:6])

print("\n=== (2) dataflows mentioning SOV / stress (in case SovCISS moved flows) ===")
st,body=get("https://data-api.ecb.europa.eu/service/dataflow/ECB?detail=allstubs", accept="application/vnd.sdmx.structure+xml;version=2.1")
if st==200:
    flows=re.findall(r'id="([^"]+)"[^>]*>.*?<[^>]*Name[^>]*>([^<]+)<', body)
    hits=[(i,n) for i,n in flows if any(w in (i+n).upper() for w in ("SOV","CISS","STRESS"))]
    for i,n in hits[:15]: print(f"    {i}: {n}")
    if not hits: print("    (none matched; flow listing may differ)")
else: print(f"  dataflow list http={st}")

print("\n=== (3) test the most likely SovCISS candidates for live data ===")
for k in ["CISS/M.U2.Z0Z.4F.EC.SOV_GDPW.IDX","CISS/M.U2.Z0Z.4F.EC.SOVCISS_GDPW.IDX",
          "CISS/D.U2.Z0Z.4F.EC.SOV_GDPW.IDX","CISS/M.U2.Z0Z.4F.EC.SS_SOV.IDX"]:
    st,body=get(f"https://data-api.ecb.europa.eu/service/data/{k}?format=csvdata&lastNObservations=1")
    val=body.strip().splitlines()[-1].split(",")[-1] if st==200 and body.strip() else ""
    print(f"    {k:42} -> {st} {('latest='+val) if st==200 else ''}")
